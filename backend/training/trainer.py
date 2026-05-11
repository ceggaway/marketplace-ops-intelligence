"""
Model Training Pipeline
=======================
Trains the repo's near-term supply depletion risk model and saves artifacts to the model registry.

Model type: LightGBM classifier (post-hoc calibrated with isotonic regression)
Target variable: `supply_depletion_event`
    1 if taxi_count at t+horizon < taxi_count at t × 0.6
    horizon_hours controls the prediction window (default 1h)

Multi-horizon training:
    Pass horizon_hours=[1, 2, 3] to train separate models for 1h, 2h, 3h ahead.

Artifacts saved per version:
    model.pkl            – serialised calibrated model
    metrics.json         – precision, recall, F1, AUC, ECE, reliability bins, etc.
    feature_schema.json  – expected feature columns and dtypes
    version_meta.json    – version id, timestamp, training data hash
"""

import json
import pickle
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.model_selection import train_test_split

from backend.paths import registry_dir
from backend.training.evaluator import evaluate

# Feature columns used for training — all derived in preprocessing/pipeline.py
# Any column listed here that is absent from the input is silently skipped by
# _get_features(), so new features are backwards-compatible with old models.
FEATURE_COLS = [
    # Time
    "hour_of_day", "day_of_week", "month", "is_weekend", "is_peak_hour",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "month_sin", "month_cos",   # seasonal cyclical encoding
    "hours_to_peak",             # minutes until next peak period
    # Supply state — max lag 24h so real data is usable after 1 day of polling
    # taxi_lag_168h re-added: pipeline.py always computes it; trainer uses it when
    # the training dataset has ≥ 7 days of history (it is filled with zone mean
    # otherwise, so it degrades gracefully on shorter datasets)
    "taxi_count",
    "taxi_lag_1h", "taxi_lag_24h", "taxi_lag_168h",
    "taxi_rolling_3h", "taxi_rolling_6h",
    # Depletion signals
    "depletion_rate_1h", "depletion_rate_3h", "supply_vs_yesterday",
    # Exp 5/6 error-analysis signals
    "delta_depletion_1h", "taxi_abs_drop_1h", "is_low_count",
    # Weather
    "rainfall_mm", "rainfall_sqrt", "is_raining", "rain_intensity",
    "temperature_c",
    # Calendar
    "is_holiday", "is_eve_holiday", "is_school_holiday",
    # Zone identity
    "zone_type_encoded",
    # External signals (LTA DataMall)
    "carpark_available_lots",
    "carpark_shortage_flag",
    "congestion_ratio",
    # Event calendar
    "is_major_event",
    "event_crowdedness",
]

TARGET_COL   = "supply_depletion_event"
LEGACY_TARGET_COL = "supply_shortage"   # accepted as alias in old datasets
REGISTRY_DIR = registry_dir()


def _make_target(df: pd.DataFrame, horizon_hours: int = 1) -> pd.Series:
    """
    Binary target: 1 if taxi_count drops by >40% within horizon_hours.

    Looks `horizon_hours` rows ahead per zone. Rows where the future value
    is unavailable (last horizon_hours rows per zone) are set to NaN and
    later dropped by the caller.
    """
    df = df.sort_values(["zone_id", "timestamp"]).copy()
    next_count = df.groupby("zone_id")["taxi_count"].shift(-horizon_hours)
    threshold = df["taxi_count"] * 0.6
    has_next = next_count.notna()
    target = pd.Series(pd.NA, index=df.index, dtype="Int64")
    target.loc[has_next] = (next_count.loc[has_next] < threshold.loc[has_next]).astype(int)
    return target


def _get_features(df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in FEATURE_COLS if c in df.columns]
    X = df[available].copy()
    bool_cols = X.select_dtypes(include="bool").columns
    X[bool_cols] = X[bool_cols].astype(int)
    return X


def train(
    feature_df: pd.DataFrame,
    target_col: str = TARGET_COL,
    version_id: str = "v1",
    horizon_hours: int = 1,
) -> dict:
    """
    Train model, save artifacts to registry, return metrics dict.

    Args:
        feature_df:   Feature-engineered DataFrame (output of pipeline.build_features).
        target_col:   Target column name. Accepts legacy "supply_shortage" as alias.
        version_id:   Registry version identifier.
        horizon_hours: Prediction horizon (1 = next hour, 2 = 2h ahead, 3 = 3h ahead).
                       Controls how far ahead the depletion target is computed.
    """
    df = feature_df.copy()

    # Accept legacy target column name from pre-rename datasets
    if target_col not in df.columns and LEGACY_TARGET_COL in df.columns:
        df = df.rename(columns={LEGACY_TARGET_COL: target_col})

    # Build target if still not present
    if target_col not in df.columns:
        df[target_col] = _make_target(df, horizon_hours=horizon_hours)

    # Drop rows where target is NaN (last horizon_hours rows per zone — no future data)
    df = df.dropna(subset=[target_col]).copy()
    df[target_col] = df[target_col].astype(int)

    X = _get_features(df)
    y = df[target_col]

    # Time-aware split: sort by timestamp and split at the time boundary so
    # the validation set is always chronologically later than the training set.
    # This prevents data leakage from future observations into training.
    timestamps = df["timestamp"] if "timestamp" in df.columns else None
    X_train, X_val, y_train, y_val = _split_train_val(X, y, timestamps=timestamps)

    base_model = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        class_weight="balanced",  # depletion events are ~20% of hours — balance recalls
        random_state=42,
        verbose=-1,
    )
    base_model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(False)],
    )

    # Post-hoc isotonic calibration on the held-out val set.
    # Isotonic regression corrects systematic over- or under-confidence in
    # the raw LightGBM probabilities without touching the ranking (AUC unchanged).
    model = CalibratedClassifierCV(base_model, method="isotonic", cv="prefit")
    model.fit(X_val, y_val)

    metrics = evaluate(model, X_val, y_val)
    metrics["train_rows"]          = int(len(X_train))
    metrics["val_rows"]            = int(len(X_val))
    metrics["features_used"]       = list(X.columns)
    metrics["target"]              = target_col
    metrics["shortage_threshold"]  = 0.60   # taxi_count(t+horizon) < taxi_count(t) * this
    metrics["horizon_hours"]       = horizon_hours
    metrics["split_type"]          = "time" if timestamps is not None else "row"

    _save_artifacts(model, X, metrics, version_id)
    return metrics


def _split_train_val(
    X: pd.DataFrame,
    y: pd.Series,
    val_fraction: float = 0.20,
    timestamps: pd.Series | None = None,
) -> tuple:
    """
    Time-aware train/validation split.

    When timestamps are available the split is made at the chronological
    val_fraction boundary — the last val_fraction of the time range is held
    out. This prevents any future data leaking into the training set.

    Falls back to a no-shuffle row split when timestamps are absent.
    """
    if timestamps is not None:
        sorted_idx = np.argsort(timestamps.values)
        cutoff = int(len(X) * (1 - val_fraction))
        train_idx = sorted_idx[:cutoff]
        val_idx   = sorted_idx[cutoff:]
        return (
            X.iloc[train_idx],
            X.iloc[val_idx],
            y.iloc[train_idx],
            y.iloc[val_idx],
        )
    return train_test_split(X, y, test_size=val_fraction, shuffle=False)


def _save_artifacts(model, X: pd.DataFrame, metrics: dict, version_id: str) -> None:
    version_dir = REGISTRY_DIR / "models" / version_id
    version_dir.mkdir(parents=True, exist_ok=True)

    with open(version_dir / "model.pkl", "wb") as f:
        pickle.dump(model, f)

    with open(version_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    schema = {col: str(dtype) for col, dtype in X.dtypes.items()}
    with open(version_dir / "feature_schema.json", "w") as f:
        json.dump({"columns": list(X.columns), "dtypes": schema}, f, indent=2)

    meta = {
        "version_id":   version_id,
        "trained_at":   datetime.now(timezone.utc).isoformat(),
        "status":       "candidate",
        "train_rows":   metrics.get("train_rows"),
        "val_rows":     metrics.get("val_rows"),
        "roc_auc":      metrics.get("roc_auc"),
        "f1":           metrics.get("f1"),
        "target":       metrics.get("target", TARGET_COL),
        "horizon_hours": metrics.get("horizon_hours", 1),
        "split_type":   metrics.get("split_type", "time"),
        "calibrated":   True,
    }
    with open(version_dir / "version_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
