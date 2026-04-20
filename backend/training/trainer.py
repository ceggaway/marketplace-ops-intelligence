"""
Model Training Pipeline
=======================
Trains the repo's near-term supply depletion risk model and saves artifacts to the model registry.

Model type: LightGBM classifier
Target variable: `supply_shortage`
    1 if taxi_count at t+1 < taxi_count at t × 0.6
    (legacy name; operationally this represents a depletion event)

Artifacts saved per version:
    model.pkl            – serialised model
    metrics.json         – precision, recall, F1, AUC, etc.
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
from sklearn.model_selection import train_test_split

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
]

TARGET_COL   = "supply_shortage"
REGISTRY_DIR = Path("data/registry")


def _make_target(df: pd.DataFrame) -> pd.Series:
    """
    Binary target: 1 if available taxi count drops by >40% in the next hour.

    This is computed by looking 1 hour ahead per zone. Rows where the next
    hour is unavailable (last row per zone) are set to NaN and dropped.
    """
    df = df.sort_values(["zone_id", "timestamp"]).copy()
    next_count = df.groupby("zone_id")["taxi_count"].shift(-1)
    threshold = df["taxi_count"] * 0.6
    target = (next_count < threshold).astype("Int64")  # nullable int — NaN where t+1 missing
    return target


def _get_features(df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in FEATURE_COLS if c in df.columns]
    X = df[available].copy()
    bool_cols = X.select_dtypes(include="bool").columns
    X[bool_cols] = X[bool_cols].astype(int)
    return X


def train(feature_df: pd.DataFrame, target_col: str = TARGET_COL, version_id: str = "v1") -> dict:
    """Train model, save artifacts to registry, return metrics dict."""
    df = feature_df.copy()

    # Build target if not present
    if target_col not in df.columns:
        df[target_col] = _make_target(df)

    # Drop rows where target is NaN (last row per zone — no future available)
    df = df.dropna(subset=[target_col]).copy()
    df[target_col] = df[target_col].astype(int)

    X = _get_features(df)
    y = df[target_col]

    X_train, X_val, y_train, y_val = _split_train_val(X, y)

    model = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=31,
        min_child_samples=20,
        subsample=0.8,
        colsample_bytree=0.8,
        class_weight="balanced",  # supply shortage is ~20% of hours — balance recalls
        random_state=42,
        verbose=-1,
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(False)],
    )

    metrics = evaluate(model, X_val, y_val)
    metrics["train_rows"]     = int(len(X_train))
    metrics["val_rows"]       = int(len(X_val))
    metrics["features_used"]  = list(X.columns)
    metrics["target"]         = TARGET_COL
    metrics["shortage_threshold"] = 0.60  # taxi_count(t+1) < taxi_count(t) * this

    _save_artifacts(model, X, metrics, version_id)
    return metrics


def _split_train_val(
    X: pd.DataFrame, y: pd.Series, val_fraction: float = 0.20
) -> tuple:
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
        "version_id":  version_id,
        "trained_at":  datetime.now(timezone.utc).isoformat(),
        "status":      "candidate",
        "train_rows":  metrics.get("train_rows"),
        "val_rows":    metrics.get("val_rows"),
        "roc_auc":     metrics.get("roc_auc"),
        "f1":          metrics.get("f1"),
        "target":      TARGET_COL,
    }
    with open(version_dir / "version_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
