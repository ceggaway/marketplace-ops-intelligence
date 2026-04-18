"""
Drift Detection
===============
Compares the current scoring run's prediction distribution against a
reference distribution (training set or previous run baseline).

Method: Population Stability Index (PSI)
    PSI < 0.10  → No significant drift
    PSI 0.10-0.25 → Moderate drift – flag for review
    PSI > 0.25   → Significant drift – emit alert, trigger rollback check

Output: drift_report.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from backend.monitoring.alerting import emit_alert

OUTPUTS_DIR         = Path("data/outputs")
PSI_ALERT_THRESHOLD = 0.25
PSI_WARN_THRESHOLD  = 0.10

# Input features to monitor for drift (must exist in feature_df)
MONITORED_FEATURES = [
    "taxi_count",
    "depletion_rate_1h",
    "supply_vs_yesterday",
    "rainfall_mm",
    "congestion_ratio",
]


def compute_drift(
    current_scores: pd.Series,
    reference_scores: pd.Series,
    run_id: str = "",
    feature_df: pd.DataFrame | None = None,
) -> dict:
    """
    Compute PSI on prediction scores and optionally per-feature PSI.
    Writes drift_report.json and returns the report dict.
    """
    psi_value = _psi(reference_scores, current_scores)

    report: dict = {
        "run_id":         run_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "psi":            round(float(psi_value), 4),
        "drift_flag":     psi_value > PSI_ALERT_THRESHOLD,
        "drift_level":    _drift_level(psi_value),
        "reference_mean": round(float(reference_scores.mean()), 4),
        "current_mean":   round(float(current_scores.mean()), 4),
        "reference_std":  round(float(reference_scores.std()), 4),
        "current_std":    round(float(current_scores.std()), 4),
        "reference_n":    int(len(reference_scores)),
        "current_n":      int(len(current_scores)),
        "feature_drift":  {},
    }

    # Per-feature drift against stored reference snapshots
    if feature_df is not None:
        report["feature_drift"] = _compute_feature_drift(feature_df, run_id)

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUTS_DIR / "drift_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # Emit alert if significant drift detected
    if report["drift_flag"]:
        drifted = [f for f, v in report["feature_drift"].items()
                   if v["drift_level"] != "stable"]
        detail = f" (features: {', '.join(drifted)})" if drifted else ""
        emit_alert(
            alert_id="DRIFT_ALERT",
            severity="high",
            message=f"Prediction drift detected — PSI={psi_value:.3f}{detail}",
        )
    elif psi_value > PSI_WARN_THRESHOLD:
        emit_alert(
            alert_id="DRIFT_WARNING",
            severity="medium",
            message=f"Moderate drift detected — PSI={psi_value:.3f}, level=warning",
        )

    return report


def _compute_feature_drift(feature_df: pd.DataFrame, run_id: str) -> dict:
    """
    Compute PSI for each monitored feature against its stored reference.
    Reference is loaded from feature_distribution.json (written by batch_scorer).
    Returns dict of {feature_name: {psi, drift_level, reference_mean, current_mean}}.
    """
    ref_path = OUTPUTS_DIR / "feature_distribution.json"
    if not ref_path.exists():
        return {}

    with open(ref_path) as f:
        ref_snap = json.load(f)

    result = {}
    for feat in MONITORED_FEATURES:
        if feat not in feature_df.columns:
            continue
        ref_entry = ref_snap.get(feat)
        if not ref_entry or "values" not in ref_entry:
            continue

        current_vals = feature_df[feat].dropna().astype(float)
        ref_vals     = pd.Series(ref_entry["values"], dtype=float)
        if current_vals.empty or ref_vals.empty:
            continue

        feat_psi = _psi(ref_vals, current_vals)
        result[feat] = {
            "psi":            round(float(feat_psi), 4),
            "drift_level":    _drift_level(feat_psi),
            "reference_mean": round(float(ref_vals.mean()), 4),
            "current_mean":   round(float(current_vals.mean()), 4),
            "reference_std":  round(float(ref_vals.std()), 4),
            "current_std":    round(float(current_vals.std()), 4),
        }

    return result


def _psi(expected: pd.Series, actual: pd.Series, buckets: int = 10) -> float:
    """
    Population Stability Index.
    PSI = sum((actual_pct - expected_pct) * ln(actual_pct / expected_pct))
    """
    expected = np.asarray(expected, dtype=float)
    actual   = np.asarray(actual,   dtype=float)

    min_val = min(expected.min(), actual.min())
    max_val = max(expected.max(), actual.max())
    if min_val == max_val:
        return 0.0

    edges = np.linspace(min_val, max_val, buckets + 1)
    exp_hist, _ = np.histogram(expected, bins=edges)
    act_hist, _ = np.histogram(actual,   bins=edges)

    exp_pct = (exp_hist / len(expected)).clip(1e-6)
    act_pct = (act_hist / len(actual)).clip(1e-6)

    return float(max(np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct)), 0.0))


def _drift_level(psi: float) -> str:
    if psi < PSI_WARN_THRESHOLD:
        return "stable"
    if psi < PSI_ALERT_THRESHOLD:
        return "warning"
    return "alert"


def load_reference_scores(version_id: str | None = None) -> pd.Series | None:
    """
    Load reference score distribution from previous run snapshot.
    Uses actual stored scores when available; no synthetic fallback.
    """
    snap_path = OUTPUTS_DIR / "score_distribution.json"
    if not snap_path.exists():
        return None
    with open(snap_path) as f:
        snap = json.load(f)
    # Prefer actual scores saved by batch_scorer (added 2026-04)
    if "scores" in snap and snap["scores"]:
        return pd.Series(snap["scores"], dtype=float)
    return None


def save_feature_snapshot(feature_df: pd.DataFrame) -> None:
    """
    Save per-feature value samples to feature_distribution.json.
    Called by batch_scorer after each run so the next run has a real reference.
    Stores up to 2000 samples per feature.
    """
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    snap = {}
    for feat in MONITORED_FEATURES:
        if feat not in feature_df.columns:
            continue
        vals = feature_df[feat].dropna().astype(float)
        if vals.empty:
            continue
        snap[feat] = {
            "mean":   round(float(vals.mean()), 4),
            "std":    round(float(vals.std()), 4),
            "n":      int(len(vals)),
            "values": [round(float(v), 6) for v in vals.iloc[:2000]],
        }
    with open(OUTPUTS_DIR / "feature_distribution.json", "w") as f:
        json.dump(snap, f, indent=2)
