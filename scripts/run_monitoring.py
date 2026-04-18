"""
Monitoring Pipeline Runner
==========================
Recomputes drift using the latest scored predictions and the most recent
reference score snapshot. Also checks whether a rollback should be triggered.

Usage:
    python scripts/run_monitoring.py              # run once
    python scripts/run_monitoring.py --loop       # run continuously (default 300s interval)
    python scripts/run_monitoring.py --loop --interval 600
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.monitoring.drift import compute_drift, load_reference_scores
from backend.monitoring.metrics import get_latest_runs
from backend.rollback.rollback import check_and_rollback

OUTPUTS_DIR = Path("data/outputs")


def run_once() -> dict | None:
    pred_path = OUTPUTS_DIR / "predictions.csv"

    if not pred_path.exists():
        print(f"[monitor] No predictions at {pred_path} — skipping")
        return None

    pred_df = pd.read_csv(pred_path)
    if "delay_risk_score" not in pred_df.columns or pred_df.empty:
        print("[monitor] Predictions file empty or missing delay_risk_score — skipping")
        return None

    reference_scores = load_reference_scores()
    if reference_scores is None:
        print("[monitor] No reference scores yet — skipping drift")
        return None

    latest_runs = get_latest_runs(n=1)
    run_id = latest_runs[-1]["run_id"] if latest_runs else ""

    # Build partial feature_df from predictions.csv for feature-level drift.
    # predictions.csv has taxi_count, depletion_rate_1h, supply_vs_yesterday —
    # 3 of the 5 monitored features. rainfall_mm and congestion_ratio are not
    # stored in predictions and will be skipped automatically.
    feature_cols = ["taxi_count", "depletion_rate_1h", "supply_vs_yesterday"]
    partial_features = pred_df[[c for c in feature_cols if c in pred_df.columns]]
    feature_df = partial_features if not partial_features.empty else None

    report = compute_drift(pred_df["delay_risk_score"], reference_scores,
                           run_id=run_id, feature_df=feature_df)

    feat_drifted = [f for f, v in report.get("feature_drift", {}).items()
                    if v["drift_level"] != "stable"]
    print(f"[monitor] {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}  "
          f"PSI={report['psi']:.4f}  Level={report['drift_level']}"
          + (f"  Drifted features: {feat_drifted}" if feat_drifted else ""))

    # Rollback check — uses the same thresholds as the scoring pipeline
    if latest_runs:
        run_meta = latest_runs[-1]
        run_meta["psi"]        = report["psi"]
        run_meta["drift_flag"] = report["drift_flag"]
        rolled_back = check_and_rollback(run_meta)
        if rolled_back:
            print("[monitor] ROLLBACK triggered — reverted to previous stable version")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitoring pipeline runner")
    parser.add_argument("--loop",     action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=300,
                        help="Loop interval in seconds (default 300)")
    args = parser.parse_args()

    if args.loop:
        print(f"[monitor] Starting monitoring loop (interval={args.interval}s). "
              "Ctrl-C to stop.")
        while True:
            run_once()
            time.sleep(args.interval)
    else:
        report = run_once()
        if report:
            print(json.dumps(report, indent=2))
