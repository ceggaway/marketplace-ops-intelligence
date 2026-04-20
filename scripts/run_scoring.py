"""
Batch Scoring Runner
====================
Daily batch pipeline: generate input → validate → preprocess → score →
recommend → monitor drift → check rollback → log metrics

Usage:
    python scripts/run_scoring.py
    python scripts/run_scoring.py --days 1
"""

import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import pandas as pd

from backend.ingestion.lta_poller import generate_synthetic_snapshot, load_last_n_snapshots, poll_once, save_snapshot
from backend.ingestion.loader import get_zone_lookup, _SG_HOLIDAYS, generate_synthetic_data
from backend.ingestion.weather import get_hourly_weather
from backend.ingestion.carpark import fetch_carpark_availability, compute_zone_carpark_features
from backend.ingestion.travel_times import fetch_travel_times, compute_zone_congestion_features
from backend.validation.validator import validate
from backend.preprocessing.pipeline import build_features
from backend.scoring.batch_scorer import run_batch
from backend.recommendations.engine import generate_recommendations
from backend.recommendations import outcome_tracker
from backend.monitoring.drift import compute_drift, load_reference_scores, save_feature_snapshot
from backend.monitoring.metrics import emit_run_metrics
from backend.rollback.rollback import check_and_rollback


def main():
    parser = argparse.ArgumentParser(description="Run the batch scoring pipeline")
    parser.add_argument("--allow-synthetic", action="store_true",
                        help="Fall back to synthetic features when real data produces 0 rows (CI/dev only).")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("  Batch Scoring Pipeline")
    print(f"{'='*60}\n")

    # ── Step 1: Fetch live LTA taxi data + real weather ──────────────────────
    print("[1/7] Fetching live LTA taxi data...")
    t0          = time.time()
    api_key     = os.environ.get("LTA_API_KEY")
    zone_lookup = get_zone_lookup()

    # Fetch current 5-min snapshot and persist it to data/raw/taxi_snapshots/.
    # For local/dev runs, degrade gracefully to recent saved snapshots or a
    # synthetic snapshot when the live endpoint is unavailable.
    snapshot = pd.DataFrame()
    try:
        snapshot = poll_once(api_key, zone_lookup)
        save_snapshot(snapshot)
        print("      Live taxi snapshot fetched from LTA")
    except Exception as exc:
        print(f"      Live taxi fetch failed ({exc})")

    # Load up to 7 days of saved snapshots for lag-feature context
    # (168h × 12 snapshots/h = 2016 files; uses whatever is available)
    recent_raw = load_last_n_snapshots(n=2016)
    if recent_raw.empty and not snapshot.empty:
        recent_raw = snapshot
    elif recent_raw.empty:
        print("      No recent snapshots available — using synthetic taxi snapshot for local scoring")
        snapshot = generate_synthetic_snapshot(zone_lookup)
        save_snapshot(snapshot)
        recent_raw = snapshot

    # Aggregate 5-min snapshots → hourly time series (one row per zone per hour)
    recent_raw["timestamp"] = (
        pd.to_datetime(recent_raw["timestamp"]).dt.tz_convert("Asia/Singapore")
    )
    recent_raw["hour_floor"] = recent_raw["timestamp"].dt.floor("h")
    hourly = (
        recent_raw
        .groupby(["zone_id", "zone_name", "region", "zone_type", "hour_floor"])["taxi_count"]
        .mean()
        .reset_index()
    )
    hourly["taxi_count"] = hourly["taxi_count"].round().astype(int)
    hourly = hourly.rename(columns={"hour_floor": "timestamp"})
    hourly = hourly.sort_values(["zone_id", "timestamp"]).reset_index(drop=True)

    # Merge real weather from Open-Meteo (free, no key required)
    try:
        end_date   = date.today().isoformat()
        start_date = (date.today() - timedelta(days=8)).isoformat()
        weather_df = get_hourly_weather(start_date, end_date)
        weather_df["timestamp"] = weather_df["timestamp"].dt.floor("h")
        hourly = hourly.merge(
            weather_df[["timestamp", "temperature_c", "rainfall_mm", "weather_code", "is_raining"]],
            on="timestamp",
            how="left",
        )
        print("      Weather merged from Open-Meteo")
    except Exception as exc:
        print(f"      Weather fetch failed ({exc}) — using defaults")

    # Fill defaults for any missing weather columns
    for col, default in [
        ("weather_code",  0),
        ("rainfall_mm",   0.0),
        ("is_raining",    False),
        ("temperature_c", 28.0),
    ]:
        if col not in hourly.columns:
            hourly[col] = default
        else:
            hourly[col] = hourly[col].fillna(default)
    hourly["weather_code"] = hourly["weather_code"].astype(int)

    # Calendar flag
    hourly["is_holiday"] = hourly["timestamp"].dt.strftime("%Y-%m-%d").isin(_SG_HOLIDAYS)

    # ── Carpark availability (LTA DataMall) ───────────────────────────────────
    try:
        cp_raw      = fetch_carpark_availability(api_key)
        cp_features = compute_zone_carpark_features(cp_raw, zone_lookup)
        hourly = hourly.merge(
            cp_features[["zone_id", "carpark_available_lots"]],
            on="zone_id", how="left",
        )
        print(f"      Carpark merged: {int(cp_features['carpark_available_lots'].sum()):,} total lots")
    except Exception as exc:
        print(f"      Carpark fetch failed ({exc}) — using default 0")

    if "carpark_available_lots" not in hourly.columns:
        hourly["carpark_available_lots"] = 0
    else:
        hourly["carpark_available_lots"] = hourly["carpark_available_lots"].fillna(0).astype(int)

    # ── Estimated travel times / congestion (LTA DataMall) ───────────────────
    try:
        tt_raw      = fetch_travel_times(api_key)
        tt_features = compute_zone_congestion_features(tt_raw, zone_lookup)
        hourly = hourly.merge(
            tt_features[["zone_id", "congestion_ratio"]],
            on="zone_id", how="left",
        )
        print(f"      Travel times merged: {len(tt_raw)} segments")
    except Exception as exc:
        print(f"      Travel times fetch failed ({exc}) — using default 1.0")

    if "congestion_ratio" not in hourly.columns:
        hourly["congestion_ratio"] = 1.0
    else:
        hourly["congestion_ratio"] = hourly["congestion_ratio"].fillna(1.0)

    raw_df  = hourly
    n_hours = hourly["timestamp"].nunique()
    print(f"      {len(raw_df):,} rows ({n_hours}h × {hourly['zone_id'].nunique()} zones) "
          f"in {time.time()-t0:.1f}s")

    # ── Step 2: Validate ─────────────────────────────────────────────────────
    print("[2/7] Validating input data...")
    clean_df, failed_df = validate(raw_df)
    print(f"      Clean: {len(clean_df):,}  |  Failed: {len(failed_df):,}")

    if clean_df.empty:
        print("ERROR: No clean data. Aborting.")
        sys.exit(1)

    # ── Step 3: Preprocess ────────────────────────────────────────────────────
    print("[3/7] Building features...")
    feature_df = build_features(clean_df)
    print(f"      {len(feature_df):,} rows × {len(feature_df.columns)} features")

    if feature_df.empty:
        if args.allow_synthetic:
            print("      [--allow-synthetic] 0 rows from real data — using synthetic features for CI.")
            syn_raw = generate_synthetic_data(days=2)
            syn_clean, _ = validate(syn_raw)
            feature_df = build_features(syn_clean)
            print(f"      Synthetic: {len(feature_df):,} rows × {len(feature_df.columns)} features")
        else:
            print("ERROR: 0 feature rows — not enough snapshot history. "
                  "Pass --allow-synthetic for CI/dev runs.")
            sys.exit(1)

    # ── Step 4: Batch scoring ─────────────────────────────────────────────────
    print("[4/7] Running batch inference...")
    t0 = time.time()
    run_meta = run_batch(feature_df)
    run_meta["avg_depletion_rate"] = float(feature_df["depletion_rate_1h"].mean())
    print(f"      Scored: {run_meta['rows_scored']:,}  |  "
          f"Flagged: {run_meta['flagged_zones']}  |  "
          f"Failed: {run_meta['failed_rows']}  |  "
          f"{run_meta['latency_ms']}ms")

    # ── Step 5: Recommendations ───────────────────────────────────────────────
    print("[5/7] Generating recommendations...")
    pred_path = Path("data/outputs/predictions.csv")
    if pred_path.exists() and pred_path.stat().st_size > 0:
        pred_df = pd.read_csv(pred_path)
        # Only generate recommendations for the current snapshot (latest timestamp).
        # predictions.csv accumulates multi-hour history; we want one row per zone.
        if "timestamp" in pred_df.columns and not pred_df.empty:
            pred_df = pred_df[pred_df["timestamp"] == pred_df["timestamp"].max()]
        rec_df  = generate_recommendations(pred_df)
        print(f"      {len(rec_df)} recommendations written ({len(pred_df)} zones in snapshot)")
        # Outcome tracking: log new recommendations and resolve any from ≥30 min ago
        outcome_tracker.log_recommendations(rec_df)
        resolved = outcome_tracker.check_and_update_outcomes(pred_df)
        if resolved:
            print(f"      Outcomes resolved: {len(resolved)} (e.g. {resolved[0]['outcome']} in {resolved[0]['zone_name']})")

    # ── Step 6: Drift detection ───────────────────────────────────────────────
    print("[6/7] Computing drift...")
    ref_scores = load_reference_scores()
    if ref_scores is not None and pred_path.exists():
        scores = pd.read_csv(pred_path)["delay_risk_score"]
        drift_report = compute_drift(
            scores, ref_scores,
            run_id=run_meta["run_id"],
            feature_df=feature_df,
        )
        run_meta["psi"]        = drift_report["psi"]
        run_meta["drift_flag"] = drift_report["drift_flag"]
        feat_drifted = [f for f, v in drift_report.get("feature_drift", {}).items()
                        if v["drift_level"] != "stable"]
        print(f"      PSI={drift_report['psi']:.4f}  Level={drift_report['drift_level']}"
              + (f"  Drifted features: {feat_drifted}" if feat_drifted else ""))
    else:
        print("      No reference scores — skipping drift (first run)")
    # Save feature snapshot for next run's reference (always, even on first run)
    save_feature_snapshot(feature_df)

    # ── Step 7: Log metrics + rollback check ─────────────────────────────────
    print("[7/7] Logging metrics + checking rollback...")
    emit_run_metrics(run_meta)
    rolled_back = check_and_rollback(run_meta)
    run_meta["rollback_status"] = rolled_back
    if rolled_back:
        print("      ROLLBACK triggered — reverted to previous stable version")
    else:
        print("      No rollback needed.")

    print(f"\n{'='*60}")
    print(f"  Scoring pipeline complete — run_id={run_meta['run_id']}")
    print(f"  Status: {run_meta.get('run_status', 'success')}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
