"""
Batch Scoring Runner
====================
Daily batch pipeline: generate input → validate → preprocess → score →
recommend → monitor drift → check rollback → log metrics

Usage:
    python scripts/run_scoring.py
    python scripts/run_scoring.py --require-live-data
    python scripts/run_scoring.py --h3
    python scripts/run_scoring.py --h3 --zone
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

import pandas as pd

from backend.paths import outputs_dir
from backend.ingestion.lta_poller import generate_synthetic_snapshot, load_last_n_snapshots, poll_once, save_snapshot
from backend.ingestion.loader import get_zone_lookup, _SG_HOLIDAYS, generate_synthetic_data
from backend.ingestion.weather import get_hourly_weather
from backend.ingestion.carpark import fetch_carpark_availability, compute_zone_carpark_features
from backend.ingestion.travel_times import fetch_travel_times, compute_zone_congestion_features
from backend.intervention.policy import generate_recommendations
from backend.validation.validator import validate
from backend.preprocessing.pipeline import build_features
from backend.scoring.batch_scorer import run_batch, run_h3_batch
from backend.recommendations import outcome_tracker
from backend.monitoring.drift import compute_drift, load_reference_scores, save_feature_snapshot
from backend.monitoring.metrics import emit_run_metrics
from backend.rollback.rollback import check_and_rollback

# Maximum age of the latest snapshot before the run is flagged as stale (minutes)
_FRESHNESS_WARN_MINUTES = 90
# Minimum zones required before scoring proceeds
_MIN_ZONE_COUNT = 40
# Maximum fraction of feature cells that may be NaN before raising a warning
_MAX_NAN_FRACTION = 0.30


def _write_failed_run(run_id: str, error: str, run_start: datetime) -> None:
    """Write a failed-run entry to pipeline.log so monitoring sees the failure."""
    record = {
        "run_id": run_id,
        "timestamp": run_start.isoformat(),
        "run_status": "failed",
        "error": error[:500],
        "rows_scored": 0,
        "failed_rows": 0,
        "flagged_zones": 0,
        "psi": None,
        "drift_flag": False,
        "latency_ms": int((datetime.now(timezone.utc) - run_start).total_seconds() * 1000),
    }
    log_path = outputs_dir() / "pipeline.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a") as fh:
        fh.write(json.dumps(record) + "\n")


def _check_feature_freshness(feature_df: pd.DataFrame, source_freshness: dict) -> list[str]:
    """Return a list of freshness warnings (empty = all good)."""
    warnings: list[str] = []
    if "timestamp" in feature_df.columns and not feature_df.empty:
        latest_ts = pd.to_datetime(feature_df["timestamp"]).max()
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.tz_localize("UTC")
        age_min = (datetime.now(timezone.utc) - latest_ts.astimezone(timezone.utc)).total_seconds() / 60
        if age_min > _FRESHNESS_WARN_MINUTES:
            warnings.append(
                f"Latest snapshot is {age_min:.0f} min old (threshold {_FRESHNESS_WARN_MINUTES} min)"
            )
    n_zones = feature_df["zone_id"].nunique() if "zone_id" in feature_df.columns else 0
    if n_zones < _MIN_ZONE_COUNT:
        warnings.append(f"Only {n_zones} zones scored (minimum {_MIN_ZONE_COUNT})")
    required_cols = ["taxi_count", "depletion_rate_1h", "hour_of_day"]
    missing = [c for c in required_cols if c not in feature_df.columns]
    if missing:
        warnings.append(f"Missing required feature columns: {missing}")
    nan_fraction = feature_df.select_dtypes(include="number").isna().mean().mean()
    if nan_fraction > _MAX_NAN_FRACTION:
        warnings.append(
            f"Feature NaN rate {nan_fraction:.1%} exceeds threshold {_MAX_NAN_FRACTION:.0%} — "
            "upstream data sources may be partially failing"
        )
    for source, info in source_freshness.items():
        if not info.get("ok", True):
            warnings.append(f"Source '{source}' is using fallback data: {info.get('reason', 'unknown')}")
    return warnings


def main():
    parser = argparse.ArgumentParser(description="Run the batch scoring pipeline")
    parser.add_argument("--allow-synthetic", action="store_true",
                        help="Fall back to synthetic features when real data produces 0 rows (CI/dev only).")
    parser.add_argument("--require-live-data", action="store_true",
                        help="Fail instead of generating synthetic taxi snapshots when live LTA data is unavailable.")
    parser.add_argument("--h3", action="store_true",
                        help="Run H3 hex-cell scoring from data/processed/h3_supply_features.csv.")
    parser.add_argument("--zone", action="store_true",
                        help="Run zone scoring when --h3 is set. Without --h3, zone scoring is the default.")
    args = parser.parse_args()

    run_start = datetime.now(timezone.utc)
    import uuid
    run_id = str(uuid.uuid4())[:8]

    print(f"\n{'='*60}")
    print("  Batch Scoring Pipeline")
    print(f"{'='*60}\n")

    try:
        run_zone = args.zone or not args.h3
        if run_zone:
            _run_pipeline(args, run_id, run_start)
        if args.h3:
            _run_h3_pipeline()
    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        print(f"\nFATAL: Pipeline failed — {error_msg}")
        print(traceback.format_exc())
        _write_failed_run(run_id, error_msg, run_start)
        sys.exit(1)


def _run_h3_pipeline() -> None:
    """Run optional H3 scoring from pre-built H3 feature data."""
    print("\n[H3] Running hex-cell scoring...")
    result = run_h3_batch()
    status = result.get("run_status")
    if status != "success":
        message = result.get("message", "H3 scoring did not complete.")
        print(f"[H3] ERROR: {message}")
        sys.exit(1)
    print(
        f"[H3] Scored {result.get('h3_cells_scored', 0):,} cells "
        f"in {result.get('latency_ms', 0)}ms "
        f"(missing model columns filled: {result.get('missing_model_cols', 0)})"
    )


def _run_pipeline(args, run_id: str, run_start: datetime) -> None:
    # Tracks per-source freshness so pre-scoring checks can surface degraded inputs
    source_freshness: dict[str, dict] = {}

    # ── Step 1: Fetch live LTA taxi data + real weather ──────────────────────
    print("[1/7] Fetching live LTA taxi data...")
    t0          = time.time()
    api_key     = os.environ.get("LTA_API_KEY")
    zone_lookup = get_zone_lookup()
    if args.require_live_data and not api_key:
        print("ERROR: --require-live-data set but LTA_API_KEY is missing.")
        sys.exit(1)

    # Fetch current 5-min snapshot and persist it to data/raw/taxi_snapshots/.
    # For local/dev runs, degrade gracefully to recent saved snapshots or a
    # synthetic snapshot when the live endpoint is unavailable.
    snapshot = pd.DataFrame()
    try:
        snapshot = poll_once(api_key, zone_lookup)
        save_snapshot(snapshot)
        source_freshness["lta_taxi"] = {"ok": True, "fetched_at": datetime.now(timezone.utc).isoformat()}
        print("      Live taxi snapshot fetched from LTA")
    except Exception as exc:
        source_freshness["lta_taxi"] = {"ok": False, "reason": str(exc), "fetched_at": None}
        print(f"      Live taxi fetch failed ({exc})")
        if args.require_live_data:
            print("ERROR: --require-live-data set and live taxi fetch failed.")
            sys.exit(1)

    # Load up to 7 days of saved snapshots for lag-feature context
    recent_raw = load_last_n_snapshots(n=2016)
    if recent_raw.empty and not snapshot.empty:
        recent_raw = snapshot
    elif recent_raw.empty:
        if args.require_live_data:
            print("ERROR: No recent live snapshots available and --require-live-data is set.")
            sys.exit(1)
        print("      No recent snapshots available — using synthetic taxi snapshot for local scoring")
        snapshot = generate_synthetic_snapshot(zone_lookup)
        save_snapshot(snapshot)
        source_freshness["lta_taxi"] = {"ok": False, "reason": "synthetic fallback", "fetched_at": datetime.now(timezone.utc).isoformat()}
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
        source_freshness["weather"] = {"ok": True, "fetched_at": datetime.now(timezone.utc).isoformat()}
        print("      Weather merged from Open-Meteo")
    except Exception as exc:
        source_freshness["weather"] = {"ok": False, "reason": str(exc), "fetched_at": None}
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
        source_freshness["carpark"] = {"ok": True, "fetched_at": datetime.now(timezone.utc).isoformat()}
        print(f"      Carpark merged: {int(cp_features['carpark_available_lots'].sum()):,} total lots")
    except Exception as exc:
        source_freshness["carpark"] = {"ok": False, "reason": str(exc), "fetched_at": None}
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
        source_freshness["travel_times"] = {"ok": True, "fetched_at": datetime.now(timezone.utc).isoformat()}
        print(f"      Travel times merged: {len(tt_raw)} segments")
    except Exception as exc:
        source_freshness["travel_times"] = {"ok": False, "reason": str(exc), "fetched_at": None}
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

    # ── Pre-scoring freshness + quality checks ────────────────────────────────
    freshness_warnings = _check_feature_freshness(feature_df, source_freshness)
    if freshness_warnings:
        print("      ⚠ Pre-scoring warnings:")
        for w in freshness_warnings:
            print(f"        - {w}")
    else:
        print("      Data freshness OK")

    # ── Step 4: Batch scoring ─────────────────────────────────────────────────
    print("[4/7] Running batch inference...")
    t0 = time.time()
    run_meta = run_batch(feature_df)
    run_meta["avg_depletion_rate"] = float(feature_df["depletion_rate_1h"].mean())
    run_meta["source_freshness"]   = source_freshness
    run_meta["freshness_warnings"] = freshness_warnings
    print(f"      Scored: {run_meta['rows_scored']:,}  |  "
          f"Flagged: {run_meta['flagged_zones']}  |  "
          f"Failed: {run_meta['failed_rows']}  |  "
          f"{run_meta['latency_ms']}ms")

    # ── Step 5: Recommendations ───────────────────────────────────────────────
    print("[5/7] Generating recommendations...")
    pred_path = outputs_dir() / "predictions.csv"
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
    if freshness_warnings:
        print(f"  ⚠ {len(freshness_warnings)} freshness warning(s) — check source_freshness in pipeline.log")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
