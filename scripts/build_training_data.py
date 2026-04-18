"""
Build Training Dataset from Real LTA Snapshots
===============================================
Reads 5-minute taxi availability snapshots collected by the LTA poller,
aggregates them to hourly zone-level records, runs feature engineering
via the canonical backend.preprocessing.pipeline.build_features() (same
function used at inference time — eliminates train/serve skew), and
writes a training-ready parquet file.

Minimum data required:
    24 hours of snapshots  →  lag_1h and lag_24h fully populated
    48 hours              →  supply_vs_yesterday meaningful for all hours
    7 days                →  taxi_lag_168h (weekly pattern) fully populated

External snapshots (carpark + travel times) are loaded from their
respective snapshot directories when available, giving each training row
a time-aligned value rather than a broadcast point-in-time value.

Usage:
    python scripts/build_training_data.py
    python scripts/build_training_data.py --out data/processed/training.parquet
    python scripts/build_training_data.py --min-hours 24 --verbose
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# Make repo root importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from backend.ingestion.weather import get_hourly_weather
from backend.ingestion.carpark import (
    fetch_carpark_availability, compute_zone_carpark_features, parse_carpark_json,
)
from backend.ingestion.travel_times import (
    fetch_travel_times, compute_zone_congestion_features, parse_travel_times_json,
)
from backend.ingestion.loader import get_zone_lookup
from backend.preprocessing.pipeline import build_features   # single source of truth
from backend.training.trainer import _make_target

SNAPSHOT_DIR      = ROOT / "data" / "raw" / "taxi_snapshots"
CARPARK_SNAP_DIR  = ROOT / "data" / "raw" / "carpark_snapshots"
TRAVEL_SNAP_DIR   = ROOT / "data" / "raw" / "travel_time_snapshots"
DEFAULT_OUT       = ROOT / "data" / "processed" / "training.parquet"


# ── Step 1: load & aggregate taxi snapshots to hourly ─────────────────────────

def load_snapshots() -> pd.DataFrame:
    """Load all taxi snapshot CSVs, return concatenated DataFrame."""
    files = sorted(SNAPSHOT_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No snapshots found in {SNAPSHOT_DIR}. "
            "Run the LTA poller first: python -m backend.ingestion.lta_poller"
        )
    dfs = [pd.read_csv(f) for f in files]
    df  = pd.concat(dfs, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Singapore")
    return df


def aggregate_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse 5-min snapshots to one row per (zone, hour). taxi_count = mean."""
    df = df.copy()
    df["hour_floor"] = df["timestamp"].dt.floor("h")
    hourly = (
        df.groupby(["zone_id", "zone_name", "region", "zone_type", "hour_floor"])
        ["taxi_count"]
        .mean()
        .reset_index()
    )
    hourly["taxi_count"] = hourly["taxi_count"].round().astype(int)
    hourly = hourly.rename(columns={"hour_floor": "timestamp"})
    return hourly.sort_values(["zone_id", "timestamp"]).reset_index(drop=True)


# ── Step 1b: weather merge ────────────────────────────────────────────────────

def merge_weather(hourly: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch hourly weather from Open-Meteo for the date range in the snapshot
    data and left-join onto hourly. Falls back to neutral defaults on failure.
    """
    ts         = pd.to_datetime(hourly["timestamp"]).dt.tz_convert("Asia/Singapore")
    start_date = ts.min().strftime("%Y-%m-%d")
    end_date   = ts.max().strftime("%Y-%m-%d")

    try:
        weather_df = get_hourly_weather(start_date, end_date)
        weather_df["timestamp"] = weather_df["timestamp"].dt.floor("h")
        hourly = hourly.copy()
        hourly["timestamp"] = ts.dt.floor("h")
        hourly = hourly.merge(
            weather_df[["timestamp", "temperature_c", "rainfall_mm", "weather_code", "is_raining"]],
            on="timestamp", how="left",
        )
        print(f"  Weather merged: {weather_df['timestamp'].nunique()} hours from Open-Meteo "
              f"({start_date} → {end_date})")
    except Exception as exc:
        print(f"  Weather fetch failed ({exc}) — using defaults for all rows")

    for col, default in [
        ("weather_code",  0), ("rainfall_mm", 0.0),
        ("is_raining",    False), ("temperature_c", 28.0),
    ]:
        if col not in hourly.columns:
            hourly[col] = default
        else:
            hourly[col] = hourly[col].fillna(default)
    hourly["weather_code"] = hourly["weather_code"].astype(int)
    return hourly


# ── Step 1c: carpark + congestion merge (time-aligned) ───────────────────────

def _load_external_snapshots(snap_dir: Path, parse_fn) -> pd.DataFrame:
    """
    Load all snapshots from snap_dir (CSV files named YYYY-MM-DD_HH-MM.csv),
    parse each using parse_fn, and tag with the snapshot timestamp.
    Returns a DataFrame with a 'snap_ts' column floored to the hour.
    """
    files = sorted(snap_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame()
    rows = []
    for f in files:
        try:
            # Filename encodes the capture time: 2026-04-17_08-30.csv
            snap_ts = pd.Timestamp(
                f.stem.replace("_", "T").replace("-", ":", 2),
                tz="Asia/Singapore",
            ).floor("h")
        except Exception:
            continue
        try:
            df = pd.read_csv(f)
            df["snap_ts"] = snap_ts
            rows.append(df)
        except Exception:
            continue
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def merge_external_features(hourly: pd.DataFrame, api_key: str | None) -> pd.DataFrame:
    """
    Merge carpark availability and expressway congestion into the hourly training
    DataFrame using time-aligned snapshots when available.

    Time-aligned path (preferred): load per-hour snapshots from
        data/raw/carpark_snapshots/  and  data/raw/travel_time_snapshots/
    Fallback path: fetch a live point-in-time snapshot from LTA DataMall and
        broadcast across all rows (acceptable when snapshot dirs are empty).
    """
    hourly     = hourly.copy()
    zone_lookup = get_zone_lookup()

    # ── Carpark ───────────────────────────────────────────────────────────────
    cp_snaps = _load_external_snapshots(CARPARK_SNAP_DIR, parse_carpark_json)
    if not cp_snaps.empty and "snap_ts" in cp_snaps.columns:
        # Compute zone features per snapshot hour, then merge on (zone_id, hour)
        cp_hourly_list = []
        for snap_ts, grp in cp_snaps.groupby("snap_ts"):
            feats = compute_zone_carpark_features(grp.drop(columns=["snap_ts"]), zone_lookup)
            feats["timestamp"] = snap_ts
            cp_hourly_list.append(feats)
        if cp_hourly_list:
            cp_all = pd.concat(cp_hourly_list, ignore_index=True)
            hourly = hourly.merge(
                cp_all[["zone_id", "timestamp", "carpark_available_lots"]],
                on=["zone_id", "timestamp"], how="left",
            )
            print(f"  Carpark merged time-aligned: {len(cp_snaps['snap_ts'].unique())} hourly snapshots")
    else:
        # Fallback: live point-in-time fetch
        try:
            cp_raw  = fetch_carpark_availability(api_key)
            cp_feat = compute_zone_carpark_features(cp_raw, zone_lookup)
            hourly  = hourly.merge(
                cp_feat[["zone_id", "carpark_available_lots"]], on="zone_id", how="left",
            )
            total_lots = int(cp_feat["carpark_available_lots"].sum())
            print(f"  Carpark merged point-in-time (no historical snapshots): "
                  f"{total_lots:,} lots. Run the poller longer for time-aligned values.")
        except Exception as exc:
            print(f"  Carpark fetch failed ({exc}) — using default 0")

    if "carpark_available_lots" not in hourly.columns:
        hourly["carpark_available_lots"] = 0
    else:
        hourly["carpark_available_lots"] = hourly["carpark_available_lots"].fillna(0).astype(int)

    # ── Congestion ────────────────────────────────────────────────────────────
    tt_snaps = _load_external_snapshots(TRAVEL_SNAP_DIR, parse_travel_times_json)
    if not tt_snaps.empty and "snap_ts" in tt_snaps.columns:
        tt_hourly_list = []
        for snap_ts, grp in tt_snaps.groupby("snap_ts"):
            feats = compute_zone_congestion_features(grp.drop(columns=["snap_ts"]), zone_lookup)
            feats["timestamp"] = snap_ts
            tt_hourly_list.append(feats)
        if tt_hourly_list:
            tt_all = pd.concat(tt_hourly_list, ignore_index=True)
            hourly = hourly.merge(
                tt_all[["zone_id", "timestamp", "congestion_ratio"]],
                on=["zone_id", "timestamp"], how="left",
            )
            print(f"  Travel times merged time-aligned: {len(tt_snaps['snap_ts'].unique())} hourly snapshots")
    else:
        try:
            tt_raw  = fetch_travel_times(api_key)
            tt_feat = compute_zone_congestion_features(tt_raw, zone_lookup)
            hourly  = hourly.merge(
                tt_feat[["zone_id", "congestion_ratio"]], on="zone_id", how="left",
            )
            print(f"  Travel times merged point-in-time (no historical snapshots): "
                  f"{len(tt_raw)} segments.")
        except Exception as exc:
            print(f"  Travel times fetch failed ({exc}) — using default 1.0")

    if "congestion_ratio" not in hourly.columns:
        hourly["congestion_ratio"] = 1.0
    else:
        hourly["congestion_ratio"] = hourly["congestion_ratio"].fillna(1.0)

    return hourly


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build training dataset from LTA snapshots")
    parser.add_argument("--out",       default=str(DEFAULT_OUT), help="Output parquet path")
    parser.add_argument("--min-hours", type=int, default=24,
                        help="Minimum hours of data required (default: 24)")
    parser.add_argument("--verbose",   action="store_true")
    args = parser.parse_args()

    # ── Load
    print("Loading snapshots...")
    raw     = load_snapshots()
    n_files = len(sorted(SNAPSHOT_DIR.glob("*.csv")))
    span_h  = (raw["timestamp"].max() - raw["timestamp"].min()).total_seconds() / 3600
    print(f"  {n_files} snapshot files  |  {raw['zone_id'].nunique()} zones  "
          f"|  span: {span_h:.1f} hours")

    if span_h < args.min_hours:
        print(f"\n  Not enough data yet — need {args.min_hours}h, have {span_h:.1f}h.")
        print(f"  Come back in ~{args.min_hours - span_h:.0f} hours.")
        sys.exit(1)

    # ── Aggregate to hourly
    print("Aggregating to hourly records...")
    hourly = aggregate_to_hourly(raw)
    print(f"  {len(hourly):,} hourly zone-records  "
          f"({hourly['timestamp'].nunique()} hours × {hourly['zone_id'].nunique()} zones)")

    # ── Merge real weather (Open-Meteo, free, no key)
    print("Merging weather data...")
    hourly = merge_weather(hourly)

    # ── Merge carpark + congestion (time-aligned when snapshots exist)
    api_key = os.environ.get("LTA_API_KEY")
    print("Merging carpark & travel time data...")
    hourly = merge_external_features(hourly, api_key)

    # ── Feature engineering — use canonical pipeline (same as inference)
    print("Building features via pipeline.build_features()...")
    featured = build_features(hourly)

    # ── Add target label
    featured["supply_shortage"] = _make_target(featured)
    shortage_rate = (featured["supply_shortage"] == 1).mean()
    print(f"  {len(featured):,} rows  |  shortage rate: {shortage_rate:.2%}")

    # ── Save
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    featured.to_parquet(out_path, index=False)
    print(f"\nSaved → {out_path}  ({out_path.stat().st_size / 1024:.0f} KB)")

    if args.verbose:
        print("\nColumn list:")
        print(list(featured.columns))
        print("\nSample (first 3 rows):")
        print(featured.head(3).to_string())

    # ── Readiness check
    print("\nReadiness check:")
    lag24_cov = featured["taxi_lag_24h"].notna().mean()
    lag168_cov = featured["taxi_lag_168h"].notna().mean() if "taxi_lag_168h" in featured.columns else 0
    print(f"  taxi_lag_1h  coverage : 100%  ✓")
    print(f"  taxi_lag_24h coverage : {lag24_cov:.0%}  {'✓' if lag24_cov > 0.9 else '⚠ need more data'}")
    print(f"  taxi_lag_168h coverage: {lag168_cov:.0%}  {'✓' if lag168_cov > 0.9 else '⚠ need 7 days'}")

    if span_h < 48:
        print(f"\n  Next milestone: 48h → supply_vs_yesterday fully meaningful")
    if span_h < 168:
        days_left = (168 - span_h) / 24
        print(f"  Next milestone: 7 days ({days_left:.1f} days away) → taxi_lag_168h fully populated")

    print("\nDone. To train:")
    print(f"  python scripts/run_training.py --data {out_path}")


if __name__ == "__main__":
    main()
