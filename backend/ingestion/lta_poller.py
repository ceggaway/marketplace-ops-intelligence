"""
LTA DataMall – Live Taxi Availability Poller
=============================================
Polls the LTA Taxi Availability API every POLL_INTERVAL_SEC seconds,
aggregates taxi GPS coordinates to zone-level counts, and appends
snapshots to data/raw/taxi_snapshots/.

The scoring pipeline reads these snapshots to build the hourly feature
window for inference.

Usage:
    # One-shot fetch (for testing)
    python -m backend.ingestion.lta_poller --once

    # Continuous polling loop (production)
    python -m backend.ingestion.lta_poller

Environment variable required for live data:
    LTA_API_KEY=<your DataMall AccountKey>

Data Source & Attribution
--------------------------
Live taxi availability data is retrieved from the LTA DataMall API:
    https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html

Usage of LTA DataMall data is subject to the Singapore Open Data Licence
and LTA DataMall Terms of Service:
    https://datamall.lta.gov.sg/content/datamall/en/tnc.html

Data is provided by the Land Transport Authority of Singapore (LTA).
All rights reserved by LTA. This project uses the data for research and
operational analytics purposes in accordance with the permitted use terms.

Without LTA_API_KEY the poller generates a synthetic snapshot so the
rest of the pipeline can be tested end-to-end without credentials.

LTA DataMall Taxi Availability endpoint:
    GET http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability
    Headers: AccountKey: <key>
    Returns: {"value": [{"Latitude": float, "Longitude": float}, ...]}
    Docs: https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html
"""

import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Load .env so the poller works both interactively and under cron
# (cron does not inherit shell environment variables)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

from backend.ingestion.loader import get_zone_lookup, _BASE_TAXI_COUNT, _ZONE_TYPE
from backend.ingestion.carpark import fetch_carpark_availability, compute_zone_carpark_features
from backend.ingestion.travel_times import fetch_travel_times, compute_zone_congestion_features

SNAPSHOT_DIR         = Path("data/raw/taxi_snapshots")
CARPARK_SNAPSHOT_DIR = Path("data/raw/carpark_snapshots")
TRAVEL_SNAPSHOT_DIR  = Path("data/raw/travel_time_snapshots")
LTA_API_URL          = "https://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability"
POLL_INTERVAL_SEC    = 300   # 5 minutes
SG_BBOX              = {"lat": (1.15, 1.48), "lon": (103.60, 104.10)}


# ---------------------------------------------------------------------------
# Live fetch
# ---------------------------------------------------------------------------

def fetch_taxi_coords(api_key: str) -> list[dict]:
    """
    Fetch current available taxi GPS coordinates from LTA DataMall.
    Returns list of {"Latitude": float, "Longitude": float} dicts.
    """
    if not _HAS_REQUESTS:
        raise RuntimeError("requests library not installed — run: pip install requests")

    resp = requests.get(
        LTA_API_URL,
        headers={"AccountKey": api_key, "accept": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def coords_to_zone_counts(coords: list[dict], zone_lookup: pd.DataFrame) -> pd.DataFrame:
    """
    Spatial join: assign each taxi coordinate to the nearest zone centroid,
    then count taxis per zone.

    Returns DataFrame with columns: zone_id, zone_name, region, zone_type, taxi_count, timestamp
    """
    try:
        from scipy.spatial import cKDTree
    except ImportError:
        raise RuntimeError("scipy not installed — run: pip install scipy")

    ts = datetime.now(timezone.utc).astimezone(
        __import__("zoneinfo", fromlist=["ZoneInfo"]).ZoneInfo("Asia/Singapore")
    )

    # Filter to SG bounding box
    valid = [
        c for c in coords
        if SG_BBOX["lat"][0] <= c.get("Latitude", 0) <= SG_BBOX["lat"][1]
        and SG_BBOX["lon"][0] <= c.get("Longitude", 0) <= SG_BBOX["lon"][1]
    ]

    if not valid:
        # Return zero-count snapshot for all zones
        df = zone_lookup[["zone_id", "zone_name", "region", "zone_type"]].copy()
        df["taxi_count"] = 0
        df["timestamp"]  = ts
        return df

    lats = [c["Latitude"]  for c in valid]
    lons = [c["Longitude"] for c in valid]
    taxi_coords = np.column_stack([lats, lons])

    zone_coords = zone_lookup[["lat", "lon"]].values
    tree = cKDTree(zone_coords)
    _, idx = tree.query(taxi_coords, k=1)

    zone_ids   = zone_lookup.iloc[idx]["zone_id"].values
    zone_names = zone_lookup.iloc[idx]["zone_name"].values
    regions    = zone_lookup.iloc[idx]["region"].values
    zone_types = zone_lookup.iloc[idx]["zone_type"].values

    assigned = pd.DataFrame({
        "zone_id":   zone_ids,
        "zone_name": zone_names,
        "region":    regions,
        "zone_type": zone_types,
    })

    counts = (
        assigned.groupby(["zone_id", "zone_name", "region", "zone_type"])
        .size()
        .reset_index(name="taxi_count")
    )

    # Ensure all 55 zones appear (zones with 0 taxis don't show up in groupby)
    full = zone_lookup[["zone_id", "zone_name", "region", "zone_type"]].copy()
    result = full.merge(counts, on=["zone_id", "zone_name", "region", "zone_type"], how="left")
    result["taxi_count"] = result["taxi_count"].fillna(0).astype(int)
    result["timestamp"]  = ts
    return result


# ---------------------------------------------------------------------------
# Synthetic fallback (no API key)
# ---------------------------------------------------------------------------

def generate_synthetic_snapshot(zone_lookup: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a realistic synthetic taxi availability snapshot for the current
    moment. Used when LTA_API_KEY is not set — allows full pipeline testing
    without credentials.
    """
    rng = np.random.default_rng(int(time.time()) % 10_000)
    now = datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        ts = now.astimezone(ZoneInfo("Asia/Singapore"))
    except ImportError:
        ts = now
    hour = ts.hour
    dow  = ts.weekday()

    rows = []
    for _, row in zone_lookup.iterrows():
        zone_type  = row.get("zone_type", "mixed")
        base_count = _BASE_TAXI_COUNT.get(zone_type, 50)

        # Crude time-of-day factor
        if 7 <= hour <= 9 and dow < 5:
            factor = rng.uniform(0.25, 0.45) if zone_type == "CBD" else rng.uniform(0.35, 0.55)
        elif 17 <= hour <= 20 and dow < 5:
            factor = rng.uniform(0.25, 0.50)
        elif 1 <= hour <= 5:
            factor = rng.uniform(0.25, 0.40)
        else:
            factor = rng.uniform(0.55, 0.80)

        taxi_count = max(0, int(base_count * factor * rng.uniform(0.88, 1.12)))
        rows.append({
            "zone_id":   int(row["zone_id"]),
            "zone_name": row["zone_name"],
            "region":    row["region"],
            "zone_type": zone_type,
            "taxi_count": taxi_count,
            "timestamp":  ts,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Snapshot persistence
# ---------------------------------------------------------------------------

def save_snapshot(snapshot_df: pd.DataFrame) -> Path:
    """Write snapshot to data/raw/taxi_snapshots/YYYY-MM-DD_HH-MM.csv"""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts    = snapshot_df["timestamp"].iloc[0]
    fname = pd.Timestamp(ts).strftime("%Y-%m-%d_%H-%M") + ".csv"
    path  = SNAPSHOT_DIR / fname
    snapshot_df.to_csv(path, index=False)
    return path


def _snapshot_fname(ts) -> str:
    return pd.Timestamp(ts).strftime("%Y-%m-%d_%H-%M") + ".csv"


def poll_and_save_carpark(api_key: str, zone_lookup: pd.DataFrame) -> None:
    """
    Fetch live carpark availability from LTA DataMall and save a zone-level
    snapshot to data/raw/carpark_snapshots/YYYY-MM-DD_HH-MM.csv.
    Called alongside poll_once() so training data has time-aligned carpark values.
    """
    if not api_key or not _HAS_REQUESTS:
        return
    try:
        cp_raw  = fetch_carpark_availability(api_key)
        cp_feat = compute_zone_carpark_features(cp_raw, zone_lookup)
        ts = datetime.now(timezone.utc).astimezone(
            __import__("zoneinfo", fromlist=["ZoneInfo"]).ZoneInfo("Asia/Singapore")
        )
        cp_feat["timestamp"] = ts
        CARPARK_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        path = CARPARK_SNAPSHOT_DIR / _snapshot_fname(ts)
        cp_feat.to_csv(path, index=False)
    except Exception as exc:
        print(f"[lta_poller] carpark snapshot failed: {exc}")


def poll_and_save_travel_times(api_key: str, zone_lookup: pd.DataFrame) -> None:
    """
    Fetch live estimated travel times from LTA DataMall and save a zone-level
    congestion snapshot to data/raw/travel_time_snapshots/YYYY-MM-DD_HH-MM.csv.
    """
    if not api_key or not _HAS_REQUESTS:
        return
    try:
        tt_raw  = fetch_travel_times(api_key)
        tt_feat = compute_zone_congestion_features(tt_raw, zone_lookup)
        ts = datetime.now(timezone.utc).astimezone(
            __import__("zoneinfo", fromlist=["ZoneInfo"]).ZoneInfo("Asia/Singapore")
        )
        tt_feat["timestamp"] = ts
        TRAVEL_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        path = TRAVEL_SNAPSHOT_DIR / _snapshot_fname(ts)
        tt_feat.to_csv(path, index=False)
    except Exception as exc:
        print(f"[lta_poller] travel times snapshot failed: {exc}")


def load_last_n_snapshots(n: int = 12) -> pd.DataFrame:
    """
    Load the most recent n snapshots (12 × 5 min = 60 min window).
    Returns concatenated DataFrame sorted by timestamp ascending.
    Used by scoring to build the hourly zone-level feature window.
    """
    files = sorted(SNAPSHOT_DIR.glob("*.csv"))[-n:]
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_csv(f) for f in files]
    return pd.concat(dfs, ignore_index=True).sort_values("timestamp")


def aggregate_snapshots_to_hourly(snapshots_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate N 5-min snapshots into a single zone-hour record.
    taxi_count = mean available taxis across the window (represents supply level).
    """
    if snapshots_df.empty:
        return pd.DataFrame()

    agg = (
        snapshots_df.groupby(["zone_id", "zone_name", "region", "zone_type"])
        ["taxi_count"]
        .mean()
        .reset_index()
    )
    agg["taxi_count"] = agg["taxi_count"].round().astype(int)
    agg["timestamp"]  = pd.Timestamp.now(tz="Asia/Singapore").floor("h")
    return agg


# ---------------------------------------------------------------------------
# Polling loop
# ---------------------------------------------------------------------------

def poll_once(api_key: str | None, zone_lookup: pd.DataFrame) -> pd.DataFrame:
    """Fetch one snapshot — live if api_key provided, synthetic otherwise."""
    if api_key and _HAS_REQUESTS:
        coords   = fetch_taxi_coords(api_key)
        snapshot = coords_to_zone_counts(coords, zone_lookup)
        print(f"[lta_poller] live snapshot: {snapshot['taxi_count'].sum()} taxis across {len(snapshot)} zones")
    else:
        snapshot = generate_synthetic_snapshot(zone_lookup)
        print(f"[lta_poller] synthetic snapshot: {snapshot['taxi_count'].sum()} taxis")
    return snapshot


def run_polling_loop(api_key: str | None = None, interval: int = POLL_INTERVAL_SEC) -> None:
    """
    Run continuous polling loop. Ctrl-C to stop.

    On each tick:
    1. Fetch taxi availability snapshot (live or synthetic)
    2. Fetch carpark availability snapshot (live only, requires api_key)
    3. Fetch expressway travel times snapshot (live only, requires api_key)
    """
    zone_lookup = get_zone_lookup()
    print(f"[lta_poller] starting — interval={interval}s, live={'yes' if api_key else 'no (synthetic)'}")

    while True:
        try:
            snapshot = poll_once(api_key, zone_lookup)
            path     = save_snapshot(snapshot)
            print(f"[lta_poller] taxi saved → {path}")
            if api_key:
                poll_and_save_carpark(api_key, zone_lookup)
                poll_and_save_travel_times(api_key, zone_lookup)
        except Exception as exc:
            print(f"[lta_poller] error: {exc}")
        time.sleep(interval)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LTA Taxi Availability Poller")
    parser.add_argument("--once", action="store_true", help="Fetch one snapshot and exit")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SEC)
    args = parser.parse_args()

    _api_key = os.environ.get("LTA_API_KEY")
    _zones   = get_zone_lookup()

    if args.once:
        snap = poll_once(_api_key, _zones)
        out  = save_snapshot(snap)
        print(f"Snapshot saved to {out}")
    else:
        run_polling_loop(api_key=_api_key, interval=args.interval)
