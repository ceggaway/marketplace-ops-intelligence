"""
Carpark Availability
====================
Parses LTA DataMall Carpark Availability API response, maps each carpark to
the nearest URA planning zone, and computes zone-level available parking lots.

Low carpark availability → people take taxis instead of driving → higher
taxi demand → supply shortage risk increases.

LTA DataMall Carpark Availability endpoint:
    GET https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailability
    Headers: AccountKey: <key>
    Returns: {"value": [{"CarParkID", "Area", "Development", "Location",
                          "AvailableLots", "LotType", "Agency"}]}
    Docs: https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html

Data Source & Attribution
--------------------------
Carpark availability data is retrieved from the LTA DataMall API, provided
by the Land Transport Authority of Singapore (LTA).

    API:     https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html
    Terms:   https://datamall.lta.gov.sg/content/datamall/en/tnc.html
    Licence: Singapore Open Data Licence v1.0
             https://data.gov.sg/open-data-licence

Usage is for research and operational analytics purposes in accordance with
the permitted use terms set out by LTA. All rights reserved by LTA.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

CARPARK_API_URL = "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2"
SAMPLE_PATH     = Path("data/raw/sample_carpark.json")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_carpark_json(data: dict | list) -> pd.DataFrame:
    """
    Parse LTA CarParkAvailability API response into a DataFrame.

    Args:
        data: raw API response dict (with "value" key) or list of records

    Returns DataFrame with columns:
        carpark_id, area, development, lat, lon,
        available_lots, lot_type, agency
    """
    records = data.get("value", data) if isinstance(data, dict) else data
    rows = []
    for r in records:
        loc = r.get("Location", "")
        if not loc:
            continue
        try:
            lat_str, lon_str = loc.strip().split()
            lat, lon = float(lat_str), float(lon_str)
        except (ValueError, AttributeError):
            continue
        rows.append({
            "carpark_id":     str(r.get("CarParkID", "")),
            "area":           r.get("Area", ""),
            "development":    r.get("Development", ""),
            "lat":            lat,
            "lon":            lon,
            "available_lots": int(r.get("AvailableLots", 0)),
            "lot_type":       r.get("LotType", "C"),
            "agency":         r.get("Agency", ""),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Zone mapping
# ---------------------------------------------------------------------------

def map_carparks_to_zones(
    carpark_df: pd.DataFrame,
    zone_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assign each carpark to the nearest URA zone centroid using a KD-tree
    spatial join on lat/lon coordinates.

    Returns carpark_df with added columns: zone_id, zone_name
    """
    from scipy.spatial import cKDTree

    if carpark_df.empty:
        return carpark_df.copy()

    zone_coords = zone_lookup[["lat", "lon"]].values
    tree        = cKDTree(zone_coords)
    cp_coords   = carpark_df[["lat", "lon"]].values
    _, idx      = tree.query(cp_coords, k=1)

    out = carpark_df.copy()
    out["zone_id"]   = zone_lookup.iloc[idx]["zone_id"].values
    out["zone_name"] = zone_lookup.iloc[idx]["zone_name"].values
    return out


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

def compute_zone_carpark_features(
    carpark_df: pd.DataFrame,
    zone_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate carpark data to zone level.

    Only counts car lots (LotType == "C") — motorcycle and heavy-vehicle
    lots are excluded as they don't reflect passenger car parking demand.

    Returns DataFrame with columns:
        zone_id, zone_name, carpark_available_lots, carpark_count

    Zones not served by any known carpark get 0 for both columns.
    """
    mapped = map_carparks_to_zones(carpark_df, zone_lookup)

    if mapped.empty:
        df = zone_lookup[["zone_id", "zone_name"]].copy()
        df["carpark_available_lots"] = 0
        df["carpark_count"]          = 0
        return df

    cars = mapped[mapped["lot_type"] == "C"]
    agg  = (
        cars.groupby(["zone_id", "zone_name"])
        .agg(
            carpark_available_lots=("available_lots", "sum"),
            carpark_count=("carpark_id", "count"),
        )
        .reset_index()
    )

    # Ensure all 55 zones present (merge left so missing zones get 0)
    full   = zone_lookup[["zone_id", "zone_name"]].copy()
    result = full.merge(agg, on=["zone_id", "zone_name"], how="left")
    result["carpark_available_lots"] = result["carpark_available_lots"].fillna(0).astype(int)
    result["carpark_count"]          = result["carpark_count"].fillna(0).astype(int)
    return result


# ---------------------------------------------------------------------------
# Live fetch
# ---------------------------------------------------------------------------

def fetch_carpark_availability(api_key: str) -> pd.DataFrame:
    """
    Fetch live carpark availability from LTA DataMall.

    Args:
        api_key: LTA DataMall AccountKey
                 Register at https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html

    Returns parsed DataFrame (same schema as parse_carpark_json).

    Data Source: Land Transport Authority of Singapore (LTA)
    API:         https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html
    Licence:     Singapore Open Data Licence v1.0
    """
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests not installed — run: pip install requests")

    resp = requests.get(
        CARPARK_API_URL,
        headers={"AccountKey": api_key, "accept": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    return parse_carpark_json(resp.json())


# ---------------------------------------------------------------------------
# Sample / fixture loader
# ---------------------------------------------------------------------------

def load_sample_carpark() -> pd.DataFrame:
    """
    Load the sample carpark fixture from data/raw/sample_carpark.json.
    Used for development and testing when no API key is available.
    """
    if SAMPLE_PATH.exists():
        with open(SAMPLE_PATH) as f:
            return parse_carpark_json(json.load(f))
    return pd.DataFrame()
