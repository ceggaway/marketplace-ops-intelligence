"""
Estimated Travel Times
======================
Parses LTA DataMall Estimated Travel Times API response, maps expressway
segments to URA planning zones, and computes zone-level congestion features.

High congestion on expressways serving a zone signals elevated demand
(commuters stuck in traffic → taxi demand spikes) and reduced supply
(drivers take longer to complete trips → fewer idle taxis).

LTA DataMall Estimated Travel Times endpoint:
    GET https://datamall2.mytransport.sg/ltaodataservice/EstTravelTimes
    Headers: AccountKey: <key>
    Returns: {"value": [{"Name", "Direction", "FarEndPoint",
                          "StartPoint", "EndPoint", "EstTime"}]}
    Docs: https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html

Data Source & Attribution
--------------------------
Estimated travel time data is retrieved from the LTA DataMall API, provided
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

TRAVEL_TIMES_API_URL = "https://datamall2.mytransport.sg/ltaodataservice/EstTravelTimes"
SAMPLE_PATH          = Path("data/raw/sample_travel_times.json")

# ---------------------------------------------------------------------------
# Segment endpoint → approximate GPS coordinates
# Used for spatial join to nearest URA zone centroid.
# Covers all major Singapore expressways: AYE, PIE, CTE, BKE, SLE, TPE, ECP, KJE, MCE.
# Coordinates are approximate centroids near each junction/landmark.
# ---------------------------------------------------------------------------
_SEGMENT_COORDS: dict[str, tuple[float, float]] = {
    # AYE (Ayer Rajah Expressway)
    "AYE/MCE INTERCHANGE":   (1.2767, 103.8389),
    "TELOK BLANGAH RD":      (1.2726, 103.8196),
    "LOWER DELTA RD":        (1.2819, 103.8239),
    "NORMANTON PARK":        (1.2955, 103.7975),
    "NORTH BUONA VISTA RD":  (1.3072, 103.7900),
    "CLEMENTI RD":           (1.3162, 103.7649),
    "WEST COAST RD":         (1.3054, 103.7649),
    "CLEMENTI AVE 6":        (1.3150, 103.7620),
    "CLEMENTI AVE 2":        (1.3140, 103.7620),
    "JURONG TOWN HALL RD":   (1.3329, 103.7436),
    "PENJURU RD":            (1.3100, 103.7050),
    "JURONG PORT RD":        (1.3080, 103.6972),
    "JURONG PIER RD":        (1.3100, 103.6850),
    "JALAN BOON LAY":        (1.3386, 103.7069),
    "YUAN CHING RD":         (1.3310, 103.7100),
    "PIONEER CIRCUS":        (1.3178, 103.6972),
    "BENOI RD":              (1.3200, 103.6950),
    "AYE/PIE INTERCHANGE":   (1.3150, 103.6900),
    "TUAS WEST RD":          (1.3020, 103.6480),
    "TUAS WEST DRIVE":       (1.3000, 103.6400),
    "TUAS CHECKPOINT":       (1.2967, 103.6376),
    "KEPPEL RD":             (1.2700, 103.8300),
    "ALEXANDRA RD":          (1.2900, 103.8200),
    "PORTSDOWN RD":          (1.3000, 103.8000),
    # PIE (Pan Island Expressway)
    "PIE/KJE INTERCHANGE":   (1.3774, 103.7719),
    "UPPER CHANGI RD EAST":  (1.3521, 103.9438),
    "TAMPINES AVE 10":       (1.3521, 103.9438),
    "TAMPINES VIADUCT":      (1.3521, 103.9200),
    "LOYANG AVE":            (1.3600, 103.9400),
    "PASIR RIS DR 3":        (1.3721, 103.9474),
    "HOUGANG AVE 8":         (1.3612, 103.8863),
    "BARTLEY RD":            (1.3554, 103.8679),
    "BRADDELL RD":           (1.3526, 103.8352),
    "THOMSON RD":            (1.3204, 103.8439),
    "ADAM RD":               (1.3138, 103.8376),
    "MOUNT PLEASANT RD":     (1.3204, 103.8300),
    "BUKIT TIMAH RD":        (1.3294, 103.7958),
    "KRANJI EXP":            (1.4374, 103.7869),
    # CTE (Central Expressway)
    "MARYMOUNT RD":          (1.3526, 103.8352),
    "ANG MO KIO AVE 1":      (1.3691, 103.8454),
    "LOWER PIERCE RD":       (1.3800, 103.8300),
    "YISHUN AVE 2":          (1.4304, 103.8354),
    "YISHUN AVE 7":          (1.4200, 103.8354),
    "SEMBAWANG RD":          (1.4491, 103.8200),
    # ECP (East Coast Parkway)
    "EAST COAST PKWY":       (1.3000, 103.9052),
    "TANJONG KATONG RD":     (1.3178, 103.8924),
    "FORT RD":               (1.2967, 103.8700),
    "CHANGI COAST RD":       (1.3644, 103.9915),
    # SLE / BKE
    "WOODLANDS AVE 12":      (1.4374, 103.7869),
    "MANDAI RD":             (1.4072, 103.8029),
    "UPPER BUKIT TIMAH RD":  (1.3774, 103.7719),
    # TPE / KPE
    "PUNGGOL CENTRAL":       (1.4043, 103.9021),
    "SENGKANG EAST RD":      (1.3868, 103.8914),
    "TAMPINES EXPY":         (1.3600, 103.9400),
    # MCE (Marina Coastal Expressway)
    "MCE/AYE INTERCHANGE":   (1.2767, 103.8389),
    "MARINA COASTAL DR":     (1.2711, 103.8636),
    "MARINA BOULEVARD":      (1.2789, 103.8536),
    # KJE (Kranji Expressway)
    "KJE/BKE INTERCHANGE":   (1.3840, 103.7470),
    "CHOA CHU KANG AVE 4":   (1.3840, 103.7470),
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_travel_times_json(data: dict | list) -> pd.DataFrame:
    """
    Parse LTA EstTravelTimes API response into a DataFrame.

    Returns DataFrame with columns:
        expressway, direction, far_endpoint, start_point, end_point, est_time
    """
    records = data.get("value", data) if isinstance(data, dict) else data
    rows = []
    for r in records:
        rows.append({
            "expressway":   r.get("Name", ""),
            "direction":    int(r.get("Direction", 1)),
            "far_endpoint": r.get("FarEndPoint", ""),
            "start_point":  r.get("StartPoint", ""),
            "end_point":    r.get("EndPoint", ""),
            "est_time":     float(r.get("EstTime", 0)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Zone mapping
# ---------------------------------------------------------------------------

def map_segments_to_zones(
    travel_df: pd.DataFrame,
    zone_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assign each segment to the nearest URA zone centroid.

    Uses the StartPoint coordinate from _SEGMENT_COORDS as a proxy for
    segment location. Falls back to EndPoint if StartPoint is unknown.
    Rows with no known coordinates are dropped.

    Returns travel_df with added columns: zone_id, zone_name
    """
    from scipy.spatial import cKDTree

    zone_coords = zone_lookup[["lat", "lon"]].values
    tree        = cKDTree(zone_coords)

    zone_ids, zone_names = [], []
    for _, row in travel_df.iterrows():
        coord = (
            _SEGMENT_COORDS.get(row["start_point"])
            or _SEGMENT_COORDS.get(row["end_point"])
        )
        if coord is None:
            zone_ids.append(None)
            zone_names.append(None)
            continue
        _, idx = tree.query([coord], k=1)
        zone_ids.append(int(zone_lookup.iloc[idx[0]]["zone_id"]))
        zone_names.append(str(zone_lookup.iloc[idx[0]]["zone_name"]))

    out = travel_df.copy()
    out["zone_id"]   = zone_ids
    out["zone_name"] = zone_names
    return out.dropna(subset=["zone_id"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

def compute_zone_congestion_features(
    travel_df: pd.DataFrame,
    zone_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregate expressway travel time data to zone level.

    congestion_ratio = zone_avg_est_time / global_median_est_time
        > 1.0  → this zone's expressways are slower than typical
        = 1.0  → at typical speed
        < 1.0  → faster than typical (unlikely in peak hours)

    Uses the global median across all segments as a normalisation baseline
    (avoids needing a hardcoded free-flow lookup per segment).

    Returns DataFrame with columns:
        zone_id, zone_name, congestion_ratio, avg_est_time_min

    Zones not adjacent to any expressway get congestion_ratio=1.0 (neutral).
    """
    mapped = map_segments_to_zones(travel_df, zone_lookup)

    if mapped.empty:
        df = zone_lookup[["zone_id", "zone_name"]].copy()
        df["congestion_ratio"]  = 1.0
        df["avg_est_time_min"]  = np.nan
        return df

    global_median = max(mapped["est_time"].median(), 1.0)
    mapped["congestion_ratio"] = (mapped["est_time"] / global_median).round(4)

    agg = (
        mapped.groupby(["zone_id", "zone_name"])
        .agg(
            avg_est_time_min=("est_time", "mean"),
            congestion_ratio=("congestion_ratio", "mean"),
        )
        .reset_index()
    )
    agg["avg_est_time_min"] = agg["avg_est_time_min"].round(4)
    agg["congestion_ratio"] = agg["congestion_ratio"].round(4)

    # All 55 zones — missing zones get neutral congestion
    full   = zone_lookup[["zone_id", "zone_name"]].copy()
    result = full.merge(agg, on=["zone_id", "zone_name"], how="left")
    result["congestion_ratio"] = result["congestion_ratio"].fillna(1.0)
    return result


# ---------------------------------------------------------------------------
# Live fetch
# ---------------------------------------------------------------------------

def fetch_travel_times(api_key: str) -> pd.DataFrame:
    """
    Fetch live estimated travel times from LTA DataMall.

    Args:
        api_key: LTA DataMall AccountKey
                 Register at https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html

    Returns parsed DataFrame (same schema as parse_travel_times_json).

    Data Source: Land Transport Authority of Singapore (LTA)
    API:         https://datamall.lta.gov.sg/content/datamall/en/dynamic-data.html
    Licence:     Singapore Open Data Licence v1.0
    """
    try:
        import requests
    except ImportError:
        raise RuntimeError("requests not installed — run: pip install requests")

    resp = requests.get(
        TRAVEL_TIMES_API_URL,
        headers={"AccountKey": api_key, "accept": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    return parse_travel_times_json(resp.json())


# ---------------------------------------------------------------------------
# Sample / fixture loader
# ---------------------------------------------------------------------------

def load_sample_travel_times() -> pd.DataFrame:
    """
    Load the sample travel times fixture from data/raw/sample_travel_times.json.
    Used for development and testing when no API key is available.
    """
    if SAMPLE_PATH.exists():
        with open(SAMPLE_PATH) as f:
            return parse_travel_times_json(json.load(f))
    return pd.DataFrame()
