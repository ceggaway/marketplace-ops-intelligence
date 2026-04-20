"""
Data Ingestion – Loader
=======================
Responsibility: Load raw data from Singapore public sources into a validated,
minimally-cleaned pandas DataFrame ready for downstream processing.

Primary dataset:
    LTA DataMall – Taxi Availability (real-time taxi GPS pings)
    → available taxi count per zone per hour is the core supply signal.

Current modeling problem:
    Predict near-term supply depletion per zone: will available taxis fall
    below 60% of their current level in the next hour?

When real API data is not available, use generate_synthetic_data() to produce
a realistic zone-hour taxi availability dataset for development and CI.

Zone system:
    Singapore URA Planning Areas — 55 zones.
    Columns: zone_id, zone_name, region, zone_type, lat, lon
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Singapore URA Planning Areas (55 zones)
# ---------------------------------------------------------------------------

SG_ZONES = [
    (1,  "Ang Mo Kio",      "North-East", 1.3691, 103.8454),
    (2,  "Bedok",           "East",       1.3236, 103.9273),
    (3,  "Bishan",          "Central",    1.3526, 103.8352),
    (4,  "Boon Lay",        "West",       1.3386, 103.7069),
    (5,  "Bukit Batok",     "West",       1.3491, 103.7495),
    (6,  "Bukit Merah",     "Central",    1.2819, 103.8239),
    (7,  "Bukit Panjang",   "West",       1.3774, 103.7719),
    (8,  "Bukit Timah",     "Central",    1.3294, 103.7958),
    (9,  "Buona Vista",     "West",       1.3072, 103.7900),
    (10, "Changi",          "East",       1.3644, 103.9915),
    (11, "Choa Chu Kang",   "West",       1.3840, 103.7470),
    (12, "Clementi",        "West",       1.3162, 103.7649),
    (13, "Downtown Core",   "Central",    1.2789, 103.8536),
    (14, "Geylang",         "East",       1.3201, 103.8918),
    (15, "Hougang",         "North-East", 1.3612, 103.8863),
    (16, "Jurong East",     "West",       1.3329, 103.7436),
    (17, "Jurong West",     "West",       1.3404, 103.7090),
    (18, "Kallang",         "Central",    1.3100, 103.8614),
    (19, "Lim Chu Kang",    "North",      1.4231, 103.7192),
    (20, "Mandai",          "North",      1.4072, 103.8029),
    (21, "Marina East",     "Central",    1.2801, 103.8700),
    (22, "Marina South",    "Central",    1.2711, 103.8636),
    (23, "Marine Parade",   "East",       1.2999, 103.9052),
    (24, "Museum",          "Central",    1.2967, 103.8492),
    (25, "Newton",          "Central",    1.3138, 103.8376),
    (26, "Novena",          "Central",    1.3204, 103.8439),
    (27, "Orchard",         "Central",    1.3048, 103.8318),
    (28, "Outram",          "Central",    1.2797, 103.8393),
    (29, "Pasir Ris",       "East",       1.3721, 103.9474),
    (30, "Paya Lebar",      "East",       1.3178, 103.8924),
    (31, "Pioneer",         "West",       1.3178, 103.6972),
    (32, "Punggol",         "North-East", 1.4043, 103.9021),
    (33, "Queenstown",      "Central",    1.2942, 103.7861),
    (34, "River Valley",    "Central",    1.2918, 103.8306),
    (35, "Rochor",          "Central",    1.3048, 103.8566),
    (36, "Seletar",         "North-East", 1.4042, 103.8699),
    (37, "Sembawang",       "North",      1.4491, 103.8200),
    (38, "Sengkang",        "North-East", 1.3868, 103.8914),
    (39, "Serangoon",       "North-East", 1.3554, 103.8679),
    (40, "Simpang",         "North",      1.4333, 103.8001),
    (41, "Singapore River", "Central",    1.2874, 103.8450),
    (42, "Southern Islands","Central",    1.2494, 103.8303),
    (43, "Straits View",    "Central",    1.2676, 103.8592),
    (44, "Sungei Kadut",    "North",      1.4148, 103.7592),
    (45, "Tampines",        "East",       1.3521, 103.9438),
    (46, "Tanglin",         "Central",    1.3059, 103.8176),
    (47, "Tengah",          "West",       1.3736, 103.7208),
    (48, "Toa Payoh",       "Central",    1.3343, 103.8563),
    (49, "Tuas",            "West",       1.2967, 103.6376),
    (50, "Western Islands", "West",       1.2494, 103.7536),
    (51, "Western Water",   "West",       1.3230, 103.6500),
    (52, "Woodlands",       "North",      1.4374, 103.7869),
    (53, "Yishun",          "North",      1.4304, 103.8354),
    (54, "Bukit Brown",     "Central",    1.3370, 103.8220),
    (55, "Tengah Park",     "West",       1.3750, 103.7150),
]

# Zone type classification
# CBD: high-density commercial, taxis get absorbed rapidly during peaks
# transport_hub: major interchange nodes, heavy flux both ways
# residential: primarily housing estates, demand spikes during evening peak
# mixed: commercial + residential blend
# industrial: low taxi demand, mostly day workers
_ZONE_TYPE: dict[str, str] = {
    "Downtown Core":   "CBD",
    "Orchard":         "CBD",
    "Marina East":     "CBD",
    "Marina South":    "CBD",
    "Singapore River": "CBD",
    "River Valley":    "CBD",
    "Museum":          "CBD",
    "Outram":          "CBD",
    "Rochor":          "CBD",
    "Straits View":    "CBD",
    "Kallang":         "CBD",
    "Changi":          "transport_hub",
    "Jurong East":     "transport_hub",
    "Woodlands":       "transport_hub",
    "Ang Mo Kio":      "transport_hub",
    "Tampines":        "transport_hub",
    "Bishan":          "transport_hub",
    "Toa Payoh":       "transport_hub",
    "Novena":          "transport_hub",
    "Tuas":            "industrial",
    "Pioneer":         "industrial",
    "Sungei Kadut":    "industrial",
    "Lim Chu Kang":    "industrial",
    "Western Islands": "industrial",
    "Western Water":   "industrial",
    "Simpang":         "industrial",
}

# Base available taxi count per zone type during a neutral off-peak hour
_BASE_TAXI_COUNT: dict[str, int] = {
    "CBD":           120,
    "transport_hub":  75,
    "residential":    35,
    "mixed":          50,
    "industrial":     10,
}

# WMO weather codes
_WMO_CLEAR = [0, 1, 2]
_WMO_CLOUD = [3, 45, 48]
_WMO_RAIN  = [61, 63, 65, 80, 81, 82, 95]

# SG public holidays 2024–2026
# Islamic holidays (Hari Raya Puasa, Hari Raya Haji) and Deepavali dates
# are subject to official MOM announcements; values below are approximate.
# Source: Ministry of Manpower (MOM) — https://www.mom.gov.sg/employment-practices/public-holidays
_SG_HOLIDAYS = {
    # 2024
    "2024-01-01", "2024-02-10", "2024-02-11",
    "2024-03-29", "2024-04-10", "2024-05-01",
    "2024-05-23", "2024-06-17", "2024-08-09",
    "2024-10-31", "2024-12-25",
    # 2025
    "2025-01-01", "2025-01-29", "2025-01-30",
    "2025-03-31", "2025-04-18", "2025-05-01",
    "2025-05-12", "2025-06-06", "2025-08-09",
    "2025-10-20", "2025-12-25",
    # 2026
    "2026-01-01", "2026-02-17", "2026-02-18",
    "2026-03-20", "2026-04-03", "2026-05-01",
    "2026-05-22", "2026-05-27", "2026-08-10",
    "2026-11-08", "2026-12-25",
}

# MOE school holiday periods (when primary & secondary schools are closed).
# Each tuple: (start_date_inclusive, end_date_inclusive) as "YYYY-MM-DD" strings.
# Source: Ministry of Education (MOE) — https://www.moe.gov.sg/calendar
# Note: year-end closure dates are approximate; verify against official MOE calendar.
_SG_SCHOOL_HOLIDAY_RANGES: list[tuple[str, str]] = [
    # 2024
    ("2024-03-09", "2024-03-17"),   # March holidays
    ("2024-05-25", "2024-06-23"),   # June / mid-year holidays
    ("2024-09-07", "2024-09-15"),   # September holidays
    ("2024-11-16", "2024-12-31"),   # Year-end holidays
    # 2025
    ("2025-03-08", "2025-03-16"),
    ("2025-05-31", "2025-06-29"),
    ("2025-09-06", "2025-09-14"),
    ("2025-11-15", "2025-12-31"),
    # 2026
    ("2026-03-14", "2026-03-22"),
    ("2026-05-30", "2026-06-28"),
    ("2026-09-05", "2026-09-13"),
    ("2026-11-14", "2026-12-31"),
]


def _is_school_holiday(date_str: str) -> bool:
    """Return True if date_str (YYYY-MM-DD) falls within a school holiday period."""
    for start, end in _SG_SCHOOL_HOLIDAY_RANGES:
        if start <= date_str <= end:
            return True
    return False


# Synthetic base carpark lots available per zone type (off-peak baseline)
_BASE_CARPARK_LOTS: dict[str, int] = {
    "CBD":           600,
    "transport_hub": 350,
    "residential":   180,
    "mixed":         280,
    "industrial":     80,
}


def _carpark_factor(hour: int, is_weekday: bool) -> float:
    """Fraction of base carpark lots that are available at a given hour."""
    if 0 <= hour <= 6:
        return 0.88  # overnight, mostly empty
    elif 7 <= hour <= 9:
        return 0.35 if is_weekday else 0.72   # morning rush fills carparks
    elif 10 <= hour <= 16:
        return 0.58 if is_weekday else 0.65   # midday occupancy
    elif 17 <= hour <= 20:
        return 0.30 if is_weekday else 0.55   # evening rush
    else:
        return 0.65   # late evening recovering


def _congestion_factor(
    hour: int, is_weekday: bool, is_raining: bool, zone_type: str,
    rng: np.random.Generator,
) -> float:
    """
    Synthetic expressway congestion ratio for a zone at a given hour.
    1.0 = free flow; >1.0 = congested.
    Industrial zones have a lower ceiling (fewer passenger vehicles).
    """
    if 7 <= hour <= 9 and is_weekday:
        base = rng.uniform(1.6, 2.6)
    elif 17 <= hour <= 20 and is_weekday:
        base = rng.uniform(1.8, 2.9)
    elif 7 <= hour <= 9 or 17 <= hour <= 20:   # weekend peak — lighter
        base = rng.uniform(1.1, 1.5)
    elif 1 <= hour <= 5:
        base = rng.uniform(0.7, 1.0)
    else:
        base = rng.uniform(0.9, 1.3)

    if is_raining:
        base += rng.uniform(0.2, 0.5)

    if zone_type == "industrial":
        base = min(base, 1.5)

    return round(float(np.clip(base, 0.5, 4.0)), 4)


def get_zone_lookup() -> pd.DataFrame:
    """Return zone lookup DataFrame including zone_type."""
    path = Path("data/raw/sg_planning_areas.csv")
    if path.exists():
        df = pd.read_csv(path)
        if "zone_type" not in df.columns:
            df["zone_type"] = df["zone_name"].map(_ZONE_TYPE).fillna("mixed")
        return df
    df = pd.DataFrame(SG_ZONES, columns=["zone_id", "zone_name", "region", "lat", "lon"])
    df["zone_type"] = df["zone_name"].map(_ZONE_TYPE).fillna("mixed")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df


def load_zone_lookup(path: str | Path = "data/raw/sg_planning_areas.csv") -> pd.DataFrame:
    """Load Singapore URA planning area metadata."""
    p = Path(path)
    if p.exists():
        df = pd.read_csv(p)
        if "zone_type" not in df.columns:
            df["zone_type"] = df["zone_name"].map(_ZONE_TYPE).fillna("mixed")
        return df
    return get_zone_lookup()


def _availability_factor(hour: int, dow: int, is_raining: bool, is_holiday: bool,
                          zone_type: str, rng: np.random.Generator) -> float:
    """
    Compute what fraction of base taxis are available (idle) at a given hour.

    Low factor = high demand absorbed most taxis (shortage risk).
    High factor = demand is low, most taxis idle (surplus).

    This mirrors real LTA taxi availability patterns:
    - Morning peak: factor drops as commuters hire taxis
    - Evening peak: factor drops again, deeper for residential zones
    - Deep night: factor low (few taxis operating)
    - Midday: moderate recovery
    """
    is_weekday = dow < 5

    if 1 <= hour <= 5:
        # Deep night: few taxis operating; those out are mostly idle
        factor = rng.uniform(0.25, 0.45)
    elif hour == 6:
        # Pre-rush: taxis coming online
        factor = rng.uniform(0.65, 0.80)
    elif 7 <= hour <= 9:
        # Morning peak: demand absorbs taxis fast
        if zone_type == "CBD":
            factor = rng.uniform(0.20, 0.45) if is_weekday else rng.uniform(0.55, 0.75)
        elif zone_type == "transport_hub":
            factor = rng.uniform(0.30, 0.55) if is_weekday else rng.uniform(0.55, 0.75)
        elif zone_type == "residential":
            factor = rng.uniform(0.35, 0.60) if is_weekday else rng.uniform(0.60, 0.80)
        else:
            factor = rng.uniform(0.45, 0.65) if is_weekday else rng.uniform(0.65, 0.80)
    elif 10 <= hour <= 16:
        # Midday lull: most trips done, taxis recovering
        if zone_type == "CBD":
            factor = rng.uniform(0.55, 0.75)
        else:
            factor = rng.uniform(0.60, 0.80)
    elif 17 <= hour <= 20:
        # Evening peak: residential zones deplete more
        if zone_type == "residential":
            factor = rng.uniform(0.20, 0.45) if is_weekday else rng.uniform(0.45, 0.65)
        elif zone_type == "CBD":
            factor = rng.uniform(0.30, 0.55) if is_weekday else rng.uniform(0.55, 0.70)
        elif zone_type == "transport_hub":
            factor = rng.uniform(0.25, 0.50) if is_weekday else rng.uniform(0.50, 0.70)
        else:
            factor = rng.uniform(0.40, 0.65) if is_weekday else rng.uniform(0.55, 0.72)
    elif 21 <= hour <= 23:
        # Post-peak: gradual recovery
        factor = rng.uniform(0.50, 0.70)
    else:
        factor = rng.uniform(0.55, 0.75)

    # Rain: demand spikes, some drivers stay off roads → fewer available taxis
    if is_raining:
        factor *= rng.uniform(0.55, 0.75)

    # Holidays: CBD demand higher, residential demand shifts
    if is_holiday:
        if zone_type == "CBD":
            factor *= rng.uniform(0.70, 0.90)
        elif zone_type == "industrial":
            factor *= rng.uniform(1.10, 1.30)  # fewer workers → more idle taxis

    return float(np.clip(factor, 0.05, 1.0))


def generate_synthetic_data(
    start: str = "2024-01-01",
    days: int = 90,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate a realistic zone-hour taxi availability dataset for 55 SG zones.

    Column: taxi_count = number of available (idle) taxis in zone at that hour.
    This is the direct supply signal from the LTA Taxi Availability API.

    The target variable (computed during training):
        supply_shortage = 1 if taxi_count(t+1) < taxi_count(t) * 0.6

    Patterns encoded:
    - Morning peak (07–09): CBD and transport hubs deplete fast
    - Evening peak (17–20): Residential zones deplete fast
    - Rain: all zones deplete faster (demand up, some supply drops)
    - SG public holidays 2024
    - Northeast Monsoon season (Nov–Jan): higher rainfall probability
    - Zone-type heterogeneity (CBD vs residential vs industrial)
    """
    rng = np.random.default_rng(seed)
    zones = get_zone_lookup()
    start_dt = pd.Timestamp(start, tz="Asia/Singapore")
    hours = pd.date_range(start_dt, periods=days * 24, freq="h", tz="Asia/Singapore")

    rows = []
    for zone_row in zones.itertuples():
        zone_type = getattr(zone_row, "zone_type", "mixed")
        base_count = _BASE_TAXI_COUNT.get(zone_type, 50)

        for ts in hours:
            hour     = ts.hour
            dow      = ts.dayofweek
            month    = ts.month
            date_str = ts.strftime("%Y-%m-%d")

            is_holiday        = date_str in _SG_HOLIDAYS
            is_school_holiday = _is_school_holiday(date_str)
            is_weekday        = dow < 5

            # Rainfall
            monsoon  = month in [11, 12, 1, 2]
            rain_p   = 0.45 if monsoon else 0.25
            if 14 <= hour <= 18:
                rain_p += 0.15
            is_raining   = bool(rng.random() < rain_p)
            weather_code = int(rng.choice(_WMO_RAIN if is_raining else
                                (_WMO_CLOUD if rng.random() < 0.3 else _WMO_CLEAR)))
            rainfall_mm  = float(rng.exponential(5.0) if is_raining else 0.0)

            # Temperature — SG mean ~28°C; varies with hour and season
            temp_base   = 28.0 + (1.5 if month in [4, 5] else -0.5 if month in [12, 1] else 0.0)
            temp_hour   = 2.0 * np.sin(np.pi * (hour - 6) / 12) if 6 <= hour <= 18 else -1.0
            temperature_c = round(float(np.clip(temp_base + temp_hour + rng.normal(0, 0.8), 23.0, 36.0)), 1)

            # Taxi count
            factor     = _availability_factor(hour, dow, is_raining, is_holiday, zone_type, rng)
            taxi_count = max(1, int(base_count * factor * rng.uniform(0.88, 1.12)))

            # Carpark available lots
            base_cp       = _BASE_CARPARK_LOTS.get(zone_type, 280)
            cp_factor     = _carpark_factor(hour, is_weekday)
            carpark_lots  = max(0, int(base_cp * cp_factor * rng.uniform(0.85, 1.15)))

            # Expressway congestion ratio
            congestion = _congestion_factor(hour, is_weekday, is_raining, zone_type, rng)

            rows.append({
                "zone_id":               int(zone_row.zone_id),
                "zone_name":             zone_row.zone_name,
                "region":                zone_row.region,
                "zone_type":             zone_type,
                "timestamp":             ts,
                "taxi_count":            taxi_count,
                "weather_code":          int(weather_code),
                "rainfall_mm":           round(rainfall_mm, 2),
                "is_raining":            is_raining,
                "is_holiday":            bool(is_holiday),
                "is_school_holiday":     bool(is_school_holiday),
                "temperature_c":         temperature_c,
                "carpark_available_lots": carpark_lots,
                "congestion_ratio":      congestion,
            })

    return pd.DataFrame(rows)


def load_raw_taxi_availability(path: str | Path) -> pd.DataFrame:
    """Load LTA DataMall taxi availability CSV (Latitude, Longitude, timestamp)."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower()
    rename = {"latitude": "lat", "longitude": "lon"}
    df = df.rename(columns=rename)
    if "timestamp" not in df.columns:
        df["timestamp"] = pd.Timestamp.now(tz="Asia/Singapore")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("Asia/Singapore")
    sg_bbox = {"lat": (1.15, 1.48), "lon": (103.60, 104.10)}
    df = df[
        df["lat"].between(*sg_bbox["lat"]) &
        df["lon"].between(*sg_bbox["lon"])
    ]
    return df.reset_index(drop=True)


def aggregate_to_zone_hour(
    taxi_df: pd.DataFrame,
    zone_lookup: pd.DataFrame,
) -> pd.DataFrame:
    """
    Bin taxi GPS pings to nearest planning area centroid, then count
    available taxis per zone per hour → taxi_count column.
    """
    from scipy.spatial import cKDTree

    zone_coords = zone_lookup[["lat", "lon"]].values
    tree = cKDTree(zone_coords)
    taxi_coords = taxi_df[["lat", "lon"]].values
    _, idx = tree.query(taxi_coords, k=1)

    taxi_df = taxi_df.copy()
    taxi_df["zone_id"]   = zone_lookup.iloc[idx]["zone_id"].values
    taxi_df["zone_name"] = zone_lookup.iloc[idx]["zone_name"].values
    taxi_df["region"]    = zone_lookup.iloc[idx]["region"].values
    taxi_df["zone_type"] = zone_lookup.iloc[idx]["zone_type"].values
    taxi_df["hour"]      = taxi_df["timestamp"].dt.floor("h")

    agg = (
        taxi_df.groupby(["zone_id", "zone_name", "region", "zone_type", "hour"])
        .size()
        .reset_index(name="taxi_count")
    )
    return agg.rename(columns={"hour": "timestamp"})


def load_sg_holidays(year: int = 2024) -> pd.DataFrame:
    """Load SG public holidays from cached file, or return hardcoded 2024 set."""
    path = Path("data/raw/sg_public_holidays.json")
    if path.exists():
        with open(path) as f:
            data = json.load(f)
        records = data.get("result", {}).get("records", data) if isinstance(data, dict) else data
        df = pd.DataFrame(records)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
        return df
    holidays = [
        {"date": "2024-01-01", "holiday_name": "New Year's Day"},
        {"date": "2024-02-10", "holiday_name": "Chinese New Year"},
        {"date": "2024-02-11", "holiday_name": "Chinese New Year"},
        {"date": "2024-03-29", "holiday_name": "Good Friday"},
        {"date": "2024-04-10", "holiday_name": "Hari Raya Puasa"},
        {"date": "2024-05-01", "holiday_name": "Labour Day"},
        {"date": "2024-05-23", "holiday_name": "Vesak Day"},
        {"date": "2024-06-17", "holiday_name": "Hari Raya Haji"},
        {"date": "2024-08-09", "holiday_name": "National Day"},
        {"date": "2024-10-31", "holiday_name": "Deepavali"},
        {"date": "2024-12-25", "holiday_name": "Christmas Day"},
    ]
    df = pd.DataFrame(holidays)
    df["date"] = pd.to_datetime(df["date"])
    return df
