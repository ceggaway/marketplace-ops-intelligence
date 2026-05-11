"""
Feature Engineering Pipeline
=============================
Transforms cleaned taxi availability records into model-ready features.
These combine supply-state signals with exogenous pressure proxies; they do
not directly observe rider demand.

Input columns expected:
    zone_id, zone_name, region, zone_type, timestamp,
    taxi_count, weather_code, rainfall_mm, is_raining, is_holiday

Features produced:
    Time:
        hour_of_day, day_of_week, month, is_weekend, is_peak_hour,
        hour_sin, hour_cos, dow_sin, dow_cos

    Supply state:
        taxi_count, taxi_lag_1h, taxi_lag_24h, taxi_lag_168h,
        taxi_rolling_3h, taxi_rolling_6h

    Supply depletion:
        depletion_rate_1h   – (taxi_lag_1h  - taxi_count) / (taxi_lag_1h  + 1)
        depletion_rate_3h   – (taxi_rolling_3h - taxi_count) / (taxi_rolling_3h + 1)
        supply_vs_yesterday – taxi_count / (taxi_lag_24h + 1)

    Weather / pressure proxies:
        rainfall_mm, is_raining, rain_intensity (0/1/2/3),
        congestion_ratio, train_disruption_flag

    Calendar:
        is_holiday, is_eve_holiday

    Zone identity:
        zone_type_encoded   – CBD=0, transport_hub=1, residential=2, mixed=3, industrial=4

Output: feature_df with all above columns + zone_id, zone_name, region, zone_type, timestamp
"""

from pathlib import Path

import numpy as np
import pandas as pd

from backend.ingestion.train_disruptions import load_train_disruption_flags
from backend.paths import processed_dir

_EVENTS_CSV = Path(__file__).resolve().parents[2] / "data" / "raw" / "sg_events.csv"

# attendance_bucket → numeric crowd intensity weight used as a feature
_ATTENDANCE_WEIGHT = {"large": 1.0, "medium": 0.6, "small": 0.3}

_PEAK_MORNING = set(range(7, 10))
_PEAK_EVENING = set(range(17, 21))
_RAIN_WMO     = {61, 63, 65, 67, 80, 81, 82, 95, 96, 99}

_ZONE_TYPE_ENCODING: dict[str, int] = {
    "CBD":           0,
    "transport_hub": 1,
    "residential":   2,
    "mixed":         3,
    "industrial":    4,
}

# SG public holidays 2024–2026 for is_eve_holiday computation.
# Source: Ministry of Manpower (MOM) — https://www.mom.gov.sg/employment-practices/public-holidays
# Islamic holidays and Deepavali are approximate; verify against official MOM announcements.
_SG_HOLIDAY_DATES = {
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
# Source: Ministry of Education (MOE) — https://www.moe.gov.sg/calendar
_SG_SCHOOL_HOLIDAY_RANGES: list[tuple[str, str]] = [
    # 2024
    ("2024-03-09", "2024-03-17"),
    ("2024-05-25", "2024-06-23"),
    ("2024-09-07", "2024-09-15"),
    ("2024-11-16", "2024-12-31"),
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


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Run full feature engineering pipeline. Returns feature DataFrame."""
    df = df.copy()
    df = _ensure_timestamp(df)
    df = _extract_time_features(df)
    df = _derive_weather_flags(df)
    df = _passthrough_external_features(df)
    df = _build_supply_lags(df)
    df = _build_depletion_features(df)
    df = _build_exp6_features(df)
    df = _encode_zone_type(df)
    df = _build_calendar_flags(df)
    df = _build_event_features(df)
    # Drop rows where 1h lag is unavailable (first row per zone)
    df = df.dropna(subset=["taxi_lag_1h"]).reset_index(drop=True)
    return df


def _ensure_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize("Asia/Singapore")
    else:
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Singapore")
    return df


def _extract_time_features(df: pd.DataFrame) -> pd.DataFrame:
    ts = df["timestamp"]
    df["hour_of_day"]  = ts.dt.hour.astype(np.int8)
    df["day_of_week"]  = ts.dt.dayofweek.astype(np.int8)   # 0=Mon
    df["month"]        = ts.dt.month.astype(np.int8)
    df["is_weekend"]   = (df["day_of_week"] >= 5).astype(bool)
    df["is_peak_hour"] = ts.dt.hour.isin(_PEAK_MORNING | _PEAK_EVENING).astype(bool)
    df["hour_sin"]     = np.sin(2 * np.pi * df["hour_of_day"] / 24).astype(np.float32)
    df["hour_cos"]     = np.cos(2 * np.pi * df["hour_of_day"] / 24).astype(np.float32)
    df["dow_sin"]      = np.sin(2 * np.pi * df["day_of_week"] / 7).astype(np.float32)
    df["dow_cos"]      = np.cos(2 * np.pi * df["day_of_week"] / 7).astype(np.float32)
    df["month_sin"]    = np.sin(2 * np.pi * df["month"] / 12).astype(np.float32)
    df["month_cos"]    = np.cos(2 * np.pi * df["month"] / 12).astype(np.float32)
    # Minutes until next peak period (07:00 or 17:00) — captures pre-peak demand build-up
    df["hours_to_peak"] = df["hour_of_day"].map(
        lambda h: min((p - h) % 24 for p in [7, 17])
    ).astype(np.int8)
    return df


def _derive_weather_flags(df: pd.DataFrame) -> pd.DataFrame:
    if "is_raining" not in df.columns:
        if "weather_code" in df.columns:
            df["is_raining"] = df["weather_code"].isin(_RAIN_WMO).astype(bool)
        else:
            df["is_raining"] = False
    if "rainfall_mm" not in df.columns:
        df["rainfall_mm"] = 0.0

    # Rain intensity: 0=none, 1=light(<2mm), 2=moderate(2-10mm), 3=heavy(>10mm)
    df["rain_intensity"] = pd.cut(
        df["rainfall_mm"],
        bins=[-0.1, 0.0, 2.0, 10.0, float("inf")],
        labels=[0, 1, 2, 3],
    ).astype(np.int8)
    # Square-root transform stabilises the heavy-rainfall tail distribution
    df["rainfall_sqrt"] = np.sqrt(df["rainfall_mm"]).astype(np.float32)
    return df


def _build_supply_lags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["zone_id", "timestamp"]).reset_index(drop=True)
    grp = df.groupby("zone_id")["taxi_count"]

    df["taxi_lag_1h"]   = grp.shift(1)
    df["taxi_lag_24h"]  = grp.shift(24)
    df["taxi_lag_168h"] = grp.shift(168)   # same hour last week

    # Rolling means (shift(1) ensures no leakage of current value)
    for window, col in [(3, "taxi_rolling_3h"), (6, "taxi_rolling_6h")]:
        rolled = (
            grp.transform(lambda s: s.shift(1).rolling(window, min_periods=1).mean())
        )
        zone_means = grp.transform("mean")
        df[col] = rolled.fillna(zone_means)

    # Fill lags with the lag's zone mean, falling back to the zone's supply
    # mean when the whole lag is unavailable, such as taxi_lag_168h on a
    # short local dataset.
    for col in ["taxi_lag_1h", "taxi_lag_24h", "taxi_lag_168h"]:
        lag_means = df.groupby("zone_id")[col].transform("mean")
        supply_means = df.groupby("zone_id")["taxi_count"].transform("mean")
        df[col] = df[col].fillna(lag_means).fillna(supply_means)

    return df


def _build_depletion_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Depletion rate: how much supply has been absorbed relative to the prior level.
    Positive = supply dropped (taxis being hired). Negative = supply recovered.

    depletion_rate_1h  = (taxi_lag_1h  - taxi_count) / (taxi_lag_1h  + 1)
    depletion_rate_3h  = (taxi_rolling_3h - taxi_count) / (taxi_rolling_3h + 1)
    supply_vs_yesterday = taxi_count / (taxi_lag_24h + 1)
        > 1.0 → today has more taxis than same hour yesterday (healthier)
        < 1.0 → today is worse than yesterday
    """
    df["depletion_rate_1h"] = (
        (df["taxi_lag_1h"] - df["taxi_count"]) / (df["taxi_lag_1h"] + 1)
    ).clip(-2, 2).astype(np.float32)

    df["depletion_rate_3h"] = (
        (df["taxi_rolling_3h"] - df["taxi_count"]) / (df["taxi_rolling_3h"] + 1)
    ).clip(-2, 2).astype(np.float32)

    df["supply_vs_yesterday"] = (
        df["taxi_count"] / (df["taxi_lag_24h"] + 1)
    ).clip(0, 5).astype(np.float32)

    return df


def _encode_zone_type(df: pd.DataFrame) -> pd.DataFrame:
    if "zone_type" in df.columns:
        df["zone_type_encoded"] = (
            df["zone_type"].map(_ZONE_TYPE_ENCODING).fillna(3).astype(np.int8)
        )
    else:
        df["zone_type_encoded"] = np.int8(3)  # default: mixed
    return df


def _build_calendar_flags(df: pd.DataFrame) -> pd.DataFrame:
    if "is_holiday" not in df.columns:
        df["is_holiday"] = False

    # Eve of holiday: demand patterns shift the day before
    tomorrow_strs = (df["timestamp"] + pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
    df["is_eve_holiday"] = tomorrow_strs.isin(_SG_HOLIDAY_DATES).astype(bool)

    # School holidays: residential zone demand shifts (less commuter traffic,
    # more leisure/family trips). Vectorised range check.
    if "is_school_holiday" not in df.columns:
        ts = df["timestamp"]
        is_school_hol = pd.Series(False, index=df.index)
        for start_str, end_str in _SG_SCHOOL_HOLIDAY_RANGES:
            start_ts = pd.Timestamp(start_str, tz="Asia/Singapore")
            end_ts   = pd.Timestamp(end_str,   tz="Asia/Singapore") + pd.Timedelta(days=1)
            is_school_hol |= (ts >= start_ts) & (ts < end_ts)
        df["is_school_holiday"] = is_school_hol.astype(bool)

    return df


def _build_exp6_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Exp 5/6 improvement signals: depletion acceleration, absolute drops, low-count flags.
    Must be called after _build_supply_lags and _build_depletion_features.
    """
    # Rate-of-change of depletion (acceleration) — catches sudden collapses missed by level signal
    depl_grp = df.groupby("zone_id")["depletion_rate_1h"]
    df["delta_depletion_1h"] = (
        df["depletion_rate_1h"] - depl_grp.shift(1)
    ).fillna(0).clip(-2, 2).astype(np.float32)

    # Absolute taxi count drop (not normalised) — complements the relative depletion rate
    df["taxi_abs_drop_1h"] = (
        df["taxi_lag_1h"] - df["taxi_count"]
    ).clip(-200, 200).astype(np.float32)

    # Flag zones already in the bottom 20th percentile of supply — pre-depleted state
    zone_p20 = df.groupby("zone_id")["taxi_count"].transform(lambda s: s.quantile(0.20))
    df["is_low_count"] = (df["taxi_count"] <= zone_p20).astype(np.int8)

    # Carpark shortage flag: lots below 25th percentile signals elevated taxi demand
    if "carpark_available_lots" in df.columns:
        zone_cp_p25 = df.groupby("zone_id")["carpark_available_lots"].transform(
            lambda s: s.quantile(0.25)
        )
        df["carpark_shortage_flag"] = (df["carpark_available_lots"] <= zone_cp_p25).astype(np.int8)
    else:
        df["carpark_shortage_flag"] = np.int8(0)

    return df


def _passthrough_external_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pass through optional external feature columns if present in the input.
    These are sourced from LTA DataMall and Open-Meteo; when absent (e.g.
    during synthetic-only runs), the columns are filled with neutral defaults
    so downstream model scoring always receives a consistent feature set.

    External columns handled:
        temperature_c          – Open-Meteo hourly temperature (°C)
        carpark_available_lots – LTA Carpark Availability (integer count)
        congestion_ratio       – LTA Estimated Travel Times (ratio ≥ 0)
        train_disruption_flag  – rail disruption indicator (0/1)
    """
    if "train_disruption_flag" not in df.columns:
        disruption = load_train_disruption_flags(df.get("timestamp"))
        if not disruption.empty and "train_disruption_flag" in disruption.columns and len(disruption) == len(df):
            df["train_disruption_flag"] = disruption["train_disruption_flag"].to_numpy()
        else:
            df["train_disruption_flag"] = 0

    defaults = {
        "temperature_c":           28.0,   # SG mean temperature
        "carpark_available_lots":  0,
        "congestion_ratio":        1.0,    # neutral / free-flow
        "train_disruption_flag":   0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def _build_event_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add major-event features from the static event calendar (data/raw/sg_events.csv).

    Features added:
        is_major_event       – binary: 1 if the zone is affected by an active event
        event_crowdedness    – float [0, 1]: attendance weight of the active event
                               (large=1.0, medium=0.6, small=0.3); 0 when no event

    Events are zone-name matched against the comma-separated `affected_zones`
    column in the CSV. If the CSV is absent or malformed, both features default
    to 0 so the pipeline degrades gracefully.
    """
    df["is_major_event"]    = np.int8(0)
    df["event_crowdedness"] = np.float32(0.0)

    if not _EVENTS_CSV.exists():
        return df

    try:
        events = pd.read_csv(_EVENTS_CSV)
    except Exception:
        return df

    required = {"name", "date", "start_hour", "end_hour", "attendance_bucket", "affected_zones"}
    if not required.issubset(events.columns):
        return df

    # Build a lookup: (date_str, hour) → max crowdedness weight for each zone name
    # We use the zone_name column to match, case-insensitive.
    zone_event_map: dict[tuple[str, str, int], float] = {}
    for _, ev in events.iterrows():
        date_str = str(ev["date"]).strip()
        start_h  = int(ev["start_hour"])
        end_h    = int(ev["end_hour"])
        weight   = float(_ATTENDANCE_WEIGHT.get(str(ev["attendance_bucket"]).strip(), 0.3))
        zones    = [z.strip().lower() for z in str(ev["affected_zones"]).split(",")]

        hours = list(range(start_h, 24)) + list(range(0, end_h + 1)) if end_h < start_h else list(range(start_h, end_h + 1))
        for h in hours:
            for zone in zones:
                key = (date_str, zone, h)
                zone_event_map[key] = max(zone_event_map.get(key, 0.0), weight)

    if not zone_event_map:
        return df

    ts       = df["timestamp"]
    date_col = ts.dt.strftime("%Y-%m-%d")
    hour_col = ts.dt.hour
    zone_col = df["zone_name"].str.strip().str.lower() if "zone_name" in df.columns else pd.Series("", index=df.index)

    crowdedness = pd.Series(0.0, index=df.index, dtype=np.float32)
    for i, (d, h, z) in enumerate(zip(date_col, hour_col, zone_col)):
        w = zone_event_map.get((d, z, h), 0.0)
        if w > 0.0:
            crowdedness.iloc[i] = w

    df["is_major_event"]    = (crowdedness > 0).astype(np.int8)
    df["event_crowdedness"] = crowdedness
    return df


# =============================================================================
# H3 HEX-LEVEL FEATURE ENGINEERING
# =============================================================================
# These functions mirror the zone-level pipeline but group by h3_cell instead
# of zone_id.  The zone pipeline is completely unchanged — H3 is opt-in only.

def build_h3_features(
    df: pd.DataFrame,
    spatial_config: dict | None = None,
) -> pd.DataFrame:
    """
    Build feature engineering for H3-cell-level aggregated taxi supply data.

    Expected input columns: timestamp, h3_cell, taxi_count
    Optional input columns (merged upstream): rainfall_mm, is_raining,
        congestion_ratio, parent_zone

    Returns a feature DataFrame with the same core column names as the zone
    pipeline where applicable, plus h3_cell-specific additions:
        sparse_cell_flag, baseline_supply, supply_gap, demand_pressure_proxy
    """
    sc = spatial_config or {}
    min_taxis_per_cell = int(sc.get("min_taxis_per_cell", 3))

    df = df.copy()
    df = _ensure_timestamp(df)
    df = _extract_time_features(df)
    df = _derive_weather_flags(df)
    df = _passthrough_h3_external(df)
    df = _build_h3_supply_lags(df)
    df = _build_depletion_features(df)          # same math, different group key
    df = _build_h3_exp6_features(df)
    df = _build_calendar_flags(df)
    df = _build_h3_sparse_flag(df, min_taxis_per_cell)
    df = _build_h3_baseline_supply(df)
    df = _build_h3_demand_pressure_proxy(df)
    df["imbalance_score"] = (
        df["supply_gap"] * 0.6 + df["demand_pressure_proxy"] * 0.4
    ).clip(0, 1).round(4).astype(np.float32)

    df = df.dropna(subset=["taxi_lag_1h"]).reset_index(drop=True)
    return df


def save_h3_features(df: pd.DataFrame) -> None:
    """Write H3 features to data/processed/h3_supply_features.csv."""
    out = processed_dir()
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / "h3_supply_features.csv", index=False)


# -- H3-specific helpers -----------------------------------------------------

def _passthrough_h3_external(df: pd.DataFrame) -> pd.DataFrame:
    """Fill optional external columns with neutral defaults when absent."""
    defaults: dict[str, object] = {
        "rainfall_mm":           0.0,
        "is_raining":            False,
        "congestion_ratio":      1.0,
        "train_disruption_flag": 0,
        "temperature_c":         28.0,
    }
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
    return df


def _build_h3_supply_lags(df: pd.DataFrame) -> pd.DataFrame:
    """Supply lags grouped by h3_cell instead of zone_id."""
    df = df.sort_values(["h3_cell", "timestamp"]).reset_index(drop=True)
    grp = df.groupby("h3_cell")["taxi_count"]

    df["taxi_lag_1h"]  = grp.shift(1)
    df["taxi_lag_24h"] = grp.shift(24)

    for window, col in [(3, "taxi_rolling_3h"), (6, "taxi_rolling_6h")]:
        df[col] = grp.transform(
            lambda s: s.shift(1).rolling(window, min_periods=1).mean()
        )

    for col in ["taxi_lag_1h", "taxi_lag_24h"]:
        cell_means   = df.groupby("h3_cell")[col].transform("mean")
        supply_means = df.groupby("h3_cell")["taxi_count"].transform("mean")
        df[col] = df[col].fillna(cell_means).fillna(supply_means)

    df["taxi_rolling_3h"] = df["taxi_rolling_3h"].fillna(
        df.groupby("h3_cell")["taxi_count"].transform("mean")
    )
    df["taxi_rolling_6h"] = df["taxi_rolling_6h"].fillna(
        df.groupby("h3_cell")["taxi_count"].transform("mean")
    )
    return df


def _build_h3_exp6_features(df: pd.DataFrame) -> pd.DataFrame:
    """Depletion acceleration and absolute-drop signals for H3 cells."""
    depl_grp = df.groupby("h3_cell")["depletion_rate_1h"]
    df["delta_depletion_1h"] = (
        df["depletion_rate_1h"] - depl_grp.shift(1)
    ).fillna(0).clip(-2, 2).astype(np.float32)

    df["taxi_abs_drop_1h"] = (
        df["taxi_lag_1h"] - df["taxi_count"]
    ).clip(-200, 200).astype(np.float32)

    # is_low_count: relative to each cell's 20th-percentile, not city-wide
    cell_p20 = df.groupby("h3_cell")["taxi_count"].transform(
        lambda s: s.quantile(0.20)
    )
    df["is_low_count"] = (df["taxi_count"] <= cell_p20).astype(np.int8)
    return df


def _build_h3_sparse_flag(df: pd.DataFrame, min_taxis: int) -> pd.DataFrame:
    """
    Mark H3 cells whose median taxi_count is below min_taxis_per_cell.

    Sparse cells have too few taxis for reliable depletion-rate estimation.
    Downstream consumers should treat their predictions as directional only.
    """
    cell_median = df.groupby("h3_cell")["taxi_count"].transform("median")
    df["sparse_cell_flag"] = (cell_median < min_taxis).astype(bool)
    return df


def _build_h3_baseline_supply(df: pd.DataFrame) -> pd.DataFrame:
    """
    Baseline supply using a three-tier fallback chain per H3 cell.

    Tier 1: median taxi_count for same (h3_cell, hour_of_day, day_of_week)
    Tier 2: median taxi_count for same (h3_cell, hour_of_day)
    Tier 3: overall median per h3_cell

    supply_gap = max(0, 1 - taxi_count / (baseline_supply + 1))
    capped at [0, 1] to keep the signal bounded.
    """
    # Tier 1
    t1 = df.groupby(["h3_cell", "hour_of_day", "day_of_week"])["taxi_count"]
    df["_base_t1"]  = t1.transform("median")
    df["_cnt_t1"]   = t1.transform("count")

    # Tier 2
    t2 = df.groupby(["h3_cell", "hour_of_day"])["taxi_count"]
    df["_base_t2"]  = t2.transform("median")
    df["_cnt_t2"]   = t2.transform("count")

    # Tier 3
    df["_base_t3"] = df.groupby("h3_cell")["taxi_count"].transform("median")

    # Apply cascade
    df["baseline_supply"] = df["_base_t1"]
    thin1 = df["_cnt_t1"] < 2
    df.loc[thin1, "baseline_supply"] = df.loc[thin1, "_base_t2"]
    thin2 = thin1 & (df["_cnt_t2"] < 2)
    df.loc[thin2, "baseline_supply"] = df.loc[thin2, "_base_t3"]

    df["baseline_supply"] = (
        df["baseline_supply"].fillna(df["taxi_count"]).clip(lower=1).astype(np.float32)
    )
    df = df.drop(columns=["_base_t1", "_base_t2", "_base_t3", "_cnt_t1", "_cnt_t2"])

    df["supply_gap"] = (
        (df["baseline_supply"] - df["taxi_count"]) / (df["baseline_supply"] + 1)
    ).clip(0, 1).round(4).astype(np.float32)
    return df


def _build_h3_demand_pressure_proxy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lightweight demand-pressure proxy for H3 cells.

    Uses time-of-day, rainfall, and weekend signals only — without zone
    commercial type or major-event data, which are not available at hex level.
    """
    peak    = df.get("is_peak_hour", pd.Series(False, index=df.index)).astype(float)
    rain_mm = df.get("rainfall_mm", pd.Series(0.0, index=df.index)).clip(0, 50)
    rain    = rain_mm / 50.0
    weekend = df.get("is_weekend", pd.Series(False, index=df.index)).astype(float) * 0.2
    holiday = df.get("is_holiday", pd.Series(False, index=df.index)).astype(float) * 0.15
    df["demand_pressure_proxy"] = (
        (peak * 0.40) + (rain * 0.35) + weekend + holiday
    ).clip(0, 1).round(4).astype(np.float32)
    return df
