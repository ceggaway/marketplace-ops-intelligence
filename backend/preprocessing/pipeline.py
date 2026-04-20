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

import numpy as np
import pandas as pd

from backend.ingestion.train_disruptions import load_train_disruption_flags

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

    # Fill lags with zone mean where unavailable (beginning of data)
    for col in ["taxi_lag_1h", "taxi_lag_24h", "taxi_lag_168h"]:
        zone_means = df.groupby("zone_id")[col].transform("mean")
        df[col] = df[col].fillna(zone_means)

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
        train_disruption_flag  – placeholder rail disruption indicator (0/1)
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
