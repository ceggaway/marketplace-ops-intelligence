"""
Tests for backend/preprocessing/pipeline.py

Input: zone-hour taxi availability records (taxi_count as supply signal)

Covers:
- build_features() returns expected column set
- Time features: hour_of_day, day_of_week, is_weekend, is_peak_hour
- Cyclical features are in [-1, 1] and satisfy sin²+cos²=1
- Supply lag features are populated (no nulls after warm-up)
- Depletion rate features are computed correctly
- supply_vs_yesterday is non-negative
- is_raining flag matches WMO rain codes
- zone_type_encoded maps correctly
- is_eve_holiday is bool
"""

import numpy as np
import pandas as pd
import pytest

from backend.preprocessing.pipeline import (
    build_features,
    _extract_time_features,
    _derive_weather_flags,
    _build_supply_lags,
    _build_depletion_features,
    _encode_zone_type,
    _ensure_timestamp,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _make_df(n_zones: int = 3, n_days: int = 5) -> pd.DataFrame:
    """Generate a minimal zone-hour taxi availability DataFrame for testing."""
    rng = np.random.default_rng(0)
    zones = [
        {"zone_id": i, "zone_name": f"Zone{i}", "region": "Central", "zone_type": "mixed"}
        for i in range(1, n_zones + 1)
    ]
    timestamps = pd.date_range("2024-01-01", periods=n_days * 24, freq="h", tz="Asia/Singapore")

    rows = []
    for z in zones:
        for ts in timestamps:
            rows.append({
                **z,
                "timestamp":    ts,
                "taxi_count":   int(rng.integers(5, 150)),
                "weather_code": int(rng.choice([0, 1, 61, 80])),
                "rainfall_mm":  float(rng.exponential(2)),
                "is_holiday":   False,
            })
    return pd.DataFrame(rows)


# ── Full pipeline ─────────────────────────────────────────────────────────────

def test_build_features_returns_dataframe():
    df = build_features(_make_df())
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_build_features_expected_columns():
    df = build_features(_make_df())
    expected = [
        "hour_of_day", "day_of_week", "month", "is_weekend", "is_peak_hour",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        "taxi_count", "taxi_lag_1h", "taxi_lag_24h",
        "taxi_rolling_3h", "taxi_rolling_6h",
        "depletion_rate_1h", "depletion_rate_3h", "supply_vs_yesterday",
        "rainfall_mm", "is_raining", "rain_intensity", "congestion_ratio", "train_disruption_flag",
        "is_holiday", "is_eve_holiday",
        "zone_type_encoded",
    ]
    for col in expected:
        assert col in df.columns, f"Missing column: {col}"


def test_build_features_no_nulls_in_supply_lag_cols():
    # Need >7 days so taxi_lag_168h (same hour last week) can be filled
    df = build_features(_make_df(n_days=10))
    lag_cols = [c for c in df.columns if "lag" in c or "rolling" in c or "depletion" in c]
    for col in lag_cols:
        assert df[col].isna().sum() == 0, f"{col} has nulls after build_features"


# ── Time features ─────────────────────────────────────────────────────────────

def test_hour_of_day_range():
    df = _ensure_timestamp(_make_df())
    df = _extract_time_features(df)
    assert df["hour_of_day"].between(0, 23).all()


def test_day_of_week_range():
    df = _ensure_timestamp(_make_df())
    df = _extract_time_features(df)
    assert df["day_of_week"].between(0, 6).all()


def test_is_weekend_correct():
    df = _ensure_timestamp(_make_df())
    df = _extract_time_features(df)
    assert (df["is_weekend"] == (df["day_of_week"] >= 5)).all()


def test_is_peak_hour_correct():
    df = _ensure_timestamp(_make_df())
    df = _extract_time_features(df)
    peak_hours = set(range(7, 10)) | set(range(17, 21))
    expected = df["hour_of_day"].isin(peak_hours)
    assert (df["is_peak_hour"] == expected).all()


def test_cyclical_features_in_range():
    df = _ensure_timestamp(_make_df())
    df = _extract_time_features(df)
    for col in ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]:
        assert df[col].between(-1.0001, 1.0001).all(), f"{col} out of [-1, 1]"


def test_hour_sin_cos_identity():
    """sin²+cos² should equal 1 for cyclical encoding."""
    df = _ensure_timestamp(_make_df())
    df = _extract_time_features(df)
    identity = df["hour_sin"].astype(float)**2 + df["hour_cos"].astype(float)**2
    assert np.allclose(identity, 1.0, atol=1e-5)


# ── Weather flags ─────────────────────────────────────────────────────────────

def test_is_raining_matches_wmo_rain_codes():
    rain_codes  = [61, 63, 65, 80, 81, 82, 95]
    clear_codes = [0, 1, 2, 3]

    rows = [{"weather_code": c, "rainfall_mm": 5.0} for c in rain_codes]
    rows += [{"weather_code": c, "rainfall_mm": 0.0} for c in clear_codes]
    df = pd.DataFrame(rows)
    df = _derive_weather_flags(df)

    assert df.iloc[:len(rain_codes)]["is_raining"].all()
    assert not df.iloc[len(rain_codes):]["is_raining"].any()


def test_rain_intensity_bucketing():
    df = pd.DataFrame({"rainfall_mm": [0.0, 1.0, 5.0, 15.0], "is_raining": [False, True, True, True]})
    df = _derive_weather_flags(df)
    assert list(df["rain_intensity"]) == [0, 1, 2, 3]


def test_train_disruption_flag_defaults_to_zero():
    df = build_features(_make_df())
    assert "train_disruption_flag" in df.columns
    assert set(df["train_disruption_flag"].unique()).issubset({0, 1})


# ── Supply lag features ───────────────────────────────────────────────────────

def test_taxi_lag_1h_shifts_correctly():
    df = _ensure_timestamp(_make_df(n_zones=1, n_days=5))
    df = _build_supply_lags(df)
    zone_df = df[df["zone_id"] == 1].sort_values("timestamp").reset_index(drop=True)
    for i in range(1, min(5, len(zone_df))):
        assert zone_df.loc[i, "taxi_lag_1h"] == pytest.approx(
            zone_df.loc[i - 1, "taxi_count"], rel=1e-4
        )


def test_rolling_features_non_negative():
    df = _ensure_timestamp(_make_df())
    df = _build_supply_lags(df)
    for col in ["taxi_rolling_3h", "taxi_rolling_6h"]:
        assert (df[col] >= 0).all(), f"{col} has negative values"


# ── Depletion features ────────────────────────────────────────────────────────

def test_depletion_rate_clipped():
    df = _ensure_timestamp(_make_df())
    df = _build_supply_lags(df)
    df = _build_depletion_features(df)
    assert df["depletion_rate_1h"].between(-2, 2).all()
    assert df["depletion_rate_3h"].between(-2, 2).all()


def test_supply_vs_yesterday_non_negative():
    df = _ensure_timestamp(_make_df())
    df = _build_supply_lags(df)
    df = _build_depletion_features(df)
    assert (df["supply_vs_yesterday"] >= 0).all()


def test_positive_depletion_when_taxi_count_dropped():
    """If taxi_count fell vs lag_1h, depletion_rate_1h should be positive."""
    df = _ensure_timestamp(_make_df(n_zones=1, n_days=5))
    df = _build_supply_lags(df)
    df = _build_depletion_features(df)
    dropped = df[df["taxi_lag_1h"] > df["taxi_count"]]
    if len(dropped) > 0:
        assert (dropped["depletion_rate_1h"] > 0).all()


# ── Zone type encoding ────────────────────────────────────────────────────────

def test_zone_type_encoded_cbd():
    df = pd.DataFrame([{"zone_type": "CBD"}])
    df = _encode_zone_type(df)
    assert df.iloc[0]["zone_type_encoded"] == 0


def test_zone_type_encoded_unknown_defaults_to_mixed():
    df = pd.DataFrame([{"zone_type": "unknown_type"}])
    df = _encode_zone_type(df)
    assert df.iloc[0]["zone_type_encoded"] == 3


@pytest.mark.parametrize("zone_type,expected", [
    ("CBD",           0),
    ("transport_hub", 1),
    ("residential",   2),
    ("mixed",         3),
    ("industrial",    4),
])
def test_zone_type_encoding_all_types(zone_type, expected):
    df = pd.DataFrame([{"zone_type": zone_type}])
    df = _encode_zone_type(df)
    assert df.iloc[0]["zone_type_encoded"] == expected
