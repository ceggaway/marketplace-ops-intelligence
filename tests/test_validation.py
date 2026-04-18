"""
Tests for backend/validation/validator.py

Input schema: taxi availability supply signal
    zone_id, timestamp, taxi_count, weather_code, is_holiday

Covers:
- Clean rows pass through unchanged
- Null required fields are isolated
- Out-of-range taxi_count and rainfall_mm are caught
- Invalid WMO weather codes are caught
- Invalid zone_id (< 1) is caught
- Invalid region name is caught
- Invalid zone_type is caught
- Empty DataFrame is handled gracefully
- Mixed good/bad rows: only bad rows go to failed_df
"""

import pandas as pd
import pytest

from backend.validation.validator import validate, REQUIRED_COLUMNS, RANGE_RULES


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _good_row(**overrides) -> dict:
    """Return a valid row dict matching the taxi availability schema."""
    base = {
        "zone_id":      1,
        "zone_name":    "Orchard",
        "region":       "Central",
        "zone_type":    "CBD",
        "timestamp":    "2024-01-01 08:00:00",
        "taxi_count":   85,
        "weather_code": 0,
        "rainfall_mm":  0.0,
        "is_raining":   False,
        "is_holiday":   False,
    }
    base.update(overrides)
    return base


def _df(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ── Happy path ────────────────────────────────────────────────────────────────

def test_clean_rows_all_pass():
    df = _df(_good_row(), _good_row(zone_id=2, zone_name="Bedok", region="East",
                                    zone_type="residential"))
    clean, failed = validate(df)
    assert len(clean) == 2
    assert len(failed) == 0


def test_clean_df_has_no_failure_reason_column():
    clean, _ = validate(_df(_good_row()))
    assert "failure_reason" not in clean.columns


def test_clean_row_values_preserved():
    row = _good_row(taxi_count=42)
    clean, _ = validate(_df(row))
    assert clean.iloc[0]["taxi_count"] == 42


# ── Null checks ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("field", REQUIRED_COLUMNS)
def test_null_required_field_fails(field):
    row = _good_row(**{field: None})
    clean, failed = validate(_df(row))
    assert len(clean) == 0
    assert len(failed) == 1
    assert f"null_{field}" in failed.iloc[0]["failure_reason"]


# ── Range checks ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("field,bad_value", [
    ("taxi_count",  -1),
    ("taxi_count",  6_000),
    ("rainfall_mm", -1.0),
    ("rainfall_mm", 400.0),
])
def test_out_of_range_fails(field, bad_value):
    row = _good_row(**{field: bad_value})
    clean, failed = validate(_df(row))
    assert len(clean) == 0
    assert len(failed) == 1
    assert f"{field}_out_of_range" in failed.iloc[0]["failure_reason"]


def test_boundary_values_pass():
    """Exact boundary values (0, max) should pass."""
    row = _good_row(taxi_count=0, rainfall_mm=0.0)
    clean, failed = validate(_df(row))
    assert len(clean) == 1
    assert len(failed) == 0


def test_max_taxi_count_passes():
    row = _good_row(taxi_count=5000)
    clean, failed = validate(_df(row))
    assert len(clean) == 1
    assert len(failed) == 0


# ── Weather code checks ───────────────────────────────────────────────────────

@pytest.mark.parametrize("bad_code", [100, 999, -1, 50])
def test_invalid_weather_code_fails(bad_code):
    row = _good_row(weather_code=bad_code)
    clean, failed = validate(_df(row))
    assert len(clean) == 0
    assert "invalid_weather_code" in failed.iloc[0]["failure_reason"]


@pytest.mark.parametrize("good_code", [0, 1, 2, 3, 61, 63, 80, 95])
def test_valid_weather_codes_pass(good_code):
    row = _good_row(weather_code=good_code)
    clean, failed = validate(_df(row))
    assert len(clean) == 1
    assert len(failed) == 0


# ── Zone id checks ────────────────────────────────────────────────────────────

def test_zero_zone_id_fails():
    row = _good_row(zone_id=0)
    clean, failed = validate(_df(row))
    assert len(failed) == 1
    assert "invalid_zone_id" in failed.iloc[0]["failure_reason"]


def test_negative_zone_id_fails():
    _, failed = validate(_df(_good_row(zone_id=-5)))
    assert "invalid_zone_id" in failed.iloc[0]["failure_reason"]


def test_positive_zone_id_passes():
    clean, failed = validate(_df(_good_row(zone_id=55)))
    assert len(clean) == 1


# ── Region checks ─────────────────────────────────────────────────────────────

def test_invalid_region_fails():
    _, failed = validate(_df(_good_row(region="Atlantis")))
    assert "invalid_region" in failed.iloc[0]["failure_reason"]


@pytest.mark.parametrize("region", ["Central", "East", "West", "North", "North-East"])
def test_valid_regions_pass(region):
    clean, failed = validate(_df(_good_row(region=region)))
    assert len(clean) == 1


# ── Zone type checks ──────────────────────────────────────────────────────────

def test_invalid_zone_type_fails():
    _, failed = validate(_df(_good_row(zone_type="airport")))
    assert "invalid_zone_type" in failed.iloc[0]["failure_reason"]


@pytest.mark.parametrize("zone_type", ["CBD", "transport_hub", "residential", "mixed", "industrial"])
def test_valid_zone_types_pass(zone_type):
    clean, failed = validate(_df(_good_row(zone_type=zone_type)))
    assert len(clean) == 1


# ── Mixed rows ────────────────────────────────────────────────────────────────

def test_mixed_rows_split_correctly():
    good1 = _good_row(zone_id=1)
    bad1  = _good_row(zone_id=2, taxi_count=-1)
    good2 = _good_row(zone_id=3)
    bad2  = _good_row(zone_id=4, weather_code=999)

    clean, failed = validate(_df(good1, bad1, good2, bad2))
    assert len(clean) == 2
    assert len(failed) == 2
    assert set(clean["zone_id"]) == {1, 3}
    assert set(failed["zone_id"]) == {2, 4}


def test_failed_rows_have_failure_reason():
    _, failed = validate(_df(_good_row(taxi_count=-1)))
    assert "failure_reason" in failed.columns
    assert failed.iloc[0]["failure_reason"] != ""


# ── Edge cases ────────────────────────────────────────────────────────────────

def test_empty_dataframe():
    clean, failed = validate(pd.DataFrame())
    assert clean.empty
    assert failed.empty


def test_missing_schema_fails_all_rows():
    """If required columns are absent, every row goes to failed."""
    df = pd.DataFrame([{"foo": 1, "bar": 2}])
    clean, failed = validate(df)
    assert clean.empty
    assert len(failed) == 1
