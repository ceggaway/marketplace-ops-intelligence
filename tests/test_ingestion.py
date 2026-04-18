"""
Tests for backend/ingestion/loader.py

Covers:
- generate_synthetic_data() produces exactly 55 zones per hour
- generate_synthetic_data() all taxi_count values >= 1
- generate_synthetic_data() contains required columns
- generate_synthetic_data() is reproducible with same seed
- generate_synthetic_data() is different with different seed
- generate_synthetic_data() produces no NaN in core columns
- _is_school_holiday() returns True inside a known school holiday range
- _is_school_holiday() returns False outside all ranges
- load_sg_holidays() returns a DataFrame with at least 11 records
- get_zone_lookup() returns a DataFrame with exactly 55 rows
"""

import pandas as pd
import pytest

from backend.ingestion.loader import (
    generate_synthetic_data,
    get_zone_lookup,
    load_sg_holidays,
    _is_school_holiday,
)

REQUIRED_COLS = [
    "zone_id", "zone_name", "region", "zone_type", "timestamp",
    "taxi_count", "weather_code", "rainfall_mm", "is_raining",
    "is_holiday", "temperature_c", "carpark_available_lots", "congestion_ratio",
]


# ── generate_synthetic_data ───────────────────────────────────────────────────

def test_synthetic_data_has_55_zones():
    df = generate_synthetic_data(days=1, seed=42)
    assert df["zone_id"].nunique() == 55


def test_synthetic_data_rows_count():
    # 55 zones × 24 hours × days
    days = 2
    df = generate_synthetic_data(days=days, seed=42)
    assert len(df) == 55 * 24 * days


def test_synthetic_data_taxi_count_positive():
    df = generate_synthetic_data(days=1, seed=42)
    assert (df["taxi_count"] >= 1).all()


def test_synthetic_data_required_columns():
    df = generate_synthetic_data(days=1, seed=42)
    for col in REQUIRED_COLS:
        assert col in df.columns, f"Missing column: {col}"


def test_synthetic_data_reproducible():
    df1 = generate_synthetic_data(days=1, seed=7)
    df2 = generate_synthetic_data(days=1, seed=7)
    assert df1["taxi_count"].tolist() == df2["taxi_count"].tolist()


def test_synthetic_data_differs_with_different_seed():
    df1 = generate_synthetic_data(days=1, seed=1)
    df2 = generate_synthetic_data(days=1, seed=99)
    # At least some taxi counts should differ
    assert df1["taxi_count"].tolist() != df2["taxi_count"].tolist()


def test_synthetic_data_no_nan_in_core_columns():
    df = generate_synthetic_data(days=1, seed=42)
    for col in ["zone_id", "taxi_count", "rainfall_mm", "temperature_c"]:
        assert df[col].isna().sum() == 0, f"NaN found in {col}"


def test_synthetic_data_rainfall_non_negative():
    df = generate_synthetic_data(days=1, seed=42)
    assert (df["rainfall_mm"] >= 0).all()


def test_synthetic_data_temperature_in_range():
    df = generate_synthetic_data(days=1, seed=42)
    assert df["temperature_c"].between(23.0, 36.0).all()


def test_synthetic_data_congestion_non_negative():
    df = generate_synthetic_data(days=1, seed=42)
    assert (df["congestion_ratio"] >= 0).all()


# ── _is_school_holiday ────────────────────────────────────────────────────────

def test_is_school_holiday_true_inside_range():
    # 2024 March holidays: 2024-03-09 to 2024-03-17
    assert _is_school_holiday("2024-03-10") is True
    assert _is_school_holiday("2024-03-09") is True
    assert _is_school_holiday("2024-03-17") is True


def test_is_school_holiday_false_outside_range():
    assert _is_school_holiday("2024-03-08") is False  # day before
    assert _is_school_holiday("2024-03-18") is False  # day after
    assert _is_school_holiday("2024-04-15") is False  # mid-term


# ── load_sg_holidays ──────────────────────────────────────────────────────────

def test_load_sg_holidays_returns_dataframe():
    df = load_sg_holidays()
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 11  # at least 11 SG holidays in 2024


def test_load_sg_holidays_has_date_column():
    df = load_sg_holidays()
    assert "date" in df.columns


# ── get_zone_lookup ───────────────────────────────────────────────────────────

def test_get_zone_lookup_55_zones(tmp_path, monkeypatch):
    # Monkeypatch Path so it writes to tmp_path instead of data/raw/
    from pathlib import Path
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "raw").mkdir(parents=True)
    df = get_zone_lookup()
    assert len(df) == 55


def test_get_zone_lookup_has_zone_type(tmp_path, monkeypatch):
    from pathlib import Path
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "raw").mkdir(parents=True)
    df = get_zone_lookup()
    assert "zone_type" in df.columns
    assert df["zone_type"].notna().all()
