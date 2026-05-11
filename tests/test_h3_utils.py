"""
Unit tests for backend/spatial/h3_utils.py and related H3 pipeline logic.

Tests are grouped by function and cover:
- latlon_to_h3: valid coords, outside-SG bbox, invalid types
- add_h3_index: happy path, None handling
- aggregate_taxis_by_h3: grouping by (timestamp, cell)
- h3_to_boundary: returns closed polygon, handles bad cell
- get_h3_neighbors: excludes self, sorted output
- h3_cell_centroid: round-trips lat/lon
- sparse_cell_flag: median-threshold logic
- baseline supply fallback: 3-tier cascade
- select_h3_action: action selection for each severity bucket
- H3 scoring output schema: run_h3_batch columns present
"""

import math
import pytest
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Import guards — skip whole file gracefully if h3 not installed
# ---------------------------------------------------------------------------

try:
    from backend.spatial.h3_utils import (
        latlon_to_h3,
        add_h3_index,
        aggregate_taxis_by_h3,
        h3_to_boundary,
        get_h3_neighbors,
        h3_cell_centroid,
        compute_h3_neighbor_surplus,
        select_h3_action,
    )
    H3_AVAILABLE = True
except ImportError:
    H3_AVAILABLE = False

pytestmark = pytest.mark.skipif(not H3_AVAILABLE, reason="h3 package not installed")

# Singapore centroid — guaranteed inside the SG bbox
SG_LAT, SG_LON = 1.3521, 103.8198
RES = 8


# ---------------------------------------------------------------------------
# latlon_to_h3
# ---------------------------------------------------------------------------

def test_latlon_to_h3_returns_string():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    assert isinstance(cell, str)
    assert len(cell) > 0


def test_latlon_to_h3_outside_sg_raises():
    # Tokyo is outside the SG bbox
    with pytest.raises(ValueError, match="outside"):
        latlon_to_h3(35.68, 139.69, RES)


def test_latlon_to_h3_consistency():
    c1 = latlon_to_h3(SG_LAT, SG_LON, RES)
    c2 = latlon_to_h3(SG_LAT, SG_LON, RES)
    assert c1 == c2


# ---------------------------------------------------------------------------
# add_h3_index
# ---------------------------------------------------------------------------

def _make_df(lats, lons):
    return pd.DataFrame({"lat": lats, "lon": lons})


def test_add_h3_index_adds_column():
    df = _make_df([SG_LAT], [SG_LON])
    out = add_h3_index(df, "lat", "lon", RES)
    assert "h3_cell" in out.columns
    assert out["h3_cell"].iloc[0] is not None


def test_add_h3_index_none_for_invalid():
    # NaN coordinates should yield None or NaN (implementation may use either)
    df = _make_df([float("nan"), SG_LAT], [float("nan"), SG_LON])
    out = add_h3_index(df, "lat", "lon", RES)
    first = out["h3_cell"].iloc[0]
    assert first is None or (isinstance(first, float) and math.isnan(first))
    assert out["h3_cell"].iloc[1] is not None


def test_add_h3_index_preserves_other_columns():
    df = _make_df([SG_LAT], [SG_LON])
    df["taxi_id"] = "abc"
    out = add_h3_index(df, "lat", "lon", RES)
    assert "taxi_id" in out.columns


# ---------------------------------------------------------------------------
# aggregate_taxis_by_h3
# ---------------------------------------------------------------------------

def test_aggregate_taxis_by_h3_groups_correctly():
    ts = "2024-01-01T08:00:00"
    df = pd.DataFrame({
        "timestamp": [ts, ts, ts],
        "lat": [SG_LAT, SG_LAT + 0.0001, 1.28],  # first two in same cell, third may differ
        "lon": [SG_LON, SG_LON + 0.0001, 103.85],
    })
    agg = aggregate_taxis_by_h3(df, "timestamp", "lat", "lon", RES)
    assert "h3_cell" in agg.columns
    assert "taxi_count" in agg.columns
    assert "timestamp" in agg.columns
    # total count must equal number of valid rows
    assert agg["taxi_count"].sum() == len(df)


def test_aggregate_taxis_by_h3_multiple_timestamps():
    df = pd.DataFrame({
        "timestamp": ["T1", "T1", "T2"],
        "lat": [SG_LAT, SG_LAT, SG_LAT],
        "lon": [SG_LON, SG_LON, SG_LON],
    })
    agg = aggregate_taxis_by_h3(df, "timestamp", "lat", "lon", RES)
    assert set(agg["timestamp"].unique()) == {"T1", "T2"}


# ---------------------------------------------------------------------------
# h3_to_boundary
# ---------------------------------------------------------------------------

def test_h3_to_boundary_returns_list_of_tuples():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    boundary = h3_to_boundary(cell)
    assert isinstance(boundary, list)
    assert len(boundary) >= 3
    assert all(len(p) == 2 for p in boundary)


def test_h3_to_boundary_is_closed():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    boundary = h3_to_boundary(cell)
    assert boundary[0] == boundary[-1]


def test_h3_to_boundary_bad_cell_returns_empty():
    result = h3_to_boundary("not_a_valid_cell")
    assert result == []


def test_h3_to_boundary_coords_in_sg_range():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    boundary = h3_to_boundary(cell)
    for lat, lon in boundary:
        assert 1.0 <= lat <= 1.6,  f"lat {lat} out of SG range"
        assert 103.5 <= lon <= 104.1, f"lon {lon} out of SG range"


# ---------------------------------------------------------------------------
# get_h3_neighbors
# ---------------------------------------------------------------------------

def test_get_h3_neighbors_excludes_self():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    neighbors = get_h3_neighbors(cell, k=1)
    assert cell not in neighbors


def test_get_h3_neighbors_k1_returns_six():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    neighbors = get_h3_neighbors(cell, k=1)
    # H3 k=1 ring has exactly 6 neighbors (for interior cells)
    assert len(neighbors) == 6


def test_get_h3_neighbors_sorted():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    neighbors = get_h3_neighbors(cell, k=1)
    assert neighbors == sorted(neighbors)


def test_get_h3_neighbors_k2_more_than_k1():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    n1 = get_h3_neighbors(cell, k=1)
    n2 = get_h3_neighbors(cell, k=2)
    assert len(n2) > len(n1)


# ---------------------------------------------------------------------------
# h3_cell_centroid
# ---------------------------------------------------------------------------

def test_h3_cell_centroid_round_trip():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    centroid = h3_cell_centroid(cell)
    assert centroid is not None
    clat, clon = centroid
    # Centroid should resolve back to the same cell
    assert latlon_to_h3(clat, clon, RES) == cell


def test_h3_cell_centroid_bad_cell_returns_none():
    result = h3_cell_centroid("invalid_cell")
    assert result is None


# ---------------------------------------------------------------------------
# compute_h3_neighbor_surplus
# ---------------------------------------------------------------------------

def test_neighbor_surplus_positive_when_neighbors_have_surplus():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    neighbors = get_h3_neighbors(cell, k=1)
    # cell_supply_map: {h3_cell: {"taxi_count": n, "baseline_supply": b}}
    supply_map = {n: {"taxi_count": 20, "baseline_supply": 5.0} for n in neighbors}
    supply_map[cell] = {"taxi_count": 2, "baseline_supply": 5.0}
    surplus = compute_h3_neighbor_surplus(cell, supply_map, k=1, safety_buffer=0.8)
    assert surplus > 0


def test_neighbor_surplus_zero_when_neighbors_depleted():
    cell = latlon_to_h3(SG_LAT, SG_LON, RES)
    neighbors = get_h3_neighbors(cell, k=1)
    supply_map = {n: {"taxi_count": 0, "baseline_supply": 5.0} for n in neighbors}
    supply_map[cell] = {"taxi_count": 0, "baseline_supply": 5.0}
    surplus = compute_h3_neighbor_surplus(cell, supply_map, k=1, safety_buffer=0.8)
    assert surplus == 0


# ---------------------------------------------------------------------------
# select_h3_action
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bucket,neighbor_surplus,threshold,expected_action", [
    # action = candidates[-1]; candidate lists per _H3_ACTION_CANDIDATES
    ("low",      0.0,  5.0, "monitor"),        # ["monitor"][-1]
    ("moderate", 0.0,  5.0, "driver_comms"),   # ["monitor","driver_comms"][-1]
    ("high",     0.0,  5.0, "driver_comms"),   # ["ops_alert","driver_comms"][-1]
    ("severe",   0.0,  5.0, "incentive"),      # ["ops_alert","driver_comms","incentive"][-1]
    ("severe",  10.0,  5.0, "rebalance"),      # adds "rebalance" when surplus ≥ threshold
])
def test_select_h3_action(bucket, neighbor_surplus, threshold, expected_action):
    action, reason = select_h3_action(bucket, neighbor_surplus, threshold)
    assert action == expected_action
    assert isinstance(reason, str) and len(reason) > 0


def test_select_h3_action_unknown_bucket_defaults_to_monitor():
    action, _ = select_h3_action("unknown_bucket", 0.0, 5.0)
    assert action == "monitor"


# ---------------------------------------------------------------------------
# Sparse cell flag logic
# ---------------------------------------------------------------------------

def test_sparse_flag_set_for_low_median():
    from backend.preprocessing.pipeline import _build_h3_sparse_flag

    df = pd.DataFrame({
        "h3_cell":    ["cell_a", "cell_a", "cell_a", "cell_b", "cell_b"],
        "taxi_count": [1, 1, 2, 10, 12],  # cell_a median=1 (sparse), cell_b median=11
    })
    result = _build_h3_sparse_flag(df, min_taxis=3)
    sparse_a = bool(result.loc[result["h3_cell"] == "cell_a", "sparse_cell_flag"].iloc[0])
    sparse_b = bool(result.loc[result["h3_cell"] == "cell_b", "sparse_cell_flag"].iloc[0])
    assert sparse_a is True
    assert sparse_b is False


# ---------------------------------------------------------------------------
# Baseline supply fallback cascade
# ---------------------------------------------------------------------------

def test_baseline_supply_three_tier_fallback():
    from backend.preprocessing.pipeline import _build_h3_baseline_supply

    base_ts = pd.Timestamp("2024-01-01 08:00:00")
    df = pd.DataFrame({
        "h3_cell":     ["cell_a", "cell_a"],
        "timestamp":   [base_ts, base_ts + pd.Timedelta(hours=1)],
        "taxi_count":  [6, 10],
        "hour_of_day": [8, 9],   # pipeline uses hour_of_day, not hour
        "day_of_week": [0, 0],
    })
    result = _build_h3_baseline_supply(df)
    assert "baseline_supply" in result.columns
    assert (result["baseline_supply"] > 0).all()
    assert "supply_gap" in result.columns
    assert result["supply_gap"].between(0, 1).all()


# ---------------------------------------------------------------------------
# H3 scoring output schema
# ---------------------------------------------------------------------------

def test_run_h3_batch_output_schema(tmp_path, monkeypatch):
    """run_h3_batch should produce a DataFrame with required columns."""
    import backend.scoring.batch_scorer as scorer_mod
    from backend.scoring.batch_scorer import run_h3_batch

    # Patch the module-level OUTPUTS_DIR constant (set at import time)
    monkeypatch.setattr(scorer_mod, "OUTPUTS_DIR", tmp_path)

    base_ts = pd.Timestamp("2024-01-01 08:00:00")
    df = pd.DataFrame({
        "h3_cell":                ["cell_a"],
        "parent_zone":            ["test_zone"],
        "timestamp":              [base_ts.isoformat()],
        "taxi_count":             [5],
        "baseline_supply":        [6.0],
        "supply_gap":             [0.2],
        "depletion_rate_1h":      [0.05],
        "imbalance_score":        [0.1],
        "demand_pressure_proxy":  [0.2],
        "lag_1h":                 [5],
        "lag_24h":                [5],
        "rolling_mean_3h":        [5.0],
        "sparse_cell_flag":       [False],
        "hour_of_day":            [8],
        "day_of_week":            [0],
        "is_peak_hour":           [1],
        "is_weekend":             [0],
        "is_holiday":             [0],
        "rainfall_mm":            [0.0],
        "is_raining":             [0],
        "congestion_ratio":       [0.5],
        "delta_depletion_1h":     [0.0],
        "taxi_abs_drop_1h":       [0],
        "is_low_count":           [0],
    })

    result = run_h3_batch(feature_df=df)
    required_keys = {"run_id", "run_status", "h3_cells_scored", "latency_ms"}
    assert required_keys.issubset(set(result.keys()))

    out_file = tmp_path / "h3_predictions.csv"
    assert out_file.exists()
    scored = pd.read_csv(out_file)

    required_cols = {
        "h3_cell", "parent_zone", "depletion_risk_score",
        "predicted_shortage", "severity_bucket",
        "recommended_action", "action_reason", "sparse_cell_flag",
    }
    assert required_cols.issubset(set(scored.columns))
    assert len(scored) == 1
