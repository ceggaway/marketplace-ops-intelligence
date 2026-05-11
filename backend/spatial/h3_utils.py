"""
H3 Spatial Utilities
====================
Hex-level taxi supply aggregation using the H3 hierarchical spatial index.

H3 resolution 8 gives cells of ~0.46 km², which is fine enough to detect
intra-zone supply gaps in dense areas (CBD, Orchard, transport hubs) without
becoming too sparse in residential areas.

All functions degrade gracefully when coordinates are missing, invalid, or
outside the Singapore bounding box. Sparse cells are flagged explicitly so
downstream consumers can apply the appropriate confidence caveats.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Singapore bounding box for coordinate validation
_SG_LAT_MIN, _SG_LAT_MAX = 1.15, 1.48
_SG_LON_MIN, _SG_LON_MAX = 103.60, 104.10

try:
    import h3 as _h3_lib

    # Support both h3 v4 (latlng_to_cell) and v3 (geo_to_h3) APIs
    if hasattr(_h3_lib, "latlng_to_cell"):
        def _cell_from_latlng(lat: float, lon: float, res: int) -> str:
            return _h3_lib.latlng_to_cell(lat, lon, res)

        def _latlng_from_cell(cell: str) -> tuple[float, float]:
            return _h3_lib.cell_to_latlng(cell)

        def _boundary_from_cell(cell: str):
            return _h3_lib.cell_to_boundary(cell)

        def _grid_disk(cell: str, k: int) -> set:
            return _h3_lib.grid_disk(cell, k)
    else:
        # h3 v3 fallback
        def _cell_from_latlng(lat: float, lon: float, res: int) -> str:  # type: ignore[misc]
            return _h3_lib.geo_to_h3(lat, lon, res)

        def _latlng_from_cell(cell: str) -> tuple[float, float]:  # type: ignore[misc]
            return _h3_lib.h3_to_geo(cell)

        def _boundary_from_cell(cell: str):  # type: ignore[misc]
            return _h3_lib.h3_to_geo_boundary(cell)

        def _grid_disk(cell: str, k: int) -> set:  # type: ignore[misc]
            return _h3_lib.k_ring(cell, k)

    _H3_AVAILABLE = True

except ImportError:
    _H3_AVAILABLE = False
    logger.warning(
        "h3 package not found. H3 spatial mode will not be available. "
        "Install with: pip install h3>=4.0"
    )


def _require_h3() -> None:
    if not _H3_AVAILABLE:
        raise ImportError(
            "h3 package is required for H3 spatial mode. "
            "Install with: pip install h3>=4.0"
        )


def _is_valid_sg_coord(lat: float, lon: float) -> bool:
    """Return True if coordinates fall within the Singapore bounding box."""
    return (
        _SG_LAT_MIN <= lat <= _SG_LAT_MAX
        and _SG_LON_MIN <= lon <= _SG_LON_MAX
    )


# ---------------------------------------------------------------------------
# Core conversion helpers
# ---------------------------------------------------------------------------

def latlon_to_h3(lat: float, lon: float, resolution: int) -> str:
    """
    Convert a GPS coordinate to an H3 cell ID.

    Raises ValueError for coordinates outside the Singapore bounding box
    so callers can distinguish invalid inputs from valid-but-sparse cells.
    """
    _require_h3()
    if not _is_valid_sg_coord(lat, lon):
        raise ValueError(
            f"Coordinates ({lat}, {lon}) are outside the Singapore bounding box "
            f"(lat {_SG_LAT_MIN}–{_SG_LAT_MAX}, lon {_SG_LON_MIN}–{_SG_LON_MAX})"
        )
    return _cell_from_latlng(lat, lon, resolution)


def add_h3_index(
    df: pd.DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
    resolution: int = 8,
) -> pd.DataFrame:
    """
    Add an h3_cell column to a DataFrame.

    Rows with missing, NaN, or out-of-bounds coordinates get h3_cell = None
    rather than raising — the caller decides whether to drop or keep them.
    """
    _require_h3()
    df = df.copy()

    if lat_col not in df.columns or lon_col not in df.columns:
        raise ValueError(f"DataFrame must have '{lat_col}' and '{lon_col}' columns")

    if df.empty:
        df["h3_cell"] = pd.Series(dtype=object)
        return df

    def _safe_convert(row: pd.Series) -> Optional[str]:
        try:
            lat, lon = float(row[lat_col]), float(row[lon_col])
            if np.isnan(lat) or np.isnan(lon):
                return None
            if not _is_valid_sg_coord(lat, lon):
                return None
            return _cell_from_latlng(lat, lon, resolution)
        except Exception:
            return None

    df["h3_cell"] = df.apply(_safe_convert, axis=1)
    return df


def aggregate_taxis_by_h3(
    df: pd.DataFrame,
    timestamp_col: str = "timestamp",
    lat_col: str = "lat",
    lon_col: str = "lon",
    resolution: int = 8,
) -> pd.DataFrame:
    """
    Group individual taxi GPS points by timestamp + H3 cell.

    Each row in the input represents one taxi GPS ping.
    Output columns: timestamp, h3_cell, taxi_count.
    """
    _require_h3()

    if df.empty:
        return pd.DataFrame(columns=["timestamp", "h3_cell", "taxi_count"])

    required = {timestamp_col, lat_col, lon_col}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise ValueError(f"DataFrame is missing required columns: {missing_cols}")

    valid = df.dropna(subset=[lat_col, lon_col]).copy()
    if valid.empty:
        return pd.DataFrame(columns=["timestamp", "h3_cell", "taxi_count"])

    valid = add_h3_index(valid, lat_col=lat_col, lon_col=lon_col, resolution=resolution)
    valid = valid.dropna(subset=["h3_cell"])
    if valid.empty:
        return pd.DataFrame(columns=["timestamp", "h3_cell", "taxi_count"])

    agg = (
        valid
        .groupby([timestamp_col, "h3_cell"])
        .size()
        .reset_index(name="taxi_count")
        .rename(columns={timestamp_col: "timestamp"})
    )
    return agg


def h3_to_boundary(h3_cell: str) -> list[tuple[float, float]]:
    """
    Return the polygon boundary of an H3 cell as (lat, lon) tuples.

    The boundary closes the polygon — first and last point are the same.
    Suitable for direct use in GeoJSON or Leaflet polygon rendering.
    Returns an empty list if the cell ID is invalid.
    """
    _require_h3()
    try:
        raw = list(_boundary_from_cell(h3_cell))
        if not raw:
            return []
        # Ensure polygon is closed
        coords = [(float(lat), float(lon)) for lat, lon in raw]
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        return coords
    except Exception as exc:
        logger.warning("Failed to get boundary for H3 cell %s: %s", h3_cell, exc)
        return []


def get_h3_neighbors(h3_cell: str, k: int = 1) -> list[str]:
    """
    Return the H3 cells in the k-ring around h3_cell, excluding the cell itself.

    k=1 returns up to 6 immediate neighbors.
    Returns an empty list if the cell ID is invalid.
    """
    _require_h3()
    try:
        disk = _grid_disk(h3_cell, k)
        return sorted(c for c in disk if c != h3_cell)
    except Exception as exc:
        logger.warning("Failed to get neighbors for H3 cell %s: %s", h3_cell, exc)
        return []


def h3_cell_centroid(h3_cell: str) -> Optional[tuple[float, float]]:
    """Return the (lat, lon) centroid of an H3 cell, or None on error."""
    _require_h3()
    try:
        lat, lon = _latlng_from_cell(h3_cell)
        return float(lat), float(lon)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Neighbor surplus for H3 rebalancing decisions
# ---------------------------------------------------------------------------

def compute_h3_neighbor_surplus(
    target_cell: str,
    cell_supply_map: dict[str, dict],
    k: int = 1,
    safety_buffer: float = 0.8,
) -> float:
    """
    Compute exportable surplus across k-ring neighbors of a target H3 cell.

    For each neighbor: exportable = max(0, taxi_count - baseline_supply * safety_buffer).
    Rebalancing is only recommended when total surplus exceeds the configured threshold.

    cell_supply_map: {h3_cell: {"taxi_count": n, "baseline_supply": b}}
    """
    neighbors = get_h3_neighbors(target_cell, k=k)
    surplus = 0.0
    for neighbor in neighbors:
        info = cell_supply_map.get(neighbor, {})
        supply = float(info.get("taxi_count", 0))
        baseline = max(float(info.get("baseline_supply", 1)), 1.0)
        exportable = max(0.0, supply - baseline * safety_buffer)
        surplus += exportable
    return round(surplus, 2)


# ---------------------------------------------------------------------------
# H3-conservative action selection
# ---------------------------------------------------------------------------

# H3 cells use more conservative action caps than planning zones.
# Rebalancing across H3 cells requires explicit neighbor surplus verification.
_H3_ACTION_CANDIDATES: dict[str, list[str]] = {
    "low":      ["monitor"],
    "moderate": ["monitor", "driver_comms"],
    "high":     ["ops_alert", "driver_comms"],
    "severe":   ["ops_alert", "driver_comms", "incentive"],
}


def select_h3_action(
    severity_bucket: str,
    neighbor_surplus: float = 0.0,
    rebalance_surplus_threshold: float = 5.0,
) -> tuple[str, str]:
    """
    Choose the recommended action for an H3 cell using conservative limits.

    Returns (recommended_action, reason).
    Rebalancing is added only when verified neighbor surplus exceeds the threshold.
    """
    candidates = list(_H3_ACTION_CANDIDATES.get(severity_bucket, ["monitor"]))

    # Promote to rebalance for severe cells when neighbor surplus is sufficient
    if severity_bucket == "severe" and neighbor_surplus >= rebalance_surplus_threshold:
        if "incentive" in candidates:
            candidates.append("rebalance")

    action = candidates[-1]
    reason = (
        f"H3 conservative policy: {severity_bucket} severity"
        + (f", neighbor surplus {neighbor_surplus:.1f} taxis" if neighbor_surplus > 0 else "")
    )
    return action, reason
