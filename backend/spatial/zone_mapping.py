"""
H3-to-Planning-Zone Parent Mapping
====================================
Maps H3 hex cells back to their containing URA planning zone for business
readability. Uses centroid-in-polygon lookup via Shapely when available.

When Shapely is absent or no zone polygons are provided the parent_zone
column is filled with "unknown" so the rest of the pipeline degrades
gracefully without crashing.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from backend.spatial.h3_utils import h3_cell_centroid

logger = logging.getLogger(__name__)

try:
    from shapely.geometry import Point, shape as shapely_shape
    _SHAPELY_AVAILABLE = True
except ImportError:
    _SHAPELY_AVAILABLE = False
    logger.debug("shapely not available; H3→zone parent mapping will return 'unknown'")

# Default path to Singapore planning area GeoJSON
_DEFAULT_GEOJSON = Path("data/raw/sg_planning_areas.geojson")


def _load_zone_polygons(geojson_path: Path = _DEFAULT_GEOJSON) -> list[dict]:
    """
    Load planning zone polygons from a GeoJSON file.

    Returns a list of dicts with keys 'name' and 'geometry' (GeoJSON geometry dict).
    Returns an empty list if the file is missing or malformed.
    """
    if not geojson_path.exists():
        logger.debug("Zone GeoJSON not found at %s; parent_zone will be 'unknown'", geojson_path)
        return []
    try:
        with open(geojson_path, encoding="utf-8") as fh:
            fc = json.load(fh)
        polygons = []
        for feat in fc.get("features", []):
            props = feat.get("properties", {})
            name = props.get("Name") or props.get("PLN_AREA_N") or props.get("name") or ""
            geom = feat.get("geometry")
            if name and geom:
                # Normalize to title case to match zone_name convention elsewhere
                polygons.append({"name": name.title(), "geometry": geom})
        return polygons
    except Exception as exc:
        logger.warning("Failed to load zone polygons from %s: %s", geojson_path, exc)
        return []


def assign_parent_zone_to_h3(
    h3_cell: str,
    zone_polygons: list[dict],
) -> str:
    """
    Return the planning zone name whose polygon contains the H3 cell centroid.

    Falls back to "unknown" when:
    - Shapely is not installed
    - The centroid cannot be computed
    - No polygon contains the centroid (cell straddles a boundary or is offshore)
    """
    if not _SHAPELY_AVAILABLE or not zone_polygons:
        return "unknown"

    centroid = h3_cell_centroid(h3_cell)
    if centroid is None:
        return "unknown"

    lat, lon = centroid
    point = Point(lon, lat)  # GeoJSON / Shapely uses (lon, lat)

    for zone in zone_polygons:
        try:
            geom = shapely_shape(zone["geometry"])
            if geom.contains(point):
                return str(zone["name"])
        except Exception:
            continue

    return "unknown"


def add_parent_zone(
    df: pd.DataFrame,
    zone_polygons: Optional[list[dict]] = None,
    geojson_path: Path = _DEFAULT_GEOJSON,
) -> pd.DataFrame:
    """
    Add a parent_zone column to a DataFrame that has an h3_cell column.

    If zone_polygons is not provided the function loads from geojson_path.
    When neither is available, or when Shapely is absent, parent_zone = "unknown".
    """
    df = df.copy()

    if "h3_cell" not in df.columns:
        df["parent_zone"] = "unknown"
        return df

    if zone_polygons is None:
        zone_polygons = _load_zone_polygons(geojson_path)

    if not zone_polygons or not _SHAPELY_AVAILABLE:
        df["parent_zone"] = "unknown"
        return df

    # Cache lookup per unique cell to avoid redundant polygon scans
    unique_cells = df["h3_cell"].dropna().unique()
    cache: dict[str, str] = {
        cell: assign_parent_zone_to_h3(cell, zone_polygons)
        for cell in unique_cells
    }
    df["parent_zone"] = df["h3_cell"].map(cache).fillna("unknown")
    return df
