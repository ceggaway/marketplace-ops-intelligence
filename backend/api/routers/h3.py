"""
H3 Hex-Level API Router
========================
Endpoints for H3 cell-level supply risk data.

All endpoints read from data/outputs/h3_predictions.csv.
They return empty responses gracefully when the H3 scoring pipeline has not
yet been run — the frontend empty-state handles this case with instructions.

Endpoints:
    GET /h3/cells              → latest H3 cell predictions (filterable)
    GET /h3/cells/{h3_cell}    → detail for one H3 cell
    GET /h3/heatmap            → cell boundaries + risk scores for map rendering
"""

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.paths import outputs_dir
from backend.spatial.h3_utils import h3_to_boundary

router  = APIRouter()
OUT_DIR = outputs_dir()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class H3CellSummary(BaseModel):
    h3_cell: str
    parent_zone: str
    taxi_count: int
    baseline_supply: float
    depletion_risk_score: float
    demand_pressure_proxy: float
    supply_gap: float
    imbalance_score: float
    predicted_shortage: float
    severity_bucket: str
    recommended_action: str
    action_reason: str
    sparse_cell_flag: bool
    timestamp: str


class H3CellDetail(H3CellSummary):
    boundary: list[list[float]]   # [[lat, lon], ...]


class H3HeatmapCell(BaseModel):
    h3_cell: str
    parent_zone: str
    taxi_count: int
    baseline_supply: float
    depletion_risk_score: float
    predicted_shortage: float
    severity_bucket: str
    sparse_cell_flag: bool
    boundary: list[list[float]]   # [[lat, lon], ...]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(value, default=None):
    """Replace NaN / Inf with a JSON-safe default."""
    if value is None:
        return default
    try:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return default
    except TypeError:
        pass
    return value


def _read_h3_predictions() -> pd.DataFrame:
    p = OUT_DIR / "h3_predictions.csv"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)


def _read_current_h3() -> pd.DataFrame:
    """Return only the most recent timestamp snapshot."""
    df = _read_h3_predictions()
    if df.empty or "timestamp" not in df.columns:
        return df
    return df[df["timestamp"] == df["timestamp"].max()].copy()


def _row_to_summary(row: pd.Series) -> dict:
    return {
        "h3_cell":               str(row.get("h3_cell", "")),
        "parent_zone":           str(_clean(row.get("parent_zone"), "unknown")),
        "taxi_count":            int(_clean(row.get("taxi_count"), 0)),
        "baseline_supply":       float(_clean(row.get("baseline_supply"), 0.0)),
        "depletion_risk_score":  float(_clean(row.get("depletion_risk_score"), 0.0)),
        "demand_pressure_proxy": float(_clean(row.get("demand_pressure_proxy"), 0.0)),
        "supply_gap":            float(_clean(row.get("supply_gap"), 0.0)),
        "imbalance_score":       float(_clean(row.get("imbalance_score"), 0.0)),
        "predicted_shortage":    float(_clean(row.get("predicted_shortage"), 0.0)),
        "severity_bucket":       str(_clean(row.get("severity_bucket"), "low")),
        "recommended_action":    str(_clean(row.get("recommended_action"), "monitor")),
        "action_reason":         str(_clean(row.get("action_reason"), "")),
        "sparse_cell_flag":      bool(_clean(row.get("sparse_cell_flag"), False)),
        "timestamp":             str(row.get("timestamp", datetime.now(timezone.utc).isoformat())),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/h3/cells", response_model=list[H3CellSummary])
def get_h3_cells(
    parent_zone:    Optional[str] = Query(None, description="Filter by parent planning zone"),
    severity_bucket: Optional[str] = Query(None, description="Filter by severity: low / moderate / high / severe"),
    sparse:         Optional[bool] = Query(None, description="Filter sparse cells (true=only sparse, false=only non-sparse)"),
):
    """
    Return the latest H3 cell risk predictions.

    Returns an empty list when the H3 scoring pipeline has not yet been run.
    Run: python scripts/run_scoring.py --h3
    """
    df = _read_current_h3()
    if df.empty:
        return []

    if parent_zone:
        df = df[df.get("parent_zone", pd.Series(dtype=str)).str.lower() == parent_zone.lower()]
    if severity_bucket:
        df = df[df.get("severity_bucket", pd.Series(dtype=str)) == severity_bucket.lower()]
    if sparse is not None:
        if "sparse_cell_flag" in df.columns:
            df = df[df["sparse_cell_flag"].astype(bool) == sparse]

    df = df.sort_values("depletion_risk_score", ascending=False)
    return [_row_to_summary(row) for _, row in df.iterrows()]


@router.get("/h3/cells/{h3_cell}", response_model=H3CellDetail)
def get_h3_cell_detail(h3_cell: str):
    """Return detail for a single H3 cell, including polygon boundary coordinates."""
    df = _read_current_h3()
    if df.empty or "h3_cell" not in df.columns:
        raise HTTPException(status_code=404, detail="No H3 predictions available. Run H3 scoring first.")

    rows = df[df["h3_cell"] == h3_cell]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"H3 cell {h3_cell} not found in latest predictions.")

    row = rows.iloc[0]
    summary = _row_to_summary(row)

    try:
        boundary = [[lat, lon] for lat, lon in h3_to_boundary(h3_cell)]
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid H3 cell {h3_cell}: {exc}")

    return {**summary, "boundary": boundary}


@router.get("/h3/heatmap", response_model=list[H3HeatmapCell])
def get_h3_heatmap(
    parent_zone:    Optional[str] = Query(None, description="Filter by parent planning zone"),
    min_risk:       Optional[float] = Query(None, description="Minimum depletion_risk_score to include"),
):
    """
    Return H3 cells with polygon boundaries for frontend heatmap rendering.

    Each cell includes its geographic boundary as [[lat, lon], ...] coordinates.
    The frontend converts these to GeoJSON polygons for Leaflet rendering.

    Returns an empty list when H3 scoring has not been run yet.
    """
    df = _read_current_h3()
    if df.empty:
        return []

    if parent_zone:
        df = df[df.get("parent_zone", pd.Series(dtype=str)).str.lower() == parent_zone.lower()]
    if min_risk is not None:
        df = df[df.get("depletion_risk_score", pd.Series(0.0)) >= min_risk]

    # Sort highest risk first so the frontend renders important cells on top
    df = df.sort_values("depletion_risk_score", ascending=False)

    results = []
    for _, row in df.iterrows():
        cell = str(row.get("h3_cell", ""))
        if not cell:
            continue
        try:
            boundary = [[lat, lon] for lat, lon in h3_to_boundary(cell)]
        except Exception:
            continue
        if not boundary:
            continue

        results.append({
            "h3_cell":              cell,
            "parent_zone":          str(_clean(row.get("parent_zone"), "unknown")),
            "taxi_count":           int(_clean(row.get("taxi_count"), 0)),
            "baseline_supply":      float(_clean(row.get("baseline_supply"), 0.0)),
            "depletion_risk_score": float(_clean(row.get("depletion_risk_score"), 0.0)),
            "predicted_shortage":   float(_clean(row.get("predicted_shortage"), 0.0)),
            "severity_bucket":      str(_clean(row.get("severity_bucket"), "low")),
            "sparse_cell_flag":     bool(_clean(row.get("sparse_cell_flag"), False)),
            "boundary":             boundary,
        })

    return results
