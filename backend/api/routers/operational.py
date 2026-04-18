"""
Operational API Router
======================
All endpoints read from flat files in data/outputs/ — no database.

Endpoints:
    GET /overview                  → KPI cards + trend data + alerts
    GET /zones                     → All zones with current risk scores
    GET /zones/{zone_id}           → Single zone detail + history
    GET /recommendations           → Action recommendation cards
"""

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException

from backend.api.schemas.responses import OverviewResponse, RecommendationCard, ZoneDetail, ZoneSummary

router   = APIRouter()
OUT_DIR  = Path("data/outputs")

# Cost per zone requiring intervention — configurable via env var
_INTERVENTION_COST_PER_ZONE = float(os.environ.get("INTERVENTION_COST_PER_ZONE", "1.50"))


def _clean(value, default=None):
    """Replace NaN/Inf with a JSON-safe default."""
    if value is None:
        return default
    try:
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return default
    except TypeError:
        pass
    return value


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_predictions() -> pd.DataFrame:
    p = OUT_DIR / "predictions.csv"
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)


def _read_current_predictions() -> pd.DataFrame:
    """Return only the most recent timestamp snapshot — current zone state."""
    df = _read_predictions()
    if df.empty or "timestamp" not in df.columns:
        return df
    return df[df["timestamp"] == df["timestamp"].max()].copy()


def _read_recommendations() -> pd.DataFrame:
    p = OUT_DIR / "recommended_actions.csv"
    if not p.exists():
        return pd.DataFrame()
    # keep_default_na=False prevents pandas from converting "N/A" strings to NaN
    return pd.read_csv(p, keep_default_na=False, na_values=[""])


def _read_pipeline_log(n: int = 48) -> list[dict]:
    p = OUT_DIR / "pipeline.log"
    if not p.exists():
        return []
    lines = p.read_text().strip().splitlines()
    records = []
    for line in lines[-n:]:
        try:
            records.append(json.loads(line))
        except Exception:
            pass
    return records


def _read_zone_score_history(zone_id: int, n: int = 48) -> list[dict]:
    """
    Read per-zone historical risk scores from zone_scores_history.jsonl.
    Written by batch_scorer after every scoring run — returns real historical
    scores for each zone, not the current score repeated.
    """
    path = OUT_DIR / "zone_scores_history.jsonl"
    if not path.exists():
        return []
    lines = path.read_text().strip().splitlines()[-n:]
    result = []
    for line in lines:
        try:
            r     = json.loads(line)
            score = r.get("zones", {}).get(str(zone_id))
            if score is not None and "timestamp" in r:
                result.append({"timestamp": r["timestamp"], "value": score})
        except Exception:
            pass
    return result


def _build_trend(log_records: list[dict], field: str) -> list[dict]:
    trend = []
    for r in log_records:
        if field in r and "timestamp" in r:
            trend.append({"timestamp": r["timestamp"], "value": r[field]})
    return trend


def _active_alerts(pred_df: pd.DataFrame, drift_flag: bool = False) -> list[dict]:
    alerts = []
    now = datetime.now(timezone.utc).isoformat()
    if not pred_df.empty:
        high_risk = pred_df[pred_df.get("risk_level", pd.Series()) == "high"] if "risk_level" in pred_df.columns else pd.DataFrame()
        if len(high_risk) >= 5:
            alerts.append({
                "alert_id":   "HIGH_RISK_ZONES",
                "zone_id":    None,
                "severity":   "high",
                "message":    f"{len(high_risk)} zones currently at high shortage risk",
                "created_at": now,
            })
        if drift_flag:
            alerts.append({
                "alert_id":   "DRIFT_DETECTED",
                "zone_id":    None,
                "severity":   "high",
                "message":    "Model prediction drift detected — review recommended",
                "created_at": now,
            })
    return alerts


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/overview", response_model=OverviewResponse)
def get_overview():
    # Current snapshot: only the most recent timestamp (55 zones)
    curr_df  = _read_current_predictions()
    log_recs = _read_pipeline_log()

    drift_psi  = 0.0
    drift_flag = False
    drift_path = OUT_DIR / "drift_report.json"
    if drift_path.exists():
        with open(drift_path) as f:
            drift = json.load(f)
        drift_psi  = float(drift.get("psi", 0.0))
        drift_flag = drift.get("drift_flag", False)

    # Minutes since last scoring run
    minutes_since = 0
    if log_recs:
        last_ts = log_recs[-1].get("timestamp") or log_recs[-1].get("logged_at")
        if last_ts:
            try:
                dt = datetime.fromisoformat(last_ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                minutes_since = int((datetime.now(timezone.utc) - dt).total_seconds() / 60)
            except Exception:
                pass

    empty_kpis = {
        "high_risk_zone_count":    0,
        "medium_risk_zone_count":  0,
        "total_taxi_supply":       0,
        "rapid_depletion_zones":   0,
        "critical_actions_count":  0,
        "model_psi":               drift_psi,
        "minutes_since_last_run":  minutes_since,
        "as_of":                   datetime.now(timezone.utc).isoformat(),
    }

    if curr_df.empty:
        return {
            "kpis":            empty_kpis,
            "demand_trend":    [],
            "delay_trend":     [],
            "fulfilment_trend": [],
            "alerts":          [],
        }

    # ── KPIs from current snapshot (real values, no heuristics) ──────────────
    high_risk_count  = int((curr_df["risk_level"] == "high").sum())   if "risk_level"        in curr_df.columns else 0
    med_risk_count   = int((curr_df["risk_level"] == "medium").sum()) if "risk_level"        in curr_df.columns else 0
    total_supply     = int(curr_df["taxi_count"].sum())                if "taxi_count"        in curr_df.columns else 0
    rapid_depletion  = int((curr_df["depletion_rate_1h"] > 0.30).sum()) if "depletion_rate_1h" in curr_df.columns else 0

    # Critical + high priority recommendations
    rec_df = _read_recommendations()
    critical_actions = 0
    if not rec_df.empty and "priority" in rec_df.columns:
        critical_actions = int(rec_df["priority"].isin(["critical", "high"]).sum())

    # ── Trends from pipeline log (one point per scoring run) ─────────────────
    demand_trend     = _build_trend(log_recs, "supply_now")           # supply right now per run
    delay_trend      = _build_trend(log_recs, "rapid_depletion_zones") # depletion count per run
    fulfilment_trend = _build_trend(log_recs, "high_risk_zones_now")  # high-risk count per run

    # Fall back to older field names for runs logged before this update
    if not demand_trend:
        demand_trend = _build_trend(log_recs, "total_taxi_count")
    if not fulfilment_trend:
        fulfilment_trend = _build_trend(log_recs, "flagged_zones")

    return {
        "kpis": {
            "high_risk_zone_count":   high_risk_count,
            "medium_risk_zone_count": med_risk_count,
            "total_taxi_supply":      total_supply,
            "rapid_depletion_zones":  rapid_depletion,
            "critical_actions_count": critical_actions,
            "model_psi":              round(drift_psi, 4),
            "minutes_since_last_run": minutes_since,
            "as_of":                  datetime.now(timezone.utc).isoformat(),
        },
        "demand_trend":     demand_trend,
        "delay_trend":      delay_trend,
        "fulfilment_trend": fulfilment_trend,
        "alerts":           _active_alerts(curr_df, drift_flag),
    }


@router.get("/zones", response_model=list[ZoneSummary])
def get_zones(risk_level: Optional[str] = None, region: Optional[str] = None):
    pred_df = _read_current_predictions()
    if pred_df.empty:
        return []

    if risk_level:
        pred_df = pred_df[pred_df.get("risk_level", pd.Series()) == risk_level.lower()]
    if region:
        pred_df = pred_df[pred_df.get("region", pd.Series()).str.lower() == region.lower()]

    rec_df = _read_recommendations()
    rec_map = {}
    if not rec_df.empty and "zone_id" in rec_df.columns:
        for _, r in rec_df.iterrows():
            rec_map[int(r["zone_id"])] = r

    zones = []
    for _, row in pred_df.iterrows():
        zone_id = int(row.get("zone_id", 0))
        rec = rec_map.get(zone_id, {})
        zones.append({
            "zone_id":             zone_id,
            "zone_name":           str(row.get("zone_name", "")),
            "region":              str(row.get("region", "")),
            "taxi_count":          int(_clean(row.get("taxi_count"), 0)),
            "current_supply":      int(_clean(row.get("taxi_count"), 0)),
            "depletion_rate_1h":   float(_clean(row.get("depletion_rate_1h"), 0.0)),
            "supply_vs_yesterday": float(_clean(row.get("supply_vs_yesterday"), 1.0)),
            "delay_risk_score":    float(_clean(row.get("delay_risk_score"), 0.0)),
            "risk_level":          str(row.get("risk_level", "low")),
            "recommendation":      str(rec.get("recommendation", "No action required")),
            "explanation_tag":     str(row.get("explanation_tag", "")),
        })

    return sorted(zones, key=lambda z: z["delay_risk_score"], reverse=True)


@router.get("/zones/{zone_id}", response_model=ZoneDetail)
def get_zone_detail(zone_id: int):
    pred_df = _read_current_predictions()
    if pred_df.empty or "zone_id" not in pred_df.columns:
        raise HTTPException(status_code=404, detail="No predictions available")

    zone_rows = pred_df[pred_df["zone_id"] == zone_id]
    if zone_rows.empty:
        raise HTTPException(status_code=404, detail=f"Zone {zone_id} not found")

    row = zone_rows.iloc[0]
    rec_df  = _read_recommendations()
    rec = {}
    if not rec_df.empty and "zone_id" in rec_df.columns:
        zone_rec = rec_df[rec_df["zone_id"] == zone_id]
        if not zone_rec.empty:
            rec = zone_rec.iloc[0].to_dict()

    # risk_score_history: real per-zone historical risk scores from zone_scores_history.jsonl.
    # demand_trend / delay_trend: we have no per-zone taxi-count or delay-time history —
    # returning empty arrays rather than system-level pipeline aggregates which would be
    # semantically misleading when presented as zone-specific data.
    score_hist   = _read_zone_score_history(zone_id, n=48)
    demand_trend: list[dict] = []
    delay_trend:  list[dict] = []

    return {
        "zone_id":             zone_id,
        "zone_name":           str(row.get("zone_name", "")),
        "region":              str(row.get("region", "")),
        "taxi_count":          int(_clean(row.get("taxi_count"), 0)),
        "current_supply":      int(_clean(row.get("taxi_count"), 0)),
        "depletion_rate_1h":   float(_clean(row.get("depletion_rate_1h"), 0.0)),
        "supply_vs_yesterday": float(_clean(row.get("supply_vs_yesterday"), 1.0)),
        "delay_risk_score":    float(_clean(row.get("delay_risk_score"), 0.0)),
        "risk_level":          str(row.get("risk_level", "low")),
        "explanation_tag":     str(row.get("explanation_tag", "")),
        "recommendation":      str(rec.get("recommendation", "No action required")),
        "demand_trend":        demand_trend,
        "delay_trend":         delay_trend,
        "risk_score_history":  score_hist,
    }


@router.get("/recommendations", response_model=list[RecommendationCard])
def get_recommendations(priority: Optional[str] = None):
    rec_df = _read_recommendations()
    if rec_df.empty:
        return []

    if priority:
        rec_df = rec_df[rec_df.get("priority", pd.Series()) == priority.lower()]

    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    if "priority" in rec_df.columns:
        rec_df["_sort"] = rec_df["priority"].map(priority_order).fillna(99)
        rec_df = rec_df.sort_values(["_sort", "delay_risk_score"], ascending=[True, False])
        rec_df = rec_df.drop(columns=["_sort"])

    # Convert NaN/NaT to None before serialisation so FastAPI emits JSON null
    # instead of pandas' default "NaT" or "NaN" strings.
    rec_df = rec_df.where(rec_df.notna(), other=None)
    return json.loads(rec_df.to_json(orient="records", date_format="iso"))
