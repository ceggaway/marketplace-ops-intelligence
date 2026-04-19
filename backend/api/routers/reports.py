"""
Reports Router
==============
GET /reports/zone-performance  — zone ranking, chronic offenders, trend analysis
GET /reports/outcomes          — intervention effectiveness from outcome tracker
GET /reports/model-impact      — PSI + metrics translated into business language
GET /reports/export/predictions — predictions.csv download
GET /reports/export/history     — pipeline run history CSV download

All reads from existing flat files — no new data sources required.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend.registry import model_registry as registry
from backend.recommendations.policy_effectiveness import effectiveness_by_action, load_outcome_records, summarise_action_outcomes

router  = APIRouter()
OUT_DIR = Path("data/outputs")
ZONE_HISTORY_RETENTION_DAYS = 14


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _label(value, default: str = "unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _psi_level(psi: float) -> str:
    if psi >= 0.25: return "significant"
    if psi >= 0.10: return "moderate"
    return "stable"


def _psi_business_impact(psi: float, precision: Optional[float]) -> str:
    level = _psi_level(psi)
    if level == "significant":
        base = (
            f"PSI={psi:.3f} indicates significant data drift — the live distribution "
            "differs materially from training data. Model predictions are less reliable; "
            "expect more false positives and missed shortages."
        )
    elif level == "moderate":
        base = (
            f"PSI={psi:.3f} indicates moderate drift — early warning signal. "
            "Model performance may be degrading; monitor closely and consider retraining soon."
        )
    else:
        base = (
            f"PSI={psi:.3f} — distribution is stable. Model is operating within "
            "expected parameters."
        )
    if precision is not None:
        base += f" Current precision: {precision:.1%}."
    return base


def _false_positive_note(precision: Optional[float]) -> str:
    if precision is None:
        return "Precision not available — retrain model to get updated metrics."
    fp_rate = 1.0 - precision
    return (
        f"At precision={precision:.1%}, roughly {fp_rate:.0%} of high-risk alerts are false positives. "
        f"For every 10 interventions dispatched to high-risk zones, ~{round(fp_rate * 10):.0f} "
        "may be unnecessary. Retraining on more recent data can reduce this."
    )


def _model_recommendation(psi: float, f1: Optional[float], trained_at: Optional[str]) -> str:
    actions = []
    if psi >= 0.25:
        actions.append("retrain immediately")
    elif psi >= 0.10:
        actions.append("schedule retraining within the week")
    if f1 is not None and f1 < 0.50:
        actions.append("review feature engineering — F1 below 0.50 suggests structural issues")
    if trained_at:
        try:
            age_days = (datetime.now(timezone.utc) - datetime.fromisoformat(trained_at)).days
            if age_days > 30:
                actions.append(f"model is {age_days} days old — refresh with recent data")
        except Exception:
            pass
    if not actions:
        return "Model is healthy. Continue regular monitoring."
    return "Recommended actions: " + "; ".join(actions) + "."


# ── Zone performance report ───────────────────────────────────────────────────

@router.get("/reports/zone-performance", response_model=dict)
def get_zone_performance(days: int = 7):
    """
    Analyse zone risk trends from zone_scores_history.jsonl.
    Returns chronic high-risk zones, most improved, and deteriorating zones.
    """
    requested_days = max(int(days), 1)
    days = min(requested_days, ZONE_HISTORY_RETENTION_DAYS)

    history_path = OUT_DIR / "zone_scores_history.jsonl"
    if not history_path.exists():
        return {
            "generated_at":   _now_iso(),
            "observation_days": days,
            "chronic_high_risk": [],
            "most_improved":     [],
            "deteriorating":     [],
            "note": "No zone score history yet — run the scoring pipeline to populate.",
        }

    lines = history_path.read_text().strip().splitlines()
    records = []
    for line in lines:
        try:
            r = json.loads(line)
            ts = r.get("timestamp", "")
            for zone_id_str, score in r.get("zones", {}).items():
                records.append({"timestamp": ts, "zone_id": int(zone_id_str), "score": float(score)})
        except Exception:
            continue

    if not records:
        return {"generated_at": _now_iso(), "observation_days": days,
                "chronic_high_risk": [], "most_improved": [], "deteriorating": [],
                "note": "History file exists but contains no parseable records."}

    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # Limit to requested window
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
    df = df[df["timestamp"] >= cutoff]

    if df.empty:
        return {"generated_at": _now_iso(), "observation_days": days,
                "chronic_high_risk": [], "most_improved": [], "deteriorating": [],
                "note": f"No data within the last {days} days."}

    # Load zone name lookup from predictions.csv
    zone_meta: dict[int, dict] = {}
    pred_path = OUT_DIR / "predictions.csv"
    if pred_path.exists():
        pred_df = pd.read_csv(pred_path)
        for _, r in pred_df.drop_duplicates("zone_id").iterrows():
            zone_meta[int(r["zone_id"])] = {
                "zone_name": str(r.get("zone_name", str(r["zone_id"]))),
                "region":    str(r.get("region", "")),
            }

    # Compute per-zone stats using mid-point split for trend
    half = pd.Timedelta(days=days / 2)
    mid  = pd.Timestamp.now(tz="UTC") - half

    entries = []
    for zone_id, grp in df.groupby("zone_id"):
        scores = grp["score"].values
        n      = len(scores)
        mean_s = float(scores.mean())
        pct_h  = float((scores >= 0.70).mean())
        pct_m  = float(((scores >= 0.40) & (scores < 0.70)).mean())
        pct_l  = float((scores < 0.40).mean())

        prior  = grp[grp["timestamp"] < mid]["score"].mean()
        recent = grp[grp["timestamp"] >= mid]["score"].mean()
        delta  = float(recent - prior) if not (pd.isna(prior) or pd.isna(recent)) else 0.0

        if delta < -0.05:
            trend = "improving"
        elif delta > 0.05:
            trend = "deteriorating"
        else:
            trend = "stable"

        meta = zone_meta.get(int(zone_id), {})
        entries.append({
            "zone_id":         int(zone_id),
            "zone_name":       meta.get("zone_name", str(zone_id)),
            "region":          meta.get("region", ""),
            "mean_score":      round(mean_s, 4),
            "pct_time_high":   round(pct_h, 4),
            "pct_time_medium": round(pct_m, 4),
            "pct_time_low":    round(pct_l, 4),
            "trend":           trend,
            "trend_delta":     round(delta, 4),
            "observations":    n,
        })

    chronic     = sorted(entries, key=lambda x: -x["pct_time_high"])[:5]
    improved    = sorted([e for e in entries if e["trend"] == "improving"],
                         key=lambda x: x["trend_delta"])[:5]
    deteriorating = sorted([e for e in entries if e["trend"] == "deteriorating"],
                            key=lambda x: -x["trend_delta"])[:5]

    note = None
    if requested_days > ZONE_HISTORY_RETENTION_DAYS:
        note = (
            f"Requested {requested_days} days, but zone score history currently retains "
            f"only the most recent {ZONE_HISTORY_RETENTION_DAYS} days."
        )

    return {
        "generated_at":    _now_iso(),
        "observation_days": days,
        "chronic_high_risk": chronic,
        "most_improved":     improved,
        "deteriorating":     deteriorating,
        "note": note,
    }


# ── Outcome / intervention effectiveness report ───────────────────────────────

@router.get("/reports/outcomes", response_model=dict)
def get_outcome_report():
    """
    Intervention effectiveness from the outcome tracker.
    Shows recovery rate, by-action breakdown, and recent resolved outcomes.
    """
    all_records = load_outcome_records(n=1000)
    resolved = [o for o in all_records if o.get("outcome") is not None]

    total_logged   = len(all_records)
    total_resolved = len(resolved)

    if total_resolved == 0:
        note = (
            "No resolved outcomes yet. The system logs recommendations and checks 30 min later. "
            "Run the scoring pipeline across multiple cycles to accumulate data."
        )
        return {
            "generated_at":     _now_iso(),
            "total_logged":     total_logged,
            "total_resolved":   0,
            "recovery_rate":    0.0,
            "improvement_rate": 0.0,
            "worsened_rate":    0.0,
            "by_action_type":   {},
            "by_follow_status": {},
            "top_contexts":     [],
            "by_zone":          [],
            "recent_outcomes":  [],
            "sample_size_note": note,
        }

    outcome_counts: dict[str, int] = {}
    for r in resolved:
        oc = r.get("outcome", "unknown")
        outcome_counts[oc] = outcome_counts.get(oc, 0) + 1

    recovery_rate    = round(outcome_counts.get("recovered", 0) / total_resolved, 4)
    improvement_rate = round((outcome_counts.get("recovered", 0) +
                               outcome_counts.get("improved", 0)) / total_resolved, 4)
    worsened_rate    = round(outcome_counts.get("worsened", 0) / total_resolved, 4)

    by_action = effectiveness_by_action(all_records)
    by_follow_status = {
        "followed": summarise_action_outcomes([r for r in all_records if r.get("followed_status") == "followed"]),
        "not_followed": summarise_action_outcomes([r for r in all_records if r.get("followed_status") == "not_followed"]),
        "unknown": summarise_action_outcomes([r for r in all_records if r.get("followed_status") not in ("followed", "not_followed")]),
    }

    # By zone — top 10 most intervened
    by_zone_counts: dict[tuple, dict] = {}
    for r in resolved:
        key = (r["zone_id"], r.get("zone_name", str(r["zone_id"])))
        by_zone_counts.setdefault(key, {"total": 0, "recovered": 0})
        by_zone_counts[key]["total"] += 1
        if r.get("outcome") == "recovered":
            by_zone_counts[key]["recovered"] += 1

    by_zone = sorted([
        {
            "zone_id":      k[0],
            "zone_name":    k[1],
            "interventions": v["total"],
            "recovery_rate": round(v["recovered"] / v["total"], 4) if v["total"] else 0.0,
        }
        for k, v in by_zone_counts.items()
    ], key=lambda x: -x["interventions"])[:10]

    context_groups: dict[tuple, list[dict]] = {}
    for r in resolved:
        key = (
            _label(r.get("action_type")),
            _label(r.get("risk_level")),
            _label(r.get("root_cause")),
            _label(r.get("intervention_window")),
        )
        context_groups.setdefault(key, []).append(r)
    top_contexts = sorted([
        {
            "action_type": key[0],
            "risk_level": key[1],
            "root_cause": key[2],
            "intervention_window": key[3],
            "resolved": stats["resolved"],
            "recovery_rate": stats["recovery_rate"],
            "improvement_rate": stats["improvement_rate"],
            "confidence_band": stats["confidence_band"],
        }
        for key, stats in (
            ((key, summarise_action_outcomes(rows)) for key, rows in context_groups.items())
        )
        if stats["resolved"] > 0
    ], key=lambda x: (-x["resolved"], -x["improvement_rate"]))[:6]

    # Recent 20 resolved outcomes
    recent = [
        {
            "recommendation_id": r.get("recommendation_id"),
            "zone_id":      r["zone_id"],
            "zone_name":    _label(r.get("zone_name"), str(r["zone_id"])),
            "action_type":  _label(r.get("action_type")),
            "priority":     _label(r.get("priority")),
            "score_at_time": r.get("score_at_time", 0.0),
            "score_after":  r.get("score_after"),
            "outcome":      _label(r.get("outcome")),
            "logged_at":    r.get("logged_at", ""),
            "followed_status": _label(r.get("followed_status"), "unknown"),
            "follow_note":  r.get("follow_note"),
        }
        for r in sorted(resolved, key=lambda x: x.get("logged_at", ""), reverse=True)[:20]
    ]

    credibility = ""
    if total_resolved < 10:
        credibility = (
            f"⚠ Only {total_resolved} resolved outcome(s). "
            "Rates are not statistically meaningful at this sample size. "
            "Accumulate at least 30+ resolved outcomes before drawing conclusions."
        )
    elif total_resolved < 50:
        credibility = f"Note: {total_resolved} resolved outcomes — directional signal only."

    return {
        "generated_at":     _now_iso(),
        "total_logged":     total_logged,
        "total_resolved":   total_resolved,
        "recovery_rate":    recovery_rate,
        "improvement_rate": improvement_rate,
        "worsened_rate":    worsened_rate,
        "by_action_type":   by_action,
        "by_follow_status": by_follow_status,
        "top_contexts":     top_contexts,
        "by_zone":          by_zone,
        "recent_outcomes":  recent,
        "sample_size_note": credibility,
    }


# ── Model impact report ───────────────────────────────────────────────────────

@router.get("/reports/model-impact", response_model=dict)
def get_model_impact():
    """
    Translate model health metrics (PSI, AUC, precision) into plain-language
    business impact statements ops managers can act on.
    """
    # PSI from drift report
    psi      = 0.0
    drift_path = OUT_DIR / "drift_report.json"
    if drift_path.exists():
        try:
            with open(drift_path) as f:
                drift = json.load(f)
            psi = float(drift.get("psi", 0.0))
        except Exception:
            pass

    # Model metrics from registry
    meta      = registry.get_active_version_meta() or {}
    precision = meta.get("precision")
    recall    = meta.get("recall")
    f1        = meta.get("f1")
    trained_at = meta.get("trained_at")
    active_ver = meta.get("version_id")

    # Version lineage
    reg_data = registry._load_registry()
    lineage  = []
    for vid, vmeta in reg_data.get("versions", {}).items():
        m = vmeta.get("metrics", {})
        lineage.append({
            "version":    vid,
            "status":     vmeta.get("status", ""),
            "trained_at": vmeta.get("trained_at", ""),
            "f1":         m.get("f1"),
            "roc_auc":    m.get("roc_auc"),
        })
    lineage.sort(key=lambda x: x.get("trained_at", ""), reverse=True)

    return {
        "generated_at":              _now_iso(),
        "active_version":            active_ver,
        "psi":                       round(psi, 4),
        "psi_level":                 _psi_level(psi),
        "psi_business_impact":       _psi_business_impact(psi, precision),
        "precision":                 precision,
        "recall":                    recall,
        "f1":                        f1,
        "estimated_false_positive_note": _false_positive_note(precision),
        "version_lineage":           lineage,
        "recommendation":            _model_recommendation(psi, f1, trained_at),
    }


# ── CSV exports ───────────────────────────────────────────────────────────────

@router.get("/reports/export/predictions")
def export_predictions():
    """Download current predictions as CSV."""
    path = OUT_DIR / "predictions.csv"
    if not path.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="predictions.csv not found.")
    content = path.read_text()
    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=predictions.csv"},
    )


@router.get("/reports/export/history")
def export_history():
    """Download pipeline run history as CSV."""
    path = OUT_DIR / "pipeline.log"
    if not path.exists():
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="pipeline.log not found.")
    lines = path.read_text().strip().splitlines()
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except Exception:
            continue
    if not records:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="No pipeline runs in log.")
    df = pd.DataFrame(records)
    csv = df.to_csv(index=False)
    return StreamingResponse(
        iter([csv]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=pipeline_history.csv"},
    )
