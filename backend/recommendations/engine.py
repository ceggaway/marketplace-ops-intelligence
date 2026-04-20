"""
Recommendation engine backed by the intervention decision-support layer.

This module converts per-zone scoring outputs into recommendation cards while
preserving legacy response fields consumed by the API and frontend.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

OUTPUTS_DIR = Path("data/outputs")

RISK_HIGH = 0.70
RISK_MED = 0.40


def _assign_risk(score: float) -> str:
    if score >= RISK_HIGH:
        return "high"
    if score >= RISK_MED:
        return "medium"
    return "low"


def _recommendation_id(zone_id: int, timestamp: str, action: str) -> str:
    raw = f"{zone_id}|{timestamp}|{action}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _priority_for_action(action: str, risk_level: str, severity_bucket: str) -> str:
    if action == "ops_alert":
        return "critical"
    if severity_bucket in {"severe", "high"} or risk_level == "high":
        return "high"
    if severity_bucket == "moderate" or risk_level == "medium":
        return "medium"
    return "low"


def _action_text(action: str, zone_name: str) -> tuple[str, str]:
    if action == "rebalance":
        return (
            f"Rebalance nearby driver supply into {zone_name}. Pull only from adjacent zones with safe exportable surplus.",
            "Near-term supply repositioning using adjacent-zone slack.",
        )
    if action == "incentive":
        return (
            f"Offer a targeted driver incentive for pickups in {zone_name}.",
            "Activate additional supply without physically draining adjacent zones.",
        )
    if action == "rebalance_plus_incentive":
        return (
            f"Rebalance nearby drivers into {zone_name} and pair it with a targeted incentive.",
            "Use both repositioning and spend to relieve sustained local stress.",
        )
    if action == "ops_alert":
        return (
            f"Escalate {zone_name} to the ops team for manual intervention.",
            "Automated levers are infeasible or insufficient for the current severity.",
        )
    return (
        f"Continue monitoring {zone_name}; no active intervention is warranted yet.",
        "Maintain watchlist posture while preserving budget and adjacent supply.",
    )


def _root_cause_from_row(row: pd.Series) -> str:
    tag = str(row.get("explanation_tag", "")).lower()
    if "rain" in tag:
        return "weather"
    if "peak" in tag:
        return "peak_pattern"
    if float(row.get("demand_pressure_score", 0.0) or 0.0) >= 0.7:
        return "demand_pressure"
    if float(row.get("depletion_rate_1h", 0.0) or 0.0) > 0.3:
        return "rapid_depletion"
    return "structural_gap"


def _build_recommendation(
    row: pd.Series,
    risk_map: dict | None = None,
    baselines: dict | None = None,
    policy_records: list[dict] | None = None,
    critical_count: int = 5,
) -> dict:
    del risk_map, baselines, policy_records, critical_count

    zone_id = int(row.get("zone_id", 0) or 0)
    zone_name = str(row.get("zone_name", "this zone"))
    region = str(row.get("region", ""))
    score = float(row.get("delay_risk_score", 0.0) or 0.0)
    risk_level = str(row.get("risk_level", _assign_risk(score)))
    severity_bucket = str(row.get("severity_bucket", "low") or "low")
    recommended_action = str(row.get("recommended_action", "monitor") or "monitor")
    recommendation, expected_impact = _action_text(recommended_action, zone_name)
    priority = _priority_for_action(recommended_action, risk_level, severity_bucket)
    predicted_shortage = float(row.get("predicted_shortage", 0.0) or 0.0)
    estimated_shortage_reduction = float(row.get("estimated_shortage_reduction", 0.0) or 0.0)
    neighbor_surplus = float(row.get("neighbor_surplus", 0.0) or 0.0)
    confidence = round(min(max(score * 0.85 + predicted_shortage * 0.15, 0.25), 0.99), 2)
    snapshot_ts = str(row.get("timestamp", datetime.now(timezone.utc).isoformat()))
    action_reason = str(row.get("action_reason", "") or "")
    root_cause = _root_cause_from_row(row)

    return {
        "recommendation_id": _recommendation_id(zone_id, snapshot_ts, recommended_action),
        "zone_id": zone_id,
        "zone_name": zone_name,
        "region": region,
        "risk_level": risk_level,
        "delay_risk_score": round(score, 4),
        "depletion_risk_score": round(float(row.get("depletion_risk_score", score) or score), 4),
        "demand_pressure_score": round(float(row.get("demand_pressure_score", 0.0) or 0.0), 4),
        "imbalance_score": round(float(row.get("imbalance_score", 0.0) or 0.0), 4),
        "imbalance_level": str(row.get("imbalance_level", "low") or "low"),
        "policy_action": str(row.get("policy_action", "no_action") or "no_action"),
        "policy_reason": str(row.get("policy_reason", "") or ""),
        "predicted_shortage": round(predicted_shortage, 4),
        "severity_bucket": severity_bucket,
        "persistence_count": int(row.get("persistence_count", 0) or 0),
        "neighbor_surplus": round(neighbor_surplus, 4),
        "recommended_action": recommended_action,
        "action_reason": action_reason,
        "estimated_action_cost": round(float(row.get("estimated_action_cost", 0.0) or 0.0), 4),
        "estimated_shortage_reduction": round(estimated_shortage_reduction, 4),
        "budget_remaining": round(float(row.get("budget_remaining", 0.0) or 0.0), 4),
        "issue_detected": f"{severity_bucket.title()} shortage stress in {zone_name} (score {predicted_shortage:.3f}).",
        "recommendation": recommendation,
        "expected_impact": expected_impact,
        "confidence": confidence,
        "priority": priority,
        "explanation_tag": str(row.get("explanation_tag", "") or ""),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "eta_minutes": None,
        "intervention_window": None,
        "adjacent_risk_zones": "",
        "network_warning": "Rebalance depends on adjacent exportable surplus." if recommended_action in {"rebalance", "rebalance_plus_incentive"} else "",
        "root_cause": root_cause,
        "action_id": recommended_action,
        "action_type": recommended_action,
        "pricing_level": "none",
        "incentive_level": "targeted" if recommended_action in {"incentive", "rebalance_plus_incentive"} else "none",
        "push_level": "none",
        "expected_recovery_rate": None,
        "expected_improvement_rate": None,
        "estimated_score_delta": round(-estimated_shortage_reduction, 4),
        "confidence_band": "medium" if priority in {"critical", "high"} else "low",
        "evidence_count": 0,
        "follow_rate": None,
        "policy_rank_reason": action_reason or "Deterministic intervention selection from predicted shortage and constraints.",
        "estimated_cost_sgd": round(float(row.get("estimated_action_cost", 0.0) or 0.0), 4),
        "expected_supply_response_30m": round(estimated_shortage_reduction * 10, 4),
        "expected_recovery_probability": round(max(predicted_shortage - estimated_shortage_reduction, 0.0), 4),
        "expected_roi": round(
            estimated_shortage_reduction / max(float(row.get("estimated_action_cost", 0.0) or 0.0), 0.1),
            4,
        ),
        "decision_objective": "reliability_first",
        "winning_reason": action_reason,
        "constraints_triggered": str(row.get("constraints_triggered", "[]") or "[]"),
        "alternative_actions": json.dumps([]),
    }


def generate_recommendations(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Generate recommendation cards from the intervention-enhanced predictions snapshot."""
    rows = [_build_recommendation(row) for _, row in predictions_df.iterrows()]
    df = pd.DataFrame(rows)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUTS_DIR / "recommended_actions.csv", index=False)
    return df
