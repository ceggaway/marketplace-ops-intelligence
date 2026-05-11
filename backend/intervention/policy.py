"""Severity bucketing, action candidates, and recommendation-card helpers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.paths import outputs_dir
from backend.modeling.shortage import classify_severity_bucket

ALL_ACTIONS = {
    "monitor",
    "driver_comms",
    "rebalance",
    "incentive",
    "rebalance_plus_incentive",
    "ops_alert",
}
OUTPUTS_DIR = outputs_dir()
RISK_HIGH = 0.70
RISK_MED = 0.40

DEFAULT_ACTION_GOVERNANCE = {
    "monitor": {"tier": 0, "label": "Monitor only", "requires_approver": "none"},
    "ops_alert": {"tier": 1, "label": "Ops alert", "requires_approver": "ops_analyst"},
    "driver_comms": {"tier": 2, "label": "Driver-comms recommendation", "requires_approver": "ops_lead"},
    "incentive": {"tier": 3, "label": "Incentive proposal", "requires_approver": "ops_lead+finance"},
    "rebalance": {"tier": 4, "label": "Rebalancing recommendation", "requires_approver": "ops_lead"},
    "rebalance_plus_incentive": {
        "tier": 4,
        "label": "Rebalancing recommendation with incentive proposal",
        "requires_approver": "ops_lead+finance",
    },
}

DEFAULT_ROI_BRIDGE = {
    "default_depletion_minutes": 30,
    "margin_assumption_sgd": 6.0,
    "zone_commercial_weight": {
        "cbd": 1.0,
        "airport": 1.2,
        "nightlife": 0.8,
        "heartland": 0.4,
        "other": 0.5,
    },
}


def bucket_severity(predicted_shortage: float, thresholds: dict) -> str:
    """Classify predicted shortage into low/moderate/high/severe."""
    return classify_severity_bucket(predicted_shortage, thresholds)


def candidate_actions_for_severity(severity_bucket: str) -> list[str]:
    """Return the allowed action set for a severity bucket."""
    if severity_bucket == "low":
        return ["monitor"]
    if severity_bucket == "moderate":
        return ["monitor", "driver_comms", "rebalance", "incentive"]
    return ["driver_comms", "rebalance", "incentive", "rebalance_plus_incentive", "ops_alert"]


def assign_risk(score: float) -> str:
    """Map depletion score into low/medium/high compatibility levels."""
    if score >= RISK_HIGH:
        return "high"
    if score >= RISK_MED:
        return "medium"
    return "low"


def recommendation_id(zone_id: int, timestamp: str, action: str) -> str:
    raw = f"{zone_id}|{timestamp}|{action}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def priority_for_action(action: str, risk_level: str, severity_bucket: str) -> str:
    if action == "ops_alert":
        return "critical"
    if severity_bucket in {"severe", "high"} or risk_level == "high":
        return "high"
    if severity_bucket == "moderate" or risk_level == "medium":
        return "medium"
    return "low"


def action_text(action: str, zone_name: str) -> tuple[str, str]:
    if action == "driver_comms":
        return (
            f"Recommend a driver-comms nudge for {zone_name}; ops lead approval required before any message is sent.",
            "Low-cost supply activation through a human-approved driver communication.",
        )
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


def root_cause_from_row(row: pd.Series) -> str:
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


def governance_for_action(action: str, config: dict | None = None) -> dict:
    """Return PRD feasibility-tier metadata for an action."""
    configured = (config or {}).get("action_governance", {})
    merged = {**DEFAULT_ACTION_GOVERNANCE, **configured}
    governance = merged.get(action, DEFAULT_ACTION_GOVERNANCE["monitor"])
    return {
        "tier": int(governance.get("tier", 0)),
        "label": str(governance.get("label", DEFAULT_ACTION_GOVERNANCE["monitor"]["label"])),
        "requires_approver": str(governance.get("requires_approver", "none")),
    }


def _infer_zone_type(row: pd.Series) -> str:
    """Infer a coarse commercial zone type from currently available fields."""
    zone_name = str(row.get("zone_name", "")).lower()
    region = str(row.get("region", "")).lower()
    if any(token in zone_name for token in ("changi", "airport")):
        return "airport"
    if any(token in zone_name for token in ("orchard", "marina", "downtown", "raffles", "shenton", "cbd")):
        return "cbd"
    if any(token in zone_name for token in ("clarke quay", "boat quay", "robertson", "geylang")):
        return "nightlife"
    if region in {"central", "downtown core"}:
        return "cbd"
    if region in {"east", "west", "north", "north-east", "northeast"}:
        return "heartland"
    return "other"


def stressor_intensity_from_row(row: pd.Series) -> float:
    """Composite stressor intensity used by the ROI opportunity bridge."""
    pressure = float(row.get("demand_pressure_score", 0.0) or 0.0)
    imbalance = float(row.get("imbalance_score", 0.0) or 0.0)
    depletion = float(row.get("depletion_risk_score", row.get("delay_risk_score", 0.0)) or 0.0)
    rainfall = min(float(row.get("rainfall_mm", 0.0) or 0.0) / 10.0, 1.0)
    if bool(row.get("is_raining", False)):
        rainfall = max(rainfall, 0.5)
    return round(max(0.0, min((0.35 * pressure) + (0.30 * imbalance) + (0.25 * depletion) + (0.10 * rainfall), 1.0)), 4)


def estimated_recoverable_opportunity(row: pd.Series, config: dict | None = None) -> float:
    """
    Estimate illustrative recoverable opportunity, not expected revenue.

    Formula follows the PRD bridge:
    depletion_minutes * zone_commercial_weight * stressor_intensity *
    historical_supply_gap * margin_assumption.
    """
    roi_config = {**DEFAULT_ROI_BRIDGE, **((config or {}).get("roi_bridge", {}) or {})}
    weights = {**DEFAULT_ROI_BRIDGE["zone_commercial_weight"], **(roi_config.get("zone_commercial_weight", {}) or {})}
    zone_type = _infer_zone_type(row)
    depletion_minutes = float(row.get("depletion_minutes", roi_config.get("default_depletion_minutes", 30)) or 30)
    margin = float(roi_config.get("margin_assumption_sgd", 6.0) or 6.0)
    baseline = max(float(row.get("baseline_supply", row.get("taxi_count", 1.0)) or 1.0), 1.0)
    current = max(float(row.get("taxi_count", 0.0) or 0.0), 0.0)
    observed_gap = max(baseline - current, 0.0)
    predicted_gap = max(float(row.get("predicted_shortage", 0.0) or 0.0), 0.0) * baseline
    historical_supply_gap = max(observed_gap, predicted_gap, float(row.get("estimated_shortage_reduction", 0.0) or 0.0) * 10.0)
    opportunity = depletion_minutes * float(weights.get(zone_type, weights["other"])) * stressor_intensity_from_row(row) * historical_supply_gap * margin
    return round(max(opportunity, 0.0), 4)


def score_confidence_interval(
    depletion_risk_score: float,
    depletion_rate_1h: float,
    persistence_count: int,
) -> tuple[float, float]:
    """
    Compute a heuristic [p10, p90] confidence interval around the risk score.

    Width is widened when:
    - The depletion rate is volatile (high rate = uncertain short-term trajectory).
    - The zone has low persistence (score is new / unstable).

    This is NOT a statistically derived interval — it is a transparent
    heuristic that communicates model uncertainty to ops analysts without
    requiring bootstrap or ensemble infrastructure.
    """
    # Base half-width: wider for mid-range scores (most uncertain), tighter at extremes
    mid_distance = 1.0 - abs(depletion_risk_score - 0.5) * 2   # 0 at extremes, 1 at 0.5
    base_hw = 0.08 + 0.10 * mid_distance

    # Volatility adjustment: rapid depletion → harder to predict → wider interval
    volatility_hw = min(float(depletion_rate_1h), 0.5) * 0.12

    # Stability adjustment: new elevations (persistence=1) → narrower confidence
    stability_adj = 0.04 if int(persistence_count) <= 1 else 0.0

    half_width = base_hw + volatility_hw - stability_adj
    p10 = round(max(0.0, depletion_risk_score - half_width), 3)
    p90 = round(min(1.0, depletion_risk_score + half_width), 3)
    return p10, p90


def counterfactual_no_action_risk(
    depletion_risk_score: float,
    depletion_rate_1h: float,
    demand_pressure_score: float,
    horizon_hours: int = 2,
) -> float:
    """
    Estimate what the depletion risk score will be in horizon_hours if no
    action is taken, by extrapolating the current depletion trend forward.

    Logic:
    - If supply is actively depleting (rate > 0), risk escalates.
    - Pressure amplifies the escalation when rate is moderate.
    - Capped at 1.0; floored at current score (risk can't spontaneously fall
      without intervention or a natural recovery — which we cannot predict).
    """
    rate = max(float(depletion_rate_1h), 0.0)
    pressure_amp = 1.0 + max(float(demand_pressure_score) - 0.4, 0.0) * 0.5
    escalation = rate * pressure_amp * horizon_hours * 0.35
    projected = float(depletion_risk_score) + escalation
    return round(min(max(projected, float(depletion_risk_score)), 1.0), 3)


def trigger_condition_from_row(row: pd.Series) -> str:
    severity = str(row.get("severity_bucket", "low") or "low")
    shortage = float(row.get("predicted_shortage", 0.0) or 0.0)
    pressure = float(row.get("demand_pressure_score", 0.0) or 0.0)
    persistence = int(row.get("persistence_count", 0) or 0)
    return (
        f"{severity} shortage risk; predicted shortage {shortage:.2f}, "
        f"pressure {pressure:.2f}, persistence {persistence} window(s)"
    )


def build_recommendation_card(row: pd.Series, config: dict | None = None) -> dict:
    """Convert a scored zone row into a recommendation card payload."""
    zone_id = int(row.get("zone_id", 0) or 0)
    zone_name = str(row.get("zone_name", "this zone"))
    region = str(row.get("region", ""))
    score = float(row.get("delay_risk_score", 0.0) or 0.0)
    risk_level = str(row.get("risk_level", assign_risk(score)))
    severity_bucket = str(row.get("severity_bucket", "low") or "low")
    recommended_action = str(row.get("recommended_action", "monitor") or "monitor")
    recommendation, expected_impact = action_text(recommended_action, zone_name)
    priority = priority_for_action(recommended_action, risk_level, severity_bucket)
    predicted_shortage = float(row.get("predicted_shortage", 0.0) or 0.0)
    estimated_shortage_reduction = float(row.get("estimated_shortage_reduction", 0.0) or 0.0)
    neighbor_surplus = float(row.get("neighbor_surplus", 0.0) or 0.0)
    confidence = round(min(max(score * 0.85 + predicted_shortage * 0.15, 0.25), 0.99), 2)
    snapshot_ts = str(row.get("timestamp", datetime.now(timezone.utc).isoformat()))
    action_reason = str(row.get("action_reason", "") or "")
    root_cause = root_cause_from_row(row)
    governance = governance_for_action(recommended_action, config)
    expected_supply_uplift = round(estimated_shortage_reduction * 10, 4)
    opportunity = estimated_recoverable_opportunity(row, config)
    estimated_cost = round(float(row.get("estimated_action_cost", 0.0) or 0.0), 4)
    opportunity_ratio = round(opportunity / max(estimated_cost, 0.1), 4)

    depletion_rate = float(row.get("depletion_rate_1h", 0.0) or 0.0)
    demand_pressure = float(row.get("demand_pressure_score", 0.0) or 0.0)
    persistence = int(row.get("persistence_count", 0) or 0)
    ci_p10, ci_p90 = score_confidence_interval(score, depletion_rate, persistence)
    projected_no_action_2h = counterfactual_no_action_risk(
        score, depletion_rate, demand_pressure, horizon_hours=2
    )

    return {
        "recommendation_id": recommendation_id(zone_id, snapshot_ts, recommended_action),
        "zone_id": zone_id,
        "zone_name": zone_name,
        "region": region,
        "risk_level": risk_level,
        "delay_risk_score": round(score, 4),
        "depletion_risk_score": round(float(row.get("depletion_risk_score", score) or score), 4),
        "risk_score_p10": ci_p10,
        "risk_score_p90": ci_p90,
        "projected_risk_no_action_2h": projected_no_action_2h,
        "demand_pressure_score": round(demand_pressure, 4),
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
        "action_tier": governance["tier"],
        "action_tier_label": governance["label"],
        "requires_approver": governance["requires_approver"],
        "trigger_condition": trigger_condition_from_row(row),
        "estimated_action_cost": estimated_cost,
        "estimated_shortage_reduction": round(estimated_shortage_reduction, 4),
        "budget_remaining": round(float(row.get("budget_remaining", 0.0) or 0.0), 4),
        "expected_supply_uplift": expected_supply_uplift,
        "estimated_recoverable_opportunity": opportunity,
        "opportunity_ratio": opportunity_ratio,
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
        "estimated_cost_sgd": estimated_cost,
        "expected_supply_response_30m": expected_supply_uplift,
        "expected_recovery_probability": round(max(predicted_shortage - estimated_shortage_reduction, 0.0), 4),
        "expected_roi": opportunity_ratio,
        "decision_objective": "reliability_first",
        "winning_reason": action_reason,
        "constraints_triggered": str(row.get("constraints_triggered", "[]") or "[]"),
        "alternative_actions": json.dumps([]),
    }


def generate_recommendations(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Generate recommendation cards directly from intervention-enhanced predictions."""
    from backend.intervention import load_intervention_config

    config = load_intervention_config()
    rows = [build_recommendation_card(row, config) for _, row in predictions_df.iterrows()]
    df = pd.DataFrame(rows)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUTS_DIR / "recommended_actions.csv", index=False)
    return df
