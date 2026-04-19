import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd

from backend.recommendations import outcome_tracker
from backend.recommendations.policy_effectiveness import effectiveness_for_context, summarise_action_outcomes


def _sample_recs() -> pd.DataFrame:
    return pd.DataFrame([{
        "recommendation_id": "rec-1",
        "action_id": "mild_surge",
        "zone_id": 1,
        "zone_name": "Orchard",
        "priority": "high",
        "risk_level": "high",
        "delay_risk_score": 0.82,
        "recommendation": "Activate surge pricing immediately",
        "pricing_level": "mild",
        "incentive_level": "none",
        "push_level": "none",
        "root_cause": "weather",
        "intervention_window": "tight",
        "eta_minutes": 12,
        "adjacent_risk_zones": "Newton",
        "alternative_actions": "[]",
        "estimated_cost_sgd": 1.2,
        "expected_supply_response_30m": 3.5,
        "expected_recovery_probability": 0.44,
        "expected_recovery_rate": 0.4,
        "expected_improvement_rate": 0.7,
        "estimated_score_delta": -0.12,
        "expected_roi": 2.1,
        "decision_objective": "reliability_first",
        "winning_reason": "Best expected net value in context.",
        "constraints_triggered": "[]",
        "confidence_band": "medium",
        "evidence_count": 8,
        "follow_rate": 0.5,
        "policy_rank_reason": "same action and risk level",
    }])


def test_log_recommendations_persists_learning_context(tmp_path):
    log = tmp_path / "outcomes.jsonl"
    with patch.object(outcome_tracker, "OUTCOME_LOG", log):
        outcome_tracker.log_recommendations(_sample_recs())
    record = json.loads(log.read_text().strip())
    assert record["recommendation_id"] == "rec-1"
    assert record["action_id"] == "mild_surge"
    assert record["root_cause"] == "weather"
    assert record["intervention_window"] == "tight"
    assert record["estimated_cost_sgd"] == 1.2
    assert record["expected_supply_response_30m"] == 3.5
    assert record["followed_status"] is None


def test_record_feedback_updates_matching_recommendation(tmp_path):
    log = tmp_path / "outcomes.jsonl"
    log.write_text(json.dumps({
        "recommendation_id": "rec-1",
        "zone_id": 1,
        "score_at_time": 0.8,
        "outcome": None,
    }) + "\n")
    with patch.object(outcome_tracker, "OUTCOME_LOG", log):
        updated = outcome_tracker.record_feedback("rec-1", "followed", "ops", "dispatched")
    assert updated is not None
    stored = json.loads(log.read_text().strip())
    assert stored["followed_status"] == "followed"
    assert stored["followed_by"] == "ops"


def test_summarise_action_outcomes_prefers_followed_resolved():
    records = [
        {"followed_status": "followed", "outcome": "recovered", "score_at_time": 0.8, "score_after": 0.4, "supply_delta_30m": 4, "estimated_cost_sgd": 1.0},
        {"followed_status": "followed", "outcome": "improved", "score_at_time": 0.8, "score_after": 0.6, "supply_delta_30m": 2, "estimated_cost_sgd": 1.0},
        {"followed_status": "not_followed", "outcome": "worsened", "score_at_time": 0.8, "score_after": 0.9, "supply_delta_30m": -1, "estimated_cost_sgd": 2.0},
    ]
    stats = summarise_action_outcomes(records)
    assert stats["follow_rate"] > 0.5
    assert stats["recovery_rate"] > 0.0
    assert stats["estimated_score_delta"] < 0.0
    assert stats["expected_supply_response_30m"] > 0.0
    assert stats["expected_roi"] > 0.0


def test_effectiveness_for_context_falls_back_to_action_level():
    records = [
        {
            "action_type": "surge_pricing",
            "risk_level": "high",
            "root_cause": "weather",
            "intervention_window": "tight",
            "adjacent_risk_flag": True,
            "followed_status": "followed",
            "outcome": "recovered",
            "score_at_time": 0.8,
            "score_after": 0.4,
        },
        {
            "action_type": "surge_pricing",
            "risk_level": "medium",
            "root_cause": "unknown",
            "intervention_window": "moderate",
            "adjacent_risk_flag": False,
            "followed_status": "followed",
            "outcome": "improved",
            "score_at_time": 0.7,
            "score_after": 0.5,
        },
    ]
    stats = effectiveness_for_context(
        action_type="surge_pricing",
        risk_level="high",
        root_cause="structural_gap",
        intervention_window="ample",
        adjacent_risk_flag=False,
        records=records,
    )
    assert stats["action_type"] == "surge_pricing"
    assert stats["evidence_count"] >= 1
    assert "action" in stats["policy_rank_reason"]
    assert "driver_response_reason" in stats
