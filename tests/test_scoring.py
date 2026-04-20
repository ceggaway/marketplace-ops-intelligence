"""Scoring and recommendation tests for the intervention-backed pipeline."""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backend.intervention.action_selector import select_action
from backend.intervention.constraints import compute_neighbor_surplus, evaluate_constraints
from backend.intervention.state_tracker import (
    apply_action,
    get_last_actions,
    get_persistence_count,
    get_remaining_budget,
    initial_state,
    update_persistence,
)
from backend.modeling.shortage import classify_severity_bucket, compute_predicted_shortage
from backend.recommendations.engine import _assign_risk, _build_recommendation
from backend.scoring.batch_scorer import _assign_risk_level, _generate_explanation_tag


CONFIG = {
    "shortage_thresholds": {"monitor_max": 0.25, "watch_max": 0.55, "severe_min": 0.85},
    "action_costs": {
        "monitor": 0.0,
        "rebalance": 0.4,
        "incentive": 1.5,
        "rebalance_plus_incentive": 2.1,
        "ops_alert": 0.2,
    },
    "action_uplifts": {
        "monitor": 0.0,
        "rebalance": 0.18,
        "incentive": 0.22,
        "rebalance_plus_incentive": 0.34,
        "ops_alert": 0.05,
    },
    "cooldown_windows_min": {
        "monitor": 0,
        "rebalance": 45,
        "incentive": 60,
        "rebalance_plus_incentive": 90,
        "ops_alert": 15,
    },
    "daily_budget": 5.0,
    "persistence_threshold": 2,
    "surplus_buffer": 0.2,
    "net_value_weights": {"shortage_reduction": 1.0, "cost": 0.35, "ops_alert_penalty": 0.6},
}


@pytest.mark.parametrize("score,expected", [
    (0.00, "low"),
    (0.39, "low"),
    (0.40, "medium"),
    (0.69, "medium"),
    (0.70, "high"),
    (1.00, "high"),
])
def test_assign_risk_level(score, expected):
    assert _assign_risk_level(score) == expected


def _row(**kwargs) -> pd.Series:
    defaults = {
        "taxi_count": 50,
        "depletion_rate_1h": 0.0,
        "depletion_rate_3h": 0.0,
        "supply_vs_yesterday": 1.0,
        "is_raining": False,
        "rainfall_mm": 0.0,
        "is_peak_hour": False,
        "is_holiday": False,
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


def test_explanation_multiple_signals():
    tag = _generate_explanation_tag(_row(depletion_rate_1h=0.45, is_raining=True, is_peak_hour=True))
    assert "rapid depletion" in tag
    assert "rain" in tag
    assert "peak hour" in tag


def test_predicted_shortage_is_bounded():
    score = compute_predicted_shortage(0.9, current_taxi_count=0, depletion_risk_score=0.8, baseline_supply=0)
    assert 0.0 <= score <= 1.0


def test_severity_bucketing_uses_thresholds():
    assert classify_severity_bucket(0.2, CONFIG["shortage_thresholds"]) == "low"
    assert classify_severity_bucket(0.4, CONFIG["shortage_thresholds"]) == "moderate"
    assert classify_severity_bucket(0.7, CONFIG["shortage_thresholds"]) == "high"
    assert classify_severity_bucket(0.9, CONFIG["shortage_thresholds"]) == "severe"


def test_persistence_requirement_blocks_active_actions():
    feasible, reasons = evaluate_constraints(
        action="rebalance",
        persistence_count=1,
        last_action_at=None,
        remaining_budget=5.0,
        estimated_cost=0.4,
        neighbor_surplus=1.0,
        now=datetime.now(timezone.utc),
        config=CONFIG,
    )
    assert not feasible
    assert "persistence_requirement" in reasons


def test_cooldown_check_blocks_recent_action():
    now = datetime.now(timezone.utc)
    feasible, reasons = evaluate_constraints(
        action="incentive",
        persistence_count=2,
        last_action_at=(now - timedelta(minutes=15)).isoformat(),
        remaining_budget=5.0,
        estimated_cost=1.5,
        neighbor_surplus=1.0,
        now=now,
        config=CONFIG,
    )
    assert not feasible
    assert "cooldown_active" in reasons


def test_daily_budget_check_blocks_incentive_action():
    feasible, reasons = evaluate_constraints(
        action="rebalance_plus_incentive",
        persistence_count=2,
        last_action_at=None,
        remaining_budget=0.5,
        estimated_cost=2.1,
        neighbor_surplus=1.0,
        now=datetime.now(timezone.utc),
        config=CONFIG,
    )
    assert not feasible
    assert "daily_budget_exhausted" in reasons


def test_neighbor_surplus_feasibility():
    snapshot = {
        "Orchard": {"taxi_count": 20, "baseline_supply": 18},
        "Newton": {"taxi_count": 25, "baseline_supply": 15},
        "Novena": {"taxi_count": 14, "baseline_supply": 16},
    }
    adjacency = {"Orchard": ["Newton", "Novena"]}
    surplus = compute_neighbor_surplus("Orchard", snapshot, adjacency, surplus_buffer=0.2)
    assert surplus > 0


def test_fallback_to_ops_alert_when_severe_infeasible():
    decision = select_action(
        predicted_shortage=0.95,
        persistence_count=0,
        neighbor_surplus=0.0,
        remaining_budget=0.0,
        last_actions_for_zone={"rebalance": datetime.now(timezone.utc).isoformat()},
        config=CONFIG,
        now=datetime.now(timezone.utc),
    )
    assert decision.recommended_action == "ops_alert"


def test_best_action_selection_returns_best_feasible_action():
    decision = select_action(
        predicted_shortage=0.9,
        persistence_count=3,
        neighbor_surplus=1.2,
        remaining_budget=5.0,
        last_actions_for_zone={},
        config=CONFIG,
        now=datetime.now(timezone.utc),
    )
    assert decision.recommended_action in {"rebalance", "incentive", "rebalance_plus_incentive"}
    assert decision.estimated_shortage_reduction > 0


def test_state_tracker_updates_persistence_and_budget():
    state = initial_state(CONFIG)
    assert get_persistence_count(state, "1") == 0
    count = update_persistence(state, "1", True)
    assert count == 1
    state = apply_action(state, "1", "incentive", 1.5, CONFIG, now=datetime.now(timezone.utc))
    assert get_remaining_budget(state) == pytest.approx(3.5)
    assert "incentive" in get_last_actions(state, "1")


def _rec_row(**kwargs) -> pd.Series:
    defaults = {
        "zone_id": 1,
        "zone_name": "Orchard",
        "region": "Central",
        "timestamp": "2026-04-20T10:00:00+00:00",
        "delay_risk_score": 0.82,
        "depletion_risk_score": 0.82,
        "demand_pressure_score": 0.75,
        "imbalance_score": 0.68,
        "imbalance_level": "high",
        "policy_action": "intervention",
        "policy_reason": "heuristic baseline",
        "predicted_shortage": 0.88,
        "severity_bucket": "severe",
        "persistence_count": 3,
        "neighbor_surplus": 1.1,
        "recommended_action": "rebalance_plus_incentive",
        "action_reason": "Selected rebalance_plus_incentive because it maximized deterministic net value under current constraints.",
        "estimated_action_cost": 2.1,
        "estimated_shortage_reduction": 0.2992,
        "budget_remaining": 2.9,
        "risk_level": "high",
        "taxi_count": 18,
        "depletion_rate_1h": 0.35,
        "supply_vs_yesterday": 0.92,
        "explanation_tag": "rapid depletion + peak hour",
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


def test_recommendation_has_required_intervention_keys():
    rec = _build_recommendation(_rec_row(), {}, {})
    required = [
        "recommendation_id",
        "recommended_action",
        "action_reason",
        "predicted_shortage",
        "severity_bucket",
        "persistence_count",
        "neighbor_surplus",
        "estimated_action_cost",
        "estimated_shortage_reduction",
        "budget_remaining",
        "recommendation",
        "priority",
    ]
    for key in required:
        assert key in rec


def test_recommendation_priority_and_confidence_are_valid():
    rec = _build_recommendation(_rec_row(recommended_action="ops_alert", severity_bucket="severe"), {}, {})
    assert rec["priority"] == "critical"
    assert 0.0 <= rec["confidence"] <= 1.0


def test_assign_risk_helper():
    assert _assign_risk(0.2) == "low"
    assert _assign_risk(0.5) == "medium"
    assert _assign_risk(0.9) == "high"
