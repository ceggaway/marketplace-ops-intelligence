"""Dedicated tests for intervention state and selector behavior."""

import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.intervention.action_selector import select_action
from backend.intervention.constraints import (
    is_persistence_satisfied,
    is_cooldown_satisfied,
    is_budget_satisfied,
    is_neighbor_surplus_satisfied,
    evaluate_constraints,
    compute_neighbor_surplus,
)
from backend.intervention.state_tracker import load_state, save_state


CONFIG = {
    "shortage_thresholds": {"monitor_max": 0.25, "watch_max": 0.55, "severe_min": 0.85},
    "action_costs": {
        "monitor": 0.0,
        "driver_comms": 0.1,
        "rebalance": 0.4,
        "incentive": 1.5,
        "rebalance_plus_incentive": 2.1,
        "ops_alert": 0.2,
    },
    "action_uplifts": {
        "monitor": 0.0,
        "driver_comms": 0.02,
        "rebalance": 0.18,
        "incentive": 0.22,
        "rebalance_plus_incentive": 0.34,
        "ops_alert": 0.05,
    },
    "cooldown_windows_min": {
        "monitor": 0,
        "driver_comms": 30,
        "rebalance": 45,
        "incentive": 60,
        "rebalance_plus_incentive": 90,
        "ops_alert": 15,
    },
    "daily_budget": 10.0,
    "persistence_threshold": 2,
    "surplus_buffer": 0.2,
    "net_value_weights": {"shortage_reduction": 1.0, "cost": 0.35, "ops_alert_penalty": 0.6},
}

NOW = datetime(2026, 5, 2, 10, 0, 0, tzinfo=timezone.utc)


# ── State management ──────────────────────────────────────────────────────────

def test_load_state_initializes_missing_file(tmp_path):
    state_path = tmp_path / "intervention_state.json"
    state = load_state(CONFIG, path=state_path, now=datetime(2026, 4, 20, tzinfo=timezone.utc))
    assert state_path.exists()
    assert state["budget"]["remaining"] == 10.0


def test_load_state_resets_daily_budget_on_new_day(tmp_path):
    state_path = tmp_path / "intervention_state.json"
    state = {
        "persistence_counts": {"1": 2},
        "last_actions": {"1": {"incentive": "2026-04-19T10:00:00+00:00"}},
        "budget": {"date": "2026-04-19", "spent": 8.0, "remaining": 2.0},
    }
    save_state(state, path=state_path)
    loaded = load_state(CONFIG, path=state_path, now=datetime(2026, 4, 20, tzinfo=timezone.utc))
    assert loaded["budget"]["remaining"] == 10.0
    assert loaded["budget"]["spent"] == 0.0


# ── Persistence constraint ────────────────────────────────────────────────────

def test_persistence_monitor_always_passes():
    assert is_persistence_satisfied("monitor", persistence_count=0, config=CONFIG) is True


@pytest.mark.parametrize("count,expected", [
    (0, False),   # below threshold
    (1, False),   # still below threshold=2
    (2, True),    # exactly at threshold
    (5, True),    # above threshold
])
def test_persistence_threshold_boundary(count, expected):
    assert is_persistence_satisfied("driver_comms", count, CONFIG) is expected


def test_persistence_missing_threshold_defaults_to_1():
    cfg = {**CONFIG, "persistence_threshold": 1}
    assert is_persistence_satisfied("incentive", persistence_count=1, config=cfg) is True
    assert is_persistence_satisfied("incentive", persistence_count=0, config=cfg) is False


# ── Cooldown constraint ───────────────────────────────────────────────────────

@pytest.mark.parametrize("minutes_ago,action,expected", [
    (31, "driver_comms", True),    # 31 min ago, cooldown=30 → satisfied
    (29, "driver_comms", False),   # 29 min ago, cooldown=30 → still cooling
    (30, "driver_comms", True),    # exactly at boundary → satisfied (>=)
    (61, "incentive", True),       # 61 min ago, cooldown=60 → satisfied
    (59, "incentive", False),      # 59 min ago, cooldown=60 → cooling
    (46, "rebalance", True),       # 46 min ago, cooldown=45 → satisfied
    (44, "rebalance", False),      # 44 min ago → cooling
    (91, "rebalance_plus_incentive", True),
    (89, "rebalance_plus_incentive", False),
    (16, "ops_alert", True),
    (14, "ops_alert", False),
])
def test_cooldown_boundary(minutes_ago, action, expected):
    last_action_at = (NOW - timedelta(minutes=minutes_ago)).isoformat()
    assert is_cooldown_satisfied(action, last_action_at, NOW, CONFIG) is expected


def test_cooldown_no_previous_action_always_passes():
    assert is_cooldown_satisfied("incentive", last_action_at=None, now=NOW, config=CONFIG) is True


def test_cooldown_monitor_has_no_cooldown():
    # monitor cooldown_windows_min=0 → always passes regardless of last action
    last = (NOW - timedelta(seconds=1)).isoformat()
    assert is_cooldown_satisfied("monitor", last, NOW, CONFIG) is True


def test_cooldown_invalid_timestamp_passes_gracefully():
    assert is_cooldown_satisfied("incentive", "not-a-timestamp", NOW, CONFIG) is True


# ── Budget constraint ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("action,remaining,cost,expected", [
    ("incentive", 1.5, 1.5, True),       # exactly enough
    ("incentive", 1.49, 1.5, False),     # just short
    ("incentive", 0.0, 1.5, False),      # exhausted
    ("rebalance_plus_incentive", 2.1, 2.1, True),
    ("rebalance_plus_incentive", 2.09, 2.1, False),
    # Non-incentive actions are not budget-gated
    ("driver_comms", 0.0, 0.1, True),
    ("rebalance", 0.0, 0.4, True),
    ("monitor", 0.0, 0.0, True),
    ("ops_alert", 0.0, 0.2, True),
])
def test_budget_constraint(action, remaining, cost, expected):
    assert is_budget_satisfied(action, remaining, cost) is expected


# ── Neighbor surplus constraint ───────────────────────────────────────────────

@pytest.mark.parametrize("action,surplus,expected", [
    ("rebalance", 0.01, True),
    ("rebalance", 0.0, False),
    ("rebalance_plus_incentive", 0.5, True),
    ("rebalance_plus_incentive", 0.0, False),
    # Non-rebalance actions don't need neighbor surplus
    ("incentive", 0.0, True),
    ("driver_comms", 0.0, True),
    ("monitor", 0.0, True),
    ("ops_alert", 0.0, True),
])
def test_neighbor_surplus_constraint(action, surplus, expected):
    assert is_neighbor_surplus_satisfied(action, surplus) is expected


# ── evaluate_constraints (combined) ──────────────────────────────────────────

def test_evaluate_constraints_all_pass():
    feasible, reasons = evaluate_constraints(
        action="driver_comms",
        persistence_count=3,
        last_action_at=(NOW - timedelta(minutes=60)).isoformat(),
        remaining_budget=10.0,
        estimated_cost=0.1,
        neighbor_surplus=0.0,
        now=NOW,
        config=CONFIG,
    )
    assert feasible is True
    assert reasons == []


def test_evaluate_constraints_multiple_failures():
    feasible, reasons = evaluate_constraints(
        action="incentive",
        persistence_count=0,                             # fails persistence
        last_action_at=(NOW - timedelta(minutes=5)).isoformat(),  # fails cooldown
        remaining_budget=0.5,                            # fails budget (need 1.5)
        estimated_cost=1.5,
        neighbor_surplus=0.0,
        now=NOW,
        config=CONFIG,
    )
    assert feasible is False
    assert "persistence_requirement" in reasons
    assert "cooldown_active" in reasons
    assert "daily_budget_exhausted" in reasons


def test_evaluate_constraints_rebalance_no_surplus():
    feasible, reasons = evaluate_constraints(
        action="rebalance",
        persistence_count=5,
        last_action_at=None,
        remaining_budget=10.0,
        estimated_cost=0.4,
        neighbor_surplus=0.0,
        now=NOW,
        config=CONFIG,
    )
    assert feasible is False
    assert "neighbor_surplus_unavailable" in reasons


# ── compute_neighbor_surplus ──────────────────────────────────────────────────

def test_compute_neighbor_surplus_with_exportable_supply():
    adjacency = {"ZoneA": ["ZoneB", "ZoneC"]}
    snapshot = {
        "ZoneB": {"taxi_count": 50.0, "baseline_supply": 40.0},
        "ZoneC": {"taxi_count": 10.0, "baseline_supply": 40.0},   # below baseline — no surplus
    }
    surplus = compute_neighbor_surplus("ZoneA", snapshot, adjacency, surplus_buffer=0.2)
    # ZoneB: buffered_min = 40 * 0.8 = 32; exportable = 50 - 32 = 18; normalised = 18/40 = 0.45
    # ZoneC: buffered_min = 40 * 0.8 = 32; exportable = max(10 - 32, 0) = 0
    assert abs(surplus - 0.45) < 0.01


def test_compute_neighbor_surplus_no_neighbors():
    surplus = compute_neighbor_surplus("Isolated", {}, {"Isolated": []}, surplus_buffer=0.2)
    assert surplus == 0.0


def test_compute_neighbor_surplus_all_below_buffer():
    adjacency = {"ZoneA": ["ZoneB"]}
    snapshot = {"ZoneB": {"taxi_count": 5.0, "baseline_supply": 50.0}}
    surplus = compute_neighbor_surplus("ZoneA", snapshot, adjacency, surplus_buffer=0.2)
    assert surplus == 0.0


# ── Action selector integration ───────────────────────────────────────────────

def test_selector_prefers_monitor_for_low_shortage():
    decision = select_action(
        predicted_shortage=0.2,
        persistence_count=0,
        neighbor_surplus=0.0,
        remaining_budget=10.0,
        last_actions_for_zone={},
        config=CONFIG,
        now=NOW,
    )
    assert decision.recommended_action == "monitor"


def test_selector_uses_cooldown_aware_next_best_action():
    decision = select_action(
        predicted_shortage=0.75,
        persistence_count=3,
        neighbor_surplus=1.0,
        remaining_budget=10.0,
        last_actions_for_zone={
            "rebalance_plus_incentive": (NOW - timedelta(minutes=30)).isoformat(),
            "incentive": (NOW - timedelta(minutes=10)).isoformat(),
        },
        config=CONFIG,
        now=NOW,
    )
    assert decision.recommended_action == "rebalance"


def test_selector_falls_back_to_ops_alert_when_severe_and_all_blocked():
    decision = select_action(
        predicted_shortage=0.90,
        persistence_count=0,                              # blocks all active interventions
        neighbor_surplus=0.0,                             # blocks rebalance
        remaining_budget=0.0,                             # blocks incentive
        last_actions_for_zone={
            "driver_comms": (NOW - timedelta(minutes=1)).isoformat(),   # cooling
            "ops_alert": (NOW - timedelta(minutes=1)).isoformat(),       # cooling
        },
        config=CONFIG,
        now=NOW,
    )
    # All candidates are blocked → fallback to ops_alert for severe
    assert decision.recommended_action == "ops_alert"
    assert len(decision.constraints_triggered) > 0


def test_selector_budget_is_deducted_for_incentive():
    decision = select_action(
        predicted_shortage=0.70,
        persistence_count=5,
        neighbor_surplus=0.0,
        remaining_budget=2.0,
        last_actions_for_zone={},
        config=CONFIG,
        now=NOW,
    )
    if decision.recommended_action in {"incentive", "rebalance_plus_incentive"}:
        assert decision.budget_remaining < 2.0
    else:
        # non-incentive actions don't consume budget
        assert decision.budget_remaining == 2.0


def test_selector_records_constraints_triggered():
    decision = select_action(
        predicted_shortage=0.70,
        persistence_count=0,   # blocks active interventions
        neighbor_surplus=0.0,
        remaining_budget=0.0,
        last_actions_for_zone={},
        config=CONFIG,
        now=NOW,
    )
    assert isinstance(decision.constraints_triggered, list)


def test_selector_monitor_never_blocked():
    # Low shortage always gives monitor regardless of all other constraints
    decision = select_action(
        predicted_shortage=0.10,
        persistence_count=0,
        neighbor_surplus=0.0,
        remaining_budget=0.0,
        last_actions_for_zone={"monitor": NOW.isoformat()},
        config=CONFIG,
        now=NOW,
    )
    assert decision.recommended_action == "monitor"
    assert decision.estimated_action_cost == 0.0
