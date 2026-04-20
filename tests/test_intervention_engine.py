"""Dedicated tests for intervention state and selector behavior."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.intervention.action_selector import select_action
from backend.intervention.state_tracker import load_state, save_state


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
    "daily_budget": 10.0,
    "persistence_threshold": 2,
    "surplus_buffer": 0.2,
    "net_value_weights": {"shortage_reduction": 1.0, "cost": 0.35, "ops_alert_penalty": 0.6},
}


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


def test_selector_prefers_monitor_for_low_shortage():
    decision = select_action(
        predicted_shortage=0.2,
        persistence_count=0,
        neighbor_surplus=0.0,
        remaining_budget=10.0,
        last_actions_for_zone={},
        config=CONFIG,
        now=datetime.now(timezone.utc),
    )
    assert decision.recommended_action == "monitor"


def test_selector_uses_cooldown_aware_next_best_action():
    now = datetime.now(timezone.utc)
    decision = select_action(
        predicted_shortage=0.75,
        persistence_count=3,
        neighbor_surplus=1.0,
        remaining_budget=10.0,
        last_actions_for_zone={
            "rebalance_plus_incentive": (now - timedelta(minutes=30)).isoformat(),
            "incentive": (now - timedelta(minutes=10)).isoformat(),
        },
        config=CONFIG,
        now=now,
    )
    assert decision.recommended_action == "rebalance"
