import pandas as pd

from backend.modeling.demand_pressure import score_demand_pressure
from backend.modeling.imbalance import (
    classify_imbalance_level,
    composite_imbalance_score,
    normalized_difference_imbalance_score,
    ratio_imbalance_score,
)
from backend.recommendations.baseline_policy import baseline_policy_reason, classify_policy_action
from backend.recommendations.evaluation import assign_holdout_bucket, compare_policy_outcomes


def test_demand_pressure_score_is_bounded():
    df = pd.DataFrame({
        "hour_of_day": [8, 14, 18],
        "day_of_week": [0, 2, 5],
        "is_weekend": [False, False, True],
        "rainfall_mm": [0.0, 3.0, 12.0],
        "rain_intensity": [0, 2, 3],
        "congestion_ratio": [1.0, 1.3, 1.6],
        "train_disruption_flag": [0, 0, 1],
    })
    scores = score_demand_pressure(df)
    assert scores.between(0.0, 1.0).all()


def test_ratio_imbalance_avoids_divide_by_zero():
    score = ratio_imbalance_score(0.8, current_supply=0, supply_baseline=0)
    assert 0.0 <= score <= 1.0


def test_normalized_difference_imbalance_is_bounded():
    score = normalized_difference_imbalance_score(0.9, current_supply=5, supply_baseline=40)
    assert 0.0 <= score <= 1.0


def test_composite_imbalance_classification():
    score = composite_imbalance_score(0.85, current_supply=8, supply_baseline=40)
    assert classify_imbalance_level(score) in {"low", "medium", "high"}


def test_baseline_policy_maps_to_expected_actions():
    assert classify_policy_action(0.2, 0.2, 40) == "no_action"
    assert classify_policy_action(0.5, 0.3, 20) == "watchlist"
    assert classify_policy_action(0.8, 0.7, 10) == "intervention"


def test_baseline_policy_reason_mentions_heuristic():
    reason = baseline_policy_reason(0.8, 0.2, 12)
    assert "Heuristic baseline policy" in reason


def test_holdout_bucket_assignment_is_stable():
    bucket_a = assign_holdout_bucket(7, "2026-04-20T10:00:00+00:00")
    bucket_b = assign_holdout_bucket(7, "2026-04-20T10:00:00+00:00")
    assert bucket_a == bucket_b


def test_compare_policy_outcomes_returns_gap():
    summary = compare_policy_outcomes([
        {"evaluation_bucket": "treatment", "outcome": "recovered"},
        {"evaluation_bucket": "treatment", "outcome": "improved"},
        {"evaluation_bucket": "holdout", "outcome": "unchanged"},
    ])
    assert summary["treatment_count"] == 2
    assert summary["holdout_count"] == 1
    assert "recovery_rate_gap" in summary
