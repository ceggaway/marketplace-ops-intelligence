"""
Tests for backend/scoring/batch_scorer.py and backend/recommendations/engine.py

Covers:
- _assign_risk_level() bucketing thresholds (unchanged)
- _generate_explanation_tag() produces correct tags for supply depletion signals
- generate_recommendations() returns correct priority for each risk level
- Recommendation action rules: rain → surge, low supply → incentive, high → reallocation
"""

import pandas as pd
import pytest

from backend.scoring.batch_scorer import _assign_risk_level, _generate_explanation_tag
from backend.recommendations.engine import _assign_risk, _build_recommendation


# ── _assign_risk_level ────────────────────────────────────────────────────────

@pytest.mark.parametrize("score,expected", [
    (0.00, "low"),
    (0.39, "low"),
    (0.40, "medium"),
    (0.55, "medium"),
    (0.69, "medium"),
    (0.70, "high"),
    (0.99, "high"),
    (1.00, "high"),
])
def test_assign_risk_level(score, expected):
    assert _assign_risk_level(score) == expected


# ── _generate_explanation_tag ─────────────────────────────────────────────────

def _row(**kwargs) -> pd.Series:
    defaults = {
        "taxi_count":        50,
        "depletion_rate_1h": 0.0,
        "depletion_rate_3h": 0.0,
        "supply_vs_yesterday": 1.0,
        "is_raining":        False,
        "rainfall_mm":       0.0,
        "is_peak_hour":      False,
        "is_holiday":        False,
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


def test_explanation_normal_conditions():
    tag = _generate_explanation_tag(_row())
    assert tag == "normal conditions"


def test_explanation_rapid_depletion():
    tag = _generate_explanation_tag(_row(depletion_rate_1h=0.40))
    assert "rapid depletion" in tag


def test_explanation_sustained_depletion():
    tag = _generate_explanation_tag(_row(depletion_rate_3h=0.30))
    assert "sustained depletion" in tag


def test_explanation_critically_low_supply():
    tag = _generate_explanation_tag(_row(taxi_count=5))
    assert "critically low supply" in tag


def test_explanation_low_supply():
    tag = _generate_explanation_tag(_row(taxi_count=15))
    assert "low supply" in tag


def test_explanation_below_yesterday():
    tag = _generate_explanation_tag(_row(supply_vs_yesterday=0.5))
    assert "below yesterday baseline" in tag


def test_explanation_rain():
    tag = _generate_explanation_tag(_row(is_raining=True))
    assert "rain" in tag


def test_explanation_rain_via_rainfall_mm():
    tag = _generate_explanation_tag(_row(rainfall_mm=5.0))
    assert "rain" in tag


def test_explanation_peak_hour():
    tag = _generate_explanation_tag(_row(is_peak_hour=True))
    assert "peak hour" in tag


def test_explanation_holiday():
    tag = _generate_explanation_tag(_row(is_holiday=True))
    assert "public holiday" in tag


def test_explanation_multiple_signals():
    tag = _generate_explanation_tag(_row(
        depletion_rate_1h=0.45,
        is_raining=True,
        is_peak_hour=True,
    ))
    assert "rapid depletion" in tag
    assert "rain" in tag
    assert "peak hour" in tag
    assert " + " in tag


# ── _build_recommendation ─────────────────────────────────────────────────────

def _rec_row(**kwargs) -> pd.Series:
    defaults = {
        "zone_id":            1,
        "zone_name":          "Orchard",
        "region":             "Central",
        "delay_risk_score":   0.5,
        "risk_level":         "medium",
        "depletion_rate_1h":  0.10,
        "taxi_count":         20,
        "supply_vs_yesterday": 1.0,
        "explanation_tag":    "",
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


def test_high_risk_recommendation_priority():
    rec = _build_recommendation(_rec_row(delay_risk_score=0.85, risk_level="high"), {}, {})
    assert rec["priority"] in ("high", "critical")


def test_medium_risk_recommendation_priority():
    rec = _build_recommendation(_rec_row(delay_risk_score=0.55, risk_level="medium"), {}, {})
    assert rec["priority"] == "medium"


def test_low_risk_no_action():
    rec = _build_recommendation(_rec_row(delay_risk_score=0.2, risk_level="low"), {}, {})
    assert rec["priority"] == "low"
    assert "no action" in rec["recommendation"].lower()


def test_rain_triggers_surge_recommendation():
    rec = _build_recommendation(_rec_row(
        delay_risk_score=0.85,
        risk_level="high",
        explanation_tag="rain + peak hour",
    ), {}, {})
    assert "surge" in rec["recommendation"].lower() or "rain" in rec["recommendation"].lower()


def test_high_risk_fast_depletion_triggers_action():
    rec = _build_recommendation(_rec_row(
        delay_risk_score=0.85,
        risk_level="high",
        depletion_rate_1h=0.45,
        taxi_count=15,
        explanation_tag="rapid depletion + low supply",
    ), {}, {})
    # Fast depletion branch: should mention incentive or escalation
    rec_lower = rec["recommendation"].lower()
    assert "incentive" in rec_lower or "escalat" in rec_lower or "notification" in rec_lower


def test_recommendation_has_required_keys():
    rec = _build_recommendation(_rec_row(), {}, {}, policy_records=[])
    required = ["zone_id", "zone_name", "risk_level", "recommendation",
                "expected_impact", "confidence", "priority", "last_updated",
                "eta_minutes", "intervention_window", "adjacent_risk_zones",
                "network_warning", "root_cause", "alternative_actions",
                "recommendation_id", "action_type", "expected_recovery_rate",
                "expected_improvement_rate", "estimated_score_delta",
                "confidence_band", "evidence_count", "follow_rate",
                "policy_rank_reason"]
    for key in required:
        assert key in rec


def test_confidence_in_range():
    for score in [0.1, 0.5, 0.85, 0.99]:
        risk = _assign_risk(score)
        rec  = _build_recommendation(_rec_row(delay_risk_score=score, risk_level=risk), {}, {}, policy_records=[])
        assert 0.0 <= rec["confidence"] <= 1.0


def test_recommendation_policy_defaults_when_no_history():
    rec = _build_recommendation(_rec_row(delay_risk_score=0.82, risk_level="high"), {}, {}, policy_records=[])
    assert rec["confidence_band"] == "low"
    assert rec["evidence_count"] == 0
    assert "rule-based default" in rec["policy_rank_reason"].lower()
