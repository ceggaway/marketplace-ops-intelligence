"""Compatibility wrapper around the intervention recommendation serializer."""

from pathlib import Path

from backend.intervention import policy as intervention_policy
from backend.paths import outputs_dir

OUTPUTS_DIR = outputs_dir()

_assign_risk = intervention_policy.assign_risk


def _build_recommendation(row, risk_map=None, baselines=None, policy_records=None, critical_count=5):
    del risk_map, baselines, policy_records, critical_count
    return intervention_policy.build_recommendation_card(row)


def generate_recommendations(predictions_df):
    original = intervention_policy.OUTPUTS_DIR
    intervention_policy.OUTPUTS_DIR = OUTPUTS_DIR
    try:
        return intervention_policy.generate_recommendations(predictions_df)
    finally:
        intervention_policy.OUTPUTS_DIR = original


__all__ = ["OUTPUTS_DIR", "_assign_risk", "_build_recommendation", "generate_recommendations"]
