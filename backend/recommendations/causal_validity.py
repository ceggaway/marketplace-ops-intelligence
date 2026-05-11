"""Validity gates for synthetic-control intervention evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


CONFIG_PATH = Path("config/config.yaml")


@dataclass(frozen=True)
class ValidityGate:
    gate: str
    status: str
    threshold: float | int | str
    observed_value: float | int | str


def load_synthetic_control_gates(config_path: Path = CONFIG_PATH) -> dict:
    """Load YAML-configured synthetic-control validity thresholds."""
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return raw.get("causal_evaluation", {}).get("synthetic_control_gates", {})


def evaluate_synthetic_control_validity(
    *,
    normalized_rmse: float | None,
    mape: float | None,
    donor_pool_size: int,
    placebo_percentile: float | None,
    post_period_min: int,
    action_tier: int,
    spillover_exclusion_applied: bool,
    config: dict | None = None,
) -> dict:
    """
    Evaluate PRD-defined gates before exposing a causal effect estimate.

    The caller should suppress effect estimates when ``all_gates_passed`` is
    false and display ``suppression_reason`` plus the gate diagnostics.
    """
    thresholds = config or load_synthetic_control_gates()
    pre_fit_cfg = thresholds.get("pre_period_fit", {})
    nrmse_max = float(pre_fit_cfg.get("normalized_rmse_max", 0.25))
    mape_max = float(pre_fit_cfg.get("mape_max", 0.15))
    donor_min = int(thresholds.get("donor_pool_size_min", 10))
    placebo_min = float(thresholds.get("placebo_percentile_min", 0.90))
    post_cfg = thresholds.get("minimum_post_period_min", {})
    min_post = int(post_cfg.get("tier_3_4" if int(action_tier) >= 3 else "tier_1_2", 60 if int(action_tier) >= 3 else 30))
    spillover_threshold = str(thresholds.get("spillover_exclusion", "same_subzone_plus_h3_k_ring_1"))

    nrmse_pass = normalized_rmse is not None and float(normalized_rmse) < nrmse_max
    mape_pass = mape is not None and float(mape) < mape_max
    gates = [
        ValidityGate(
            gate="pre_period_fit",
            status="pass" if nrmse_pass or mape_pass else "fail",
            threshold=f"normalized_rmse < {nrmse_max} OR mape < {mape_max}",
            observed_value=f"normalized_rmse={normalized_rmse}, mape={mape}",
        ),
        ValidityGate(
            gate="donor_pool_size",
            status="pass" if int(donor_pool_size) >= donor_min else "fail",
            threshold=donor_min,
            observed_value=int(donor_pool_size),
        ),
        ValidityGate(
            gate="spillover_exclusion",
            status="pass" if spillover_exclusion_applied else "fail",
            threshold=spillover_threshold,
            observed_value="applied" if spillover_exclusion_applied else "not_applied",
        ),
        ValidityGate(
            gate="placebo_test",
            status="pass" if placebo_percentile is not None and float(placebo_percentile) >= placebo_min else "fail",
            threshold=placebo_min,
            observed_value="not_evaluated" if placebo_percentile is None else round(float(placebo_percentile), 4),
        ),
        ValidityGate(
            gate="minimum_post_period",
            status="pass" if int(post_period_min) >= min_post else "fail",
            threshold=min_post,
            observed_value=int(post_period_min),
        ),
    ]
    gate_dicts = [gate.__dict__ for gate in gates]
    failing = [gate for gate in gate_dicts if gate["status"] == "fail"]
    return {
        "validity_gates": gate_dicts,
        "all_gates_passed": not failing,
        "suppression_reason": None if not failing else f"insufficient counterfactual quality: {failing[0]['gate']}",
    }
