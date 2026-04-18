"""
Promotion Gate
==============
Decides whether a candidate model is safe to promote to active.

Checks (all must pass):
    1. Performance check  – F1 / AUC must exceed baseline thresholds
    2. Regression check   – no metric must regress > 5% vs active model
    3. Schema check       – candidate feature schema must match active schema
    4. Integration test   – score a small fixture; output shape + value range correct
    5. Stability check    – prediction variance must not be 3× worse than active

Result:
    PromotionResult(passed=True/False, checks={check: pass/fail}, reason=str)
"""

import json
import pickle
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from backend.registry import model_registry as registry
from backend.training.evaluator import compare_to_active

REGISTRY_DIR = Path("data/registry")
MODELS_DIR   = REGISTRY_DIR / "models"

MIN_F1  = 0.50
MIN_AUC = 0.65
MAX_VARIANCE_RATIO = 3.0


@dataclass
class PromotionResult:
    passed: bool
    checks: dict[str, bool] = field(default_factory=dict)
    reason: str = ""


def run_gate(candidate_version_id: str) -> PromotionResult:
    """Run all promotion checks. Returns PromotionResult."""
    checks: dict[str, bool] = {}
    reasons: list[str] = []

    candidate_dir     = MODELS_DIR / candidate_version_id
    candidate_metrics = _load_json(candidate_dir / "metrics.json")
    candidate_schema  = _load_json(candidate_dir / "feature_schema.json")

    # 1. Performance thresholds
    f1  = candidate_metrics.get("f1",      0.0)
    auc = candidate_metrics.get("roc_auc", 0.0)
    perf_pass = (f1 >= MIN_F1) and (auc >= MIN_AUC)
    checks["performance"] = perf_pass
    if not perf_pass:
        reasons.append(f"performance below threshold (F1={f1:.3f} min={MIN_F1}, AUC={auc:.3f} min={MIN_AUC})")

    # 2. Regression vs active
    active_meta = registry.get_active_version_meta()
    if active_meta:
        active_metrics = {k: active_meta.get(k, 0.0) for k in ["precision","recall","f1","roc_auc"]}
        comparison = compare_to_active(candidate_metrics, active_metrics)
        regression_pass = comparison["all_pass"]
    else:
        regression_pass = True
    checks["regression"] = regression_pass
    if not regression_pass:
        reasons.append("metric regression > 5% vs active model")

    # 3. Feature schema match — only fail if features are removed; new features are ok
    reg_data  = registry._load_registry()
    active_id = reg_data.get("active_version")
    if active_id:
        active_schema = _load_json(MODELS_DIR / active_id / "feature_schema.json")
        cand_cols   = set(candidate_schema.get("columns", []))
        active_cols = set(active_schema.get("columns", []))
        removed = active_cols - cand_cols
        schema_pass = len(removed) == 0
        if not schema_pass:
            reasons.append(f"schema regression: features removed={removed}")
    else:
        schema_pass = True
    checks["schema"] = schema_pass

    # 4. Integration test
    try:
        int_pass, int_reason = _run_integration_test(candidate_dir, candidate_schema)
    except Exception as e:
        int_pass, int_reason = False, str(e)
    checks["integration"] = int_pass
    if not int_pass:
        reasons.append(f"integration test failed: {int_reason}")

    # 5. Prediction stability — each model uses its own schema fixture
    if active_id and (MODELS_DIR / active_id / "model.pkl").exists():
        try:
            active_schema_for_stab = _load_json(MODELS_DIR / active_id / "feature_schema.json")
            stab_pass, stab_reason = _check_stability(
                candidate_dir, candidate_schema,
                MODELS_DIR / active_id, active_schema_for_stab,
            )
        except Exception as e:
            stab_pass, stab_reason = False, str(e)
    else:
        stab_pass, stab_reason = True, ""
    checks["stability"] = stab_pass
    if not stab_pass:
        reasons.append(f"stability check failed: {stab_reason}")

    passed = all(checks.values())
    reason = "; ".join(reasons) if reasons else "all checks passed"
    return PromotionResult(passed=passed, checks=checks, reason=reason)


def _load_json(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def _make_fixture(schema: dict, n: int = 100) -> pd.DataFrame:
    rng  = np.random.default_rng(0)
    cols = schema.get("columns", [])
    data = {}
    for col in cols:
        if col in ("is_weekend", "is_peak_hour", "is_raining", "is_holiday"):
            data[col] = rng.integers(0, 2, n)
        elif col in ("hour_sin", "hour_cos", "dow_sin", "dow_cos"):
            data[col] = rng.uniform(-1, 1, n).astype(np.float32)
        elif "ratio" in col or "proxy" in col:
            data[col] = rng.uniform(0, 1, n)
        elif "rainfall" in col:
            data[col] = rng.exponential(2, n)
        elif "lag" in col or "rolling" in col or "count" in col or "demand" in col:
            data[col] = rng.integers(0, 200, n).astype(float)
        elif "delay" in col:
            data[col] = rng.uniform(1, 60, n)
        else:
            data[col] = rng.integers(0, 24, n).astype(float)
    return pd.DataFrame(data)


def _run_integration_test(candidate_dir: Path, schema: dict) -> tuple[bool, str]:
    model_path = candidate_dir / "model.pkl"
    if not model_path.exists():
        return False, "model.pkl not found"
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    X = _make_fixture(schema, n=100)
    try:
        probs = model.predict_proba(X)[:, 1]
    except Exception as e:
        return False, f"predict_proba failed: {e}"
    if probs.shape[0] != 100:
        return False, f"expected 100 predictions, got {probs.shape[0]}"
    if not ((probs >= 0) & (probs <= 1)).all():
        return False, "predictions outside [0, 1]"
    return True, ""


def _check_stability(
    candidate_dir: Path, candidate_schema: dict,
    active_dir: Path, active_schema: dict,
) -> tuple[bool, str]:
    """Each model is scored on a fixture built from its own schema to avoid shape mismatches."""
    X_cand   = _make_fixture(candidate_schema, n=500)
    X_active = _make_fixture(active_schema, n=500)

    def _std(model_dir: Path, X: pd.DataFrame) -> float:
        with open(model_dir / "model.pkl", "rb") as f:
            m = pickle.load(f)
        return float(m.predict_proba(X)[:, 1].std())

    cand_std   = _std(candidate_dir, X_cand)
    active_std = _std(active_dir, X_active)
    if active_std == 0:
        return True, ""
    ratio = cand_std / active_std
    if ratio > MAX_VARIANCE_RATIO:
        return False, f"variance ratio {ratio:.2f} > {MAX_VARIANCE_RATIO}"
    return True, ""
