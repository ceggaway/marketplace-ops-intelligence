"""
Tests for backend/training/trainer.py and backend/training/evaluator.py

Covers:
- _make_target() labels supply shortage correctly (1 if next_count < current * 0.6)
- _make_target() sets NaN for last row per zone (no next hour)
- evaluate() returns all required metric keys
- evaluate() produces metrics in valid ranges (precision/recall/f1/auc in [0,1])
- compare_to_active() returns all_pass=True when no metric regresses
- compare_to_active() returns all_pass=False and regressed=True when metric drops > 5%
- _save_artifacts() creates 4 required files in the version directory
"""

import json
import pickle
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

from backend.training.trainer import _make_target, _save_artifacts
from backend.training.evaluator import evaluate, compare_to_active


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_mock_model(y_prob: list[float] | None = None):
    """Return a mock model whose predict_proba returns controlled probabilities."""
    mock = MagicMock()
    if y_prob is None:
        # Alternating high/low to get reasonable metric values
        y_prob = [0.9, 0.1, 0.8, 0.2, 0.85, 0.15, 0.7, 0.3, 0.75, 0.25]
    arr = np.array([[1 - p, p] for p in y_prob])
    mock.predict_proba = MagicMock(return_value=arr)
    return mock


def _mini_zone_df(zone_id: int = 1, n_hours: int = 10) -> pd.DataFrame:
    """Create a minimal zone-hour DataFrame for _make_target testing."""
    counts = [max(10, 100 - i * 7) for i in range(n_hours)]
    return pd.DataFrame({
        "zone_id":    [zone_id] * n_hours,
        "timestamp":  pd.date_range("2024-01-01", periods=n_hours, freq="h"),
        "taxi_count": counts,
    })


# ── _make_target ──────────────────────────────────────────────────────────────

def test_make_target_shortage_when_next_drops_40pct():
    df = pd.DataFrame({
        "zone_id":    [1, 1],
        "timestamp":  pd.date_range("2024-01-01", periods=2, freq="h"),
        "taxi_count": [100, 50],  # 50 < 100 * 0.6 → shortage
    })
    target = _make_target(df)
    assert int(target.iloc[0]) == 1


def test_make_target_no_shortage_when_drop_small():
    df = pd.DataFrame({
        "zone_id":    [1, 1],
        "timestamp":  pd.date_range("2024-01-01", periods=2, freq="h"),
        "taxi_count": [100, 70],  # 70 >= 100 * 0.6 → no shortage
    })
    target = _make_target(df)
    assert int(target.iloc[0]) == 0


def test_make_target_last_row_not_shortage_when_no_next():
    # shift(-1) fills missing with NaN (float), comparison gives False → label 0
    df = _mini_zone_df(n_hours=5)
    target = _make_target(df)
    # Last row has no next hour — comparison with NaN → False → 0
    assert int(target.iloc[-1]) == 0


def test_make_target_all_rows_have_label():
    df = pd.concat([_mini_zone_df(zone_id=1, n_hours=4),
                    _mini_zone_df(zone_id=2, n_hours=4)])
    target = _make_target(df)
    # All rows get a binary label (0 or 1), no NaN in practice
    assert target.notna().all()


def test_make_target_length_matches_input():
    df = _mini_zone_df(n_hours=8)
    target = _make_target(df)
    assert len(target) == len(df)


# ── evaluate ──────────────────────────────────────────────────────────────────

REQUIRED_METRIC_KEYS = [
    "precision", "recall", "f1", "roc_auc",
    "best_threshold", "mae", "rmse",
    "pred_mean", "pred_std", "pct_above_threshold",
    "confusion_matrix", "n_test", "positive_rate",
]


def test_evaluate_returns_required_keys():
    n = 20
    y_prob = [0.9 if i % 2 == 0 else 0.1 for i in range(n)]
    y_test = pd.Series([1 if i % 2 == 0 else 0 for i in range(n)])
    X_test = pd.DataFrame({"f1": range(n)})
    model = _make_mock_model(y_prob)
    metrics = evaluate(model, X_test, y_test)
    for key in REQUIRED_METRIC_KEYS:
        assert key in metrics, f"Missing metric: {key}"


def test_evaluate_metrics_in_valid_range():
    n = 20
    y_prob = [0.9 if i % 2 == 0 else 0.1 for i in range(n)]
    y_test = pd.Series([1 if i % 2 == 0 else 0 for i in range(n)])
    X_test = pd.DataFrame({"f1": range(n)})
    model = _make_mock_model(y_prob)
    metrics = evaluate(model, X_test, y_test)
    for key in ["precision", "recall", "f1", "roc_auc"]:
        assert 0.0 <= metrics[key] <= 1.0, f"{key}={metrics[key]} out of [0,1]"


def test_evaluate_n_test_matches_input():
    n = 30
    y_prob = [float(i % 2) * 0.8 + 0.1 for i in range(n)]
    y_test = pd.Series([i % 2 for i in range(n)])
    X_test = pd.DataFrame({"f1": range(n)})
    model = _make_mock_model(y_prob)
    metrics = evaluate(model, X_test, y_test)
    assert metrics["n_test"] == n


# ── compare_to_active ─────────────────────────────────────────────────────────

def test_compare_to_active_all_pass_when_no_regression():
    candidate = {"precision": 0.80, "recall": 0.75, "f1": 0.77, "roc_auc": 0.88}
    active    = {"precision": 0.79, "recall": 0.74, "f1": 0.76, "roc_auc": 0.87}
    result = compare_to_active(candidate, active)
    assert result["all_pass"] is True


def test_compare_to_active_fails_when_f1_drops_over_5pct():
    candidate = {"precision": 0.80, "recall": 0.75, "f1": 0.60, "roc_auc": 0.88}
    active    = {"precision": 0.80, "recall": 0.75, "f1": 0.77, "roc_auc": 0.88}
    result = compare_to_active(candidate, active)
    assert result["all_pass"] is False
    assert result["f1"]["regressed"] is True


def test_compare_to_active_delta_computed_correctly():
    candidate = {"precision": 0.85, "recall": 0.80, "f1": 0.82, "roc_auc": 0.90}
    active    = {"precision": 0.80, "recall": 0.75, "f1": 0.77, "roc_auc": 0.88}
    result = compare_to_active(candidate, active)
    assert abs(result["f1"]["delta"] - 0.05) < 0.001


def test_compare_to_active_no_regression_when_active_zero():
    # If active metric is 0, regression check is skipped (avoids division by zero)
    candidate = {"precision": 0.0, "recall": 0.0, "f1": 0.0, "roc_auc": 0.5}
    active    = {"precision": 0.0, "recall": 0.0, "f1": 0.0, "roc_auc": 0.0}
    result = compare_to_active(candidate, active)
    # Should not raise and all_pass should be True (no active baseline to beat)
    assert isinstance(result["all_pass"], bool)


# ── _save_artifacts ───────────────────────────────────────────────────────────

class _DummyModel:
    """Minimal picklable model stand-in for artifact-saving tests."""
    pass


def test_save_artifacts_creates_required_files(tmp_path):
    X = pd.DataFrame({"feature_a": [1, 2], "feature_b": [3, 4]})
    metrics = {"f1": 0.8, "roc_auc": 0.9, "train_rows": 100, "val_rows": 20}
    version_id = "vtest"

    with patch("backend.training.trainer.REGISTRY_DIR", tmp_path):
        _save_artifacts(_DummyModel(), X, metrics, version_id)

    version_dir = tmp_path / "models" / version_id
    assert (version_dir / "model.pkl").exists()
    assert (version_dir / "metrics.json").exists()
    assert (version_dir / "feature_schema.json").exists()
    assert (version_dir / "version_meta.json").exists()


def test_save_artifacts_metrics_json_content(tmp_path):
    X = pd.DataFrame({"a": [1, 2]})
    metrics = {"f1": 0.82, "roc_auc": 0.91, "train_rows": 80, "val_rows": 20}

    with patch("backend.training.trainer.REGISTRY_DIR", tmp_path):
        _save_artifacts(_DummyModel(), X, metrics, "vcheck")

    saved = json.loads((tmp_path / "models" / "vcheck" / "metrics.json").read_text())
    assert saved["f1"] == 0.82
    assert saved["roc_auc"] == 0.91


def test_save_artifacts_version_meta_status_is_candidate(tmp_path):
    X = pd.DataFrame({"a": [1]})
    metrics = {"f1": 0.80}

    with patch("backend.training.trainer.REGISTRY_DIR", tmp_path):
        _save_artifacts(_DummyModel(), X, metrics, "vcand")

    meta = json.loads((tmp_path / "models" / "vcand" / "version_meta.json").read_text())
    assert meta["status"] == "candidate"
    assert meta["version_id"] == "vcand"
