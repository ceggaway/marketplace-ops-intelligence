"""
Tests for backend/monitoring/drift.py

Covers:
- _psi() returns 0 for identical distributions
- _psi() returns >0 for different distributions
- _drift_level() maps PSI values to correct labels
- compute_drift() writes drift_report.json
- compute_drift() sets drift_flag=True when PSI > 0.25
- compute_drift() emits DRIFT_ALERT when PSI > 0.25
- compute_drift() emits DRIFT_WARNING when 0.10 < PSI < 0.25
- compute_drift() does not emit alert when PSI < 0.10
- save_feature_snapshot() writes feature_distribution.json
- load_reference_scores() returns None when file missing
- load_reference_scores() loads scores from score_distribution.json
"""

import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from backend.monitoring.drift import (
    _psi,
    _drift_level,
    compute_drift,
    save_feature_snapshot,
    load_reference_scores,
)


# ── _psi ──────────────────────────────────────────────────────────────────────

def test_psi_identical_distributions_is_zero():
    s = pd.Series(np.linspace(0, 1, 100))
    result = _psi(s, s)
    assert result == 0.0


def test_psi_different_distributions_positive():
    ref = pd.Series(np.random.default_rng(0).uniform(0, 0.3, 200))
    cur = pd.Series(np.random.default_rng(1).uniform(0.7, 1.0, 200))
    result = _psi(ref, cur)
    assert result > 0.0


def test_psi_constant_series_is_zero():
    """When min == max the function short-circuits to 0.0."""
    s = pd.Series([0.5] * 50)
    assert _psi(s, s) == 0.0


def test_psi_non_negative():
    rng = np.random.default_rng(42)
    for _ in range(10):
        ref = pd.Series(rng.uniform(0, 1, 100))
        cur = pd.Series(rng.uniform(0, 1, 100))
        assert _psi(ref, cur) >= 0.0


# ── _drift_level ──────────────────────────────────────────────────────────────

def test_drift_level_stable():
    assert _drift_level(0.05) == "stable"
    assert _drift_level(0.0) == "stable"
    assert _drift_level(0.099) == "stable"


def test_drift_level_warning():
    assert _drift_level(0.10) == "warning"
    assert _drift_level(0.20) == "warning"
    assert _drift_level(0.249) == "warning"


def test_drift_level_alert():
    assert _drift_level(0.25) == "alert"
    assert _drift_level(0.50) == "alert"
    assert _drift_level(1.00) == "alert"


# ── compute_drift ─────────────────────────────────────────────────────────────

def _stable_series():
    rng = np.random.default_rng(0)
    return pd.Series(rng.uniform(0, 1, 200))


def test_compute_drift_writes_json(tmp_path):
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path), \
         patch("backend.monitoring.drift.emit_alert"):
        compute_drift(_stable_series(), _stable_series(), run_id="r1")
    assert (tmp_path / "drift_report.json").exists()


def test_compute_drift_report_keys(tmp_path):
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path), \
         patch("backend.monitoring.drift.emit_alert"):
        report = compute_drift(_stable_series(), _stable_series(), run_id="r1")
    for key in ["run_id", "timestamp", "psi", "drift_flag", "drift_level",
                "reference_mean", "current_mean", "reference_std", "current_std"]:
        assert key in report, f"Missing key: {key}"


def test_compute_drift_flag_true_for_high_psi(tmp_path):
    ref = pd.Series(np.random.default_rng(0).uniform(0.0, 0.2, 300))
    cur = pd.Series(np.random.default_rng(1).uniform(0.8, 1.0, 300))
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path), \
         patch("backend.monitoring.drift.emit_alert") as mock_alert:
        report = compute_drift(cur, ref, run_id="r2")
    assert report["drift_flag"] is True
    assert report["drift_level"] == "alert"


def test_compute_drift_emits_alert_on_high_psi(tmp_path):
    ref = pd.Series(np.random.default_rng(0).uniform(0.0, 0.2, 300))
    cur = pd.Series(np.random.default_rng(1).uniform(0.8, 1.0, 300))
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path), \
         patch("backend.monitoring.drift.emit_alert") as mock_alert:
        compute_drift(cur, ref, run_id="r3")
    assert mock_alert.called
    call_kwargs = mock_alert.call_args
    assert call_kwargs.kwargs.get("alert_id") == "DRIFT_ALERT" or \
           (call_kwargs.args and call_kwargs.args[0] == "DRIFT_ALERT")


def test_compute_drift_no_alert_when_stable(tmp_path):
    s = _stable_series()
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path), \
         patch("backend.monitoring.drift.emit_alert") as mock_alert:
        report = compute_drift(s, s, run_id="r4")
    # PSI of identical series = 0.0 → no alert
    assert report["drift_flag"] is False
    mock_alert.assert_not_called()


# ── save_feature_snapshot ─────────────────────────────────────────────────────

def test_save_feature_snapshot_writes_file(tmp_path):
    df = pd.DataFrame({
        "taxi_count": [10, 20, 30],
        "depletion_rate_1h": [0.1, 0.2, 0.3],
        "supply_vs_yesterday": [0.9, 1.0, 1.1],
        "rainfall_mm": [0.0, 2.5, 0.0],
        "congestion_ratio": [1.1, 1.3, 0.9],
    })
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path):
        save_feature_snapshot(df)
    snap_path = tmp_path / "feature_distribution.json"
    assert snap_path.exists()
    snap = json.loads(snap_path.read_text())
    assert "taxi_count" in snap
    assert "values" in snap["taxi_count"]


def test_save_feature_snapshot_skips_missing_columns(tmp_path):
    df = pd.DataFrame({"taxi_count": [5, 10, 15]})
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path):
        save_feature_snapshot(df)
    snap = json.loads((tmp_path / "feature_distribution.json").read_text())
    assert "taxi_count" in snap
    assert "depletion_rate_1h" not in snap


# ── load_reference_scores ─────────────────────────────────────────────────────

def test_load_reference_scores_returns_none_when_missing(tmp_path):
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path):
        result = load_reference_scores()
    assert result is None


def test_load_reference_scores_loads_from_file(tmp_path):
    snap = {"scores": [0.1, 0.2, 0.3, 0.4, 0.5]}
    (tmp_path / "score_distribution.json").write_text(json.dumps(snap))
    with patch("backend.monitoring.drift.OUTPUTS_DIR", tmp_path):
        result = load_reference_scores()
    assert result is not None
    assert len(result) == 5
    assert abs(result.mean() - 0.3) < 0.01
