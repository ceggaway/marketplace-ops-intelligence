"""
Tests for backend/rollback/rollback.py

Covers:
- _should_rollback() returns False for a clean run
- _should_rollback() returns True when failed row pct > 20%
- _should_rollback() returns True when PSI > 0.25
- _should_rollback() returns True when drift_flag is True even with low PSI
- _should_rollback() returns False when rows_scored == 0 (no denominator)
- check_and_rollback() returns False and skips rollback on clean run
- check_and_rollback() performs rollback and emits alert on failure condition
- check_and_rollback() returns False and emits ROLLBACK_FAILED when registry raises
- check_and_rollback() writes audit log entry
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from backend.rollback.rollback import _should_rollback, check_and_rollback


# ── _should_rollback ──────────────────────────────────────────────────────────

_CLEAN_RUN = {
    "run_id": "r1",
    "model_version": "v1",
    "rows_scored": 1000,
    "failed_rows": 5,
    "psi": 0.05,
    "drift_flag": False,
}


def test_should_rollback_false_for_clean_run():
    should, reason = _should_rollback(_CLEAN_RUN)
    assert should is False
    assert reason == ""


def test_should_rollback_true_for_high_failed_rows():
    # fail_pct = failed / (rows_scored + failed) → 300/1300 ≈ 0.23 > 0.20
    run = {**_CLEAN_RUN, "failed_rows": 300, "rows_scored": 1000}
    should, reason = _should_rollback(run)
    assert should is True
    assert "failed row" in reason.lower()


def test_should_rollback_false_when_failed_rows_at_threshold():
    # Exactly at 20% — boundary; using rows_scored + failed_rows as denominator
    # 200 / (1000 + 200) = 0.1667 → below threshold
    run = {**_CLEAN_RUN, "failed_rows": 200, "rows_scored": 1000}
    should, _ = _should_rollback(run)
    # 200/(1200) ≈ 0.167 < 0.20 → no rollback
    assert should is False


def test_should_rollback_true_for_high_psi():
    run = {**_CLEAN_RUN, "psi": 0.30, "drift_flag": False}
    should, reason = _should_rollback(run)
    assert should is True
    assert "PSI" in reason or "psi" in reason.lower()


def test_should_rollback_true_when_drift_flag_set():
    run = {**_CLEAN_RUN, "psi": 0.05, "drift_flag": True}
    should, reason = _should_rollback(run)
    assert should is True


def test_should_rollback_false_when_rows_scored_zero():
    # rows_scored=0 → skip failure-rate check entirely
    run = {**_CLEAN_RUN, "rows_scored": 0, "failed_rows": 100}
    should, _ = _should_rollback(run)
    # Only drift check could trigger; psi and drift_flag are clean
    assert should is False


# ── check_and_rollback ────────────────────────────────────────────────────────

def test_check_and_rollback_returns_false_for_clean_run(tmp_path):
    with patch("backend.rollback.rollback.AUDIT_LOG_PATH", tmp_path / "audit.log"), \
         patch("backend.rollback.rollback.registry.rollback") as mock_rollback, \
         patch("backend.rollback.rollback.emit_alert") as mock_alert:
        result = check_and_rollback(_CLEAN_RUN)
    assert result is False
    mock_rollback.assert_not_called()
    mock_alert.assert_not_called()


def test_check_and_rollback_performs_rollback_on_high_psi(tmp_path):
    run = {**_CLEAN_RUN, "psi": 0.35, "drift_flag": True}
    with patch("backend.rollback.rollback.AUDIT_LOG_PATH", tmp_path / "audit.log"), \
         patch("backend.rollback.rollback.registry.rollback", return_value="v0") as mock_rb, \
         patch("backend.rollback.rollback.emit_alert") as mock_alert:
        result = check_and_rollback(run)
    assert result is True
    mock_rb.assert_called_once()
    mock_alert.assert_called_once()
    call_kwargs = mock_alert.call_args.kwargs
    assert call_kwargs.get("alert_id") == "ROLLBACK_OCCURRED"


def test_check_and_rollback_returns_false_when_registry_raises(tmp_path):
    run = {**_CLEAN_RUN, "psi": 0.35, "drift_flag": True}
    with patch("backend.rollback.rollback.AUDIT_LOG_PATH", tmp_path / "audit.log"), \
         patch("backend.rollback.rollback.registry.rollback",
               side_effect=RuntimeError("No previous stable version")), \
         patch("backend.rollback.rollback.emit_alert") as mock_alert:
        result = check_and_rollback(run)
    assert result is False
    # Should emit ROLLBACK_FAILED alert
    assert mock_alert.called
    alert_id = mock_alert.call_args.kwargs.get("alert_id")
    assert alert_id == "ROLLBACK_FAILED"


def test_check_and_rollback_writes_audit_log(tmp_path):
    audit_path = tmp_path / "audit.log"
    run = {**_CLEAN_RUN, "psi": 0.35, "drift_flag": True}
    with patch("backend.rollback.rollback.AUDIT_LOG_PATH", audit_path), \
         patch("backend.rollback.rollback.registry.rollback", return_value="v0"), \
         patch("backend.rollback.rollback.emit_alert"):
        check_and_rollback(run)
    assert audit_path.exists()
    lines = audit_path.read_text().strip().splitlines()
    assert len(lines) >= 1
    record = json.loads(lines[0])
    assert record["event"] == "rollback"
    assert record["to_version"] == "v0"


def test_check_and_rollback_writes_failed_audit_on_registry_error(tmp_path):
    audit_path = tmp_path / "audit.log"
    run = {**_CLEAN_RUN, "psi": 0.35, "drift_flag": True}
    with patch("backend.rollback.rollback.AUDIT_LOG_PATH", audit_path), \
         patch("backend.rollback.rollback.registry.rollback",
               side_effect=RuntimeError("No previous")), \
         patch("backend.rollback.rollback.emit_alert"):
        check_and_rollback(run)
    record = json.loads(audit_path.read_text().strip())
    assert record["event"] == "rollback_failed"
