"""
Tests for backend/monitoring/alerting.py

Covers:
- emit_alert() returns correct dict structure
- emit_alert() writes JSONL to alerts.log
- get_active_alerts() returns empty when file missing
- get_active_alerts() respects n limit and returns latest
- Malformed JSONL lines are silently skipped
- Webhook is not fired when ALERT_WEBHOOK_URL is unset
- Webhook fires when ALERT_WEBHOOK_URL is set
- Webhook failure does not raise or crash the pipeline
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from backend.monitoring.alerting import emit_alert, get_active_alerts


# ── emit_alert ────────────────────────────────────────────────────────────────

def test_emit_alert_returns_required_keys(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        result = emit_alert("TEST_ALERT", "high", "Something went wrong")
    assert result["alert_id"] == "TEST_ALERT"
    assert result["severity"] == "high"
    assert result["message"] == "Something went wrong"
    assert result["zone_id"] is None
    assert "created_at" in result


def test_emit_alert_stores_zone_id(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        result = emit_alert("ZONE_ALERT", "medium", "Zone issue", zone_id=12)
    assert result["zone_id"] == 12


def test_emit_alert_writes_jsonl(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        emit_alert("A1", "info", "First")
        emit_alert("A2", "high", "Second")
    lines = log.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["alert_id"] == "A1"
    assert json.loads(lines[1])["alert_id"] == "A2"


def test_emit_alert_appends_not_overwrites(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        emit_alert("A1", "info", "msg1")
        emit_alert("A2", "info", "msg2")
        emit_alert("A3", "info", "msg3")
    lines = log.read_text().strip().splitlines()
    assert len(lines) == 3


# ── get_active_alerts ─────────────────────────────────────────────────────────

def test_get_active_alerts_empty_when_file_missing(tmp_path):
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", tmp_path / "none.log"):
        result = get_active_alerts()
    assert result == []


def test_get_active_alerts_returns_all_when_n_large(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        for i in range(5):
            emit_alert(f"A{i}", "info", f"Alert {i}")
        alerts = get_active_alerts(n=50)
    assert len(alerts) == 5


def test_get_active_alerts_returns_last_n(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        for i in range(10):
            emit_alert(f"A{i}", "info", f"Alert {i}")
        alerts = get_active_alerts(n=3)
    assert len(alerts) == 3
    assert alerts[-1]["alert_id"] == "A9"
    assert alerts[0]["alert_id"] == "A7"


def test_get_active_alerts_skips_malformed_lines(tmp_path):
    log = tmp_path / "alerts.log"
    log.write_text('{"alert_id": "GOOD1"}\nNOT_JSON\n{broken\n{"alert_id": "GOOD2"}\n')
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log):
        alerts = get_active_alerts()
    ids = [a["alert_id"] for a in alerts]
    assert "GOOD1" in ids
    assert "GOOD2" in ids
    assert len(alerts) == 2


# ── webhook ───────────────────────────────────────────────────────────────────

def test_no_webhook_call_without_env_var(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log), \
         patch.dict("os.environ", {"ALERT_WEBHOOK_URL": ""}), \
         patch("backend.monitoring.alerting.urllib.request.urlopen") as mock_open:
        emit_alert("TEST", "info", "No webhook")
    mock_open.assert_not_called()


def test_webhook_fires_when_env_var_set(tmp_path):
    log = tmp_path / "alerts.log"
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=mock_ctx)
    mock_ctx.__exit__ = MagicMock(return_value=False)
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log), \
         patch.dict("os.environ", {"ALERT_WEBHOOK_URL": "http://hook.test/endpoint"}), \
         patch("backend.monitoring.alerting.urllib.request.Request") as mock_req, \
         patch("backend.monitoring.alerting.urllib.request.urlopen", return_value=mock_ctx) as mock_open:
        emit_alert("HOOK_TEST", "high", "Webhook fire")
    mock_open.assert_called_once()


def test_webhook_failure_does_not_raise(tmp_path):
    log = tmp_path / "alerts.log"
    with patch("backend.monitoring.alerting.ALERTS_LOG_PATH", log), \
         patch.dict("os.environ", {"ALERT_WEBHOOK_URL": "http://bad-host.invalid/hook"}), \
         patch("backend.monitoring.alerting.urllib.request.urlopen",
               side_effect=Exception("Network error")):
        # Must not raise
        result = emit_alert("FAIL_TEST", "high", "Should fail silently")
    assert result["alert_id"] == "FAIL_TEST"
