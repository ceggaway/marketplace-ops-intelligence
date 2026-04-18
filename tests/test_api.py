"""
Tests for FastAPI endpoints — backend/api/routers/

Uses httpx TestClient (no real server needed, no ports opened).
All tests patch file reads so they run without requiring data/outputs/ files.

Covers:
- GET /health                    → 200 {"status": "ok"}
- GET /api/v1/overview           → 200, expected top-level keys present
- GET /api/v1/zones              → 200, returns a list
- GET /api/v1/zones/{id}         → 200 for existing zone, 404 for missing
- GET /api/v1/recommendations    → 200, returns a list
- GET /api/v1/model/status       → 200, expected keys present
- GET /api/v1/pipeline/latest-run → 200
- GET /api/v1/monitoring/drift   → 200
- GET /api/v1/monitoring/history → 200, returns a list
- GET /api/v1/alerts             → 200, returns a list
- Filter params (?risk_level=high, ?priority=high) are accepted without error
"""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from backend.api.main import app

client = TestClient(app)


# ── Helpers ───────────────────────────────────────────────────────────────────

_SAMPLE_PREDICTIONS = pd.DataFrame([
    {
        "zone_id": 1, "zone_name": "Orchard", "region": "Central",
        "timestamp": "2024-01-01 08:00:00+08:00",
        "delay_risk_score": 0.85, "risk_level": "high",
        "taxi_count": 42, "depletion_rate_1h": 0.45, "supply_vs_yesterday": 0.65,
        "explanation_tag": "rapid depletion + peak hour",
    },
    {
        "zone_id": 2, "zone_name": "Bedok", "region": "East",
        "timestamp": "2024-01-01 08:00:00+08:00",
        "delay_risk_score": 0.35, "risk_level": "low",
        "taxi_count": 80, "depletion_rate_1h": 0.05, "supply_vs_yesterday": 1.10,
        "explanation_tag": "normal conditions",
    },
])

_SAMPLE_RECS = pd.DataFrame([
    {
        "zone_id": 1, "zone_name": "Orchard", "region": "Central",
        "risk_level": "high", "delay_risk_score": 0.85,
        "issue_detected": "High delay risk",
        "recommendation": "Increase driver incentive",
        "expected_impact": "Reduce wait time by 5 min",
        "confidence": 0.85, "priority": "high",
        "explanation_tag": "demand spike",
        "last_updated": "2024-01-01T08:00:00+00:00",
    },
])

_SAMPLE_LOG = [
    {
        "run_id": "abc123", "timestamp": "2024-01-01T08:00:00+00:00",
        "rows_scored": 1320, "failed_rows": 0, "flagged_zones": 200,
        "drift_flag": False, "rollback_status": False,
        "model_version": "v1", "run_status": "success",
        "latency_ms": 500, "avg_delay_min": 15.0, "psi": 0.05,
        "logged_at": "2024-01-01T08:00:01+00:00",
    }
]

_SAMPLE_DRIFT = {
    "run_id": "abc123", "timestamp": "2024-01-01T08:00:00+00:00",
    "psi": 0.05, "drift_flag": False, "drift_level": "stable",
    "reference_mean": 0.30, "current_mean": 0.32,
    "reference_std": 0.15, "current_std": 0.16,
    "reference_n": 1000, "current_n": 1000,
}

_SAMPLE_REGISTRY = {
    "active_version": "v1",
    "candidate_version": None,
    "versions": {
        "v1": {
            "status": "active",
            "trained_at": "2024-01-01T00:00:00+00:00",
            "promoted_at": "2024-01-01T01:00:00+00:00",
            "metrics": {"f1": 0.92, "roc_auc": 0.95},
        }
    },
}

_SAMPLE_META = {
    "version_id": "v1",
    "status": "active",
    "trained_at": "2024-01-01T00:00:00+00:00",
    "promoted_at": "2024-01-01T01:00:00+00:00",
    "f1": 0.92, "roc_auc": 0.95,
    "precision": 0.90, "recall": 0.94,
    "train_rows": 25000, "val_rows": 6000,
}


def _patch_files(predictions=True, recs=True, log=True, drift=True, registry=True):
    """Context manager that patches all file reads used by the API routers."""
    patches = []

    if predictions:
        patches.append(patch(
            "backend.api.routers.operational.pd.read_csv",
            side_effect=lambda p, **kw: _SAMPLE_PREDICTIONS if "predictions" in str(p)
                else (_SAMPLE_RECS if "recommended" in str(p) else pd.DataFrame()),
        ))
        patches.append(patch(
            "backend.api.routers.operational.Path.exists", return_value=True
        ))

    if log:
        patches.append(patch(
            "backend.monitoring.metrics.LOG_PATH",
            new_callable=lambda: _make_log_path_mock,
        ))

    if registry:
        patches.append(patch(
            "backend.registry.model_registry._load_registry",
            return_value=_SAMPLE_REGISTRY,
        ))
        patches.append(patch(
            "backend.registry.model_registry.get_active_version_meta",
            return_value=_SAMPLE_META,
        ))

    return patches


def _make_log_path_mock():
    """Return a mock Path that reads back sample log lines."""
    m = MagicMock(spec=Path)
    m.exists.return_value = True
    m.read_text.return_value = "\n".join(json.dumps(r) for r in _SAMPLE_LOG)
    return m


# ── /health ───────────────────────────────────────────────────────────────────

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ── /overview ─────────────────────────────────────────────────────────────────

def test_overview_200():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_pipeline_log", return_value=_SAMPLE_LOG), \
         patch("backend.api.routers.operational.OUT_DIR", Path("/tmp")), \
         patch("pathlib.Path.exists", return_value=False):
        resp = client.get("/api/v1/overview")
    assert resp.status_code == 200
    body = resp.json()
    assert "kpis" in body
    assert "alerts" in body
    assert "demand_trend" in body


def test_overview_kpis_have_required_fields():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_pipeline_log", return_value=[]), \
         patch("pathlib.Path.exists", return_value=False):
        resp = client.get("/api/v1/overview")
    kpis = resp.json()["kpis"]
    for field in ["high_risk_zone_count", "medium_risk_zone_count", "total_taxi_supply",
                  "rapid_depletion_zones", "critical_actions_count", "model_psi",
                  "minutes_since_last_run", "as_of"]:
        assert field in kpis, f"Missing kpi field: {field}"


# ── /zones ────────────────────────────────────────────────────────────────────

def test_zones_200():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS):
        resp = client.get("/api/v1/zones")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_zones_risk_level_filter():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS):
        resp = client.get("/api/v1/zones?risk_level=high")
    assert resp.status_code == 200
    zones = resp.json()
    assert all(z["risk_level"] == "high" for z in zones)


def test_zones_region_filter():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS):
        resp = client.get("/api/v1/zones?region=Central")
    assert resp.status_code == 200
    zones = resp.json()
    assert all(z["region"] == "Central" for z in zones)


def test_zones_sorted_by_score_descending():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS):
        resp = client.get("/api/v1/zones")
    zones = resp.json()
    if len(zones) > 1:
        scores = [z["delay_risk_score"] for z in zones]
        assert scores == sorted(scores, reverse=True)


# ── /zones/{zone_id} ──────────────────────────────────────────────────────────

def test_zone_detail_200():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS), \
         patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS), \
         patch("backend.api.routers.operational._read_pipeline_log", return_value=_SAMPLE_LOG):
        resp = client.get("/api/v1/zones/1")
    assert resp.status_code == 200
    body = resp.json()
    assert body["zone_id"] == 1
    assert "delay_risk_score" in body


def test_zone_detail_404_for_unknown():
    with patch("backend.api.routers.operational._read_predictions", return_value=_SAMPLE_PREDICTIONS):
        resp = client.get("/api/v1/zones/9999")
    assert resp.status_code == 404


def test_zone_detail_404_when_no_predictions():
    with patch("backend.api.routers.operational._read_predictions", return_value=pd.DataFrame()):
        resp = client.get("/api/v1/zones/1")
    assert resp.status_code == 404


# ── /recommendations ──────────────────────────────────────────────────────────

def test_recommendations_200():
    with patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS):
        resp = client.get("/api/v1/recommendations")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_recommendations_priority_filter():
    with patch("backend.api.routers.operational._read_recommendations", return_value=_SAMPLE_RECS):
        resp = client.get("/api/v1/recommendations?priority=high")
    assert resp.status_code == 200
    recs = resp.json()
    assert all(r["priority"] == "high" for r in recs)


# ── /model/status ─────────────────────────────────────────────────────────────

def test_model_status_200():
    with patch("backend.registry.model_registry.get_active_version_meta", return_value=_SAMPLE_META), \
         patch("backend.registry.model_registry._load_registry", return_value=_SAMPLE_REGISTRY):
        resp = client.get("/api/v1/model/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["active_version"] == "v1"
    assert "training_metrics" in body


# ── /model/versions ───────────────────────────────────────────────────────────

def test_model_versions_200():
    with patch("backend.registry.model_registry._load_registry", return_value=_SAMPLE_REGISTRY):
        resp = client.get("/api/v1/model/versions")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


# ── /pipeline/latest-run ──────────────────────────────────────────────────────

def test_latest_run_200():
    with patch("backend.monitoring.metrics.get_latest_runs", return_value=_SAMPLE_LOG):
        resp = client.get("/api/v1/pipeline/latest-run")
    assert resp.status_code == 200
    body = resp.json()
    assert "run_id" in body


def test_latest_run_never_run():
    with patch("backend.api.routers.ml_health.get_latest_runs", return_value=[]):
        resp = client.get("/api/v1/pipeline/latest-run")
    assert resp.status_code == 200
    assert resp.json()["run_status"] == "never_run"


# ── /monitoring/drift ─────────────────────────────────────────────────────────

def test_drift_report_200(tmp_path):
    drift_file = tmp_path / "drift_report.json"
    drift_file.write_text(json.dumps(_SAMPLE_DRIFT))
    with patch("backend.api.routers.ml_health.OUT_DIR", tmp_path):
        resp = client.get("/api/v1/monitoring/drift")
    assert resp.status_code == 200
    body = resp.json()
    assert body["psi"] == _SAMPLE_DRIFT["psi"]


def test_drift_report_default_when_missing():
    with patch("backend.api.routers.ml_health.OUT_DIR", Path("/nonexistent")):
        resp = client.get("/api/v1/monitoring/drift")
    assert resp.status_code == 200
    body = resp.json()
    assert body["psi"] == 0.0
    assert body["drift_flag"] is False


# ── /monitoring/history ───────────────────────────────────────────────────────

def test_monitoring_history_200():
    with patch("backend.api.routers.ml_health.get_latest_runs", return_value=_SAMPLE_LOG):
        resp = client.get("/api/v1/monitoring/history")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_monitoring_history_n_param():
    with patch("backend.api.routers.ml_health.get_latest_runs", return_value=_SAMPLE_LOG) as mock:
        client.get("/api/v1/monitoring/history?n=5")
        mock.assert_called_once_with(n=5)


# ── /alerts ───────────────────────────────────────────────────────────────────

def test_alerts_200():
    with patch("backend.api.routers.ml_health.get_active_alerts", return_value=[]), \
         patch("backend.api.routers.ml_health.get_latest_runs", return_value=[]):
        resp = client.get("/api/v1/alerts")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


def test_alerts_includes_persisted_alerts():
    persisted = [{
        "alert_id": "DRIFT_ALERT",
        "severity": "high",
        "message": "PSI=0.31 — significant drift",
        "zone_id": None,
        "created_at": "2024-01-01T08:00:00+00:00",
    }]
    with patch("backend.api.routers.ml_health.get_active_alerts", return_value=persisted), \
         patch("backend.api.routers.ml_health.get_latest_runs", return_value=[]):
        resp = client.get("/api/v1/alerts")
    assert resp.status_code == 200
    alerts = resp.json()
    assert any(a["alert_id"] == "DRIFT_ALERT" for a in alerts)


def test_alerts_deduplicates_by_alert_id():
    """alerts.log entry should not appear twice if both log and on-the-fly check fire."""
    persisted = [{
        "alert_id": "HIGH_FAILED_ROWS",
        "severity": "medium",
        "message": "persisted alert",
        "zone_id": None,
        "created_at": "2024-01-01T08:00:00+00:00",
    }]
    runs_with_failures = [{**_SAMPLE_LOG[0], "failed_rows": 200, "rows_scored": 1000}]
    with patch("backend.api.routers.ml_health.get_active_alerts", return_value=persisted), \
         patch("backend.api.routers.ml_health.get_latest_runs", return_value=runs_with_failures):
        resp = client.get("/api/v1/alerts")
    assert resp.status_code == 200
    ids = [a["alert_id"] for a in resp.json()]
    assert ids.count("HIGH_FAILED_ROWS") == 1


# ── POST /pipeline/retrain ────────────────────────────────────────────────────

def test_retrain_endpoint_starts_process():
    """POST /pipeline/retrain should return 200 with status/version/message keys."""
    # The script lives at scripts/run_training.py in the real project tree, so
    # script.exists() returns True naturally. We only need to suppress Popen.
    with patch("backend.api.routers.ml_health.subprocess.Popen") as mock_popen:
        mock_popen.return_value = MagicMock()
        resp = client.post("/api/v1/pipeline/retrain")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "started"
    assert "version" in body
    assert "message" in body
    mock_popen.assert_called_once()


def test_retrain_endpoint_500_when_script_missing():
    """POST /pipeline/retrain should return 500 when training script not found."""
    with patch("backend.api.routers.ml_health.Path.exists", return_value=False):
        resp = client.post("/api/v1/pipeline/retrain")
    assert resp.status_code == 500
