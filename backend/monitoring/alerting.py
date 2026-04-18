"""
Alerting
========
Persists alerts to data/outputs/alerts.log (JSONL) and optionally fires
an HTTP webhook when a new alert is emitted.

Webhook:
    Set ALERT_WEBHOOK_URL in .env to receive POST requests for every alert.
    Payload: {"alert_id", "severity", "message", "created_at", "zone_id"}

    Compatible with Slack incoming webhooks, PagerDuty Events API v2,
    or any generic HTTP endpoint.

Usage:
    from backend.monitoring.alerting import emit_alert, get_active_alerts

    emit_alert("DRIFT_ALERT", "high", "PSI=0.31 — significant drift detected")
    alerts = get_active_alerts(n=20)
"""

import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ALERTS_LOG_PATH = Path("data/outputs/alerts.log")


def emit_alert(
    alert_id: str,
    severity: str,
    message: str,
    zone_id: int | None = None,
) -> dict:
    """
    Write an alert to alerts.log and fire the webhook (if configured).

    Args:
        alert_id: Unique identifier, e.g. "DRIFT_ALERT", "ROLLBACK_OCCURRED"
        severity: "high" | "medium" | "info"
        message:  Human-readable description
        zone_id:  Optional zone this alert relates to

    Returns the alert dict that was persisted.
    """
    now = datetime.now(timezone.utc).isoformat()
    alert = {
        "alert_id":   alert_id,
        "severity":   severity,
        "message":    message,
        "zone_id":    zone_id,
        "created_at": now,
    }

    _write_alert(alert)
    _fire_webhook(alert)

    return alert


def get_active_alerts(n: int = 20) -> list[dict]:
    """Return the last n alerts from alerts.log."""
    if not ALERTS_LOG_PATH.exists():
        return []
    lines = ALERTS_LOG_PATH.read_text().strip().splitlines()
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return records[-n:]


def _write_alert(alert: dict) -> None:
    ALERTS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_LOG_PATH, "a") as f:
        f.write(json.dumps(alert) + "\n")


def _fire_webhook(alert: dict) -> None:
    """POST alert payload to ALERT_WEBHOOK_URL if set. Silently ignores errors."""
    url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return
    try:
        payload = json.dumps(alert).encode()
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception:
        pass  # webhook failures must never crash the pipeline
