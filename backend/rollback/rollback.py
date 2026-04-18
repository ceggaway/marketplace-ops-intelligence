"""
Rollback Manager
================
Automatically reverts to the previous stable model version if quality
degrades below acceptable thresholds.

Triggers:
    - Failed rows in latest batch exceed 20% of total
    - Drift PSI > 0.25 (severe drift)

Audit log: data/outputs/audit.log (JSONL)
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from backend.registry import model_registry as registry
from backend.monitoring.alerting import emit_alert

AUDIT_LOG_PATH           = Path("data/outputs/audit.log")
FAILED_ROW_PCT_THRESHOLD = 0.20
PSI_ROLLBACK_THRESHOLD   = 0.25


def check_and_rollback(run_meta: dict) -> bool:
    """
    Check run_meta for failure conditions. If triggered, perform rollback.
    Returns True if rollback was performed.
    """
    should, reason = _should_rollback(run_meta)
    if not should:
        return False

    from_ver = run_meta.get("model_version", "unknown")
    try:
        previous_version = registry.rollback()
        _write_audit_event(
            event="rollback",
            from_version=from_ver,
            to_version=previous_version,
            reason=reason,
            run_id=run_meta.get("run_id", ""),
        )
        emit_alert(
            alert_id="ROLLBACK_OCCURRED",
            severity="high",
            message=f"Model rolled back: {from_ver} → {previous_version}. Reason: {reason}",
        )
        return True
    except RuntimeError as e:
        _write_audit_event(
            event="rollback_failed",
            from_version=from_ver,
            to_version=None,
            reason=f"rollback attempted but failed: {e}",
            run_id=run_meta.get("run_id", ""),
        )
        emit_alert(
            alert_id="ROLLBACK_FAILED",
            severity="high",
            message=f"Rollback failed for {from_ver}: {e}",
        )
        return False


def _should_rollback(run_meta: dict) -> tuple[bool, str]:
    rows_scored = run_meta.get("rows_scored", 0)
    failed_rows = run_meta.get("failed_rows", 0)
    psi         = run_meta.get("psi", 0.0)
    drift_flag  = run_meta.get("drift_flag", False)

    if rows_scored > 0:
        fail_pct = failed_rows / (rows_scored + failed_rows)
        if fail_pct > FAILED_ROW_PCT_THRESHOLD:
            return True, f"failed row pct {fail_pct:.1%} > {FAILED_ROW_PCT_THRESHOLD:.0%}"

    if psi > PSI_ROLLBACK_THRESHOLD or drift_flag:
        return True, f"PSI drift {psi:.3f} > {PSI_ROLLBACK_THRESHOLD}"

    return False, ""


def _write_audit_event(
    event: str,
    from_version: str,
    to_version: str | None,
    reason: str,
    run_id: str = "",
) -> None:
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "event":        event,
        "from_version": from_version,
        "to_version":   to_version,
        "reason":       reason,
        "run_id":       run_id,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }
    with open(AUDIT_LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")
