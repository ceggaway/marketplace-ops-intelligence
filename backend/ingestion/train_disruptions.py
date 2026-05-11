"""
Train Disruption Signal — LTA DataMall Integration
====================================================
Fetches live MRT/LRT service alerts from the LTA DataMall
`/TrainServiceAlerts` endpoint and returns a per-timestamp disruption flag.

API endpoint: GET https://datamall2.mytransport.sg/ltaodataservice/TrainServiceAlerts
Auth: AccountKey header (same LTA_API_KEY used by the taxi poller)

The response contains a list of active messages tagged by AffectedSegments
and Status.  We treat any response with Status != "Normal" or any
AffectedSegments present as an active disruption (flag=1).

Fallback: if the API key is absent, the endpoint is unreachable, or the
response is malformed, we return all-zero flags so the pipeline continues
uninterrupted. The fallback reason is logged so ops can investigate.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import pandas as pd

try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

_LTA_BASE = "https://datamall2.mytransport.sg/ltaodataservice"
_ENDPOINT = "/TrainServiceAlerts"
_TIMEOUT  = 5   # seconds — disruption check must not block the pipeline

logger = logging.getLogger(__name__)


def fetch_live_disruption_status(api_key: str) -> dict:
    """
    Call LTA DataMall TrainServiceAlerts and return a parsed status dict.

    Returns:
        {
            "is_disrupted": bool,
            "status": str,           # "Normal" | "Disrupted" | etc.
            "affected_lines": list,  # e.g. ["EWL", "CCL"]
            "fetched_at": str,       # ISO timestamp
        }
    Raises requests.RequestException on network failure.
    """
    if not _REQUESTS_AVAILABLE:
        raise RuntimeError("requests library is not installed")

    resp = _requests.get(
        f"{_LTA_BASE}{_ENDPOINT}",
        headers={"AccountKey": api_key, "accept": "application/json"},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()

    # The response has a top-level "value" key; the nested Message list and
    # AffectedSegments list indicate the severity and affected lines.
    value = data.get("value", {})
    status = str(value.get("Status", 1))

    # Status == 1 → normal operations; any other integer → disruption
    # Some API versions return string "Normal" instead.
    is_disrupted = status not in {"1", "Normal", 1}

    affected_lines: list[str] = []
    for segment in value.get("AffectedSegments", []) or []:
        line = segment.get("Line", "")
        if line and line not in affected_lines:
            affected_lines.append(line)

    # Also check the Messages array — any message with Status != "Normal" counts
    for msg in value.get("Message", []) or []:
        if str(msg.get("Status", "Normal")) not in {"Normal", "1", 1}:
            is_disrupted = True

    return {
        "is_disrupted":   is_disrupted,
        "status":         status,
        "affected_lines": affected_lines,
        "fetched_at":     datetime.now(timezone.utc).isoformat(),
    }


def load_train_disruption_flags(timestamps: pd.Series | None = None) -> pd.DataFrame:
    """
    Return a disruption flag DataFrame aligned to the provided timestamps.

    Attempts a live LTA DataMall fetch when LTA_API_KEY is set in the
    environment. On any failure, returns all-zero flags with a logged warning.

    The flag is broadcast across all rows: if there is an active disruption
    right now, every zone gets flag=1. This is intentionally coarse — future
    work can add line-to-zone mapping for finer spatial attribution.
    """
    n = len(timestamps) if timestamps is not None and len(timestamps) > 0 else 0
    empty_schema = pd.DataFrame({
        "timestamp":            pd.Series(dtype="datetime64[ns, UTC]"),
        "train_disruption_flag": pd.Series(dtype="int8"),
    })

    if n == 0:
        return empty_schema

    ts = pd.to_datetime(timestamps, utc=True)
    default_flags = pd.DataFrame({
        "timestamp":            ts,
        "train_disruption_flag": pd.Series(0, index=range(n), dtype="int8"),
    })

    api_key = os.environ.get("LTA_API_KEY")
    if not api_key:
        logger.debug("LTA_API_KEY not set — using all-zero train disruption flags")
        return default_flags

    if not _REQUESTS_AVAILABLE:
        logger.warning("requests not installed — using all-zero train disruption flags")
        return default_flags

    try:
        status = fetch_live_disruption_status(api_key)
        flag_value = int(status["is_disrupted"])
        if flag_value:
            logger.info(
                "Active train disruption detected: %s (lines: %s)",
                status["status"],
                ", ".join(status["affected_lines"]) or "unknown",
            )
        return pd.DataFrame({
            "timestamp":            ts,
            "train_disruption_flag": pd.Series(flag_value, index=range(n), dtype="int8"),
        })
    except Exception as exc:
        logger.warning("Train disruption fetch failed (%s) — using all-zero flags", exc)
        return default_flags
