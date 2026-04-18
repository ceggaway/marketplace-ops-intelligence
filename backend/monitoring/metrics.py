"""
Monitoring Metrics
==================
Tracks pipeline health metrics after each scoring run.

Storage:
    Appended to data/outputs/pipeline.log (JSONL format — one JSON per line)
"""

import json
from datetime import datetime, timezone
from pathlib import Path

LOG_PATH = Path("data/outputs/pipeline.log")


def emit_run_metrics(run_meta: dict) -> None:
    """Append run metrics to pipeline.log as a JSONL record."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = {**run_meta, "logged_at": datetime.now(timezone.utc).isoformat()}
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(record) + "\n")


def get_latest_runs(n: int = 10) -> list[dict]:
    """Read the last n run records from pipeline.log."""
    if not LOG_PATH.exists():
        return []
    lines = LOG_PATH.read_text().strip().splitlines()
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return records[-n:]
