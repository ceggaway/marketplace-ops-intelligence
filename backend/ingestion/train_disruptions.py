"""
Placeholder interface for train disruption signals.

The current project does not ingest live rail disruption feeds yet. This
module provides a safe default schema so demand-pressure features can depend on
an explicit connector boundary without fabricating unavailable data.
"""

from __future__ import annotations

import pandas as pd


def load_train_disruption_flags(timestamps: pd.Series | None = None) -> pd.DataFrame:
    """
    Return a placeholder disruption frame with the expected schema.

    TODO:
    - integrate an LTA/MOT-compatible disruption feed
    - align disruption geography with planning zones
    - replace this all-clear default with real operational events
    """
    if timestamps is None or len(timestamps) == 0:
        return pd.DataFrame({"timestamp": pd.Series(dtype="datetime64[ns, UTC]"), "train_disruption_flag": pd.Series(dtype="int8")})
    ts = pd.to_datetime(timestamps, utc=True)
    return pd.DataFrame({
        "timestamp": ts,
        "train_disruption_flag": pd.Series(0, index=range(len(ts)), dtype="int8"),
    })

