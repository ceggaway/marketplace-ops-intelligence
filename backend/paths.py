"""Runtime filesystem paths.

All mutable runtime state defaults to the repo-local ``data/`` directory, but
deployments can set ``MARKETPLACE_DATA_DIR`` to point at a persistent volume.
"""

from __future__ import annotations

import os
from pathlib import Path


def data_dir() -> Path:
    return Path(os.environ.get("MARKETPLACE_DATA_DIR", "data"))


def raw_dir() -> Path:
    return data_dir() / "raw"


def processed_dir() -> Path:
    return data_dir() / "processed"


def outputs_dir() -> Path:
    return data_dir() / "outputs"


def registry_dir() -> Path:
    return data_dir() / "registry"


def logs_dir() -> Path:
    return data_dir() / "logs"


def data_path(*parts: str) -> Path:
    return data_dir().joinpath(*parts)
