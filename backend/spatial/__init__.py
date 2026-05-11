"""Spatial utilities for H3 hex-level taxi supply aggregation."""

from pathlib import Path

import yaml

_CONFIG_PATH = Path("config/config.yaml")


def load_spatial_config(config_path: Path = _CONFIG_PATH) -> dict:
    """Load spatial settings from the shared YAML config."""
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return raw.get("spatial", {})
