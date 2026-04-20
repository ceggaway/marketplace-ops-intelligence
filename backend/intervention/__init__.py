"""Deterministic intervention engine for zone-level marketplace actions."""

from pathlib import Path

import yaml

CONFIG_PATH = Path("config/config.yaml")
ADJACENCY_PATH = Path("config/zone_adjacency.yaml")


def load_intervention_config(config_path: Path = CONFIG_PATH) -> dict:
    """Load intervention settings from the shared YAML config."""
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return raw.get("intervention", {})


def load_zone_adjacency(config_path: Path = ADJACENCY_PATH) -> dict[str, list[str]]:
    """Load zone adjacency mapping used by rebalance feasibility checks."""
    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return {str(zone): [str(n) for n in neighbors or []] for zone, neighbors in raw.items()}
