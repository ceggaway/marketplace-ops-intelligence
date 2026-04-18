"""
Zone Proximity Graph
====================
Builds an adjacency graph of Singapore planning zones using Haversine distance.
Used by the recommendation engine to detect network effects — if recommending
drivers move to zone A, do adjacent zones have enough slack to absorb the loss?

Adjacency threshold: 5 km  (typical intra-zone travel for a Singapore taxi)
"""

from math import atan2, cos, radians, sin, sqrt
from typing import NamedTuple

from backend.ingestion.loader import SG_ZONES

ADJACENCY_KM = 5.0


class ZoneNode(NamedTuple):
    zone_id: int
    name: str
    lat: float
    lon: float


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _build() -> tuple[dict[int, list[int]], dict[int, ZoneNode]]:
    nodes: dict[int, ZoneNode] = {}
    for z in SG_ZONES:
        # SG_ZONES tuples: (id, name, region, lat, lon)
        nodes[z[0]] = ZoneNode(zone_id=z[0], name=z[1], lat=z[3], lon=z[4])

    graph: dict[int, list[int]] = {}
    for z1 in nodes.values():
        neighbours = [
            z2.zone_id
            for z2 in nodes.values()
            if z2.zone_id != z1.zone_id
            and _haversine_km(z1.lat, z1.lon, z2.lat, z2.lon) <= ADJACENCY_KM
        ]
        graph[z1.zone_id] = sorted(neighbours)

    return graph, nodes


# Module-level singletons — computed once on import
ZONE_GRAPH, ZONE_NODES = _build()
ZONE_NAMES: dict[int, str] = {zid: n.name for zid, n in ZONE_NODES.items()}


def get_adjacent_ids(zone_id: int) -> list[int]:
    return ZONE_GRAPH.get(zone_id, [])
