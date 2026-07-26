"""
Shared data models for the sensor -> fog -> cloud pipeline.

A single canonical envelope is used on every hop so that the fog node and the
cloud backend can be evolved independently of the sensor implementations.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


def now_ms() -> int:
    """Epoch milliseconds. Used everywhere so timestamps are comparable."""
    return int(time.time() * 1000)


@dataclass
class SensorReading:
    """One raw observation emitted by one sensor at one instant."""

    sensor_id: str          # e.g. "temp-b1-f2-01"
    sensor_type: str        # e.g. "temperature"
    zone: str               # logical location, e.g. "floor-2-east"
    value: float            # calibrated physical value
    unit: str               # e.g. "C", "%", "ppm", "count", "W"
    ts: int = field(default_factory=now_ms)
    seq: int = 0            # per-sensor monotonic counter (gap detection)
    reading_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    quality: float = 1.0    # 0..1 self-reported confidence

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SensorReading":
        return SensorReading(**d)


@dataclass
class FogEnvelope:
    """
    Batch produced by a fog node and shipped to the cloud ingest endpoint.

    Carrying aggregates + anomalies (rather than every raw sample) is what makes
    the fog tier worthwhile: it is the point where data volume is reduced.
    """

    fog_id: str
    window_start: int
    window_end: int
    aggregates: list                     # list[dict] one per (sensor_type, zone)
    anomalies: list                      # list[dict] events worth alerting on
    raw_sample: list = field(default_factory=list)   # thinned raw for charting
    stats: Dict[str, Any] = field(default_factory=dict)
    batch_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    schema_version: str = "1.0"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
