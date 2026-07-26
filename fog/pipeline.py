"""
Fog-tier stream processing.

The fog node does four things to every reading before anything leaves the
building, in this order:

1. VALIDATE   - drop NaNs, out-of-range values and stale/duplicate sequences.
2. SMOOTH     - exponentially weighted moving average to suppress sensor noise.
3. AGGREGATE  - tumbling time windows per (sensor_type, zone).
4. SCORE      - anomaly detection on the smoothed stream (see anomaly.py).

Doing 1-3 at the edge is the whole justification for the tier: a 5 s window
turns ~65 raw readings/s into ~6 aggregate records, a >90% reduction in what
crosses the WAN, while the latency-sensitive anomaly decision is made locally
and does not wait for a cloud round trip.
"""
from __future__ import annotations

import math
from collections import defaultdict, deque
from typing import Dict, Iterable, List, Optional, Tuple

# Physically plausible ranges. Anything outside is a sensor fault, not an event.
VALID_RANGES: Dict[str, Tuple[float, float]] = {
    "temperature": (-20.0, 60.0),
    "humidity": (0.0, 100.0),
    "co2": (300.0, 5000.0),
    "occupancy": (0.0, 500.0),
    "power": (0.0, 20000.0),
}


class Validator:
    """Stateless range checks plus per-sensor duplicate/regression detection."""

    def __init__(self) -> None:
        self.last_seq: Dict[str, int] = {}
        self.dropped = 0
        self.gaps = 0

    def accept(self, r: dict) -> bool:
        value = r.get("value")
        if value is None or (isinstance(value, float) and math.isnan(value)):
            self.dropped += 1
            return False
        lo, hi = VALID_RANGES.get(r.get("sensor_type", ""), (-1e9, 1e9))
        if not (lo <= value <= hi):
            self.dropped += 1
            return False
        if r.get("quality", 1.0) < 0.25:      # sensor declared itself unhealthy
            self.dropped += 1
            return False
        sid, seq = r["sensor_id"], int(r.get("seq", 0))
        prev = self.last_seq.get(sid)
        if prev is not None:
            if seq <= prev:                    # replay or duplicate
                self.dropped += 1
                return False
            if seq > prev + 1:                 # we missed frames; record it
                self.gaps += seq - prev - 1
        self.last_seq[sid] = seq
        return True


class Smoother:
    """Per-sensor EWMA. alpha=0.3 keeps ~5 samples of memory."""

    def __init__(self, alpha: float = 0.3) -> None:
        self.alpha = alpha
        self.state: Dict[str, float] = {}

    def smooth(self, sensor_id: str, value: float) -> float:
        prev = self.state.get(sensor_id)
        out = value if prev is None else self.alpha * value + (1 - self.alpha) * prev
        self.state[sensor_id] = out
        return out


class WindowAggregator:
    """
    Tumbling-window aggregation keyed by (sensor_type, zone).

    Keeping count/min/max/mean/p95/last means the cloud can render every chart
    in the dashboard without ever seeing an individual raw sample.
    """

    def __init__(self, window_seconds: int = 5) -> None:
        self.window_ms = window_seconds * 1000
        self.buckets: Dict[Tuple[str, str], List[float]] = defaultdict(list)
        self.units: Dict[str, str] = {}
        self.window_start: Optional[int] = None
        self.window_end: Optional[int] = None

    def add(self, r: dict, smoothed: float) -> None:
        if self.window_start is None:
            self.window_start = r["ts"]
        self.window_end = max(self.window_end or 0, r["ts"])
        self.buckets[(r["sensor_type"], r["zone"])].append(smoothed)
        self.units[r["sensor_type"]] = r.get("unit", "")

    def ready(self, now_ms_value: int) -> bool:
        return self.window_start is not None and (now_ms_value - self.window_start) >= self.window_ms

    @staticmethod
    def _p95(values: List[float]) -> float:
        s = sorted(values)
        idx = min(len(s) - 1, int(round(0.95 * (len(s) - 1))))
        return s[idx]

    def emit(self) -> List[dict]:
        out: List[dict] = []
        for (stype, zone), values in self.buckets.items():
            if not values:
                continue
            out.append({
                "sensor_type": stype,
                "zone": zone,
                "unit": self.units.get(stype, ""),
                "count": len(values),
                "min": round(min(values), 3),
                "max": round(max(values), 3),
                "mean": round(sum(values) / len(values), 3),
                "p95": round(self._p95(values), 3),
                "last": round(values[-1], 3),
                "window_start": self.window_start,
                "window_end": self.window_end,
            })
        self.buckets.clear()
        self.window_start = None
        self.window_end = None
        return out


class Thinner:
    """
    Keeps a bounded reservoir of raw samples per window so the dashboard can
    still show a raw trace without shipping everything (reservoir sampling).
    """

    def __init__(self, keep: int = 20) -> None:
        self.keep = keep
        self.items: deque = deque(maxlen=keep)
        self.seen = 0

    def offer(self, r: dict, smoothed: float) -> None:
        self.seen += 1
        self.items.append({
            "sensor_id": r["sensor_id"],
            "sensor_type": r["sensor_type"],
            "zone": r["zone"],
            "ts": r["ts"],
            "raw": r["value"],
            "smoothed": round(smoothed, 3),
        })

    def drain(self) -> List[dict]:
        out = list(self.items)
        self.items.clear()
        self.seen = 0
        return out
