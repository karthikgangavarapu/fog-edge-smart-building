"""
Edge anomaly detection.

Two complementary detectors run locally so that an alert can be raised in
milliseconds even if the WAN link is down:

* ``ZScoreDetector``: a statistical outlier test over a
  rolling window per sensor. Catches drift and spikes the range check misses.
* ``ThresholdDetector``: domain rules (e.g. CO2 > 1000 ppm is a ventilation
  breach under most indoor air-quality guidance). Catches values that are
  statistically normal but operationally wrong.

Severity is attached so the cloud can prioritise without re-deriving context.
"""
from __future__ import annotations

import statistics
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional

# (warning, critical) thresholds per sensor type.
RULES: Dict[str, tuple] = {
    "co2": (1000.0, 1500.0),
    "temperature": (26.0, 30.0),
    "humidity": (65.0, 75.0),
    "power": (2800.0, 3500.0),
    "occupancy": (40.0, 60.0),
}


class ZScoreDetector:
    def __init__(self, threshold: float = 3.0, min_history: int = 20, window: int = 120) -> None:
        self.threshold = threshold
        self.min_history = min_history
        self.history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=window))

    def check(self, sensor_id: str, value: float) -> Optional[float]:
        hist = self.history[sensor_id]
        score = None
        if len(hist) >= self.min_history:
            mu = statistics.fmean(hist)
            sigma = statistics.pstdev(hist)
            if sigma > 1e-9:
                z = abs(value - mu) / sigma
                if z >= self.threshold:
                    score = round(z, 2)
        hist.append(value)
        return score


class ThresholdDetector:
    @staticmethod
    def check(sensor_type: str, value: float) -> Optional[str]:
        rule = RULES.get(sensor_type)
        if not rule:
            return None
        warn, crit = rule
        if value >= crit:
            return "critical"
        if value >= warn:
            return "warning"
        return None


class AnomalyEngine:
    """
    Runs both detectors and applies alert de-duplication.

    Without the cooldown a 10 Hz sensor sitting just above a threshold emits ten
    identical events per second. That floods the uplink and is useless to an
    operator, so repeats of the same (sensor, kind, severity) are suppressed for
    ``cooldown_ms``. Suppression is counted, not silently discarded, so the
    dashboard can still show that the condition persisted.
    """

    def __init__(self, zscore_threshold: float = 3.0, min_history: int = 20,
                 cooldown_ms: int = 10_000) -> None:
        self.z = ZScoreDetector(zscore_threshold, min_history)
        self.t = ThresholdDetector()
        self.cooldown_ms = cooldown_ms
        self._last_emitted: Dict[tuple, int] = {}
        self.count = 0
        self.suppressed = 0

    def _allow(self, r: dict, kind: str, severity: str) -> bool:
        key = (r["sensor_id"], kind, severity)
        last = self._last_emitted.get(key)
        if last is not None and r["ts"] - last < self.cooldown_ms:
            self.suppressed += 1
            return False
        self._last_emitted[key] = r["ts"]
        return True

    def evaluate(self, r: dict, smoothed: float) -> List[dict]:
        events: List[dict] = []
        z = self.z.check(r["sensor_id"], smoothed)
        if z is not None and self._allow(r, "statistical_outlier", "warning"):
            events.append(self._event(r, smoothed, "statistical_outlier", "warning",
                                      f"z-score {z} exceeds threshold"))
        sev = self.t.check(r["sensor_type"], smoothed)
        if sev and self._allow(r, "threshold_breach", sev):
            events.append(self._event(r, smoothed, "threshold_breach", sev,
                                      f"{r['sensor_type']} at {round(smoothed, 2)}{r.get('unit', '')}"))
        self.count += len(events)
        return events

    @staticmethod
    def _event(r: dict, value: float, kind: str, severity: str, detail: str) -> dict:
        return {
            "sensor_id": r["sensor_id"],
            "sensor_type": r["sensor_type"],
            "zone": r["zone"],
            "ts": r["ts"],
            "value": round(value, 3),
            "kind": kind,
            "severity": severity,
            "detail": detail,
            "detected_at": "fog",
        }
