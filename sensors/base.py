"""
Base class for virtual sensors.

Design notes
------------
* Every sensor owns its own *sampling frequency* (how often it takes a reading)
  and its own *dispatch rate* (how many samples it batches before publishing).
  Both are configurable per sensor in ``config/sensors.yaml``, which satisfies
  the "configurable frequency & dispatch rates" requirement.
* The signal model is a bounded random walk around a diurnal baseline plus
  Gaussian noise. This produces data that *looks* like a real building rather
  than uniform noise, which makes the dashboards and anomaly detector
  meaningful during the demo.
* Faults (drift, stuck-at, spike, dropout) can be injected on demand so the fog
  layer's cleaning logic can be demonstrated.
"""
from __future__ import annotations

import math
import random
from typing import List, Optional

from .models import SensorReading, now_ms


class VirtualSensor:
    """Abstract virtual sensor. Subclasses define the physical signal."""

    sensor_type: str = "generic"
    unit: str = ""

    def __init__(
        self,
        sensor_id: str,
        zone: str,
        sample_hz: float = 1.0,
        dispatch_every: int = 5,
        noise: float = 0.0,
        fault_rate: float = 0.0,
        seed: Optional[int] = None,
    ) -> None:
        self.sensor_id = sensor_id
        self.zone = zone
        self.sample_hz = sample_hz              # samples per second
        self.dispatch_every = max(1, dispatch_every)
        self.noise = noise
        self.fault_rate = fault_rate
        self.rng = random.Random(seed if seed is not None else hash(sensor_id) & 0xFFFF)
        self.seq = 0
        self._buffer: List[SensorReading] = []
        self._drift = 0.0
        self._stuck_value: Optional[float] = None
        self._stuck_remaining = 0

    # ---- signal model -------------------------------------------------
    @property
    def period(self) -> float:
        """Seconds between samples."""
        return 1.0 / self.sample_hz if self.sample_hz > 0 else 1.0

    def diurnal(self, t: float, amplitude: float) -> float:
        """A 24h sinusoid, compressed so a demo run shows a full 'day'."""
        day_seconds = 60.0  # 1 real minute == 1 simulated day (demo friendly)
        return amplitude * math.sin(2 * math.pi * (t % day_seconds) / day_seconds)

    def _clean_value(self, t: float) -> float:
        raise NotImplementedError

    def _apply_faults(self, value: float) -> tuple[float, float]:
        """Return (value, quality) after optional fault injection."""
        quality = 1.0
        if self._stuck_value is not None:
            # A stuck sensor recovers after a bounded number of samples, the way
            # a real transducer does once its ADC is re-read or the bus resets.
            self._stuck_remaining -= 1
            if self._stuck_remaining <= 0:
                self._stuck_value = None
            else:
                return self._stuck_value, 0.2
        if self.fault_rate and self.rng.random() < self.fault_rate:
            fault = self.rng.choice(["spike", "drift", "stuck", "dropout"])
            if fault == "spike":
                # Deliberately kept inside the physically-valid range: these
                # spikes survive the fog range check and must instead be caught
                # by the statistical detector (see report, Sec. III-B).
                value *= self.rng.choice([1.35, 0.65, 1.5])
                quality = 0.3
            elif fault == "drift":
                # Bounded so a long run does not walk the signal out of range.
                self._drift = max(-2.0, min(2.0, self._drift + self.rng.uniform(-0.5, 0.5)))
                quality = 0.7
            elif fault == "stuck":
                self._stuck_value = value
                self._stuck_remaining = self.rng.randint(5, 15)
                quality = 0.2
            elif fault == "dropout":
                return float("nan"), 0.0
        return value + self._drift, quality

    def read(self, t: float) -> SensorReading:
        """Take one sample at simulated time ``t`` (seconds since start)."""
        value = self._clean_value(t)
        if self.noise:
            value += self.rng.gauss(0.0, self.noise)
        value, quality = self._apply_faults(value)
        self.seq += 1
        return SensorReading(
            sensor_id=self.sensor_id,
            sensor_type=self.sensor_type,
            zone=self.zone,
            value=round(value, 3) if value == value else value,  # NaN-safe
            unit=self.unit,
            ts=now_ms(),
            seq=self.seq,
            quality=quality,
        )

    # ---- dispatch policy ----------------------------------------------
    def offer(self, reading: SensorReading) -> Optional[List[SensorReading]]:
        """
        Buffer a reading; return a batch when ``dispatch_every`` is reached.

        Decoupling sampling from dispatch is deliberate: a vibration-style
        sensor can sample fast but publish rarely, which is exactly the
        trade-off constrained IoT links force on you.
        """
        self._buffer.append(reading)
        if len(self._buffer) >= self.dispatch_every:
            batch, self._buffer = self._buffer, []
            return batch
        return None

    def flush(self) -> List[SensorReading]:
        batch, self._buffer = self._buffer, []
        return batch
