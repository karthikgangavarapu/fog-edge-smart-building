"""
The five virtual sensor types for the smart-building / HVAC domain.

Each subclass only has to describe its own physical behaviour; sampling,
batching, fault injection and serialisation are inherited from VirtualSensor.
"""
from __future__ import annotations

import math

from .base import VirtualSensor


class TemperatureSensor(VirtualSensor):
    """Zone air temperature. Slow diurnal swing around a 21 C setpoint."""

    sensor_type = "temperature"
    unit = "C"

    def _clean_value(self, t: float) -> float:
        return 21.0 + self.diurnal(t, 5.5)


class HumiditySensor(VirtualSensor):
    """Relative humidity, anti-correlated with temperature."""

    sensor_type = "humidity"
    unit = "%"

    def _clean_value(self, t: float) -> float:
        return max(15.0, min(85.0, 45.0 - self.diurnal(t, 8.0)))


class CO2Sensor(VirtualSensor):
    """
    CO2 ppm. Rises while the zone is occupied and decays when ventilation
    catches up. Modelled as a leaky integrator so it lags occupancy.
    """

    sensor_type = "co2"
    unit = "ppm"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._level = 450.0

    def _clean_value(self, t: float) -> float:
        occupancy_pressure = max(0.0, self.diurnal(t, 1.0)) * 55.0
        self._level += occupancy_pressure - 0.08 * (self._level - 430.0)
        return max(400.0, self._level)


class OccupancySensor(VirtualSensor):
    """People count from a PIR/ToF counter. Integer, bursty, never negative."""

    sensor_type = "occupancy"
    unit = "count"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._count = 0

    def _clean_value(self, t: float) -> float:
        target = max(0.0, 12.0 * self.diurnal(t, 1.0))
        # move gradually toward the target so the series is not pure noise
        self._count += (1 if self._count < target else -1) * self.rng.randint(0, 2)
        self._count = max(0, self._count)
        return float(self._count)


class PowerMeterSensor(VirtualSensor):
    """
    Sub-metered HVAC electrical load in watts. Highest sample rate of the five,
    which is why its dispatch batch is the largest. This is the sensor that
    proves the frequency/dispatch decoupling.
    """

    sensor_type = "power"
    unit = "W"

    def _clean_value(self, t: float) -> float:
        base = 1900.0 + 800.0 * max(0.0, self.diurnal(t, 1.0))
        compressor_cycle = 250.0 * math.sin(2 * math.pi * t / 7.0)
        return max(0.0, base + compressor_cycle)


REGISTRY = {
    "temperature": TemperatureSensor,
    "humidity": HumiditySensor,
    "co2": CO2Sensor,
    "occupancy": OccupancySensor,
    "power": PowerMeterSensor,
}


def build(kind: str, **kwargs) -> VirtualSensor:
    """Factory used by the config loader."""
    if kind not in REGISTRY:
        raise ValueError(f"unknown sensor type {kind!r}; known: {sorted(REGISTRY)}")
    return REGISTRY[kind](**kwargs)
