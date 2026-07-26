"""Unit tests for the sensor tier."""
import math

from sensors.sensor_types import REGISTRY, build
from sensors.transport import InProcTransport


def test_all_five_sensor_types_registered():
    assert set(REGISTRY) == {"temperature", "humidity", "co2", "occupancy", "power"}


def test_readings_are_within_plausible_range():
    for kind in REGISTRY:
        s = build(kind, sensor_id=f"{kind}-1", zone="z1", noise=0.0, fault_rate=0.0)
        values = [s.read(t).value for t in range(0, 60)]
        assert all(v == v for v in values), f"{kind} produced NaN without faults"
        assert len(set(values)) > 1, f"{kind} produced a constant signal"


def test_dispatch_rate_is_honoured():
    """A sensor with dispatch_every=5 must publish exactly every 5th reading."""
    s = build("temperature", sensor_id="t1", zone="z1", dispatch_every=5)
    batches = [s.offer(s.read(t)) for t in range(10)]
    emitted = [b for b in batches if b]
    assert len(emitted) == 2
    assert all(len(b) == 5 for b in emitted)


def test_sampling_period_follows_frequency():
    fast = build("power", sensor_id="p", zone="z", sample_hz=10.0)
    slow = build("humidity", sensor_id="h", zone="z", sample_hz=0.5)
    assert math.isclose(fast.period, 0.1)
    assert math.isclose(slow.period, 2.0)


def test_sequence_numbers_are_monotonic():
    s = build("co2", sensor_id="c1", zone="z1")
    seqs = [s.read(t).seq for t in range(20)]
    assert seqs == list(range(1, 21))


def test_inproc_transport_wildcard_matching():
    t = InProcTransport()
    got = []
    t.subscribe("building/hq/#", lambda topic, p: got.append(topic))
    t.subscribe("building/hq/+/temperature/+", lambda topic, p: got.append("typed"))
    t.publish("building/hq/floor-2-east/temperature/t1", {"readings": []})
    t.drain()
    assert "building/hq/floor-2-east/temperature/t1" in got
    assert "typed" in got
