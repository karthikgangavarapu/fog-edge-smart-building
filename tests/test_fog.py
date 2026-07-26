"""Unit tests for the fog processing pipeline."""
import time

from fog.anomaly import AnomalyEngine
from fog.pipeline import Smoother, Thinner, Validator, WindowAggregator
from fog.spool import Spool


def reading(**kw):
    base = dict(sensor_id="t1", sensor_type="temperature", zone="z1",
                value=21.0, unit="C", ts=int(time.time() * 1000), seq=1, quality=1.0)
    base.update(kw)
    return base


def test_validator_rejects_out_of_range():
    v = Validator()
    assert v.accept(reading(seq=1, value=21.0))
    assert not v.accept(reading(seq=2, value=500.0))
    assert v.dropped == 1


def test_validator_rejects_nan_and_low_quality():
    v = Validator()
    assert not v.accept(reading(seq=1, value=float("nan")))
    assert not v.accept(reading(seq=2, value=21.0, quality=0.1))
    assert v.dropped == 2


def test_validator_rejects_replayed_sequence():
    v = Validator()
    assert v.accept(reading(seq=5))
    assert not v.accept(reading(seq=5))    # duplicate
    assert not v.accept(reading(seq=3))    # regression


def test_validator_counts_gaps():
    v = Validator()
    v.accept(reading(seq=1))
    v.accept(reading(seq=4))
    assert v.gaps == 2


def test_smoother_reduces_variance():
    s = Smoother(alpha=0.3)
    noisy = [20, 30, 20, 30, 20, 30, 20, 30]
    out = [s.smooth("t1", float(x)) for x in noisy]
    spread_in = max(noisy) - min(noisy)
    spread_out = max(out[3:]) - min(out[3:])
    assert spread_out < spread_in


def test_window_aggregator_computes_stats():
    agg = WindowAggregator(window_seconds=5)
    for i, val in enumerate([10.0, 20.0, 30.0]):
        agg.add(reading(seq=i, value=val), val)
    out = agg.emit()
    assert len(out) == 1
    a = out[0]
    assert a["count"] == 3 and a["min"] == 10.0 and a["max"] == 30.0
    assert a["mean"] == 20.0 and a["last"] == 30.0


def test_aggregator_separates_type_and_zone():
    agg = WindowAggregator()
    agg.add(reading(zone="z1"), 21.0)
    agg.add(reading(zone="z2"), 25.0)
    agg.add(reading(sensor_type="co2", zone="z1", unit="ppm"), 800.0)
    assert len(agg.emit()) == 3


def test_thinner_bounds_payload_size():
    """Data reduction is the point of the fog tier, so prove it is bounded."""
    th = Thinner(keep=20)
    for i in range(500):
        th.offer(reading(seq=i), 21.0)
    assert len(th.drain()) == 20


def test_threshold_anomaly_detected():
    eng = AnomalyEngine()
    events = eng.evaluate(reading(sensor_type="co2", unit="ppm", value=1600.0), 1600.0)
    assert any(e["kind"] == "threshold_breach" and e["severity"] == "critical"
               for e in events)


def test_zscore_anomaly_detected_after_history():
    eng = AnomalyEngine(zscore_threshold=3.0, min_history=20)
    for i in range(30):
        eng.evaluate(reading(seq=i, value=21.0 + (i % 2) * 0.1), 21.0 + (i % 2) * 0.1)
    events = eng.evaluate(reading(seq=99, value=24.0), 24.0)
    assert any(e["kind"] == "statistical_outlier" for e in events)


def test_repeat_alerts_are_suppressed():
    """A sustained breach must not flood the uplink with identical events."""
    eng = AnomalyEngine(cooldown_ms=10_000)
    t0 = 1_000_000
    first = eng.evaluate(reading(sensor_type="co2", unit="ppm", value=1600.0,
                                 ts=t0, sensor_id="c1"), 1600.0)
    repeat = eng.evaluate(reading(sensor_type="co2", unit="ppm", value=1610.0,
                                  ts=t0 + 500, sensor_id="c1"), 1610.0)
    later = eng.evaluate(reading(sensor_type="co2", unit="ppm", value=1620.0,
                                 ts=t0 + 20_000, sensor_id="c1"), 1620.0)
    assert len(first) == 1 and repeat == [] and len(later) == 1
    assert eng.suppressed == 1


def test_spool_survives_and_acks(tmp_path):
    spool = Spool(str(tmp_path / "s.db"))
    spool.push("b1", {"batch_id": "b1"})
    spool.push("b2", {"batch_id": "b2"})
    assert spool.depth() == 2
    row_id, payload = spool.peek(1)[0]
    assert payload["batch_id"] == "b1"     # FIFO ordering preserved
    spool.ack(row_id)
    assert spool.depth() == 1
    spool.push("b2", {"batch_id": "b2"})   # re-pushing an unacked batch is a no-op
    assert spool.depth() == 1
