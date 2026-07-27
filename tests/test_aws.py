"""
Unit tests for the AWS document builder.

The Lambda handlers are thin shells around build_items, so testing that pure
function covers the logic that actually matters without needing boto3, moto or
a live AWS account.
"""
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "aws"))

from documents import RAW_TTL_SECONDS, build_items, from_decimal, to_decimal


def envelope(batch_id="b1"):
    return {
        "batch_id": batch_id, "fog_id": "fog-test", "schema_version": "1.0",
        "window_start": 1000, "window_end": 6000,
        "aggregates": [{"sensor_type": "temperature", "zone": "z1", "unit": "C",
                        "count": 5, "min": 20.0, "max": 22.0, "mean": 21.0,
                        "p95": 21.9, "last": 21.2, "window_start": 1000,
                        "window_end": 6000}],
        "anomalies": [{"sensor_id": "t1", "sensor_type": "temperature", "zone": "z1",
                       "ts": 5000, "value": 31.0, "kind": "threshold_breach",
                       "severity": "critical", "detail": "hot"}],
        "raw_sample": [{"sensor_id": "t1", "sensor_type": "temperature", "zone": "z1",
                        "ts": 5500, "raw": 31.2, "smoothed": 30.9}],
        "stats": {},
    }


def test_one_item_per_record():
    items = build_items(envelope(), now_epoch=1_700_000_000)
    assert len(items) == 4          # aggregate, anomaly, raw, plus the registry
    assert {i["doc_type"] for i in items} == {"aggregate", "anomaly", "raw", "zone"}


def test_registry_item_lets_the_dashboard_discover_series():
    """
    The dashboard needs to know which (type, zone) pairs exist. Scanning for
    them misses partitions, so every aggregate re-asserts a registry entry.
    """
    items = build_items(envelope(), now_epoch=0)
    reg = next(i for i in items if i["doc_type"] == "zone")
    assert reg["PK"] == "ZONES" and reg["SK"] == "temperature#z1"


def test_partition_keys_group_by_type_and_zone():
    """Per-zone dashboard queries must be single-partition to stay cheap."""
    items = build_items(envelope(), now_epoch=0)
    agg = next(i for i in items if i["doc_type"] == "aggregate")
    assert agg["PK"] == "AGG#temperature#z1"


def test_sort_keys_are_zero_padded_for_time_ordering():
    """Lexical order must equal chronological order, or the charts scramble."""
    env = envelope()
    def agg_sk(items):
        return next(i["SK"] for i in items if i["doc_type"] == "aggregate")

    env["aggregates"][0]["window_end"] = 999
    early = agg_sk(build_items(env, now_epoch=0))
    env["aggregates"][0]["window_end"] = 1_700_000_000_000
    late = agg_sk(build_items(env, now_epoch=0))
    assert early < late


def test_raw_samples_carry_a_ttl_but_aggregates_do_not():
    """Raw traces are disposable; aggregates are the record of what happened."""
    items = build_items(envelope(), now_epoch=1_000_000)
    raw = next(i for i in items if i["doc_type"] == "raw")
    agg = next(i for i in items if i["doc_type"] == "aggregate")
    assert raw["expires_at"] == 1_000_000 + RAW_TTL_SECONDS
    assert "expires_at" not in agg


def test_identical_records_produce_identical_keys():
    """A redelivered batch must overwrite rather than duplicate."""
    first = build_items(envelope(), now_epoch=1)
    second = build_items(envelope(), now_epoch=2)
    assert [(i["PK"], i["SK"]) for i in first] == [(i["PK"], i["SK"]) for i in second]


def test_empty_envelope_produces_nothing():
    assert build_items({"batch_id": "x", "aggregates": []}, now_epoch=0) == []


def test_registry_keys_are_stable_across_batches():
    """Re-asserting the registry must overwrite, never accumulate duplicates."""
    a = [i for i in build_items(envelope("b1"), 0) if i["doc_type"] == "zone"]
    b = [i for i in build_items(envelope("b2"), 0) if i["doc_type"] == "zone"]
    assert [(i["PK"], i["SK"]) for i in a] == [(i["PK"], i["SK"]) for i in b]


def test_floats_are_converted_for_dynamodb():
    """DynamoDB rejects float outright, so the conversion is not optional."""
    converted = to_decimal({"mean": 21.5, "nested": [1.25, {"x": 0.5}]})
    assert isinstance(converted["mean"], Decimal)
    assert isinstance(converted["nested"][0], Decimal)
    assert isinstance(converted["nested"][1]["x"], Decimal)


def test_decimals_are_converted_back_for_the_dashboard():
    out = from_decimal({"a": Decimal("21.5"), "b": Decimal("5"), "c": [Decimal("1.5")]})
    assert out == {"a": 21.5, "b": 5, "c": [1.5]}
    assert isinstance(out["b"], int)
