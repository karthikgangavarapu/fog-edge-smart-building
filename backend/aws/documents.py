"""
Pure translation from a fog envelope into DynamoDB items.

Kept free of boto3 and of any Lambda context so it can be unit tested directly,
which is what tests/test_aws.py does. The Lambda handler is then a thin shell
around this function.

Single-table design
-------------------
One DynamoDB table holds three item types, separated by the partition key:

    PK                          SK                    meaning
    AGG#<sensor_type>#<zone>    <window_end>          one aggregated window
    ANOM#<sensor_type>          <ts>#<sensor_id>      one anomaly event
    RAW#<sensor_type>           <ts>#<sensor_id>      a thinned raw sample
    BATCH#<batch_id>            META                  idempotency ledger entry

The dashboard only ever queries by (sensor_type, zone) ordered by time, which
is exactly a single-partition Query. That is the cheapest read DynamoDB offers
and it is why the key was chosen this way (see report, Sec. III-D).
"""
from __future__ import annotations

from typing import Any, Dict, List

RAW_TTL_SECONDS = 3600      # raw traces exist only to draw charts


def build_items(envelope: Dict[str, Any], now_epoch: int) -> List[Dict[str, Any]]:
    """Translate one fog envelope into a list of DynamoDB items."""
    batch_id = envelope.get("batch_id", "")
    fog_id = envelope.get("fog_id", "")
    items: List[Dict[str, Any]] = []

    for a in envelope.get("aggregates", []):
        items.append({
            "PK": f"AGG#{a['sensor_type']}#{a['zone']}",
            # window_end is zero padded so lexical sort equals time order
            "SK": f"{int(a.get('window_end') or 0):013d}",
            "doc_type": "aggregate",
            "batch_id": batch_id, "fog_id": fog_id,
            "sensor_type": a["sensor_type"], "zone": a["zone"],
            "unit": a.get("unit", ""), "count": a["count"],
            "min": a["min"], "max": a["max"], "mean": a["mean"],
            "p95": a["p95"], "last": a["last"],
            "window_start": a.get("window_start"), "window_end": a.get("window_end"),
        })

    for x in envelope.get("anomalies", []):
        items.append({
            "PK": f"ANOM#{x['sensor_type']}",
            "SK": f"{int(x['ts']):013d}#{x['sensor_id']}#{x['kind']}",
            "doc_type": "anomaly",
            "batch_id": batch_id, "fog_id": fog_id,
            "sensor_id": x["sensor_id"], "sensor_type": x["sensor_type"],
            "zone": x["zone"], "ts": x["ts"], "value": x["value"],
            "kind": x["kind"], "severity": x["severity"],
            "detail": x.get("detail", ""),
        })

    for s in envelope.get("raw_sample", []):
        items.append({
            "PK": f"RAW#{s['sensor_type']}",
            "SK": f"{int(s['ts']):013d}#{s['sensor_id']}",
            "doc_type": "raw",
            "batch_id": batch_id,
            "sensor_id": s["sensor_id"], "sensor_type": s["sensor_type"],
            "zone": s["zone"], "ts": s["ts"],
            "raw": s["raw"], "smoothed": s["smoothed"],
            # DynamoDB deletes these automatically, so the charts stay cheap.
            "expires_at": now_epoch + RAW_TTL_SECONDS,
        })

    return items


def to_decimal(obj: Any) -> Any:
    """
    DynamoDB rejects float, so every number becomes Decimal on the way in.

    json.loads(..., parse_float=Decimal) handles the top level, but values that
    arrive as Python floats after arithmetic still need converting, hence this.
    """
    from decimal import Decimal
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_decimal(v) for v in obj]
    return obj


def from_decimal(obj: Any) -> Any:
    """Inverse of to_decimal, applied on the way out to the dashboard."""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        f = float(obj)
        return int(f) if f.is_integer() else f
    if isinstance(obj, dict):
        return {k: from_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [from_decimal(v) for v in obj]
    return obj
