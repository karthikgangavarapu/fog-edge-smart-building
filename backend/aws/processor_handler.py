"""
Lambda 2 of 3: PROCESSOR.

    SQS queue --(event source mapping)--> this Lambda --> DynamoDB

Lambda polls the queue and scales the number of concurrent executions with the
backlog, so the queue depth is both the buffer that absorbs a burst and the
signal that adds capacity.

Idempotency
-----------
SQS is at-least-once, so this function must tolerate seeing the same batch
twice. It claims the batch first with a conditional PutItem on
``BATCH#<batch_id>``; if the condition fails the batch has already been
applied and the message is acknowledged without writing anything again. That
turns at-least-once delivery into effectively-once persistence.

Partial batch failures are reported back to SQS so that one poisonous message
does not force the redelivery of the nine good ones alongside it.
"""
from __future__ import annotations

import json
import logging
import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from documents import build_items, to_decimal

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE_NAME = os.environ["TABLE_NAME"]
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


def _claim_batch(batch_id: str) -> bool:
    """Return True if this invocation is the first to see the batch."""
    if not batch_id:
        return True
    try:
        table.put_item(
            Item={"PK": f"BATCH#{batch_id}", "SK": "META",
                  "doc_type": "batch", "received_at": int(time.time())},
            ConditionExpression="attribute_not_exists(PK)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def handler(event, context):
    failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            # parse_float=Decimal because DynamoDB will not accept float.
            envelope = json.loads(record["body"], parse_float=Decimal)
            batch_id = envelope.get("batch_id", "")

            if not _claim_batch(batch_id):
                log.info("duplicate batch %s ignored", batch_id)
                continue

            items = build_items(envelope, now_epoch=int(time.time()))
            if items:
                with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as writer:
                    for item in items:
                        writer.put_item(Item=to_decimal(item))
            log.info("persisted %d items for batch %s", len(items), batch_id)

        except Exception:
            # Reported as a partial failure: only this message is redelivered,
            # and after maxReceiveCount it lands in the dead-letter queue.
            log.exception("failed to process message %s", message_id)
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
