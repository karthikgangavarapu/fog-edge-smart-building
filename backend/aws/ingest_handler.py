"""
Lambda 1 of 3: INGEST.

    fog node --HTTPS--> API Gateway (HTTP API) --> this Lambda --> SQS queue

Deliberately trivial. It authenticates the caller, checks the envelope is
well formed, puts one message on the queue and returns 202 Accepted. It never
touches DynamoDB.

Why: the execution time is flat and independent of payload complexity, so
Lambda's concurrency scaling tracks request rate almost linearly. Everything
slow and failure-prone happens downstream of the queue, where it can be
retried without the fog node waiting. This is the queue-based load levelling
pattern (see report, Sec. III-C).
"""
from __future__ import annotations

import json
import logging
import os

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

QUEUE_URL = os.environ["TELEMETRY_QUEUE_URL"]
API_KEY = os.environ.get("FOG_API_KEY", "")

sqs = boto3.client("sqs")


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    # API Gateway lowercases header names in the HTTP API payload format.
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    if API_KEY and headers.get("x-api-key") != API_KEY:
        return _response(401, {"error": "unauthorized"})

    try:
        envelope = json.loads(event.get("body") or "{}")
    except ValueError:
        return _response(400, {"error": "invalid json"})

    if "batch_id" not in envelope or "aggregates" not in envelope:
        return _response(400, {"error": "malformed envelope"})

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(envelope),
        # Carried through so the processor can log which batch it is handling
        # even before parsing the body.
        MessageAttributes={
            "batch_id": {"DataType": "String", "StringValue": envelope["batch_id"]},
        },
    )

    log.info("accepted batch %s from fog %s", envelope["batch_id"], envelope.get("fog_id"))
    # 202, not 201: accepted for asynchronous processing.
    return _response(202, {"status": "accepted", "batch_id": envelope["batch_id"]})
