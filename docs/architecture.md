# Architecture notes

## Tier responsibilities

| Tier | Runs where | Owns |
|---|---|---|
| Sensor | device / simulator | signal generation, sampling frequency, dispatch batching |
| Fog | building gateway | validation, smoothing, aggregation, anomaly detection, buffering, uplink |
| Cloud | AWS | durable storage, cross-site queries, dashboards, elasticity |

The dividing line is latency and data volume. Anything that must be decided in
milliseconds, or that would be wasteful to ship, is done at the fog tier.
Anything that needs history, cross-building context or elastic compute is done
in the cloud.

## Why a queue between ingest and persistence

The fog node's dispatch rate is bursty: a whole campus of fog nodes reconnecting
after a network blip will deliver their spooled backlog at once. If the ingest
endpoint wrote synchronously to the database, that burst would be absorbed by
request latency and eventually by timeouts and lost data.

Placing SQS between the two means:

* ingest execution time is flat and small, so Lambda concurrency scales with
  request rate rather than with database latency;
* the visible backlog is both the buffer and the scaling signal, because the
  Lambda event source mapping adds pollers as the queue grows;
* a failed write is retried by the platform and dead-lettered after
  `maxReceiveCount`, rather than being silently dropped.

Measured locally: 2000 batches accepted in 1.2 s (1673 req/s, p95 24.8 ms, zero
errors) while the queue absorbed a high-water backlog of 1890 messages, which
then drained to zero with no loss.

## Delivery semantics

The chain is at-least-once end to end:

1. MQTT QoS 1 from sensor to fog.
2. The fog spool only deletes a batch after the backend returns 2xx.
3. SQS redelivers until the processor reports success.

De-duplication makes this *effectively* once at the sink. Every batch carries a
`batch_id`. The local backend records it in a `processed_batches` ledger inside
the same transaction as the writes; the processor Lambda claims it with a
conditional `PutItem` on `BATCH#<batch_id>` before writing anything, and the
DynamoDB item keys are deterministic, so a redelivery overwrites in place
rather than duplicating.

Partial batch failures are reported back to SQS with `ReportBatchItemFailures`,
so one poisonous message does not force the redelivery of the nine good ones
alongside it.

## Data model

One DynamoDB table holds everything, separated by partition key:

| PK | SK | Item |
|---|---|---|
| `AGG#<sensor_type>#<zone>` | zero-padded `window_end` | one aggregated window |
| `ANOM#<sensor_type>` | `ts#sensor_id#kind` | one anomaly event |
| `RAW#<sensor_type>` | `ts#sensor_id` | a thinned raw sample, TTL one hour |
| `BATCH#<batch_id>` | `META` | idempotency ledger entry |

The dashboard only queries by sensor type and zone ordered by time, which is
exactly a single-partition Query, the cheapest read DynamoDB offers. Sort keys
are zero-padded so that lexical order equals chronological order.

## An honest negative result

Increasing the local backend's consumer count from 1 to 4 did not improve drain
throughput (115 vs 118 msg/s). The bottleneck is SQLite's single-writer lock,
not the consumers. This is exactly why the cloud design uses DynamoDB
partitioned by sensor type and zone: consumer concurrency only pays off when
the sink itself is horizontally partitioned. Scaling the compute tier without
scaling the data tier moves the queue, it does not remove it.

## Cost controls

* DynamoDB on-demand billing, so an idle building costs nothing.
* A one-hour TTL on raw sample traces; aggregates are kept indefinitely.
* `MaximumConcurrency: 20` on the event source mapping, so a burst cannot fan
  out into an unbounded number of concurrent writers.
* No always-on compute anywhere: all three functions scale to zero.
