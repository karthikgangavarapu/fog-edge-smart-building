# Architecture notes

## Tier responsibilities

| Tier | Runs where | Owns |
|---|---|---|
| Sensor | device / simulator | signal generation, sampling frequency, dispatch batching |
| Fog | building gateway | validation, smoothing, aggregation, anomaly detection, buffering, uplink |
| Cloud | Azure | durable storage, cross-site queries, dashboards, elasticity |

The dividing line is latency and data volume. Anything that must be decided in
milliseconds, or that would be wasteful to ship, is done at the fog tier.
Anything that needs history, cross-building context or elastic compute is done
in the cloud.

## Why a queue between ingest and persistence

The fog node's dispatch rate is bursty: a whole campus of fog nodes reconnecting
after a network blip will deliver their spooled backlog at once. If the ingest
endpoint wrote synchronously to the database, that burst would be absorbed by
request latency and eventually by timeouts and lost data.

Placing Service Bus between the two means:

* ingest execution time is flat and small, so the Consumption plan can scale it
  out linearly with request rate;
* the queue depth is both the buffer and the autoscaling signal for the
  processor (Azure's target-based scaler adds instances as the backlog grows);
* a failed write is retried by the platform and dead-lettered after
  `maxDeliveryCount`, rather than being silently dropped.

Measured locally: 2000 batches accepted in 1.2 s (1673 req/s, p95 24.8 ms, zero
errors) while the queue absorbed a high-water backlog of 1890 messages, which
then drained to zero with no loss.

## Delivery semantics

The chain is at-least-once end to end:

1. MQTT QoS 1 from sensor to fog.
2. The fog spool only deletes a batch after the backend returns 2xx.
3. Service Bus redelivers until the processor completes.

De-duplication makes this *effectively* once at the sink. Every batch carries a
`batch_id`; the local backend records it in a `processed_batches` ledger inside
the same transaction as the writes, and the Cosmos documents use deterministic
IDs derived from `batch_id`, so a redelivery upserts rather than duplicates.

## An honest negative result

Increasing the local backend's consumer count from 1 to 4 did not improve drain
throughput (115 vs 118 msg/s). The bottleneck is SQLite's single-writer lock,
not the consumers. This is exactly why the cloud design uses Cosmos DB
partitioned on `/zone`: consumer concurrency only pays off when the sink itself
is horizontally partitioned. Scaling the compute tier without scaling the data
tier moves the queue, it does not remove it.

## Cost controls

* Cosmos serverless: billed per request unit, near-zero when idle.
* Per-document TTL of one hour on raw sample traces; aggregates are kept.
* A selective indexing policy: only `doc_type`, `sensor_type`, `zone`,
  `window_end` and `ts` are indexed, since those are the only predicates the
  dashboard queries use.
* `maxConcurrentCalls: 32` caps processor fan-out so a burst cannot exhaust the
  RU budget.
