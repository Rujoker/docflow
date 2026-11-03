# 10 — Kafka for analytics

A bridge copies domain events from RabbitMQ (`docflow.events`) into the Kafka topic `document-events` (3 partitions, key = `documentId`). The Analytics service reads it in consumer group `analytics`, commits offsets manually after processing.

Why Kafka here and not another RabbitMQ queue: analytics needs the full history, must be able to recompute from scratch, and a new consumer must see past events. A queue deletes what it delivered; a log keeps it.

## Keys and partitions

- With `documentId` as the key, events spread across all 3 partitions.
- With a constant key (`"all"`) every event went to one partition — a hot partition. The other consumers in the group sit idle.
- The bad key stays in the data: after replay, P0 held 15 events (10 of them keyed `all`), P1 — 10, P2 — 5. Kafka does not rewrite history until retention removes it.

## Consumers vs partitions

With 4 Analytics instances and 3 partitions, one instance got an empty assignment. Killing a working instance triggered `Revoked` / `Assigned` and the idle one took over. Parallelism in a group is capped by the number of partitions.

## Replay

Stopped Analytics, reset the group: `kafka-consumer-groups.sh --group analytics --topic document-events --reset-offsets --to-earliest --execute` (only works while the group is inactive).

- On start every partition was re-read from offset 0: 30 events, 120 issues — 30 uploads of `policy-1.txt` × 4 issues. Nothing lost or duplicated on the way through outbox → RabbitMQ → bridge → Kafka.
- Partitions were read one after another (P0, then P1, then P2): ordering holds only within a partition.
- A new group (`Analytics__GroupId=analytics-v2`) read the whole history independently. The RabbitMQ equivalent — a new queue bound to the exchange — would only receive events published after it was created.

## Slow processing triggers a rebalance

`MaxPollIntervalMs = 10000`, `SessionTimeoutMs = 6000` (librdkafka requires `max.poll.interval.ms >= session.timeout.ms`), `Thread.Sleep(15000)` in the handler.

- After 10 s the broker considered the consumer dead and removed it from the group: `Revoked partitions: 0,1,2`.
- When the handler finished, `Commit` failed with `Broker: Unknown member`, the exception escaped the loop and the host stopped.
- The offset was never committed, so the same message comes back on restart. A slow handler turns an ordinary message into a redelivery loop.
- Fixes: raise `max.poll.interval.ms`, fetch fewer records per poll, or move long work off the poll loop and `Pause` / `Resume` the partition.

## Poison message

Produced a record with header `message-type: DocumentAnalyzed` and value `not-json`.

- Before the fix: `JsonException` escaped the loop, `BackgroundServiceExceptionBehavior.StopHost` stopped the process. Offset 11 was never committed, so every restart read the same record and crashed again. With a single instance every partition stops, not just the one with the bad record; with several instances the partition moves to the next consumer after a rebalance and kills it too.
- Why it blocks: a Kafka consumer's position is one number per partition. Unlike RabbitMQ there is no per-message reject — the only way past a record is to commit an offset beyond it.
- After the fix: deserialization errors are caught, the record is produced to `document-events.dlt` with the original key, value and headers plus `dlt-error` and `dlt-source`, then the offset is committed and reading continues.

```
message-type:DocumentAnalyzed,dlt-error:'not-json' is an invalid JSON literal. ...,dlt-source:document-events/1/11	not-json
```

Only `JsonException` goes to the DLT. Other failures still stop the consumer: sending transient errors to the DLT would skip records that only needed a retry. The DLT publish is synchronous with `acks=all`; if it fails the offset is not committed and the record is read again.

## Kafka or RabbitMQ in this project

Commands to analyze a document go through RabbitMQ: each message is a unit of work that needs a per-message ack, delayed retries, a DLQ and load balancing across workers. Analytics reads from Kafka because it needs the history: it can recompute everything, and a new consumer gets past events without any change on the producer side.
