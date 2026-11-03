# 10b — Debezium CDC and duplicate-safe analytics

Two independent paths now carry the same outbox event to Kafka:

- **relay → RabbitMQ → bridge → `document-events`** (polling outbox relay, v0.6.0–v0.11.0);
- **outbox → WAL → Debezium → `outbox.docflow.events`** (logical replication, `pgoutput`, Outbox Event Router).

Both are at-least-once. Analytics deduplicates by the outbox row id, which both paths carry as the `message-id` header.

## Analytics state in Postgres

In one Postgres transaction per record:

1. `INSERT INTO analytics.processed_events (message_id) ... ON CONFLICT DO NOTHING` — 0 rows means a duplicate;
2. update `analytics.stats` only if the id is new;
3. upsert the next offset for the topic-partition into `analytics.offsets`.

On partition assignment the consumer seeks to the offsets stored in Postgres, not to the group's committed offsets. Results and position change atomically, so a crash can never leave "counted but not advanced" or the reverse. Delivery stays at-least-once; the effect is exactly-once, without Kafka transactions. The Kafka commit is kept only so `kafka-consumer-groups.sh --describe` still shows lag.

## Experiments

### Duplicate from the bridge

The bridge produced every record twice (simulating a crash between the Kafka write and the RabbitMQ ack). 5 documents → 10 records in Kafka → stats +5 analyzed, +20 issues (36 / 144). Each second copy was logged as `duplicate … skipped`.

### Who owns the position

Reset the `analytics` group to earliest in Kafka, then restarted Analytics:

```
Assigned, resuming from store: document-events [[0]] @17, document-events [[1]] @12, document-events [[2]] @8
```

The Kafka reset was ignored, nothing was re-read, stats unchanged. P1 resumed at 12 — right after the poison record at offset 11, whose skip is also stored.

### Replay

Replay is now done on the database side: truncate `analytics.processed_events` and `analytics.offsets`, zero the stats, restart. The full log was re-read: 41 `DocumentAnalyzed` records, 36 counted — the 5 duplicates from the experiment above were dropped again. The poison record was sent to the DLT once more: the DLT is at-least-once too, so DLT entries should be reasoned about by `dlt-source`, not counted.

### Initial snapshot re-published the whole outbox

With the default `snapshot.mode = initial`, Debezium's first start read every existing row of `outbox_messages` with a plain `SELECT` and published it — the insert-only publication filter does not apply to the snapshot. 42 historical events went to Kafka, all with key `00000000-…` (the default value `aggregate_id` got in the migration), so they all landed in one partition.

Analytics counted only 6 of them: 3 `DocumentAnalyzed` (9 issues) and 3 `DocumentFailed` from before the bridge existed, which had never reached Kafka. The other 36 had the same `message-id` as their bridge copies and were skipped. Stats went from 36 / 0 / 144 to 39 / 3 / 153.

Fix for new connectors: `"snapshot.mode": "no_data"` — start from the current WAL position.

### Both paths at once

5 documents with the bridge and Debezium both running. Every event arrived twice, once per topic; stats +5. Debezium's copy consistently arrived first: it reads the WAL right after commit, while the bridge path waits for the relay poll (up to 1 s) and an extra broker hop.

### Bridge stopped

With the bridge stopped, events still reached Analytics through Debezium. Messages piled up in the `kafka-bridge.document-events` queue and were skipped as duplicates once the bridge came back.

### Connect down, and a slot that would not advance

Slot lag measured with `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)`:

| Step | Slot active | Lag |
| --- | --- | --- |
| Connect running, idle | yes | 4 kB |
| `docker compose stop connect`, 10 documents | no | 79 kB |
| 20 s after `docker compose start connect` | yes | 152 kB |
| 60 s later | yes | 85 kB |
| with `heartbeat.interval.ms` only, 90 s | yes | 83–84 kB, `confirmed_flush_lsn` frozen |
| with a heartbeat table in the publication | yes | 0.9 kB, stable |

- Nothing was lost: all 10 events reached Analytics after the restart. 10 documents wrote ~8 kB of WAL each (outbox, documents, inbox, the relay's `processed_at` updates), though only one row per document is published.
- Debezium confirms the slot position only when Kafka Connect flushes offsets (every 60 s by default), which is why lag peaked right after the restart.
- After catching up the slot stayed ~85 kB behind. Debezium can only confirm an LSN it has received, and the publication hands it nothing but event inserts. Everything else written to WAL afterwards — Analytics writing to `analytics.*`, relay updates, background activity — sits behind the slot. With no new events the slot would never move while WAL keeps growing: a quiet captured table in a busy database fills the disk without a single error.
- `heartbeat.interval.ms` alone did not help: the heartbeat topic appeared, `confirmed_flush_lsn` did not move.
- `heartbeat.action.query = SELECT pg_logical_emit_message(...)`, the table-less option for Postgres 14+, did not help either in this setup: the messages were written to WAL but no message events reached the connector.
- What worked: a `debezium_heartbeat` table added to the publication and `heartbeat.action.query = INSERT INTO debezium_heartbeat DEFAULT VALUES` every 10 s. The insert is an ordinary published change, so the connector always has a fresh LSN to confirm. A predicate keeps the Outbox Event Router on `docflow.public.outbox_messages` only, so heartbeat rows pass through untouched. The table needs periodic cleanup; deletes are not published.

Other safeguards: alert on slot lag, and `max_slot_wal_keep_size` to cap retained WAL (Postgres then invalidates the slot and the connector needs a fresh snapshot).

### Publication filter

`kafka-topics.sh --list` shows no `outbox.docflow.commands`, although commands live in the same table: the publication carries only `exchange = 'docflow.events'` and only inserts. Without `publish = 'insert'` every relay `UPDATE processed_at` would reach the Event Router, which expects creates only and would log a warning for each.

### Stale stored offsets after recreating Kafka

Recreating the Kafka container (new listeners, no volume) recreated `document-events` from scratch, but `analytics.offsets` still pointed at offsets 12 and 8 of the old topic. librdkafka reported `Offset out of range` and reset to the beginning. Positions stored outside Kafka outlive the topic, so recreating a topic means resetting them too. Deduplication made it harmless here.
