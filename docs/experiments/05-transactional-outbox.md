# 05 — Transactional outbox

The API no longer publishes to RabbitMQ. It writes the document and an `outbox_messages` row in one `SaveChangesAsync` (one transaction). `OutboxRelay`, a background service in the API, takes up to 20 unsent rows with `FOR UPDATE SKIP LOCKED`, publishes them with publisher confirms, sets `processed_at` and commits.

The outbox row id is used as the broker `MessageId`, so a republished message carries the same id as the original.

## Broker down

`docker compose stop rabbitmq`, then 5 uploads. The API kept accepting documents; the relay logged the broker as unreachable and kept retrying, the rows sat in `outbox_messages` with `processed_at = NULL`. After `docker compose start rabbitmq` the relay delivered all of them on its own and `processed_at` was filled in. Compare with [03](03-reliable-publishing.md): the same outage used to give 503 and an orphan file.

## Crash between publish and commit

API started with `Chaos__CrashAfterPublish=true`: the relay publishes the command and the process dies before committing. The command is already in `analyze-document`, the outbox row still has `processed_at = NULL`. After a restart without the flag the relay sends it again, and the worker processes the same document **twice**. Both deliveries carry the **same `MessageId`** (the outbox row id), so the duplicate is detectable — the idempotent consumer in [06](06-idempotent-consumer.md) relies on exactly that.

## Two API instances

Two API instances on ports 5101 and 5102, each running its own relay over the same table, 50 uploads split between them. Every command was published exactly once — no repeated `MessageId` in the worker log.

What the locking clause does:

| Query | Duplicates | Relays in parallel |
| --- | --- | --- |
| `FOR UPDATE SKIP LOCKED` | no | yes, each relay takes rows the others haven't locked |
| `FOR UPDATE` | no | no: a relay waits for the other's lock, then re-checks `processed_at IS NULL` and skips the rows already sent |
| plain `SELECT` | yes: both relays read and publish the same rows | nominally, but the work is done twice |

## Publish failure in the middle of a batch

A batch of 5, and publishing the 5th message throws.

- `SaveChangesAsync` is never reached, the transaction rolls back on dispose, `processed_at` stays `NULL` for all 5 rows.
- On the next poll the relay takes the same batch: messages 1–4, already accepted by the broker, are **published again** (same `MessageId`), the 5th is retried.
- Result: duplicates, never a lost message. The outbox gives at-least-once delivery, so the consumer has to be idempotent.

If the 5th message fails **every** time (for example `NO_ROUTE` with `mandatory: true` after a wrong routing key), the batch never advances: messages 1–4 are re-sent on every poll, and everything queued behind the batch waits forever. A poison message, only in the outbox instead of a queue.

Mitigations (not implemented here):

- mark each row as sent right after its own publish instead of the whole batch at the end — at most one duplicate instead of a batch;
- an attempt counter and last error on the outbox row; after N failures the row is marked as failed and skipped, so it can't block the rest.
