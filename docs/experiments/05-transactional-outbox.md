# 05 — Transactional outbox

The API no longer publishes to RabbitMQ. It writes the document and an `outbox_messages` row in one `SaveChangesAsync` (one transaction). `OutboxRelay`, a background service in the API, takes up to 20 unsent rows with `FOR UPDATE SKIP LOCKED`, publishes them with publisher confirms, sets `processed_at` and commits.

The outbox row id is used as the broker `MessageId`, so a republished message carries the same id as the original.

## Publish failure in the middle of a batch

A batch of 5, and publishing the 5th message throws.

- `SaveChangesAsync` is never reached, the transaction rolls back on dispose, `processed_at` stays `NULL` for all 5 rows.
- On the next poll the relay takes the same batch: messages 1–4, already accepted by the broker, are **published again** (same `MessageId`), the 5th is retried.
- Result: duplicates, never a lost message. The outbox gives at-least-once delivery, so the consumer has to be idempotent.

If the 5th message fails **every** time (for example `NO_ROUTE` with `mandatory: true` after a wrong routing key), the batch never advances: messages 1–4 are re-sent on every poll, and everything queued behind the batch waits forever. A poison message, only in the outbox instead of a queue.

Mitigations (not implemented here):

- mark each row as sent right after its own publish instead of the whole batch at the end — at most one duplicate instead of a batch;
- an attempt counter and last error on the outbox row; after N failures the row is marked as failed and skipped, so it can't block the rest.
