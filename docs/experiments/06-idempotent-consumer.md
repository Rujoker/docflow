# 06 — Idempotent consumer, domain events, Notifier

The worker now owns the document's state. For each `AnalyzeDocument` it:

1. skips the message if its `MessageId` is already in `processed_messages` for consumer `worker` (protection by message);
2. skips it if the document is no longer `Pending` (protection by state);
3. calls the LLM outside any database transaction;
4. in one `SaveChangesAsync` stores the result, the `processed_messages` row and a `DocumentAnalyzed` outbox row;
5. treats a unique-key violation at commit as "another instance was faster" and drops its own result;
6. acks only after the commit.

On giving up it marks the document `Failed` and adds `DocumentFailed` to the outbox before publishing to the DLQ. Events go out through the worker's own `OutboxRelay` to the `docflow.events` topic exchange. `DocFlow.Notifier` consumes `notifier.document-events` (`document.*`) and keeps its own `processed_messages` rows under consumer `notifier`.

## Same message delivered twice

Worker stopped, a document uploaded, a second copy of its command published by hand with the same `message_id`, worker started.

- Expected and observed: the LLM is called **once**. The single worker with `prefetchCount: 1` handles the copies one after another; by the time the second arrives, the first is committed and the `processed_messages` check skips it before the LLM is reached.
- A second LLM call needs the duplicate to arrive before the first copy commits — a crash between the LLM call and the commit, or two workers taking both copies at once (both covered below).

## Same operation, different MessageId

A second copy of the command with a changed `message_id`. The `processed_messages` check doesn't catch it; the state check does — the document is already `Analyzed`, the copy is skipped before the LLM. The two layers cover different cases: the message id catches redelivery of the same message (broker retry, outbox relay resend), the state catches a repeat of the same operation under a new id (client-side retry, manual resend).

## Crash after the LLM call, before the commit

Worker started with `Chaos__CrashBeforeCommit=true`: the LLM call succeeds and the process dies before `SaveChangesAsync`. Nothing reached the database — the document is still `Pending`, there is no `processed_messages` row, the message is still unacked. After a restart the message is redelivered and the LLM is called again. The document ends up `Analyzed` once and the user gets one notification, but the analysis is **paid twice**.

Wrapping the LLM call in the database transaction doesn't help: a rollback undoes database writes, not an HTTP call that already happened, and a transaction held open for seconds ties up locks and pooled connections. An external side effect can't be made atomic with a database write; the chance of repeating it can only be reduced.

How to reduce it — claim the document before the call:

1. Short transaction: `UPDATE documents SET status = 'Processing', processing_started_at = now() WHERE id = @id AND status = 'Pending'`. One row updated — the document is ours; zero — someone else has it, skip.
2. LLM call outside any transaction.
3. Second transaction: result, `Analyzed`, `processed_messages` row, event.

New failure mode: a crash after step 1 leaves the document in `Processing`. The redelivered message sees `Processing`, is skipped and acked, and the document is stuck forever. The fix is a lease:

- allow the claim when the document is `Pending` **or** has been `Processing` for longer than a timeout;
- plus a background sweeper that returns stale `Processing` documents to `Pending` and re-enqueues the command through the outbox, for the case where the message is already gone.

The trade-off: if a real analysis runs longer than the lease, a second worker takes the document over and the LLM is called twice. The timeout is set with a margin over the slowest normal run, so a duplicate call becomes a rare exception instead of a certainty after every crash. Not implemented here.

## Two workers race for the same message

Two copies with the same `message_id`, two workers started back to back so each takes one copy.

- Expected and observed: the LLM is called **twice**. Both checks are plain reads, and both workers run them before either has committed: no `processed_messages` row, document still `Pending`. Both call the LLM; the first commit wins, the second fails on the `(message_id, consumer)` primary key and its result is discarded (`Duplicate message ... detected at commit`).
- One result, one event, one notification — but two paid calls. Check-then-act on reads doesn't protect against concurrency; the unique key at commit is the real guard. The atomic claim (`UPDATE ... WHERE status = 'Pending'`) from the previous section closes this gap as well: only one worker gets the row.

## Subscriber down

Notifier stopped, three documents analyzed, Notifier started again. Each queue bound to `docflow.events` got its own copy of every event: `notifier.document-events` (`document.*`) and `audit.all` (`#`) held 3 each. The Notifier caught up on all three when it came back — the queue buffered them while it was down.

`audit.all` has no consumer and no limits, which makes it a liability rather than a harmless leftover:

- it grows forever;
- once the broker hits its memory or disk watermark, it blocks **all** publishers, so the relays in the API and the worker stall and the whole pipeline stops;
- nothing is dropped or dead-lettered on its own: overflow only goes to a DLQ when the queue has both `x-max-length` and a dead-letter exchange.

Protection: a length limit (`x-max-length` / `x-max-length-bytes`) with an overflow policy (`drop-head`, dead-lettered if the queue has a DLX, or `reject-publish`), a queue TTL when old events are worthless, alerts on queue depth and on queues without consumers, and deleting queues that are no longer used.

## Outbox relay resend, revisited

The crash from [05](05-transactional-outbox.md) — the API dies after publishing and before marking the outbox row — repeated with the idempotent worker. The resent command carries the same `MessageId` and arrives long after the first copy was committed, so it is skipped by the `processed_messages` check: one LLM call, one notification. The at-least-once delivery of the outbox is now harmless end to end.
