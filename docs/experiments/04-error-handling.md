# 04 — Error handling: retries, dead-lettering, poison messages

The worker splits failures into two kinds:

- **transient** (`LlmTransientException`: 429/503/529, Polly `TimeoutRejectedException`) — worth retrying;
- **permanent** (`InvalidDocumentException`, unreadable file, broken JSON) — retrying cannot help.

## Two retry levels

| Level | Where | Delay | Message state while waiting |
| --- | --- | --- | --- |
| Inner | Polly in the worker: 2 retries, exponential backoff with jitter, 30 s timeout per attempt | ~0.3–0.6 s | Unacked, held by the worker |
| Outer | Broker: `nack(requeue: false)` → `docflow.retry` → `analyze-document.retry` (TTL 10 s) → back to `docflow.commands` | 10 s | Ready in the retry queue, worker is free |

After `MaxRetries` outer retries, or immediately on a permanent error, the worker publishes the original body to `docflow.dlq` → `analyze-document.dlq` with headers `error-type`, `error-message`, `attempts`, `failed-at`, then acks the original.

The attempt number comes from the `x-death` header: the broker increments `count` for `analyze-document` each time the message is dead-lettered from it.

```
attempt = x-death[analyze-document].count + 1
attempt <= MaxRetries  → retry
attempt >  MaxRetries  → DLQ
```

So the number of broker attempts is `MaxRetries + 1`.

## Transient and poison messages

Behaved as designed: `transient.txt` eventually succeeded after one or more outer retries; `poison.txt` went straight to the DLQ without any retry.

## Dependency that never recovers

`down.txt` always fails with a simulated 503. `MaxRetries = 3`, Polly `MaxRetryAttempts = 2`.

- Log shows **4** attempts (`attempt 1…4`), then the message lands in the DLQ.
- Actual LLM calls: **12**. Each broker attempt runs 1 call + 2 Polly retries, and Polly retries are not logged.

```
LLM calls = (MaxRetries + 1) × (MaxRetryAttempts + 1) = 4 × 3 = 12
```

Time from upload to DLQ: **~59 s** — three 10 s waits in the retry queue (30 s) plus twelve 1–3 s calls and Polly backoff (~29 s). Half of the time a document spends failing is pure waiting, the other half is paid calls that never had a chance.

Takeaways:

- Retry levels multiply, they don't add. The logs understated the real number of calls threefold.
- For a paid API, a provider outage burns 12 calls per document for nothing. Mitigations: a circuit breaker that stops calling the provider after a run of failures, retries configured at one deliberate level, and the SDK's own retries switched off.
- Polly retries need to be observable (`OnRetry` logging or tracing), otherwise the cost is invisible.

## Crash between DLQ publish and ack

The DLQ publish and the ack of the original are two separate operations. If the worker dies in between, the original stays unacked and is redelivered when the channel closes. It is not dead-lettered by the broker, so `x-death` doesn't change, the attempt number is the same, the message fails again and is published to the DLQ a second time.

Result: a **duplicate in the DLQ**, never a lost message. At-least-once again — whoever processes the DLQ has to tolerate duplicates (dedupe by `MessageId`).

## Redrive from the DLQ

Moved all messages from `analyze-document.dlq` back to `analyze-document` with the management UI (shovel plugin, "Move messages").

- The DLQ messages have no `x-death` header: the worker published them as new messages, so the broker's death history stayed with the acked originals.
- `poison.txt`: failed again with `InvalidDocumentException` and went straight back to the DLQ.
- `down.txt`: started from `attempt 1` and went through the full cycle again — 4 attempts, 12 LLM calls, back to the DLQ.

Takeaways:

- Redrive only makes sense after the cause is fixed (the dependency is back, a bug fix is deployed). Otherwise it is one more paid round of failures.
- A poison message keeps coming back until the code or the data changes. It is fixed or discarded, not moved around.
- Because the worker republishes into the DLQ, the attempt counter resets on redrive: the message gets a fresh retry budget, but its history is lost (`attempts` is overwritten). Tracking how many times a message went around needs a separate redrive counter header.
- "Move messages" moves everything at once, with no filtering by error type. In production a small tool that selects messages by `error-type` is the usual approach.

## Per-message TTL (not run, reasoning only)

Two ways to set a TTL in RabbitMQ: per queue (`x-message-ttl`, used by `analyze-document.retry`) and per message (`Expiration`). Expiry is only checked at the **head** of the queue.

With per-message TTLs in one queue, a message with a 60 s expiration followed by one with 5 s: the second one is due after 5 s but sits behind the first, so it is dead-lettered only after 60 s. Short delays get stuck behind long ones, which breaks exponential backoff built this way.

This setup is not affected: every message in `analyze-document.retry` has the same 10 s TTL, so queue order equals expiry order.

Ways to get different delays:

- one retry queue per delay level (`analyze-document.retry.10s`, `.30s`, `.60s`), each with its own queue TTL;
- the delayed message exchange plugin (`x-delay` header) — also what MassTransit's delayed redelivery uses;
- scheduled messages in brokers that support them natively (Azure Service Bus).
