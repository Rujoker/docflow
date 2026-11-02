# 07 — MassTransit v8 variant

Branch `masstransit` replaces the hand-written RabbitMQ code with MassTransit 8 (the last Apache-2.0 major version; v9 is commercial). What disappears from the code:

- `Topology`, `RabbitPublisher` and `OutboxRelay` — MassTransit declares the topology and delivers outbox messages itself;
- `OutboxMessage`, `ProcessedMessage` and their queries — replaced by the EF Core outbox/inbox tables `outbox_message`, `outbox_state` and `inbox_state`;
- the manual retry counter, the retry queue with TTL and the hand-made DLQ — replaced by `UseMessageRetry`, `UseDelayedRedelivery` and the `_error` queue.

What stays: the consumers' business logic and the state check in `AnalyzeDocumentConsumer`.

## Setup

- API: `AddEntityFrameworkOutbox` + `UseBusOutbox`. `Send` to `queue:analyze-document` is written to `outbox_message` in the same `SaveChangesAsync` as the document; a background delivery service sends it to the broker.
- Worker: on every endpoint, from outer to inner — `UseDelayedRedelivery` (3 × 10 s, ignores `InvalidDocumentException`), `UseMessageRetry` (300 ms, 1 s, ignores `InvalidDocumentException`), `UseEntityFrameworkOutbox` (inbox + outbox for messages published from the consumer).
- RabbitMQ image `masstransit/rabbitmq` with the delayed message exchange plugin, required by `UseDelayedMessageScheduler`.
- Failures: after the last attempt MassTransit moves the message to `analyze-document_error` and publishes `Fault<AnalyzeDocument>`. `AnalyzeDocumentFaultConsumer` marks the document `Failed` and publishes `DocumentFailed`.

## Topology

The topology is generated from types and consumer names (kebab-case formatter):

| Object | Kind | Purpose |
|---|---|---|
| `analyze-document` | exchange (fanout) + queue | Worker endpoint; the API sends here directly |
| `DocFlow.Contracts:AnalyzeDocument` | exchange (fanout) → `analyze-document` | consume topology for `Publish`; not used by the API, which uses `Send` |
| `analyze-document_error` | queue | messages that failed all attempts, with `MT-Fault-*` headers |
| `analyze-document_skipped` | queue | messages the endpoint has no consumer for |
| `analyze-document-fault` | exchange + queue | endpoint of `AnalyzeDocumentFaultConsumer` |
| `MassTransit:Fault--DocFlow.Contracts:AnalyzeDocument--` | exchange → `analyze-document-fault` | where `Fault<AnalyzeDocument>` is published |
| `DocFlow.Contracts:DocumentAnalyzed` | exchange → `document-analyzed-notification` | event; Notifier endpoint |
| `DocFlow.Contracts:DocumentFailed` | exchange → `document-failed-notification` | event; Notifier endpoint |

Compared to `main`: there routing keys and a topic exchange `docflow.events` with binding `document.*` decided who gets what. Here every message type has its own fanout exchange, and a subscriber binds its endpoint exchange to the type exchange. Routing is by type, not by key: a new subscriber needs no routing key, only a consumer of that type.

`Send` vs `Publish`: `Send` goes to a known address (a command, one recipient); `Publish` goes to the type exchange (an event, any number of subscribers).

## Poison message

`poison.txt`: the LLM throws `InvalidDocumentException`, which both retry and redelivery ignore.

- One LLM call, no retries, no redelivery.
- The message goes straight to `analyze-document_error`.
- `Fault<AnalyzeDocument>` → the document becomes `Failed` → `DocumentFailed` → Notifier logs the failure email.
- Headers on the message in `_error`: `MT-Reason: fault`, `MT-Fault-ExceptionType`, `MT-Fault-Message`, `MT-Fault-StackTrace`, `MT-Fault-Timestamp`, `MT-Fault-ConsumerType`, `MT-Fault-MessageType`, `MT-Fault-RetryCount`, plus `MT-Host-*` identifying the process that gave up.

On `main` the same information was written by `RabbitPublisher.PublishToDeadLetterAsync` into custom headers (`error-type`, `error-message`, `attempts`, `failed-at`).

## LLM always down

`down.txt`: the LLM throws `LlmTransientException` on every call.

Number of calls: retry is nested inside redelivery, so each of the 4 deliveries (1 + 3 redeliveries) makes 3 attempts (1 + 2 retries): **4 × 3 = 12 LLM calls** — the same 12 as on `main` (`(MaxRetries + 1) × (MaxRetryAttempts + 1)`). The final message in `_error` carries `MT-Redelivery-Count: 3` and `MT-Fault-RetryCount: 2`.

Time to `_error`:

- redelivery pauses: 3 × 10 s = 30 s;
- retry pauses: 4 × (0.3 + 1) s = 5.2 s;
- the fake LLM itself: 12 × 1–3 s = 12–36 s;
- total about 47–71 s, about a minute on average.

Where the message is during a redelivery pause: in no queue. The message is acked from `analyze-document` and re-sent through an exchange of type `x-delayed-message` (argument `x-delayed-type`); the plugin keeps it inside the exchange and routes it to the queue when the delay expires. That is why the queue looks empty in the UI between attempts. Messages held by the plugin are not visible in the management UI as queue depth and are kept on a single node, so the delayed exchange is a convenience, not a replacement for a quorum queue.

Compared to `main`: there the delay was a retry queue with a TTL and a DLX back to the main queue, and the attempt counter came from `x-death`. Here MassTransit counts redeliveries in its own header.

## Same message delivered twice

Worker stopped, `policy-1.txt` uploaded, a copy of the message published by hand to exchange `analyze-document` with the same body, `message_id` and `content_type: application/vnd.masstransit+json`, worker started.

- MassTransit reads `messageId` from the JSON envelope, not only from the AMQP property, so the copy must keep the body unchanged.
- The inbox (`UseEntityFrameworkOutbox` on the endpoint) stores one row in `inbox_state` per `(message_id, consumer_id)`. The second copy finds that row and is acked without calling the consumer.
- One LLM call. The line `Document ... is Analyzed, skipping` does **not** appear: the duplicate is stopped by the inbox before the consumer runs, the state check is not reached.
- `inbox_state` rows are removed after the duplicate detection window (30 minutes by default). A duplicate arriving later reaches the consumer and is stopped by the state check — the two layers still cover different cases, as on `main`.

## Code size

`git diff main --shortstat -- src ':!src/DocFlow.Persistence/Migrations'`: 21 files changed, 169 insertions, 671 deletions. Deleted: topology, publisher, outbox relay, inbox entities and queries. Added: bus configuration in three `Program.cs` files, `BusConfiguration`, the fault consumer and two Notifier consumers.

The trade: less code to own, but behavior moves into configuration. The order of `UseDelayedRedelivery` / `UseMessageRetry` decides whether calls multiply; the exchange names come from type names, so renaming a contract's namespace changes the topology.
