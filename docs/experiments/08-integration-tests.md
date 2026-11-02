# 08 — Integration tests with Testcontainers

`tests/DocFlow.Tests` starts real PostgreSQL 17 and RabbitMQ 4 in Docker, runs the API (`WebApplicationFactory<Program>`) and the worker (`AddWorker` in a test host) in the test process, and checks the pipeline end to end. The LLM is replaced by `CountingLlmClient`: the same fake, but it counts calls per document text, and every test uses a unique text.

Containers are expensive, so one `DocFlowFixture` is shared by all tests (collection fixture). Results are awaited with `Eventually` (polling every 200 ms, 30 s timeout) instead of `Thread.Sleep`.

| Test | Checks |
|---|---|
| `Uploaded_document_is_analyzed_once` | upload → `Analyzed`, one issue, one LLM call |
| `Duplicate_message_is_processed_once` | two copies of `AnalyzeDocument` with the same `MessageId` → `Analyzed`, one LLM call |
| `Poison_document_fails_without_retries` | `#poison` → `Failed`, one LLM call |
| `Analyzed_document_publishes_event` | a temporary queue bound to `docflow.events` receives `DocumentAnalyzed` for the document |

## Waiting for "nothing happened"

The duplicate test can't wait for the second copy: when it is handled correctly, nothing observable changes. Checking the call counter right after the document becomes `Analyzed` would pass even if the second copy hadn't been delivered yet.

The test publishes a third message — a marker for another document — after the two copies. The worker has one consumer with `prefetchCount: 1`, so messages are handled strictly in order: once the marker document is `Analyzed`, the second copy has been fully processed, and the counter can be checked.

## Bug found: one unroutable event blocks the outbox

First run: the three tests that upload through `POST /documents` stayed `Pending` for the whole 30 s timeout; the duplicate test, which publishes the command directly, passed.

Cause: the API and the worker share the `outbox_messages` table. The relay publishes with `mandatory: true` and publisher confirms, so an unroutable message throws, and the whole batch is rolled back and retried a second later. In the tests the Notifier wasn't running, so nothing was bound to `docflow.events` for `document.analyzed`. The first `DocumentAnalyzed` event became unroutable, stayed at the head of the outbox (ordered by `created_at`), and every row behind it — including the `AnalyzeDocument` commands from uploads — was never published.

Fix in the tests: the fixture declares the Notifier topology (`Topology.DeclareNotifierAsync`), as in production where the Notifier exists. After that all 7 tests passed.

In production the same thing happens if an event has no subscriber, e.g. before the Notifier has ever started: one poison row stops the whole outbox (head-of-line blocking). Possible fixes: publish events without `mandatory` (no subscribers is not an error for an event), or mark a failing row and move on instead of rolling back the batch.

## Does the duplicate test catch a broken guard?

- `IsProcessedAsync` check commented out: the test **passes**. The second copy reaches the consumer and is stopped by the state check (`Document ... is Analyzed, skipping`) before the LLM call.
- State check commented out as well: the test **fails** — the second copy calls the LLM again.

The two guards back each other up, so breaking one is not enough to see a failure. The test proves that duplicates don't cause a second LLM call, not which guard stops them.

## Time

`dotnet test`: 21.9 s for 7 tests. Containers are ready about 6.7 s after start (Ryuk 1.4 s, PostgreSQL 2.8 s, RabbitMQ 6.7 s, the last two started in parallel); the rest is the tests themselves, mostly the fake LLM's 1–3 s per call. A first run on a machine without the images also pays for `docker pull`.
