# 01 — RabbitMQ topology and routing

Topology built by hand in the management UI, exported to [`infra/rabbitmq/definitions.json`](../../infra/rabbitmq/definitions.json).

| Exchange | Type | Queue | Binding key |
| --- | --- | --- | --- |
| `docflow.commands` | direct | `analyze-document` | `analyze` |
| `docflow.events` | topic | `notifier.document-events` | `document.*` |
| `docflow.events` | topic | `audit.all` | `#` |
| `docflow.events` | topic | `alerts.failed` | `document.failed` |

All queues are quorum queues.

## Routing

| Routing key | Expected | Actual |
| --- | --- | --- |
| `document.analyzed` | | |
| `document.failed` | | |
| `document.analyzed.eu` | | |
| `billing.charged` | | |

Why `document.analyzed.eu` did not reach `notifier.document-events`:

## Experiments

### Unroutable message

Published to `docflow.commands` with key `analyse` (typo).

- Expected:
- Actual:
- Takeaway:

### Durable vs transient

Classic transient queue `temp` bound to `docflow.commands` with key `temp`, one message in each queue, then `docker compose restart rabbitmq`.

- Expected:
- Actual:
- Takeaway:

### Volume removal

`docker compose down` vs `docker compose down -v`.

- Expected:
- Actual:
- Takeaway:

## Restoring the topology

```bash
curl -u guest:guest -H "content-type: application/json" \
  -X POST http://localhost:15672/api/definitions \
  -d @infra/rabbitmq/definitions.json
```

Or in the UI: Overview → Import definitions.
