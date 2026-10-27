# 03 — Reliable publishing

Publisher confirms with tracking (`BasicPublishAsync` completes only after the broker acks), persistent messages, `mandatory: true`.

## Unroutable message

Published with routing key `analyse` (typo) instead of `analyze`.

- Before (no `mandatory`): the broker dropped the message silently, the API returned 202.
- Now: `PublishReturnException: 312 NO_ROUTE`, the API returns 503.
- The uploaded file stays in `storage/` anyway — see below.

## Broker down

`docker compose stop rabbitmq`, then upload a document.

- API returns 503.
- The file is already saved in `storage/`, but no command exists for it: an orphan file nobody will process or clean up.
- This is the **dual-write problem**: two writes to two systems (disk, broker) without a shared transaction. Reordering only changes the failure mode — publish first and a crash before saving leaves the worker with a command pointing to a missing file. Fixed later with a transactional outbox (v0.6.0).

## Broker restart

After `docker compose start rabbitmq` the API recovered without a restart: RabbitMQ.Client automatic recovery restored the connection, and the publisher recreates a channel if the broker closed it.

## Cost of waiting for confirms

1000 sequential publishes to a local broker, one channel, unroutable key (no queue, so no disk write):

| Mode | Total | Per message |
| --- | --- | --- |
| Await each confirm | 308 ms | 0.31 ms |
| Fire-and-forget | 20 ms | 0.02 ms |

~15× slower when every publish waits for its confirm. The cost is one round-trip per message; over a real network (≈1 ms RTT) and with quorum queues (confirm only after Raft replication to a majority and fsync) it grows by an order of magnitude or more.

How to keep the guarantee without paying per message:

- publish a batch, then await all confirms together (RabbitMQ.Client 7.x supports concurrent publishes on one channel with confirm tracking);
- in the outbox relay, confirm per batch and mark the whole batch as sent;
- don't serialize every publish behind one lock on one channel — use a small pool of channels.
