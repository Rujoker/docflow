# Debezium outbox connector

Streams domain events from `outbox_messages` to the Kafka topic `outbox.docflow.events` through logical replication, without RabbitMQ.

```bash
docker compose up -d
docker compose exec -T postgres psql -U docflow < infra/debezium/publication.sql
curl -X PUT -H "Content-Type: application/json" \
  http://localhost:8083/connectors/docflow-outbox/config -d @infra/debezium/outbox-connector.json
curl -s http://localhost:8083/connectors/docflow-outbox/status
```

Run Analytics on both paths:

```bash
Analytics__Topics=document-events,outbox.docflow.events dotnet run --project src/DocFlow.Analytics
```

Replication slot lag:

```bash
docker compose exec postgres psql -U docflow -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots;"
```

The publication only carries inserts of events: commands (`docflow.commands`) and the relay's `processed_at` updates never reach Kafka. `snapshot.mode = no_data` skips the initial table snapshot so existing outbox rows are not re-published.

The `debezium_heartbeat` table is part of the publication so the slot keeps advancing when no events are published: every 10 s the connector inserts a row (`heartbeat.action.query`) and confirms its LSN. Without it the slot stalls behind the rest of the database's WAL. Clean it up periodically:

```sql
DELETE FROM debezium_heartbeat WHERE created_at < now() - interval '1 day';
```
