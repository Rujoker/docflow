CREATE TABLE IF NOT EXISTS debezium_heartbeat (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE PUBLICATION docflow_outbox
    FOR TABLE outbox_messages WHERE (exchange = 'docflow.events'),
        TABLE debezium_heartbeat
    WITH (publish = 'insert');
