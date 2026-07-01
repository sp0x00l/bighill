CREATE TABLE IF NOT EXISTS bighill_data_ingestion_db.outbox_messages (
    outbox_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    dispatch_key text NOT NULL UNIQUE,
    topic text NOT NULL,
    event_type text NOT NULL,
    resource_key uuid NOT NULL,
    payload bytea NOT NULL,
    headers jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'PENDING',
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    processing_owner text NOT NULL DEFAULT '',
    claim_token text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    sent_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT outbox_messages_status_check CHECK (status IN ('PENDING', 'PROCESSING', 'SENT'))
);

CREATE INDEX IF NOT EXISTS index_outbox_messages_pending
ON bighill_data_ingestion_db.outbox_messages(status, next_attempt_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_processing
ON bighill_data_ingestion_db.outbox_messages(status, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS index_outbox_messages_resource_key
ON bighill_data_ingestion_db.outbox_messages(resource_key, created_at);
