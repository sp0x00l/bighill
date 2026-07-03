CREATE TABLE IF NOT EXISTS bighill_data_ingestion_db.upload_sessions (
    upload_id uuid PRIMARY KEY,
    dataset_id uuid NOT NULL,
    user_id uuid NOT NULL,
    client_nonce text NOT NULL DEFAULT '',
    file_name text NOT NULL DEFAULT '',
    staging_key text NOT NULL DEFAULT '',
    final_key text NOT NULL DEFAULT '',
    storage_location text NOT NULL DEFAULT '',
    declared_format text NOT NULL,
    declared_content_type text NOT NULL,
    declared_size_bytes bigint NOT NULL DEFAULT 0,
    actual_size_bytes bigint NOT NULL DEFAULT 0,
    checksum text NOT NULL DEFAULT '',
    status text NOT NULL,
    table_namespace text NOT NULL,
    table_name text NOT NULL,
    table_format text NOT NULL,
    catalog_provider text NOT NULL,
    processing_profile text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    CONSTRAINT upload_sessions_status_check CHECK (status IN ('PENDING', 'PROMOTED', 'REJECTED', 'EXPIRED'))
);

CREATE INDEX IF NOT EXISTS index_upload_sessions_dataset_user
ON bighill_data_ingestion_db.upload_sessions(dataset_id, user_id, created_at);

CREATE INDEX IF NOT EXISTS index_upload_sessions_pending_expiry
ON bighill_data_ingestion_db.upload_sessions(status, expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS index_upload_sessions_client_nonce
ON bighill_data_ingestion_db.upload_sessions(dataset_id, user_id, client_nonce)
WHERE client_nonce <> '';
