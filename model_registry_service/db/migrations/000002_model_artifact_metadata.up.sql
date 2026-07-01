ALTER TABLE bighill_model_registry_db.models
    ADD COLUMN IF NOT EXISTS artifact_format text NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS artifact_checksum text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS artifact_size_bytes bigint NOT NULL DEFAULT 0;
