ALTER TABLE bighill_model_registry_db.models
    DROP COLUMN IF EXISTS artifact_size_bytes,
    DROP COLUMN IF EXISTS artifact_checksum,
    DROP COLUMN IF EXISTS artifact_format;
