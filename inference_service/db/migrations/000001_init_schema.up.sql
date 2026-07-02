CREATE TYPE inference_model_status_enum AS ENUM ('PENDING', 'CANDIDATE', 'EVALUATED', 'READY', 'FAILED');
CREATE TYPE inference_model_load_status_enum AS ENUM ('NOT_LOADED', 'LOADED', 'FAILED');
CREATE TYPE inference_dataset_processing_state_enum AS ENUM ('PENDING', 'RAW_MATERIALIZED', 'FEATURE_MATERIALIZED', 'EMBEDDINGS_MATERIALIZED', 'FAILED');
CREATE TYPE inference_request_status_enum AS ENUM ('COMPLETED', 'FAILED');

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_models (
    model_id uuid PRIMARY KEY,
    training_run_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    idempotency_key uuid NOT NULL,
    name text NOT NULL,
    model_version integer NOT NULL,
    base_model text NOT NULL,
    artifact_location text NOT NULL DEFAULT '',
    artifact_format text NOT NULL DEFAULT '',
    artifact_checksum text NOT NULL DEFAULT '',
    artifact_size_bytes bigint NOT NULL DEFAULT 0,
    adapter_uri text NOT NULL DEFAULT '',
    serving_target text NOT NULL DEFAULT '',
    serving_model text NOT NULL DEFAULT '',
    serving_load_status inference_model_load_status_enum NOT NULL DEFAULT 'NOT_LOADED',
    metrics_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status inference_model_status_enum NOT NULL,
    failure_reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_models_training_run_id
ON bighill_inference_db.inference_models(training_run_id);

CREATE INDEX IF NOT EXISTS index_inference_models_dataset_id
ON bighill_inference_db.inference_models(dataset_id);

CREATE INDEX IF NOT EXISTS index_inference_models_status
ON bighill_inference_db.inference_models(status);

CREATE TRIGGER inference_models_updated_at
BEFORE UPDATE ON bighill_inference_db.inference_models
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_datasets (
    dataset_id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    idempotency_key uuid NOT NULL UNIQUE,
    dataset_version integer NOT NULL DEFAULT 1,
    processing_state inference_dataset_processing_state_enum NOT NULL DEFAULT 'PENDING',
    storage_location text NOT NULL DEFAULT '',
    table_namespace text NOT NULL DEFAULT '',
    table_name text NOT NULL DEFAULT '',
    table_format text NOT NULL DEFAULT '',
    catalog_provider text NOT NULL DEFAULT '',
    processing_profile text NOT NULL DEFAULT '',
    schema_version integer NOT NULL DEFAULT 1,
    schema_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    raw_snapshot_id uuid,
    feature_snapshot_id uuid,
    embedding_snapshot_id uuid,
    vector_store text NOT NULL DEFAULT '',
    collection_name text NOT NULL DEFAULT '',
    embedding_dimensions integer NOT NULL DEFAULT 0,
    embedding_count bigint NOT NULL DEFAULT 0,
    embedding_strategy_version text NOT NULL DEFAULT '',
    embedding_chunker_name text NOT NULL DEFAULT '',
    embedding_chunker_version text NOT NULL DEFAULT '',
    embedding_chunk_size integer NOT NULL DEFAULT 0,
    embedding_chunk_overlap integer NOT NULL DEFAULT 0,
    embedding_provider text NOT NULL DEFAULT '',
    embedding_model text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_datasets_processing_state
ON bighill_inference_db.inference_datasets(processing_state);

CREATE INDEX IF NOT EXISTS index_inference_datasets_embedding_snapshot_id
ON bighill_inference_db.inference_datasets(embedding_snapshot_id);

CREATE TRIGGER inference_datasets_updated_at
BEFORE UPDATE ON bighill_inference_db.inference_datasets
FOR EACH ROW
EXECUTE FUNCTION updated_at_column();

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_requests (
    request_id uuid PRIMARY KEY,
    dataset_id uuid NOT NULL,
    model_id uuid,
    embedding_snapshot_id uuid,
    query_text text NOT NULL,
    top_k integer NOT NULL,
    metadata_filters jsonb NOT NULL DEFAULT '{}'::jsonb,
    retrieved_context_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    retrieved_contexts jsonb NOT NULL DEFAULT '[]'::jsonb,
    prompt_text text NOT NULL DEFAULT '',
    answer_text text NOT NULL DEFAULT '',
    prompt_strategy_version text NOT NULL,
    generation_provider text NOT NULL,
    generation_model text NOT NULL,
    latency_ms bigint NOT NULL,
    status inference_request_status_enum NOT NULL,
    error_message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_requests_dataset_id
ON bighill_inference_db.inference_requests(dataset_id);

CREATE INDEX IF NOT EXISTS index_inference_requests_embedding_snapshot_id
ON bighill_inference_db.inference_requests(embedding_snapshot_id);

CREATE INDEX IF NOT EXISTS index_inference_requests_created_at
ON bighill_inference_db.inference_requests(created_at);

CREATE TABLE IF NOT EXISTS bighill_inference_db.inference_feedback (
    feedback_id uuid PRIMARY KEY,
    idempotency_key uuid NOT NULL UNIQUE,
    request_id uuid NOT NULL REFERENCES bighill_inference_db.inference_requests(request_id),
    user_id uuid NOT NULL,
    accepted boolean NOT NULL,
    rating integer NOT NULL,
    comment text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_inference_feedback_request_id
ON bighill_inference_db.inference_feedback(request_id);

CREATE TABLE IF NOT EXISTS bighill_inference_db.preference_examples (
    preference_example_id uuid PRIMARY KEY,
    feedback_id uuid NOT NULL UNIQUE REFERENCES bighill_inference_db.inference_feedback(feedback_id),
    request_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    model_id uuid NOT NULL,
    prompt_text text NOT NULL,
    accepted_answer text NOT NULL DEFAULT '',
    rejected_answer text NOT NULL DEFAULT '',
    rating integer NOT NULL,
    feedback_label text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS index_preference_examples_dataset_id
ON bighill_inference_db.preference_examples(dataset_id);

CREATE INDEX IF NOT EXISTS index_preference_examples_model_id
ON bighill_inference_db.preference_examples(model_id);
