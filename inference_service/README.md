# inference_service

## What It Does

`inference_service` owns online RAG inference. It reads local projections of datasets and models, retrieves relevant contexts from `feature_materializer_service`, builds prompts, calls the configured generator, records inference audits, and captures user feedback.

It also exports preference datasets from feedback so the training loop can improve models from real use.

## MLOps / Platform Pieces

- gRPC inference API.
- Postgres for local read models, inference request audits, feedback, and preference snapshots.
- Kafka subscribers for model and dataset update facts.
- Postgres transactional outbox for preference-dataset-ready facts.
- RAG retrieval over embedding search.
- Query transformation/self-query before retrieval.
- TEI-compatible reranker support.
- Token-aware context packing.
- Generation adapters selected by the serving protocol recorded on each model projection.
- Self-hosted runtime support for Ollama-style HTTP generation and OpenAI-compatible chat completions.

## How It Fits

- Consumes model updates from `model_registry_service`.
- Consumes dataset updates from `data_registry_service`.
- Calls `feature_materializer_service` for vector retrieval.
- Publishes preference dataset facts consumed by `training_service`.
- Enforces that only registry-approved and serving-loaded models are used.

## Local Development

Local and CI use the same runtime contract as staging and production: generation protocol, endpoint, and served model name come from the model record projected from `model_registry_service`. The service fails closed when a loaded model is missing `serving_protocol`, `serving_target`, or `serving_model`.

Self-query retrieval uses the same served model record as the inference request. There is no separately configured query-transformer model, endpoint, or protocol.

`INFERENCE_SERVICE_GENERATION_MAX_OUTPUT_TOKENS` caps generated answer length for every self-hosted runtime adapter. Local-dev and CI use `24` so CPU-bound Ollama e2e calls stay inside the configured generation timeout. Staging and production use `256`, which is enough for the RAG/API responses. Increase it deliberately, for example to `512`, only for workloads that need longer generated responses.

`INFERENCE_SERVICE_HTTP_WRITE_TIMEOUT_SECONDS` must be greater than `INFERENCE_SERVICE_GENERATION_REQUEST_TIMEOUT_SECONDS`. Generation can legitimately occupy the whole generation timeout, and a shorter HTTP write timeout closes the response stream before the handler can write the completed answer.
