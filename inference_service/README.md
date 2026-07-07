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

`INFERENCE_SERVICE_QUERY_TRANSFORMER_*` config is only for the optional utility model used by self-query retrieval. It is not a fallback generator for user inference.
