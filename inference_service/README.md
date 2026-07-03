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
- Optional query transformation/self-query before retrieval.
- TEI-compatible reranker support.
- Token-aware context packing.
- Generator adapters for local deterministic execution, Ollama-style HTTP generation, and vLLM/OpenAI-compatible serving.

## How It Fits

- Consumes model updates from `model_registry_service`.
- Consumes dataset updates from `data_registry_service`.
- Calls `feature_materializer_service` for vector retrieval.
- Publishes preference dataset facts consumed by `training_service`.
- Enforces that only registry-approved and serving-loaded models are used.

## Local Development

Local and CI can run with deterministic generation and local serving status. Production-like deployments should use real generator, reranker, and serving endpoints configured through `INFERENCE_SERVICE_` env vars.
