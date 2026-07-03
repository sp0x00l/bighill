# model_registry_service

## What It Does

`model_registry_service` is the promotion authority for trained models. It records training/evaluation results, applies promotion gates, tracks serving load status, and publishes model facts for inference.

The registry owns model state in Postgres. Kubernetes or local serving status is observed as external runtime state, then committed through registry with the transactional outbox so model facts remain tied to the state write.

## MLOps / Platform Pieces

- Postgres model registry and metrics metadata.
- Kafka subscribers for training-completed/training-failed facts.
- Kafka publisher for model-updated facts.
- Postgres transactional outbox.
- Evaluation threshold gating for SFT and DPO/alignment runs.
- Kubernetes `ServedModel` CR observation in staging/prod.
- Local served-model store for local-dev/CI loop testing.

## How It Fits

- Consumes training and evaluation outcomes from `training_service`.
- Writes desired serving intent after a model passes evaluation.
- Observes serving load status from `model_serving_service`/Kubernetes.
- Publishes model updates consumed by `inference_service`.

## Local Development

Local and CI can use the local serving backend. Staging/prod use the Kubernetes `ServedModel` CRD configured with `MODEL_REGISTRY_SERVICE_K8S_*` and `MODEL_REGISTRY_SERVICE_SERVING_BACKEND=kubernetes`.
