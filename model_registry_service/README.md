# model_registry_service

## What It Does

`model_registry_service` is the promotion authority for trained models. It records training/evaluation results, applies promotion gates, tracks serving load status, and publishes model facts for inference.

The registry owns model state in Postgres. Kubernetes or local serving status is observed as external runtime state, then committed through registry with the transactional outbox so model facts remain tied to the state write.

Training outputs do not deploy directly. A completed training fact creates a `CANDIDATE` and emits `promotion_requested` with the candidate and current champion evidence URIs. `training_service` reacts to that fact and publishes `promotion_report_ready`; registry then parses the evaluation metadata, rejects fake/no-held-out metrics, enforces absolute floors, compares against the current loaded champion for the same `(user_id, model name)` lineage when the eval set and evaluator match, records the promotion report URI/deltas, and only then moves the model to `EVALUATED` and emits serving intent. Runtime `LOADED` status is still required before the model becomes `READY`.

If the champion and candidate metrics are not comparable because the eval dataset, metric suite, or evaluator version changed, the registry falls back to absolute-floor-only promotion and records a `champion metrics incomparable; floor-only` reason. That avoids freezing a lineage during eval methodology upgrades. Deepchecks and Evidently evidence are produced through the same event choreography, but they are binding only when the registry gate policy and `TRAINING_SERVICE_PROMOTION_PROFILE` require them.

## MLOps / Platform Pieces

- Postgres model registry and metrics metadata.
- Kafka subscribers for training-completed/training-failed and promotion-report-ready facts.
- Kafka publisher for model-updated and promotion-requested facts.
- Postgres transactional outbox.
- Candidate promotion gating for SFT and DPO/alignment runs.
- Champion/challenger comparison against the latest loaded `READY` model in the same tenant/model lineage.
- Kubernetes `ServedModel` CR observation in staging/prod.
- Local served-model store for local-dev/CI loop testing.

## How It Fits

- Consumes training and evaluation outcomes published by `training_service`.
- Records training output as `CANDIDATE`, publishes a `promotion_requested` choreography event, consumes `promotion_report_ready` when the evidence job completes, evaluates the report against absolute metric floors and the current champion, then writes desired serving intent only after promotion passes.
- Observes serving load status from `model_serving_service`/Kubernetes.
- Publishes model updates consumed by `inference_service`.

## Local Development

Local and CI can use the local serving backend. Staging/prod use the Kubernetes `ServedModel` CRD configured with `MODEL_REGISTRY_SERVICE_K8S_*` and `MODEL_REGISTRY_SERVICE_SERVING_BACKEND=kubernetes`.
