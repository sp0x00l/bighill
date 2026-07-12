# training_service

## What It Does

`training_service` owns the training and evaluation workflow. It starts Temporal workflows from explicit training commands, resolves datasets/models/preference datasets at the infra boundary, prepares training/evaluation requests, submits GPU jobs through the configured executor, and publishes training outcomes.

The service is the control plane for model creation. Python/GPU work runs in the service-owned `training_jobs` package; durable orchestration stays in Temporal and Go.

## MLOps / Platform Pieces

- Temporal workflows and activities.
- REST training commands for SFT and DPO runs.
- Kafka subscribers for promotion requests.
- Kafka publisher for training-completed/training-failed facts.
- KubeRay/Ray job execution for training and evaluation.
- Axolotl recipe generation for SFT, LoRA/QLoRA, and DPO-style alignment runs.
- Evaluation profiles for RAG, external evaluators, and DPO parent-vs-candidate gates.
- Object storage manifests for model artifacts and evaluation reports.

## How It Fits

- Starts SFT training from materialized datasets on explicit request.
- Starts DPO/alignment training from selected preference datasets on explicit request.
- Runs evaluation before model registry promotion.
- Publishes completed or failed training facts to `model_registry_service`.

## Local Development

Training triggers are normally disabled in local-dev/CI unless the required executor is available. The service can still run its worker and subscribers, while real GPU execution is provided by KubeRay/Ray in environments configured with `TRAINING_SERVICE_EXECUTOR_PROVIDER`.
