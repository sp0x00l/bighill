# training_service

## What It Does

`training_service` owns the training and evaluation workflow. It listens for dataset and preference-dataset facts, starts Temporal workflows, prepares training/evaluation requests, submits GPU jobs through the configured executor, and publishes training outcomes.

The service is the control plane for model creation. Python/GPU work runs in the service-owned `training_jobs` package; durable orchestration stays in Temporal and Go.

## MLOps / Platform Pieces

- Temporal workflows and activities.
- Kafka subscribers for dataset updates and preference-dataset-ready facts.
- Kafka publisher for training-completed/training-failed facts.
- KubeRay/Ray job execution for training and evaluation.
- Axolotl recipe generation for SFT, LoRA/QLoRA, and DPO-style alignment runs.
- Evaluation profiles for RAG, external evaluators, and DPO parent-vs-candidate gates.
- Object storage manifests for model artifacts and evaluation reports.

## How It Fits

- Starts SFT training from dataset updates when training triggers are enabled.
- Starts DPO/alignment training from preference dataset snapshots.
- Runs evaluation before model registry promotion.
- Publishes completed or failed training facts to `model_registry_service`.

## Local Development

Training triggers are normally disabled in local-dev/CI unless the required executor is available. The service can still run its worker and subscribers, while real GPU execution is provided by KubeRay/Ray in environments configured with `TRAINING_SERVICE_EXECUTOR_PROVIDER`.
