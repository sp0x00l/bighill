# training_jobs

## What It Is

`training_jobs` is not a long-running service. It is the Python job package and container runtime launched by `training_service` through Ray/KubeRay.

The Go control plane builds recipes and submits jobs; this package performs the model-training or evaluation work inside the job image.

## MLOps / Platform Pieces

- Axolotl command execution for SFT, LoRA/QLoRA, and DPO-style recipes.
- Ragas-compatible evaluation support where configured.
- External evaluator command hook for custom benchmark suites.
- S3-compatible object storage for model artifacts, metrics, reports, and manifests.
- Shared Go/Python job contract fixture under `training_jobs/contracts`.

## How It Fits

- `python -m training_jobs.train` runs the training entrypoint.
- `python -m training_jobs.evaluate` runs the evaluation entrypoint.
- Reads env vars supplied by `training_service`.
- Writes artifact and evaluation manifests that the Go workflow reads back.

## Local Development

The package is built into `training-jobs.Dockerfile`. Unit tests should validate both Python behavior and the shared job contract so the Go env/manifest boundary does not drift.
