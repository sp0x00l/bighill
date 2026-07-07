# api_gateway

## What It Does

`api_gateway` is the HTTP edge for the platform. It exposes the public API used by clients and the e2e tests, validates requests through the auth lambda, and forwards authenticated traffic to the owning backend services.

The gateway is intentionally thin: it does not own domain state. Profile, data registry, ingestion, materialization, inference, and training state all remain inside their service-owned databases.

## Platform Pieces

- AWS SAM local runtime for API Gateway + Lambda development.
- Go Lambda handlers for the API proxy and request authorizer.
- OpenAPI/SAM templates for route wiring.
- E2E Ginkgo tests that drive cross-service workflows such as dataset upload, materialization, RAG inference, RBAC, and training-run triggering.

## How It Fits

- Authenticates and authorizes incoming API calls.
- Routes profile/auth calls to `profile_service`.
- Routes dataset/source/upload flows to `data_registry_service` and `ingestion_service`.
- Routes inference and feedback flows to `inference_service`.
- Keeps orchestration and state transitions out of the edge layer.

## Local Development

Run the root infra and services first, then start the gateway through the existing scripts. The gateway tests live under `api_gateway/test` and are normally exercised from the root `make test` flow.

## Real Hugging Face E2E

The default API e2e suite uses deterministic local fixtures for Hugging Face onboarding. To prove the live integration, start the stack with the real onboarding command and run the opt-in spec with:

```sh
source .env.huggingface-e2e
```

The local `.env.huggingface-e2e` file is ignored by git and should contain the Hugging Face token plus:

```sh
export INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_COMMAND="python -m training_jobs.model_onboard"
export BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true
export BIGHILL_E2E_HUGGINGFACE_REPO_ID=meta-llama/Llama-3-8B
export BIGHILL_E2E_HUGGINGFACE_REVISION=main
export BIGHILL_E2E_HUGGINGFACE_BASE_MODEL=meta-llama/Llama-3-8B
export BIGHILL_E2E_HUGGINGFACE_TIMEOUT_SECONDS=5400
```

Do not use the root `make test` or `make test-api` target for this specific check; those targets intentionally override `INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_COMMAND` to the API test stub. The token must have access to the gated `meta-llama/Llama-3-8B` repository.

The spec writes the token through the profile API, invokes `/v1/private/models/onboard/huggingface`, and fails unless the response contains a real 40-character Hugging Face commit SHA. Fixture commits use `local-*`; the API test stub uses `api-e2e-*`.
