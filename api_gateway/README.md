# api_gateway

## What It Does

`api_gateway` is the HTTP edge for the platform. It exposes the public API used by clients and the e2e tests, validates requests through the auth lambda, and forwards authenticated traffic to the owning backend services.

The gateway is intentionally thin: it does not own domain state. Profile, data registry, ingestion, materialization, inference, and training state all remain inside their service-owned databases.

## Platform Pieces

- AWS SAM local runtime for API Gateway + Lambda development.
- Go Lambda handlers for the API proxy and request authorizer.
- OpenAPI/SAM templates for route wiring.
- E2E Ginkgo tests that drive cross-service workflows such as dataset upload, materialization, RAG inference, and the full ML loop.

## How It Fits

- Authenticates and authorizes incoming API calls.
- Routes profile/auth calls to `profile_service`.
- Routes dataset/source/upload flows to `data_registry_service` and `data_ingestion_service`.
- Routes inference and feedback flows to `inference_service`.
- Keeps orchestration and state transitions out of the edge layer.

## Local Development

Run the root infra and services first, then start the gateway through the existing scripts. The gateway tests live under `api_gateway/test` and are normally exercised from the root `make test` flow.
