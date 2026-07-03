# profile_service

## What It Does

`profile_service` owns users, profiles, authentication, sessions, and profile-domain events. It is the identity and tenant boundary for the rest of the platform.

The service keeps auth/profile state in its own database and publishes profile facts for other services that need user context.

## Platform Pieces

- HTTP API for registration, login, OAuth callbacks, profile operations, and logout.
- Postgres for profile and credential state.
- Redis for session/revocation data.
- KMS/JWT integration for token signing and validation.
- OAuth provider support.
- Kafka publisher for profile-domain facts.
- Postgres transactional outbox for atomic event publication.

## How It Fits

- Issues and validates credentials used by `api_gateway`.
- Owns session revocation checks.
- Publishes profile events for downstream services.
- Keeps identity concerns out of data, training, inference, and serving services.

## Local Development

Configuration is provided through `PROFILE_SERVICE_` env vars from `scripts/config.sh`, Helm, docker-compose, and launch settings. The service runs as part of the root local-dev/test workflow.
