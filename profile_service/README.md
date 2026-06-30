# profile_service

## Purpose & Responsibilities
- Manage user profile data, contact details, and password lifecycle.
- Issue JWT access tokens on credential verification and track session revocation in Redis.
- Publish profile events for downstream account bootstrap.

## Architecture & Integration
- **Layer:** HTTP-only command service (no gRPC business API); CQRS-aligned.
- **Ports:** HTTP 8082, health 5052.
- **Database:** `bighill_profile_db` (profiles and auth data; see `db/migrations`).
- **Kafka:** Produces `PROFILE_SERVICE_KAFKA_PUBLISHER_TOPIC` (default `profile`) via `pkg/infra/network/messaging/publisher.go`; no consumers.
- **Data contracts:** `data_contracts/protobufs/profile_event.proto`.
- **Dependencies:** Postgres, Redis (`PROFILE_SERVICE_REDIS_ADDRESS`) for session state, and KMS for JWT signing.
- **Upstream auth:** expects `X-User-ID` (and `X-Session-ID` for logout) from the API gateway/auth layer.

## API Surface
Public:
- `POST /public/v1/profiles` creates a profile account. Requires `X-Request-ID`/`X-Idempotency-Key`. Payload: `email`, `phoneNumber` (E.164), `countryCode`, `password`.
- `POST /public/v1/profiles/password/verify` verifies credentials and returns `{ "isValid": true, "token": "<jwt>" }` on success.

Private (requires `X-User-ID`):
- `GET /private/v1/profiles` reads the profile.
- `PUT /private/v1/profiles` replaces the profile (full payload with name, DOB, address, and contact info; validation enforced).
- `DELETE /private/v1/profiles` soft-deletes the profile.
- `PUT /private/v1/profiles/password` updates password and revokes sessions.

## Data Model & Storage
- `profiles` table: soft delete via `deleted`, unique email (case-insensitive) and phone when not deleted, and unique idempotency key.
- Passwords are Argon2id hashed (`profile_service/pkg/app/profile_usecase.go`).

## Messaging
- Kafka topic `PROFILE_SERVICE_KAFKA_PUBLISHER_TOPIC` (default `profile`).
- Publishes `UserCreatedEvent` from `data_contracts/protobufs/profile_event.proto` (currently gated to non-production).

## Operational Notes
- Health checks include CPU, memory, Postgres, and Kafka connectivity (`PROFILE_SERVICE_HEALTHCHECK_PORT`).
- Redis is required for login/logout and session revocation; outages block authentication flows.

## Known Gaps & Limitations
- `UserDeletedEvent` is defined but not emitted on delete; `UserCreatedEvent` is suppressed in production builds.
- No refresh-token endpoint or token introspection; only access tokens are issued.
- No local rate limiting or abuse controls; rely on gateway/edge protections.

## Testing
- `make test ENV=local-dev` (runs Ginkgo integration + unit tests; requires Postgres, Redis, KMS, and Kafka).
