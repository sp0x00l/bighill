ENV ?= local-dev
ARCH ?= arm64
START_MODE ?= run
BUILD_MODE ?= prebuilt
EXCLUDE_SERVICES ?=
CI_TEST_EXCLUDE_SERVICES ?=
GO_BIN ?= $(shell go env GOPATH)/bin

export PATH := $(GO_BIN):$(PATH)

SERVICE_TEST_DIRS := shared_lib pdf_extractor_lib profile_service ingestion_service data_registry_service feature_materializer_service data_stream_service inference_service model_registry_service model_serving_service training_service

.PHONY: install install-dev install-all build-all build-query-engine test test-query-engine test-hf start start-test stop restart start-servers stop-servers start-infra stop-infra start-data-sources stop-data-sources test-servers test-api test-api-w-hf kafka-clean kafka-restart kafka-error kafka-test docker-build docker-clean docker-start docker-start-intel docker-start-services docker-stop docker-stop-services reinstall-kafka k8s-validate k8s-deploy k8s-deploy-infra k8s-deploy-services k8s-deploy-service

install: install-all

install-dev:
	@scripts/install-dev.sh

install-all:
	@scripts/install-all.sh

build-all:
	@scripts/build-servers.sh $(ENV)

test:
	@set -e; \
	for service in $(SERVICE_TEST_DIRS); do \
		echo "==> make test -C $$service"; \
		$(MAKE) -C $$service test ENV=$(ENV); \
	done; \
	$(MAKE) test-api ENV=$(ENV) START_MODE=$(START_MODE) CI_TEST_EXCLUDE_SERVICES="$(CI_TEST_EXCLUDE_SERVICES)"

test-hf:
	@$(MAKE) -C "$(CURDIR)/ingestion_service" test-hf ENV=$(ENV)

build-query-engine:
	@$(MAKE) -C data_stream_service/internal/infra/queryengine build

test-query-engine:
	@$(MAKE) -C data_stream_service/internal/infra/queryengine test

start:
	@scripts/start-infra.sh $(ENV)
	@EXCLUDE_SERVICES=$(EXCLUDE_SERVICES) scripts/start-servers.sh $(START_MODE) $(ENV)
	@api_gateway/scripts/check-docker.sh
	@cd api_gateway && ./scripts/run.sh

start-test:
	@CI_TEST_EXCLUDE_SERVICES=$(CI_TEST_EXCLUDE_SERVICES) scripts/start-servers.sh $(START_MODE) $(ENV)
	@api_gateway/scripts/check-docker.sh
	@cd api_gateway && ./scripts/run.sh

stop:
	@cd api_gateway && ./scripts/stop.sh
	@scripts/stop-servers.sh
	@scripts/stop-infra.sh $(ENV)

restart:
	@cd api_gateway && ./scripts/stop.sh
	@scripts/stop-servers.sh
	@EXCLUDE_SERVICES=$(EXCLUDE_SERVICES) scripts/start-servers.sh build $(ENV)
	@cd api_gateway && ./scripts/run.sh

start-servers:
	@scripts/start-servers.sh $(START_MODE) $(ENV)

stop-servers:
	@scripts/stop-servers.sh

start-infra:
	@scripts/start-infra.sh $(ENV)

stop-infra:
	@scripts/stop-infra.sh $(ENV)

start-data-sources:
	@scripts/start-data-sources.sh

stop-data-sources:
	@scripts/stop-data-sources.sh

test-servers:
	@scripts/stop-servers.sh
	@scripts/stop-infra.sh $(ENV)
	@scripts/start-infra.sh $(ENV)
	@scripts/test-servers.sh $(ENV)

test-api:
	@set -e; \
	cleanup() { cd "$(CURDIR)/api_gateway" && ./scripts/stop.sh || true; cd "$(CURDIR)" && scripts/stop-servers.sh || true; cd "$(CURDIR)" && scripts/stop-infra.sh $(ENV) || true; }; \
	trap cleanup EXIT; \
	cd "$(CURDIR)/api_gateway" && ./scripts/stop.sh || true; \
	cd "$(CURDIR)" && scripts/stop-servers.sh || true; \
	cd "$(CURDIR)" && scripts/stop-infra.sh $(ENV) || true; \
	cd "$(CURDIR)" && scripts/start-infra.sh $(ENV); \
	cd "$(CURDIR)" && scripts/kafka/kafka-clean-topics.sh $(ENV); \
	cd "$(CURDIR)" && scripts/kafka/kafka-create-topics.sh $(ENV); \
	cd "$(CURDIR)" && api_gateway/scripts/check-docker.sh; \
	cd "$(CURDIR)" && CI_TEST_EXCLUDE_SERVICES="$(CI_TEST_EXCLUDE_SERVICES)" scripts/start-servers.sh $(START_MODE) $(ENV); \
	cd "$(CURDIR)/api_gateway" && ./scripts/run.sh; \
	$(MAKE) -C "$(CURDIR)/api_gateway" test ENV=$(ENV)

test-api-w-hf:
	@set -e; \
	if [ ! -f "$(CURDIR)/.env.huggingface-e2e" ]; then echo ".env.huggingface-e2e is required for test-api-w-hf"; exit 1; fi; \
	set -a; . "$(CURDIR)/.env.huggingface-e2e"; set +a; \
	if [ "$${BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD:-}" != "true" ]; then echo "BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true is required in .env.huggingface-e2e"; exit 1; fi; \
	: "$${BIGHILL_E2E_HUGGINGFACE_TOKEN:?BIGHILL_E2E_HUGGINGFACE_TOKEN is required in .env.huggingface-e2e}"; \
	: "$${BIGHILL_E2E_HUGGINGFACE_REPO_ID:?BIGHILL_E2E_HUGGINGFACE_REPO_ID is required in .env.huggingface-e2e}"; \
	$(MAKE) -C "$(CURDIR)/ingestion_service" test-hf ENV=$(ENV); \
	HF_E2E_START_MODE="$${BIGHILL_E2E_START_MODE:-build}"; \
	cleanup() { cd "$(CURDIR)/api_gateway" && ./scripts/stop.sh || true; cd "$(CURDIR)" && scripts/stop-servers.sh || true; cd "$(CURDIR)" && scripts/stop-infra.sh $(ENV) || true; }; \
	trap cleanup EXIT; \
	cd "$(CURDIR)/api_gateway" && ./scripts/stop.sh || true; \
	cd "$(CURDIR)" && scripts/stop-servers.sh || true; \
	cd "$(CURDIR)" && scripts/stop-infra.sh $(ENV) || true; \
	cd "$(CURDIR)" && scripts/start-infra.sh $(ENV); \
	cd "$(CURDIR)" && scripts/kafka/kafka-clean-topics.sh $(ENV); \
	cd "$(CURDIR)" && scripts/kafka/kafka-create-topics.sh $(ENV); \
	cd "$(CURDIR)" && api_gateway/scripts/check-docker.sh; \
	cd "$(CURDIR)" && CI_TEST_EXCLUDE_SERVICES="$(CI_TEST_EXCLUDE_SERVICES)" scripts/start-servers.sh "$$HF_E2E_START_MODE" $(ENV); \
	cd "$(CURDIR)/api_gateway" && ./scripts/run.sh; \
	cd "$(CURDIR)/api_gateway" && ginkgo -timeout=1200s -v --output-dir=../test_results/api_gateway_tests -procs=1 --focus "Hugging Face real model onboarding" ./test

kafka-clean:
	# @scripts/stop-servers.sh
	@scripts/kafka/kafka-clean-topics.sh $(ENV)
	@scripts/kafka/kafka-create-topics.sh $(ENV)
	# @scripts/start-servers.sh

kafka-restart:
	@scripts/kafka/kafka-restart.sh

kafka-error:
	@cat $(shell brew --prefix)/var/log/kafka/kafka_output.log 

kafka-test:
	@scripts/kafka/kafka-test.sh

docker-build:
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-build.sh $(ENV) arm64 full $(EXCLUDE)

docker-build-intel:
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-build.sh $(ENV) amd64 full $(EXCLUDE)

docker-clean:
	@scripts/docker-clean.sh

docker-start:
	@api_gateway/scripts/local-template.sh
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-db-migrations.sh $(ENV)
	@scripts/docker-start.sh $(ENV) arm64

docker-start-intel:
	@api_gateway/scripts/local-template.sh
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-db-migrations.sh $(ENV)
	@scripts/docker-start.sh $(ENV) amd64

docker-start-services:
	@api_gateway/scripts/local-template.sh
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-db-migrations.sh $(ENV)
	@scripts/docker-start.sh $(ENV) arm64

docker-stop:
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-stop.sh $(ENV) arm64

docker-stop-services:
	@api_gateway/scripts/check-docker.sh
	@scripts/docker-stop.sh $(ENV) arm64

reinstall-kafka:
	@scripts/kafka/kafka-clean-topics.sh
	@scripts/kafka/kafka-reinstall-dev.sh
	@scripts/kafka/kafka-config.sh
	@scripts/kafka/kafka-restart.sh
	@scripts/kafka/kafka-create-topics.sh

k8s-validate:
	@infra/scripts/validate-deploy.sh

k8s-deploy:
	@infra/scripts/k8s-deploy-infra.sh $(ENV)
	@infra/scripts/k8s-deploy-services.sh $(ENV)

k8s-deploy-infra:
	@infra/scripts/k8s-deploy-infra.sh $(ENV)

k8s-deploy-services:
	@infra/scripts/k8s-deploy-services.sh $(ENV)

k8s-deploy-service:
	@infra/scripts/k8s-deploy-service.sh $(ENV) $(SERVICE)
