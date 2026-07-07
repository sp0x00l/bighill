ENV ?= local-dev
ARCH ?= arm64
START_MODE ?= run
BUILD_MODE ?= prebuilt
EXCLUDE_SERVICES ?=
CI_TEST_EXCLUDE_SERVICES ?=
GO_BIN ?= $(shell go env GOPATH)/bin

export PATH := $(GO_BIN):$(PATH)

.PHONY: install install-dev install-all build-all build-query-engine test test-query-engine start start-test stop restart start-servers stop-servers start-infra stop-infra start-data-sources stop-data-sources test-servers test-api kafka-clean kafka-create-topics kafka-restart kafka-error kafka-test docker-build docker-clean docker-start docker-start-intel docker-start-services docker-stop docker-stop-services reinstall-kafka upgrade-go kafka-clean-test-topics k8s-validate k8s-deploy k8s-deploy-infra k8s-deploy-services k8s-deploy-service

install: install-all

install-dev:
	@scripts/install-dev.sh

install-all:
	@scripts/install-all.sh

build-all:
	@scripts/build-servers.sh $(ENV)

test:
	@scripts/check-service-env-vars.sh
	@cd data_contracts && make install && make build
	@$(MAKE) -C training_jobs test
	@shared_lib/scripts/install.sh
	@shared_lib/scripts/test.sh $(ENV)
	@$(MAKE) build-query-engine

	@set -e; \
	cleanup() { cd "$(CURDIR)/api_gateway" && ./scripts/stop.sh || true; cd "$(CURDIR)" && scripts/stop-servers.sh || true; cd "$(CURDIR)" && scripts/stop-infra.sh $(ENV) || true; }; \
	trap cleanup EXIT; \
	scripts/stop-servers.sh; \
	scripts/stop-infra.sh $(ENV); \
	scripts/start-infra.sh $(ENV); \
	CI_TEST_EXCLUDE_SERVICES=$(CI_TEST_EXCLUDE_SERVICES) scripts/install-services.sh; \
	scripts/kafka/kafka-clean-topics.sh $(ENV); \
	scripts/kafka/kafka-create-topics.sh $(ENV); \
	api_gateway/scripts/check-docker.sh; \
	export INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_COMMAND="python3 $(CURDIR)/api_gateway/test/data/huggingface_onboard_stub.py"; \
	CI_TEST_EXCLUDE_SERVICES=$(CI_TEST_EXCLUDE_SERVICES) scripts/start-servers.sh build $(ENV); \
	api_gateway/scripts/install.sh; \
	api_gateway/scripts/build.sh auth; \
	api_gateway/scripts/build.sh api; \
	cd api_gateway && ./scripts/run.sh; \
	./scripts/test.sh $(ENV)

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
	scripts/stop-servers.sh; \
	scripts/stop-infra.sh $(ENV); \
	scripts/start-infra.sh $(ENV); \
	export INGESTION_SERVICE_HUGGINGFACE_DOWNLOAD_COMMAND="python3 $(CURDIR)/api_gateway/test/data/huggingface_onboard_stub.py"; \
	scripts/start-servers.sh $(START_MODE) $(ENV); \
	api_gateway/scripts/install.sh; \
	api_gateway/scripts/build.sh auth; \
	api_gateway/scripts/build.sh api; \
	cd api_gateway && ./scripts/run.sh; \
	./scripts/test.sh $(ENV)

kafka-clean:
	# @scripts/stop-servers.sh
	@scripts/kafka/kafka-clean-topics.sh $(ENV)
	@scripts/kafka/kafka-create-topics.sh $(ENV)
	# @scripts/start-servers.sh

kafka-clean-test-topics:
	@scripts/kafka/kafka-clean-test-topics.sh

kafka-create-topics:
	@scripts/kafka/kafka-config.sh
	@scripts/kafka/kafka-create-topics.sh

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

upgrade-go:
	@scripts/upgrade-go.sh

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
