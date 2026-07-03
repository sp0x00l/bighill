.PHONY: test build release-prod release-staging lint run install cert install-helm upgrade-helm
.DEFAULT_GOAL := run
SHELL         := /bin/bash

ENV ?= local-dev

test:
	@scripts/test.sh $(ENV)

build:
	@scripts/install.sh
	@scripts/build.sh $(ENV)

release-prod:
	@scripts/install.sh
	@scripts/build.sh prod

release-staging:
	@scripts/install.sh
	@scripts/build.sh staging

lint:
	@scripts/lint.sh

run:
	@scripts/run.sh $(ENV)

install:
	@scripts/install.sh

cert:
	@scripts/cert.sh

install-helm:
	@scripts/config.sh
	@helm install ${DATA_STREAM_SERVICE_BUILD_VERSION} bighill/data-stream-service helm/data-stream-service --set service.port=${DATA_STREAM_SERVICE_API_HTTP_PORT}

upgrade-helm:
	@scripts/config.sh
	@helm upgrade ${DATA_STREAM_SERVICE_BUILD_VERSION} bighill/data-stream-service helm/data-stream-service --set service.port=${DATA_STREAM_SERVICE_API_HTTP_PORT}
	
