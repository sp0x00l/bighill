.PHONY: test build release-prod release-staging lint run watch install
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

watch:
	@scripts/watch.sh

install:
	@scripts/install.sh
