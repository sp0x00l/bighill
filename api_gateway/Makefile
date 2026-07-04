.PHONY: install build build-BighillApiFunction build-BighillAuthFunction clean run stop test lint

ENV ?= local-dev

install:
	@scripts/install.sh

build-BighillApiFunction:
	@scripts/build.sh api

build-BighillAuthFunction:
	@scripts/build.sh auth

clean: 
	rm -rf ./build 

build:
	@scripts/local-template.sh
	@scripts/stop.sh
	@scripts/install.sh
	@scripts/build.sh auth
	@scripts/build.sh api

run:
	@scripts/local-template.sh
	@scripts/check-docker.sh; 
	@scripts/run.sh

stop:
	@cd ..; ./scripts/stop-servers.sh; ./scripts/stop-infra.sh $(ENV)

test:
	@scripts/local-template.sh
	@scripts/test.sh $(ENV)

lint: 
	sam validate --region eu-west-1 --lint
