.PHONY: test build install lint run 

ENV ?= local-dev

test:
	@scripts/test.sh $(ENV)

build:
	@scripts/build.sh $(ENV)

install:
	@scripts/install.sh $(ENV)

lint: 
	@golangci-lint run

run:
	@scripts/run.sh $(ENV)
