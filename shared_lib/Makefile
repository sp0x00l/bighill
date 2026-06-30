.PHONY: test install lint

ENV ?= local-dev

test:
	@scripts/config.sh $(ENV)
	@scripts/test.sh

install:
	@scripts/install.sh

lint: 
	golangci-lint run
