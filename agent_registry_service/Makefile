ENV ?= local-dev

.PHONY: install build test

install:
	@scripts/install.sh

build:
	@scripts/build.sh $(ENV)

test:
	@scripts/test.sh $(ENV)
