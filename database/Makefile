.PHONY: all install migrate start stop delete install-dev
.DEFAULT_GOAL := run
SHELL         := /bin/bash

install:
	@scripts/db-stop.sh
	@scripts/db-delete.sh
	@scripts/db-install.sh
	@scripts/db-start.sh
	@scripts/db-migrate.sh

migrate:
	@scripts/db-migrate.sh $(SERVICE)

start:
	@scripts/db-start.sh

stop:
	@scripts/db-stop.sh

delete:
	@scripts/db-stop.sh
	@scripts/db-delete.sh

install-dev:
	@scripts/install-dev.sh
