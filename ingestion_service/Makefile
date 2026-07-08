.PHONY: test test-hf build release-prod release-staging lint run install
.DEFAULT_GOAL := run
SHELL         := /bin/bash

ENV ?= local-dev

test:
	@scripts/test.sh $(ENV)

test-hf:
	@set -e; \
	if [ ! -f "../.env.huggingface-e2e" ]; then echo "../.env.huggingface-e2e is required for make test-hf"; exit 1; fi; \
	set -a; . "../.env.huggingface-e2e"; set +a; \
	if [ "$${BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD:-}" != "true" ]; then echo "BIGHILL_E2E_HUGGINGFACE_REAL_DOWNLOAD=true is required in ../.env.huggingface-e2e"; exit 1; fi; \
	: "$${BIGHILL_E2E_HUGGINGFACE_TOKEN:?BIGHILL_E2E_HUGGINGFACE_TOKEN is required in ../.env.huggingface-e2e}"; \
	: "$${BIGHILL_E2E_HUGGINGFACE_REPO_ID:?BIGHILL_E2E_HUGGINGFACE_REPO_ID is required in ../.env.huggingface-e2e}"; \
	HF_PYTHON="$${BIGHILL_E2E_HUGGINGFACE_PYTHON:-}"; \
	if [ -z "$$HF_PYTHON" ]; then \
		PYENV_ROOT="$${PYENV_ROOT:-$$HOME/.pyenv}"; \
		if [ -x "$$PYENV_ROOT/versions/3.11.9/bin/python" ]; then HF_PYTHON="$$PYENV_ROOT/versions/3.11.9/bin/python"; \
		elif command -v python3 >/dev/null 2>&1; then HF_PYTHON="$$(command -v python3)"; \
		elif command -v python >/dev/null 2>&1; then HF_PYTHON="$$(command -v python)"; \
		else echo "python 3.11+ with training_jobs runtime dependencies is required; run make install-dev"; exit 1; fi; \
	fi; \
	"$$HF_PYTHON" -c 'import sys; assert sys.version_info >= (3, 11), "Python 3.11+ is required"; import huggingface_hub' || { echo "Hugging Face e2e Python dependencies are missing for $$HF_PYTHON; run make install-dev or set BIGHILL_E2E_HUGGINGFACE_PYTHON"; exit 1; }; \
	echo "Using Hugging Face onboarding Python: $$HF_PYTHON"; \
	"$$HF_PYTHON" scripts/check-huggingface-e2e.py

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
