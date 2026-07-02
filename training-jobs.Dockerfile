ARG TARGETARCH=amd64
ARG PYTHON_VERSION=3.11

FROM --platform=linux/${TARGETARCH} python:${PYTHON_VERSION}-slim AS runtime
LABEL bighill="training-jobs"

ARG INSTALL_AXOLOTL=true

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV TRAINING_AXOLOTL_COMMAND="axolotl train"
ENV TRAINING_SERVING_LOAD_STATUS="LOADED"

WORKDIR /opt/bighill/training_jobs

RUN apt-get update && \
    apt-get install -y --no-install-recommends git build-essential curl && \
    rm -rf /var/lib/apt/lists/*

COPY ./training_jobs/pyproject.toml ./pyproject.toml
COPY ./training_jobs/training_jobs ./training_jobs

RUN pip install --upgrade pip && \
    pip install ".[runtime]" && \
    if [ "$INSTALL_AXOLOTL" = "true" ]; then pip install axolotl; fi

ENTRYPOINT ["python", "-m", "training_jobs.train"]
