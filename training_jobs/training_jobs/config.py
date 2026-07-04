from __future__ import annotations

import os
from pathlib import Path

from training_jobs.storage import StorageConfig


def read_storage_config() -> StorageConfig:
    local_s3_dir = os.environ.get("BIGHILL_LOCAL_S3_STORAGE_DIR", "").strip()
    region = (
        os.environ.get("TRAINING_ARTIFACT_BUCKET_REGION")
        or os.environ.get("BIGHILL_ARTIFACT_BUCKET_REGION")
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "eu-west-1"
    ).strip()
    return StorageConfig(
        artifact_bucket_region=region,
        local_s3_storage_dir=Path(local_s3_dir).expanduser().resolve() if local_s3_dir else None,
    )
