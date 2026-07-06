#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from urllib.parse import urlparse


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def repo_root() -> Path:
    current = Path.cwd().resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "shared_lib").exists():
            return candidate
    raise RuntimeError("repository root not found")


def local_s3_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise RuntimeError(f"unsupported URI {uri}")
    return repo_root() / "tmp" / "local_s3_storage" / parsed.netloc / parsed.path.lstrip("/")


def write_object(uri: str, content_type: str, content: bytes) -> None:
    path = local_s3_path(uri)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    (path.with_name(path.name + ".metadata.json")).write_text(
        json.dumps({"content_type": content_type}),
        encoding="utf-8",
    )


def main() -> None:
    resource_id = require_env("INGESTION_SERVICE_MODEL_RESOURCE_ID")
    model_name = require_env("INGESTION_SERVICE_MODEL_NAME")
    model_version = require_env("INGESTION_SERVICE_MODEL_VERSION")
    base_model = require_env("INGESTION_SERVICE_MODEL_BASE_MODEL")
    repo_id = require_env("INGESTION_SERVICE_HUGGINGFACE_REPO_ID")
    revision = os.environ.get("INGESTION_SERVICE_HUGGINGFACE_REVISION", "main").strip() or "main"
    require_env("INGESTION_SERVICE_HUGGINGFACE_TOKEN")
    output_uri = require_env("INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI").rstrip("/")
    artifact_type = os.environ.get("INGESTION_SERVICE_MODEL_ARTIFACT_TYPE", "BASE_MODEL").strip() or "BASE_MODEL"
    artifact_format = os.environ.get("INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT", "HF_MODEL").strip() or "HF_MODEL"

    artifact_uri = f"{output_uri}/{resource_id}/snapshot"
    manifest_uri = f"{output_uri}/{resource_id}/manifest.json"
    files = {
        "config.json": b'{"architectures":["BighillE2EHFModel"],"model_type":"bighill_e2e_hf"}',
        "model.safetensors": b"safe tensors fixture bytes from api e2e hf stub",
    }
    digest = hashlib.sha256()
    size = 0
    for relative_path, content in files.items():
        digest.update(relative_path.encode("utf-8"))
        digest.update(content)
        size += len(content)
        write_object(f"{artifact_uri}/{relative_path}", "application/octet-stream", content)

    manifest = {
        "resource_id": resource_id,
        "storage_location": artifact_uri,
        "manifest_location": manifest_uri,
        "artifact_type": artifact_type,
        "artifact_format": artifact_format,
        "artifact_size_bytes": size,
        "artifact_checksum": "sha256:" + digest.hexdigest(),
        "model_name": model_name,
        "model_version": model_version,
        "base_model": base_model,
        "source_uri": f"https://huggingface.co/{repo_id}",
        "hf_repo_id": repo_id,
        "hf_revision": revision,
        "hf_commit_sha": "api-e2e-" + digest.hexdigest()[:16],
    }
    write_object(manifest_uri, "application/json", json.dumps(manifest, sort_keys=True).encode("utf-8"))
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()

