from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from training_jobs.config import read_storage_config
from training_jobs import storage

local_fixture_env = "INGESTION_SERVICE_HUGGINGFACE_LOCAL_FIXTURE_ROOT"


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def optional_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def snapshot_download(*, repo_id: str, revision: str, token: str, local_dir: Path) -> str:
    from huggingface_hub import snapshot_download as hf_snapshot_download

    return hf_snapshot_download(
        repo_id=repo_id,
        revision=revision,
        token=token,
        local_dir=str(local_dir),
        local_dir_use_symlinks=False,
    )


def resolve_commit_sha(*, repo_id: str, revision: str, token: str) -> str:
    from huggingface_hub import HfApi

    info = HfApi().model_info(repo_id=repo_id, revision=revision, token=token)
    sha = (getattr(info, "sha", "") or "").strip()
    if not sha:
        raise RuntimeError("Hugging Face model metadata did not include a resolved commit sha")
    return sha


def resolve_local_fixture_snapshot(*, repo_id: str, revision: str) -> tuple[Path, str] | None:
    root = optional_env(local_fixture_env)
    if not root:
        return None
    snapshot = Path(root).expanduser().resolve() / repo_id
    if not snapshot.is_dir():
        return None
    return snapshot, f"local-{revision}"


def validate_snapshot(snapshot_dir: Path) -> None:
    files = {p.relative_to(snapshot_dir).as_posix() for p in snapshot_dir.rglob("*") if p.is_file()}
    if "config.json" not in files:
        raise RuntimeError("downloaded Hugging Face model is missing config.json")
    has_weights = any(path.endswith(".safetensors") for path in files) or "model.safetensors.index.json" in files
    if not has_weights:
        raise RuntimeError("downloaded Hugging Face model is missing safetensors weights")


def main() -> None:
    resource_id = require_env("INGESTION_SERVICE_MODEL_RESOURCE_ID")
    repo_id = require_env("INGESTION_SERVICE_HUGGINGFACE_REPO_ID")
    revision = optional_env("INGESTION_SERVICE_HUGGINGFACE_REVISION", "main")
    token = require_env("INGESTION_SERVICE_HUGGINGFACE_TOKEN")
    output_root = require_env("INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI").rstrip("/")
    model_name = require_env("INGESTION_SERVICE_MODEL_NAME")
    model_version = require_env("INGESTION_SERVICE_MODEL_VERSION")
    base_model = require_env("INGESTION_SERVICE_MODEL_BASE_MODEL")
    artifact_type = optional_env("INGESTION_SERVICE_MODEL_ARTIFACT_TYPE", "BASE_MODEL")
    artifact_format = optional_env("INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT", "HF_MODEL")
    storage_config = read_storage_config()

    with tempfile.TemporaryDirectory() as tmp:
        local_dir = Path(tmp) / "snapshot"
        fixture = resolve_local_fixture_snapshot(repo_id=repo_id, revision=revision)
        if fixture is not None:
            snapshot_path, commit = fixture
        else:
            commit = resolve_commit_sha(repo_id=repo_id, revision=revision, token=token)
            snapshot_path = Path(snapshot_download(repo_id=repo_id, revision=revision, token=token, local_dir=local_dir))
        validate_snapshot(snapshot_path)
        artifact_uri = f"{output_root}/{resource_id}/snapshot"
        manifest_uri = f"{output_root}/{resource_id}/manifest.json"
        artifact = storage.upload_directory(snapshot_path, artifact_uri, storage_config)
        manifest = {
            "resource_id": resource_id,
            "storage_location": artifact.uri,
            "manifest_location": manifest_uri,
            "artifact_type": artifact_type,
            "artifact_format": artifact_format,
            "artifact_size_bytes": artifact.size_bytes,
            "artifact_checksum": artifact.checksum,
            "model_name": model_name,
            "model_version": model_version,
            "base_model": base_model,
            "source_uri": f"https://huggingface.co/{repo_id}",
            "hf_repo_id": repo_id,
            "hf_revision": revision,
            "hf_commit_sha": commit,
        }
        storage.write_json_bytes(manifest_uri, json.dumps(manifest, sort_keys=True).encode("utf-8"), storage_config)
        print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
