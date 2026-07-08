from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

from training_jobs.config import read_storage_config
from training_jobs import storage


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


def file_download(*, repo_id: str, revision: str, token: str, filename: str, local_dir: Path) -> str:
    from huggingface_hub import hf_hub_download

    return hf_hub_download(
        repo_id=repo_id,
        revision=revision,
        token=token,
        filename=filename,
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


def resolve_file_sha256(*, repo_id: str, revision: str, token: str, filename: str) -> str:
    from huggingface_hub import HfApi

    info = HfApi().model_info(repo_id=repo_id, revision=revision, token=token, files_metadata=True)
    for sibling in getattr(info, "siblings", []) or []:
        name = str(getattr(sibling, "rfilename", "") or "").strip()
        if name != filename:
            continue
        lfs = getattr(sibling, "lfs", None)
        if isinstance(lfs, dict):
            return str(lfs.get("sha256") or "").strip()
        return str(getattr(lfs, "sha256", "") or "").strip()
    return ""


def validate_login(*, token: str) -> str:
    from huggingface_hub import HfApi

    info = HfApi().whoami(token=token)
    name = str(info.get("name") or info.get("email") or "").strip()
    if not name:
        raise RuntimeError("Hugging Face token login validation failed")
    return name


def validate_snapshot(snapshot_dir: Path) -> None:
    files = {p.relative_to(snapshot_dir).as_posix() for p in snapshot_dir.rglob("*") if p.is_file()}
    if "config.json" not in files:
        raise RuntimeError("downloaded Hugging Face model is missing config.json")
    has_weights = any(path.endswith(".safetensors") for path in files) or "model.safetensors.index.json" in files
    if not has_weights:
        raise RuntimeError("downloaded Hugging Face model is missing safetensors weights")


def validate_gguf_file(path: Path, *, require_chat_template: bool) -> None:
    from bighill_model_artifacts.gguf import inspect_gguf

    inspection = inspect_gguf(path)
    if require_chat_template and not inspection.chat_template_present:
        raise RuntimeError("downloaded GGUF model is missing tokenizer.chat_template")


def is_gguf_artifact(artifact_format: str, file_name: str) -> bool:
    return is_gguf_format(artifact_format) or is_gguf_file(file_name)


def is_gguf_chat_model(artifact_format: str) -> bool:
    return normalize_token(artifact_format) in {"GGUF", "GGUF_MODEL"}


def is_gguf_file(file_name: str) -> bool:
    return file_name.lower().endswith(".gguf")


def is_gguf_format(artifact_format: str) -> bool:
    return normalize_token(artifact_format) in {"GGUF", "GGUF_MODEL", "GGUF_LORA_ADAPTER"}


def normalize_token(value: str) -> str:
    return value.strip().upper().replace("-", "_")


def infer_exact_file_artifact_format(*, artifact_type: str, artifact_format: str, hf_file: str, format_was_explicit: bool) -> str:
    normalized_format = normalize_token(artifact_format)
    if not is_gguf_file(hf_file):
        return normalized_format
    if normalized_format and is_gguf_format(normalized_format):
        return normalized_format
    if format_was_explicit:
        raise RuntimeError("GGUF Hugging Face files must use GGUF_MODEL or GGUF_LORA_ADAPTER artifact format")
    if normalize_token(artifact_type) == "LORA_ADAPTER":
        return "GGUF_LORA_ADAPTER"
    return "GGUF_MODEL"


def provider_status(err: Exception) -> int:
    response = getattr(err, "response", None)
    return int(getattr(response, "status_code", 0) or 0)


def provider_message(err: Exception) -> str:
    lines = [line.strip() for line in str(err).splitlines() if line.strip()]
    return lines[-1] if lines else err.__class__.__name__


def hugging_face_error_code(err: Exception, status: int) -> str:
    name = err.__class__.__name__
    if name == "GatedRepoError":
        return "GATED_REPO"
    if name == "RepositoryNotFoundError":
        return "REPO_NOT_FOUND"
    if status == 401:
        return "UNAUTHORIZED"
    if status == 403:
        return "FORBIDDEN"
    if status == 404:
        return "NOT_FOUND"
    if status == 429:
        return "RATE_LIMITED"
    if status >= 500:
        return "SERVER_ERROR"
    return "HTTP_ERROR" if status else "HUGGING_FACE_ERROR"


def emit_provider_error(err: Exception, *, repo_id: str, revision: str) -> None:
    status = provider_status(err)
    payload = {
        "provider": "Hugging Face",
        "http_status": status,
        "error_code": hugging_face_error_code(err, status),
        "message": provider_message(err),
        "repo_id": repo_id,
        "revision": revision,
    }
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)


def run() -> None:
    resource_id = require_env("INGESTION_SERVICE_MODEL_RESOURCE_ID")
    repo_id = require_env("INGESTION_SERVICE_HUGGINGFACE_REPO_ID")
    revision = optional_env("INGESTION_SERVICE_HUGGINGFACE_REVISION", "main")
    token = require_env("INGESTION_SERVICE_HUGGINGFACE_TOKEN")
    output_root = require_env("INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI").rstrip("/")
    model_name = require_env("INGESTION_SERVICE_MODEL_NAME")
    model_version = require_env("INGESTION_SERVICE_MODEL_VERSION")
    base_model = require_env("INGESTION_SERVICE_MODEL_BASE_MODEL")
    artifact_type = optional_env("INGESTION_SERVICE_MODEL_ARTIFACT_TYPE", "BASE_MODEL")
    artifact_format_env = os.environ.get("INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT", "").strip()
    artifact_format = artifact_format_env or "HF_MODEL"
    hf_file = optional_env("INGESTION_SERVICE_HUGGINGFACE_FILE")
    if hf_file:
        artifact_format = infer_exact_file_artifact_format(
            artifact_type=artifact_type,
            artifact_format=artifact_format,
            hf_file=hf_file,
            format_was_explicit=bool(artifact_format_env),
        )
    storage_config = read_storage_config()

    with tempfile.TemporaryDirectory() as tmp:
        local_dir = Path(tmp) / ("file" if hf_file else "snapshot")
        validate_login(token=token)
        commit = resolve_commit_sha(repo_id=repo_id, revision=revision, token=token)
        if hf_file:
            downloaded_file = Path(file_download(repo_id=repo_id, revision=revision, token=token, filename=hf_file, local_dir=local_dir))
            expected_sha256 = resolve_file_sha256(repo_id=repo_id, revision=revision, token=token, filename=hf_file)
            if expected_sha256:
                actual_checksum, _ = storage.file_digest(downloaded_file)
                if actual_checksum != "sha256:" + expected_sha256.lower():
                    raise RuntimeError("downloaded Hugging Face file checksum did not match repository LFS metadata")
            snapshot_path = local_dir
        else:
            downloaded_file = None
            snapshot_path = Path(snapshot_download(repo_id=repo_id, revision=revision, token=token, local_dir=local_dir))
        if hf_file:
            if downloaded_file is None:
                raise RuntimeError("downloaded Hugging Face file path was not resolved")
            if not downloaded_file.is_file():
                raise RuntimeError(f"downloaded Hugging Face file is missing: {hf_file}")
            if is_gguf_artifact(artifact_format, hf_file):
                validate_gguf_file(downloaded_file, require_chat_template=is_gguf_chat_model(artifact_format))
            artifact_uri = f"{output_root}/{resource_id}/{Path(hf_file).name}"
            artifact = storage.upload_file(downloaded_file, artifact_uri, storage_config)
        else:
            validate_snapshot(snapshot_path)
            artifact_uri = f"{output_root}/{resource_id}/snapshot"
            artifact = storage.upload_directory(snapshot_path, artifact_uri, storage_config)
        manifest_uri = f"{output_root}/{resource_id}/manifest.json"
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
            "hf_file": hf_file,
        }
        storage.write_json_bytes(manifest_uri, json.dumps(manifest, sort_keys=True).encode("utf-8"), storage_config)
        print(json.dumps(manifest, sort_keys=True))


def main() -> None:
    repo_id = os.environ.get("INGESTION_SERVICE_HUGGINGFACE_REPO_ID", "").strip()
    revision = os.environ.get("INGESTION_SERVICE_HUGGINGFACE_REVISION", "main").strip() or "main"
    try:
        run()
    except Exception as err:
        module = err.__class__.__module__
        if module.startswith("huggingface_hub"):
            emit_provider_error(err, repo_id=repo_id, revision=revision)
        else:
            payload = {
                "provider": "Hugging Face",
                "http_status": 0,
                "error_code": "ONBOARDING_FAILED",
                "message": provider_message(err),
                "repo_id": repo_id,
                "revision": revision,
            }
            print(json.dumps(payload, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
