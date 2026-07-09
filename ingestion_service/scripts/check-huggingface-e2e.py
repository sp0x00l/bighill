from __future__ import annotations

import os
import sys

from huggingface_hub import HfApi
from huggingface_hub.errors import GatedRepoError, HfHubHTTPError, RepositoryNotFoundError


DEFAULT_GGUF_REPO = "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF"
DEFAULT_GGUF_FILE = "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf"


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def model_info(api: HfApi, *, repo_id: str, revision: str, token: str):
    try:
        return api.model_info(repo_id=repo_id, revision=revision, token=token, files_metadata=True)
    except TypeError:
        return api.model_info(repo_id=repo_id, revision=revision, token=token)


def sibling_attr(sibling: object, name: str):
    if isinstance(sibling, dict):
        return sibling.get(name)
    return getattr(sibling, name, None)


def lfs_attr(sibling: object, name: str):
    lfs = sibling_attr(sibling, "lfs")
    if isinstance(lfs, dict):
        return lfs.get(name)
    return getattr(lfs, name, None)


def find_sibling(info: object, filename: str):
    for sibling in getattr(info, "siblings", []) or []:
        if sibling_attr(sibling, "rfilename") == filename:
            return sibling
    return None


def main() -> int:
    token = require_env("BIGHILL_E2E_HUGGINGFACE_TOKEN")
    repo_id = require_env("BIGHILL_E2E_HUGGINGFACE_REPO_ID")
    revision = os.environ.get("BIGHILL_E2E_HUGGINGFACE_REVISION", "main").strip() or "main"
    model_file = os.environ.get("BIGHILL_E2E_HUGGINGFACE_FILE", "").strip()
    artifact_format = os.environ.get("BIGHILL_E2E_HUGGINGFACE_ARTIFACT_FORMAT", "").strip().upper().replace("-", "_")
    if not model_file and repo_id == DEFAULT_GGUF_REPO:
        model_file = DEFAULT_GGUF_FILE

    try:
        api = HfApi()
        whoami = api.whoami(token=token)
        username = str(whoami.get("name") or whoami.get("email") or "").strip()
        if not username:
            raise RuntimeError("Hugging Face token login validation failed")
        info = model_info(api, repo_id=repo_id, revision=revision, token=token)
        filename = model_file or "config.json"
        sibling = find_sibling(info, filename)
        if sibling is None:
            raise RuntimeError(f"file {filename!r} was not found in the Hugging Face repo metadata")
        if artifact_format in {"GGUF", "GGUF_MODEL", "GGUF_LORA_ADAPTER"} and not filename.lower().endswith(".gguf"):
            raise RuntimeError(f"artifact format {artifact_format} requires a .gguf file, got {filename!r}")
    except GatedRepoError as err:
        print(
            f"Hugging Face repo access denied for {repo_id}@{revision}. "
            "The token is valid, but this account is not authorized for the gated model files. "
            f"Request/accept access on https://huggingface.co/{repo_id}.",
            file=sys.stderr,
        )
        print(str(err).splitlines()[-1], file=sys.stderr)
        return 1
    except (RepositoryNotFoundError, HfHubHTTPError, Exception) as err:
        print(f"Hugging Face preflight failed for {repo_id}@{revision}: {err}", file=sys.stderr)
        return 1

    file_suffix = f" file {model_file}" if model_file else ""
    size = sibling_attr(sibling, "size") or lfs_attr(sibling, "size")
    oid = lfs_attr(sibling, "sha256") or lfs_attr(sibling, "oid")
    size_suffix = f", size={size}" if size else ""
    oid_suffix = f", lfs_sha256={oid}" if oid else ""
    print(f"Hugging Face repo access verified for {repo_id}@{revision}{file_suffix} ({info.sha}) as {username}{size_suffix}{oid_suffix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
