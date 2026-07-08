from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import GatedRepoError, HfHubHTTPError, RepositoryNotFoundError
from bighill_model_artifacts.gguf import inspect_gguf


DEFAULT_GGUF_REPO = "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF"
DEFAULT_GGUF_FILE = "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf"


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


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
        info = api.model_info(repo_id=repo_id, revision=revision, token=token)
        local_dir = tempfile.mkdtemp(prefix="bighill-hf-e2e-preflight-")
        filename = model_file or "config.json"
        downloaded = Path(
            hf_hub_download(
                repo_id=repo_id,
                revision=revision,
                filename=filename,
                token=token,
                local_dir=local_dir,
            )
        )
        if filename.lower().endswith(".gguf") or artifact_format in {"GGUF", "GGUF_MODEL", "GGUF_LORA_ADAPTER"}:
            inspection = inspect_gguf(downloaded)
            if artifact_format in {"", "GGUF", "GGUF_MODEL"} and not inspection.chat_template_present:
                raise RuntimeError("GGUF preflight failed: tokenizer.chat_template is required for chat completions")
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
    print(f"Hugging Face repo access verified for {repo_id}@{revision}{file_suffix} ({info.sha}) as {username}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
