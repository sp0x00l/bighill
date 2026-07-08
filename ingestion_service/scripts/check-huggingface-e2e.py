from __future__ import annotations

import os
import sys
import tempfile

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import GatedRepoError, HfHubHTTPError, RepositoryNotFoundError


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def main() -> int:
    token = require_env("BIGHILL_E2E_HUGGINGFACE_TOKEN")
    repo_id = require_env("BIGHILL_E2E_HUGGINGFACE_REPO_ID")
    revision = os.environ.get("BIGHILL_E2E_HUGGINGFACE_REVISION", "main").strip() or "main"

    try:
        api = HfApi()
        whoami = api.whoami(token=token)
        username = str(whoami.get("name") or whoami.get("email") or "").strip()
        if not username:
            raise RuntimeError("Hugging Face token login validation failed")
        info = api.model_info(repo_id=repo_id, revision=revision, token=token)
        local_dir = tempfile.mkdtemp(prefix="bighill-hf-e2e-preflight-")
        hf_hub_download(
            repo_id=repo_id,
            revision=revision,
            filename="config.json",
            token=token,
            local_dir=local_dir,
        )
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

    print(f"Hugging Face repo access verified for {repo_id}@{revision} ({info.sha}) as {username}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
