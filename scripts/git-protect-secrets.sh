#!/usr/bin/env bash
set -euo pipefail

MODE="${1:---cached}"

case "$MODE" in
  --cached)
    SOURCE_CMD=(git grep --cached -I -n -E 'hf_[A-Za-z0-9]{20,}' --)
    ;;
  --worktree)
    SOURCE_CMD=(git grep -I -n -E 'hf_[A-Za-z0-9]{20,}' --)
    ;;
  *)
    echo "usage: scripts/git-protect-secrets.sh [--cached|--worktree]" >&2
    exit 2
    ;;
esac

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "git-protect-secrets: not inside a git worktree" >&2
  exit 2
fi

MATCHES="$("${SOURCE_CMD[@]}" 2>/dev/null || true)"
if [ -z "$MATCHES" ]; then
  exit 0
fi

echo "Refusing to continue because staged content contains a Hugging Face token." >&2
echo "$MATCHES" | sed -E 's/hf_[A-Za-z0-9]{20,}/<redacted-huggingface-token>/g' >&2
echo "Move the token to an ignored local env file and unstage the secret-bearing change." >&2
exit 1
