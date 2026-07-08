#!/usr/bin/env sh
set -eu

if [ -z "${BIGHILL_ROOT:-}" ]; then
    BIGHILL_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
fi

PYTHON_BIN="${BIGHILL_MODEL_ARTIFACTS_PYTHON:-}"
if [ -z "$PYTHON_BIN" ]; then
    PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
    if [ -x "$PYENV_ROOT/versions/3.11.9/bin/python" ]; then
        PYTHON_BIN="$PYENV_ROOT/versions/3.11.9/bin/python"
    elif command -v python3.11 >/dev/null 2>&1; then
        PYTHON_BIN="$(command -v python3.11)"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN="$(command -v python3)"
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN="$(command -v python)"
    else
        echo "Python 3.11+ is required for GGUF inspection" >&2
        exit 1
    fi
fi

if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' >/dev/null 2>&1; then
    echo "Python 3.11+ is required for GGUF inspection; got $("$PYTHON_BIN" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))' 2>/dev/null || echo unknown) from $PYTHON_BIN" >&2
    exit 1
fi

export PYTHONPATH="$BIGHILL_ROOT/shared_py${PYTHONPATH:+:$PYTHONPATH}"
exec "$PYTHON_BIN" -m bighill_model_artifacts.gguf "$@"
