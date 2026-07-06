from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

_ACTIVE_PROCESSES: set[subprocess.Popen[bytes]] = set()
_LOCK = threading.Lock()
_HANDLERS_INSTALLED = False
_TERMINATION_GRACE_SECONDS = 10.0


def run_child(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
) -> int:
    _install_signal_handlers()
    process = subprocess.Popen(
        list(argv),
        env=dict(env) if env is not None else None,
        cwd=str(cwd) if cwd is not None else None,
        start_new_session=True,
    )
    with _LOCK:
        _ACTIVE_PROCESSES.add(process)
    try:
        return process.wait()
    finally:
        with _LOCK:
            _ACTIVE_PROCESSES.discard(process)


def _install_signal_handlers() -> None:
    global _HANDLERS_INSTALLED
    with _LOCK:
        if _HANDLERS_INSTALLED:
            return
        signal.signal(signal.SIGTERM, _forward_signal_and_exit)
        signal.signal(signal.SIGINT, _forward_signal_and_exit)
        _HANDLERS_INSTALLED = True


def _forward_signal_and_exit(signum: int, _frame: object) -> None:
    _terminate_active_children(signum)
    raise SystemExit(128 + signum)


def _terminate_active_children(signum: int) -> None:
    with _LOCK:
        processes = list(_ACTIVE_PROCESSES)
    for process in processes:
        _terminate_process_group(process, signum)


def _terminate_process_group(process: subprocess.Popen[bytes], signum: int) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signum)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + _TERMINATION_GRACE_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
