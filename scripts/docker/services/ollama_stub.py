#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
from argparse import ArgumentParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


DEFAULT_MODEL = "llama3.1:8b"


def context_excerpt(prompt: str) -> str:
    match = re.search(r"Retrieved context:\n(?P<context>.*?)(?:\nQuestion:|\Z)", prompt, re.S)
    if not match:
        return "the supplied prompt did not include retrieved context."
    lines = []
    for line in match.group("context").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("[context:"):
            continue
        lines.append(stripped)
    excerpt = " ".join(lines)
    excerpt = re.sub(r"\s+", " ", excerpt).strip()
    if not excerpt:
        return "the retrieved context was empty."
    return excerpt[:500]


class OllamaStubHandler(BaseHTTPRequestHandler):
    server_version = "BigHillOllamaStub/1.0"

    def do_GET(self) -> None:
        if self.path in {"/", "/health"}:
            self.write_json(200, {"status": "ok"})
            return
        if self.path == "/api/tags":
            self.write_json(200, {"models": [{"name": DEFAULT_MODEL}]})
            return
        self.write_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        try:
            if self.path != "/api/generate":
                self.write_json(404, {"error": "not found"})
                return
            payload = self.read_json()
            prompt = str(payload.get("prompt", "")).strip()
            if not prompt:
                self.write_json(400, {"error": "prompt is required"})
                return
            response = "Based on the retrieved context: " + context_excerpt(prompt)
            self.write_json(200, {
                "model": str(payload.get("model", DEFAULT_MODEL)),
                "response": response,
                "done": True,
            })
        except Exception as exc:
            self.write_json(500, {"error": str(exc)})

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def write_json(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def daemonize(pid_file: str, log_file: str) -> None:
    first_pid = os.fork()
    if first_pid > 0:
        os._exit(0)

    os.setsid()

    second_pid = os.fork()
    if second_pid > 0:
        os._exit(0)

    os.chdir("/")
    os.umask(0o022)
    os.makedirs(os.path.dirname(pid_file), exist_ok=True)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    with open(os.devnull, "rb", buffering=0) as stdin:
        os.dup2(stdin.fileno(), sys.stdin.fileno())
    log = open(log_file, "ab", buffering=0)
    os.dup2(log.fileno(), sys.stdout.fileno())
    os.dup2(log.fileno(), sys.stderr.fileno())

    with open(pid_file, "w", encoding="utf-8") as pid:
        pid.write(str(os.getpid()))
        pid.write("\n")


def main() -> None:
    parser = ArgumentParser(description="Ollama-compatible local generation endpoint")
    parser.add_argument("--port", type=int, default=11434)
    parser.add_argument("--daemonize", action="store_true")
    parser.add_argument("--pid-file", default="")
    parser.add_argument("--log-file", default="")
    args = parser.parse_args()

    if args.daemonize:
        if not args.pid_file or not args.log_file:
            raise SystemExit("--pid-file and --log-file are required with --daemonize")
        daemonize(args.pid_file, args.log_file)

    ThreadingHTTPServer(("0.0.0.0", args.port), OllamaStubHandler).serve_forever()


if __name__ == "__main__":
    main()
