#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import math
import os
import sys
from argparse import ArgumentParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


DIMENSIONS = 384


def vector_for(text: str) -> list[float]:
    seed = hashlib.sha256(text.encode("utf-8")).digest()
    values: list[float] = []
    counter = 0
    while len(values) < DIMENSIONS:
        digest = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        for index in range(0, len(digest), 4):
            raw = int.from_bytes(digest[index : index + 4], "big", signed=False)
            values.append((raw / 0xFFFFFFFF) * 2.0 - 1.0)
            if len(values) == DIMENSIONS:
                break
        counter += 1
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [value / norm for value in values]


def score(query: str, text: str) -> float:
    query_terms = {term for term in query.lower().split() if term}
    text_terms = {term for term in text.lower().split() if term}
    overlap = len(query_terms & text_terms)
    return float(overlap) + (len(text) % 97) / 1000.0


class TEIStubHandler(BaseHTTPRequestHandler):
    server_version = "BigHillTEIStub/1.0"

    def do_GET(self) -> None:
        if self.path in {"/", "/health"}:
            self.write_json(200, {"status": "ok"})
            return
        self.write_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        try:
            payload = self.read_json()
            if self.path == "/embed":
                inputs = payload.get("inputs", [])
                if isinstance(inputs, str):
                    inputs = [inputs]
                if not isinstance(inputs, list):
                    self.write_json(400, {"error": "inputs must be a string or list"})
                    return
                self.write_json(200, [vector_for(str(item)) for item in inputs])
                return
            if self.path == "/rerank":
                query = str(payload.get("query", ""))
                texts = payload.get("texts", [])
                if not isinstance(texts, list):
                    self.write_json(400, {"error": "texts must be a list"})
                    return
                results = [
                    {"index": index, "score": score(query, str(text))}
                    for index, text in enumerate(texts)
                ]
                results.sort(key=lambda item: item["score"], reverse=True)
                self.write_json(200, results)
                return
            self.write_json(404, {"error": "not found"})
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
    parser = ArgumentParser(description="TEI-compatible local embedding/rerank endpoint")
    parser.add_argument("--daemonize", action="store_true")
    parser.add_argument("--pid-file", default="")
    parser.add_argument("--log-file", default="")
    args = parser.parse_args()

    if args.daemonize:
        if not args.pid_file or not args.log_file:
            raise SystemExit("--pid-file and --log-file are required with --daemonize")
        daemonize(args.pid_file, args.log_file)

    ThreadingHTTPServer(("0.0.0.0", 8080), TEIStubHandler).serve_forever()


if __name__ == "__main__":
    main()
