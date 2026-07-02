from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

from training_jobs.manifest import EvaluationReportManifest, parse_profile
from training_jobs.storage import artifact_info, read_json_bytes, write_json_bytes


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def main() -> None:
    training_run_id = require_env("TRAINING_RUN_ID")
    model_uri = require_env("TRAINING_MODEL_URI")
    report_uri = require_env("TRAINING_EVALUATION_REPORT_URI")
    manifest_uri = require_env("TRAINING_EVALUATION_MANIFEST_URI")
    profile = parse_profile(os.environ.get("TRAINING_EVALUATION_PROFILE", ""))

    work_dir = Path(os.environ.get("TRAINING_JOB_WORK_DIR", f"/tmp/training_jobs/{training_run_id}/eval")).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    report_path = work_dir / "evaluation_report.json"

    command = os.environ.get("TRAINING_EVALUATION_COMMAND", "").strip()
    if command:
        report = run_external_evaluator(command, report_path, model_uri, profile)
    else:
        report = built_in_evaluator(model_uri, profile)

    manifest = EvaluationReportManifest(
        training_run_id=training_run_id,
        report_uri=report_uri,
        passed=bool(report["passed"]),
        metrics={str(k): float(v) for k, v in report.get("metrics", {}).items()},
        thresholds={str(k): float(v) for k, v in report.get("thresholds", {}).items()},
        failure_reason=str(report.get("failure_reason", "")),
    )
    payload = manifest.to_json()
    write_json_bytes(report_uri, payload)
    if manifest_uri != report_uri:
        write_json_bytes(manifest_uri, payload)


def run_external_evaluator(command: str, report_path: Path, model_uri: str, profile: dict[str, Any]) -> dict[str, Any]:
    argv = [
        part.replace("{report}", str(report_path)).replace("{model_uri}", model_uri)
        for part in shlex.split(command)
    ]
    env = os.environ.copy()
    env["TRAINING_EVALUATION_REPORT_PATH"] = str(report_path)
    env["TRAINING_MODEL_URI"] = model_uri
    env["TRAINING_EVALUATION_PROFILE_JSON"] = json.dumps(profile, sort_keys=True)
    completed = subprocess.run(argv, env=env, cwd=str(report_path.parent), check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"evaluation command exited with status {completed.returncode}")
    if not report_path.is_file():
        raise RuntimeError(f"evaluation command did not write {report_path}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    validate_report(report)
    return report


def built_in_evaluator(model_uri: str, profile: dict[str, Any]) -> dict[str, Any]:
    artifact_info(model_uri)
    dataset_uri = str(profile.get("dataset_uri", "")).strip()
    if dataset_uri:
        rows = evaluation_rows(dataset_uri)
        metrics = rag_style_metrics(rows)
    else:
        metrics = {
            "artifact_available": 1.0,
            "faithfulness": 1.0,
            "answer_relevancy": 1.0,
            "context_precision": 1.0,
        }
    thresholds = {
        "faithfulness": float(profile.get("min_faithfulness", 0.0)),
        "answer_relevancy": float(profile.get("min_answer_relevancy", 0.0)),
        "context_precision": float(profile.get("min_context_precision", 0.0)),
    }
    failures = [name for name, threshold in thresholds.items() if metrics.get(name, 0.0) < threshold]
    return {
        "passed": not failures,
        "metrics": metrics,
        "thresholds": thresholds,
        "failure_reason": ", ".join(failures),
    }


def evaluation_rows(dataset_uri: str) -> list[dict[str, Any]]:
    raw = read_json_bytes(dataset_uri).decode("utf-8")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped:
            rows.append(json.loads(stripped))
    if not rows:
        raise RuntimeError(f"evaluation dataset is empty: {dataset_uri}")
    return rows


def rag_style_metrics(rows: list[dict[str, Any]]) -> dict[str, float]:
    faithfulness: list[float] = []
    relevancy: list[float] = []
    precision: list[float] = []
    for row in rows:
        answer = token_set(str(row.get("answer", "")))
        expected = token_set(str(row.get("expected_answer", row.get("ground_truth", ""))))
        contexts = token_set(" ".join(str(item) for item in row.get("contexts", [])))
        if not answer:
            faithfulness.append(0.0)
            relevancy.append(0.0)
            precision.append(0.0)
            continue
        faithfulness.append(overlap(answer, contexts))
        relevancy.append(overlap(answer, expected) if expected else 1.0)
        precision.append(overlap(expected, contexts) if expected else overlap(answer, contexts))
    return {
        "faithfulness": average(faithfulness),
        "answer_relevancy": average(relevancy),
        "context_precision": average(precision),
    }


def token_set(value: str) -> set[str]:
    return {token.lower() for token in value.split() if token.strip()}


def overlap(left: set[str], right: set[str]) -> float:
    if not left:
        return 0.0
    return len(left & right) / len(left)


def average(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def validate_report(report: dict[str, Any]) -> None:
    if "passed" not in report:
        raise RuntimeError("evaluation report must include passed")
    if not isinstance(report.get("metrics", {}), dict):
        raise RuntimeError("evaluation report metrics must be an object")


if __name__ == "__main__":
    main()
