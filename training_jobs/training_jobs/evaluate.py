from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any, Iterable

from training_jobs.config import read_storage_config
from training_jobs.manifest import EvaluationReportManifest, parse_profile
from training_jobs.storage import StorageConfig
from training_jobs.storage import artifact_info, read_json_bytes, write_json_bytes

REQUIRED_ENV_KEYS = (
    "TRAINING_EVALUATION_MANIFEST_URI",
    "TRAINING_EVALUATION_PROFILE",
    "TRAINING_EVALUATION_REPORT_URI",
    "TRAINING_MODEL_URI",
    "TRAINING_RUN_ID",
)

OPTIONAL_ENV_KEYS = (
    "TRAINING_EVALUATION_COMMAND",
    "TRAINING_JOB_WORK_DIR",
)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def main() -> None:
    required = {key: require_env(key) for key in REQUIRED_ENV_KEYS}
    training_run_id = required["TRAINING_RUN_ID"]
    model_uri = required["TRAINING_MODEL_URI"]
    report_uri = required["TRAINING_EVALUATION_REPORT_URI"]
    manifest_uri = required["TRAINING_EVALUATION_MANIFEST_URI"]
    profile = parse_profile(required["TRAINING_EVALUATION_PROFILE"])
    openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if openai_api_key and not str(profile.get("judge_api_key", "")).strip():
        profile["judge_api_key"] = openai_api_key
    storage_config = read_storage_config()

    work_dir = Path(os.environ.get("TRAINING_JOB_WORK_DIR", f"/tmp/training_jobs/{training_run_id}/eval")).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)
    report_path = work_dir / "evaluation_report.json"

    command = os.environ.get("TRAINING_EVALUATION_COMMAND", "").strip()
    evaluator_name = normalized_profile_value(profile, "evaluator", "name", default="built_in")
    if command:
        report = run_external_evaluator(command, report_path, model_uri, profile)
        report.setdefault("evaluator_name", "external")
        report.setdefault("evaluator_version", str(profile.get("evaluator_version", "external-v1")))
        report.setdefault("metric_suite", str(profile.get("metric_suite", "external")))
    elif evaluator_name == "ragas" or normalized_profile_value(profile, "metric_suite", default="") in {"ragas", "rag"}:
        report = run_ragas_evaluator(model_uri, profile, storage_config)
    else:
        report = built_in_evaluator(model_uri, profile, storage_config)

    manifest = EvaluationReportManifest(
        training_run_id=training_run_id,
        report_uri=report_uri,
        passed=bool(report["passed"]),
        metrics={str(k): float(v) for k, v in report.get("metrics", {}).items()},
        thresholds={str(k): float(v) for k, v in report.get("thresholds", {}).items()},
        evaluator_name=str(report.get("evaluator_name", evaluator_name)),
        evaluator_version=str(report.get("evaluator_version", profile.get("evaluator_version", ""))),
        metric_suite=str(report.get("metric_suite", profile.get("metric_suite", ""))),
        eval_dataset_uri=str(report.get("eval_dataset_uri", profile.get("dataset_uri", ""))),
        eval_dataset_mode=str(report.get("eval_dataset_mode", profile.get("dataset_mode", ""))),
        judge_provider=str(report.get("judge_provider", profile.get("judge_provider", ""))),
        judge_model=str(report.get("judge_model", profile.get("judge_model", ""))),
        judge_template_version=str(report.get("judge_template_version", profile.get("judge_template_version", ""))),
        failure_reason=str(report.get("failure_reason", "")),
    )
    payload = manifest.to_json()
    write_json_bytes(report_uri, payload, storage_config)
    if manifest_uri != report_uri:
        write_json_bytes(manifest_uri, payload, storage_config)


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


def built_in_evaluator(model_uri: str, profile: dict[str, Any], storage_config: StorageConfig) -> dict[str, Any]:
    artifact_info(model_uri, storage_config)
    dataset_uri = str(profile.get("dataset_uri", "")).strip()
    if dataset_uri:
        rows = evaluation_rows(dataset_uri, storage_config)
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
        "evaluator_name": "built_in",
        "evaluator_version": str(profile.get("evaluator_version", "v1")),
        "metric_suite": str(profile.get("metric_suite", "rag_smoke")),
        "eval_dataset_uri": dataset_uri,
        "eval_dataset_mode": str(profile.get("dataset_mode", "synthetic" if not dataset_uri else "labeled")),
        "judge_provider": str(profile.get("judge_provider", "")),
        "judge_model": str(profile.get("judge_model", "")),
        "judge_template_version": str(profile.get("judge_template_version", "")),
        "failure_reason": ", ".join(failures),
    }


def run_ragas_evaluator(model_uri: str, profile: dict[str, Any], storage_config: StorageConfig) -> dict[str, Any]:
    artifact_info(model_uri, storage_config)
    dataset_uri = str(profile.get("dataset_uri", "")).strip()
    if not dataset_uri:
        raise RuntimeError("ragas evaluation requires dataset_uri")
    rows = evaluation_rows(dataset_uri, storage_config)
    dataset = ragas_dataset(rows)
    metrics = ragas_metrics(profile)
    llm = ragas_llm(profile)
    result = call_ragas_evaluate(dataset, metrics, llm)
    metric_values = extract_ragas_metrics(result)
    thresholds = evaluation_thresholds(profile, metric_values.keys())
    failures = [name for name, threshold in thresholds.items() if metric_values.get(name, 0.0) < threshold]
    return {
        "passed": not failures,
        "metrics": metric_values,
        "thresholds": thresholds,
        "evaluator_name": "ragas",
        "evaluator_version": str(profile.get("evaluator_version", "ragas-v1")),
        "metric_suite": str(profile.get("metric_suite", "rag")),
        "eval_dataset_uri": dataset_uri,
        "eval_dataset_mode": str(profile.get("dataset_mode", infer_dataset_mode(rows))),
        "judge_provider": str(profile.get("judge_provider", "")),
        "judge_model": str(profile.get("judge_model", "")),
        "judge_template_version": str(profile.get("judge_template_version", "")),
        "failure_reason": ", ".join(failures),
    }


def evaluation_rows(dataset_uri: str, storage_config: StorageConfig) -> list[dict[str, Any]]:
    raw = read_json_bytes(dataset_uri, storage_config).decode("utf-8")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped:
            rows.append(json.loads(stripped))
    if not rows:
        raise RuntimeError(f"evaluation dataset is empty: {dataset_uri}")
    return rows


def ragas_dataset(rows: list[dict[str, Any]]) -> Any:
    try:
        from datasets import Dataset
    except ImportError as err:
        raise RuntimeError("ragas evaluation requires the datasets package") from err
    return Dataset.from_list([ragas_row(row) for row in rows])


def ragas_row(row: dict[str, Any]) -> dict[str, Any]:
    question = first_string(row, "question", "query", "user_input")
    answer = first_string(row, "answer", "response")
    contexts = first_list(row, "contexts", "retrieved_contexts", "reference_contexts")
    ground_truth = first_string(row, "ground_truth", "expected_answer", "reference", "reference_answer")
    return {
        "question": question,
        "answer": answer,
        "contexts": contexts,
        "ground_truth": ground_truth,
        "user_input": question,
        "response": answer,
        "retrieved_contexts": contexts,
        "reference": ground_truth,
    }


def ragas_metrics(profile: dict[str, Any]) -> list[Any]:
    try:
        import ragas.metrics as metric_module
    except ImportError as err:
        raise RuntimeError("ragas evaluation requires the ragas package") from err
    metric_names = profile.get("metrics", ["faithfulness", "answer_relevancy", "context_precision"])
    if isinstance(metric_names, str):
        metric_names = [name.strip() for name in metric_names.split(",") if name.strip()]
    metrics: list[Any] = []
    for name in metric_names:
        metrics.append(ragas_metric(metric_module, str(name)))
    return metrics


def ragas_metric(metric_module: Any, name: str) -> Any:
    candidates = [
        name,
        name.lower(),
        "".join(part.capitalize() for part in name.split("_")),
        {
            "answer_relevancy": "ResponseRelevancy",
            "answer_relevance": "ResponseRelevancy",
            "context_precision": "ContextPrecision",
            "context_recall": "ContextRecall",
            "faithfulness": "Faithfulness",
        }.get(name.lower(), ""),
    ]
    for candidate in candidates:
        if candidate and hasattr(metric_module, candidate):
            metric = getattr(metric_module, candidate)
            return metric() if isinstance(metric, type) else metric
    raise RuntimeError(f"unsupported ragas metric {name!r}")


def ragas_llm(profile: dict[str, Any]) -> Any:
    judge_model = str(profile.get("judge_model", "")).strip()
    if not judge_model:
        return None
    provider = str(profile.get("judge_provider", "openai")).strip() or "openai"
    base_url = str(profile.get("judge_base_url", "")).strip()
    api_key = str(profile.get("judge_api_key", "not-needed")).strip()
    if provider != "openai":
        return None
    try:
        from openai import OpenAI
        from ragas.llms import llm_factory
    except ImportError as err:
        raise RuntimeError("ragas OpenAI-compatible judge requires openai and ragas.llms") from err
    kwargs = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    client = OpenAI(**kwargs)
    return llm_factory(judge_model, provider="openai", client=client)


def call_ragas_evaluate(dataset: Any, metrics: list[Any], llm: Any) -> Any:
    try:
        from ragas import evaluate as ragas_evaluate
    except ImportError as err:
        raise RuntimeError("ragas evaluation requires the ragas package") from err
    kwargs = {"metrics": metrics}
    if llm is not None:
        kwargs["llm"] = llm
    return ragas_evaluate(dataset, **kwargs)


def extract_ragas_metrics(result: Any) -> dict[str, float]:
    if isinstance(result, dict):
        return numeric_metrics(result)
    if hasattr(result, "to_pandas"):
        frame = result.to_pandas()
        return numeric_metrics({column: float(frame[column].mean()) for column in frame.columns if is_numeric_series(frame[column])})
    if hasattr(result, "scores"):
        return numeric_metrics(result.scores)
    if hasattr(result, "to_dict"):
        return numeric_metrics(result.to_dict())
    raise RuntimeError("unsupported ragas result shape")


def numeric_metrics(raw: Any) -> dict[str, float]:
    if not isinstance(raw, dict):
        return {}
    metrics: dict[str, float] = {}
    for key, value in raw.items():
        if isinstance(value, (int, float)):
            metrics[str(key)] = float(value)
        elif isinstance(value, list) and value and all(isinstance(item, (int, float)) for item in value):
            metrics[str(key)] = average([float(item) for item in value])
    return metrics


def is_numeric_series(series: Any) -> bool:
    try:
        return bool(series.notna().any()) and all(isinstance(item, (int, float)) for item in series.dropna().tolist())
    except Exception:
        return False


def evaluation_thresholds(profile: dict[str, Any], metric_names: Iterable[str]) -> dict[str, float]:
    thresholds = profile.get("thresholds", {})
    if not isinstance(thresholds, dict):
        thresholds = {}
    resolved: dict[str, float] = {}
    for name in metric_names:
        if name in thresholds:
            resolved[name] = float(thresholds[name])
            continue
        min_key = "min_" + name
        if min_key in profile:
            resolved[name] = float(profile[min_key])
    return resolved


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


def first_string(row: dict[str, Any], *names: str) -> str:
    for name in names:
        value = row.get(name)
        if value is not None:
            return str(value)
    return ""


def first_list(row: dict[str, Any], *names: str) -> list[str]:
    for name in names:
        value = row.get(name)
        if isinstance(value, list):
            return [str(item) for item in value]
        if isinstance(value, str) and value:
            return [value]
    return []


def infer_dataset_mode(rows: list[dict[str, Any]]) -> str:
    return "labeled" if any(first_string(row, "ground_truth", "expected_answer", "reference", "reference_answer") for row in rows) else "judge"


def normalized_profile_value(profile: dict[str, Any], *names: str, default: str) -> str:
    for name in names:
        value = str(profile.get(name, "")).strip().lower()
        if value:
            return value
    return default


def validate_report(report: dict[str, Any]) -> None:
    if "passed" not in report:
        raise RuntimeError("evaluation report must include passed")
    if not isinstance(report.get("metrics", {}), dict):
        raise RuntimeError("evaluation report metrics must be an object")


if __name__ == "__main__":
    main()
