from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import shlex
from pathlib import Path
from typing import Any

from training_jobs.config import read_storage_config
from training_jobs.manifest import PromotionReportManifest, parse_profile
from training_jobs.process import run_child
from training_jobs.storage import StorageConfig, read_json_bytes
from training_jobs.storage import write_json_bytes

REQUIRED_ENV_KEYS: tuple[str, ...] = ()

OPTIONAL_ENV_KEYS = (
    "TRAINING_ARTIFACT_BUCKET_REGION",
    "TRAINING_JOB_WORK_DIR",
)

JOB_SPEC_KEYS = (
    "candidate_metrics_metadata",
    "candidate_report_uri",
    "model_id",
    "promotion_profile",
    "report_manifest_uri",
    "report_uri",
    "training_run_id",
    "user_id",
)

OPTIONAL_JOB_SPEC_KEYS = (
    "champion_metrics_metadata",
    "champion_model_id",
    "champion_report_uri",
)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def main(argv: list[str] | None = None) -> None:
    request = read_job_spec(argv)
    storage_config = read_storage_config()
    model_id = request["model_id"]
    work_dir = Path(os.environ.get("TRAINING_JOB_WORK_DIR", f"/tmp/training_jobs/{model_id}/promotion")).resolve()
    work_dir.mkdir(parents=True, exist_ok=True)

    candidate_report = load_report(request["candidate_report_uri"], request["candidate_metrics_metadata"], storage_config)
    profile = parse_profile(request["promotion_profile"])
    promotion = promotion_profile(profile)
    champion_report_uri = request.get("champion_report_uri", "").strip()
    if champion_report_uri:
        champion_report = load_report(
            champion_report_uri,
            request.get("champion_metrics_metadata", "{}"),
            storage_config,
        )
        promotion["champion_report_uri"] = champion_report_uri
        promotion["champion_score_rows_uri"] = str(champion_report.get("score_rows_uri", ""))
    else:
        champion_report = {}
    promotion["candidate_score_rows_uri"] = str(candidate_report.get("score_rows_uri", ""))

    failure_reason = ""
    try:
        evidence_report = add_promotion_evidence(candidate_report, {"promotion": promotion}, work_dir, storage_config)
    except Exception as err:
        evidence_report = candidate_report
        failure_reason = str(err)

    manifest = PromotionReportManifest(
        user_id=request["user_id"],
        model_id=model_id,
        training_run_id=request["training_run_id"],
        promotion_report_uri=request["report_uri"],
        deepchecks_passed=bool(evidence_report.get("deepchecks_passed", False)),
        deepchecks_report_uri=str(evidence_report.get("deepchecks_report_uri", "")),
        evidently_passed=bool(evidence_report.get("evidently_passed", False)),
        evidently_report_uri=str(evidence_report.get("evidently_report_uri", "")),
        deltas=metric_deltas(candidate_report.get("metrics", {}), champion_report.get("metrics", {})),
        failure_reason=failure_reason,
    )
    payload = manifest.to_json()
    write_json_bytes(request["report_uri"], payload, storage_config)
    if request["report_manifest_uri"] != request["report_uri"]:
        write_json_bytes(request["report_manifest_uri"], payload, storage_config)


def read_job_spec(argv: list[str] | None = None) -> dict[str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-spec-b64", required=True)
    parsed = parser.parse_args(argv)
    try:
        raw = base64.urlsafe_b64decode(pad_base64(parsed.job_spec_b64)).decode("utf-8")
        payload = json.loads(raw)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, ValueError) as err:
        raise RuntimeError("promotion job spec is invalid") from err
    if not isinstance(payload, dict):
        raise RuntimeError("promotion job spec must be an object")
    request: dict[str, str] = {}
    for key in JOB_SPEC_KEYS:
        value = str(payload.get(key, "")).strip()
        if not value and key != "promotion_profile":
            raise RuntimeError(f"promotion job spec {key} is required")
        request[key] = value
    for key in OPTIONAL_JOB_SPEC_KEYS:
        request[key] = str(payload.get(key, "")).strip()
    return request


def pad_base64(value: str) -> bytes:
    stripped = value.strip()
    padding = "=" * (-len(stripped) % 4)
    return (stripped + padding).encode("ascii")


def load_report(report_uri: str, metrics_metadata: str, storage_config: StorageConfig) -> dict[str, Any]:
    report = json.loads(read_json_bytes(report_uri, storage_config).decode("utf-8"))
    metadata = json.loads(metrics_metadata or "{}")
    merged = dict(report)
    for key, value in metadata.items():
        if key not in merged or merged.get(key) in ("", {}, []):
            merged[key] = value
    return merged


def add_promotion_evidence(
    report: dict[str, Any],
    profile: dict[str, Any],
    work_dir: Path,
    storage_config: StorageConfig,
) -> dict[str, Any]:
    promotion = promotion_profile(profile)
    enriched = dict(report)

    deepchecks = deepchecks_evidence(enriched, promotion, work_dir, storage_config)
    if deepchecks is not None:
        enriched["deepchecks_passed"] = bool(deepchecks["passed"])
        enriched["deepchecks_report_uri"] = str(deepchecks.get("report_uri", ""))

    evidently = evidently_evidence(enriched, promotion, work_dir, storage_config)
    if evidently is not None:
        enriched["evidently_passed"] = bool(evidently["passed"])
        enriched["evidently_report_uri"] = str(evidently.get("report_uri", ""))

    return enriched


def promotion_profile(profile: dict[str, Any]) -> dict[str, Any]:
    raw = profile.get("promotion", {})
    if not isinstance(raw, dict):
        raw = {}
    merged = dict(raw)
    for key in (
        "require_deepchecks",
        "deepchecks_command",
        "deepchecks_report_uri",
        "require_evidently",
        "evidently_command",
        "evidently_report_uri",
        "champion_report_uri",
        "candidate_score_rows_uri",
        "champion_score_rows_uri",
    ):
        if key in profile:
            merged[key] = profile[key]
    return merged


def deepchecks_evidence(
    report: dict[str, Any],
    profile: dict[str, Any],
    work_dir: Path,
    storage_config: StorageConfig,
) -> dict[str, Any] | None:
    required = bool(profile.get("require_deepchecks", False))
    command = str(profile.get("deepchecks_command", "")).strip()
    if command:
        return run_evidence_command(command, "deepchecks", report, profile, work_dir)
    if not required:
        return None
    return run_native_deepchecks(report, profile, work_dir, storage_config)


def evidently_evidence(
    report: dict[str, Any],
    profile: dict[str, Any],
    work_dir: Path,
    storage_config: StorageConfig,
) -> dict[str, Any] | None:
    required = bool(profile.get("require_evidently", False))
    command = str(profile.get("evidently_command", "")).strip()
    if not str(profile.get("champion_report_uri", "")).strip():
        return None
    if command:
        return run_evidence_command(command, "evidently", report, profile, work_dir)
    if not required:
        return None
    return run_native_evidently(report, profile, work_dir, storage_config)


def run_evidence_command(
    command: str,
    name: str,
    report: dict[str, Any],
    profile: dict[str, Any],
    work_dir: Path,
) -> dict[str, Any]:
    input_path = work_dir / f"{name}_input.json"
    output_path = work_dir / f"{name}_output.json"
    input_path.write_text(
        json.dumps({"report": report, "promotion": profile}, sort_keys=True),
        encoding="utf-8",
    )
    argv = [
        part.replace("{input}", str(input_path)).replace("{output}", str(output_path))
        for part in shlex.split(command)
    ]
    returncode = run_child(argv, cwd=work_dir)
    if returncode != 0:
        raise RuntimeError(f"{name} evidence command exited with status {returncode}")
    if not output_path.is_file():
        raise RuntimeError(f"{name} evidence command did not write {output_path}")
    evidence = json.loads(output_path.read_text(encoding="utf-8"))
    validate_evidence(name, evidence)
    return evidence


def run_native_deepchecks(
    report: dict[str, Any],
    profile: dict[str, Any],
    work_dir: Path,
    storage_config: StorageConfig,
) -> dict[str, Any]:
    try:
        import pandas as pd
        from deepchecks.tabular import Dataset
        from deepchecks.tabular.suites import data_integrity
    except ImportError as err:
        raise RuntimeError("deepchecks evidence requires the deepchecks package") from err

    dataset_uri = str(report.get("eval_dataset_uri", "")).strip()
    if not dataset_uri:
        raise RuntimeError("deepchecks evidence requires eval_dataset_uri")
    rows = read_jsonl(dataset_uri, storage_config)
    if not rows:
        raise RuntimeError("deepchecks evidence requires non-empty eval rows")

    result = data_integrity().run(Dataset(pd.DataFrame(rows)))
    report_path = work_dir / "deepchecks_report.html"
    if hasattr(result, "save_as_html"):
        result.save_as_html(str(report_path))
    passed = bool(result.passed()) if hasattr(result, "passed") else True
    return {
        "passed": passed,
        "report_uri": str(profile.get("deepchecks_report_uri", report_path)),
    }


def run_native_evidently(
    report: dict[str, Any],
    profile: dict[str, Any],
    work_dir: Path,
    storage_config: StorageConfig,
) -> dict[str, Any]:
    try:
        import pandas as pd
        from evidently.metric_preset import DataDriftPreset
        from evidently.report import Report
    except ImportError as err:
        raise RuntimeError("evidently evidence requires the evidently package") from err

    champion_score_rows_uri = str(profile.get("champion_score_rows_uri", "")).strip()
    candidate_score_rows_uri = str(profile.get("candidate_score_rows_uri", report.get("score_rows_uri", ""))).strip()
    if not champion_score_rows_uri or not candidate_score_rows_uri:
        raise RuntimeError("evidently evidence requires champion and candidate score_rows_uri")

    reference = pd.DataFrame(read_jsonl(champion_score_rows_uri, storage_config))
    current = pd.DataFrame(read_jsonl(candidate_score_rows_uri, storage_config))
    if reference.empty or current.empty:
        raise RuntimeError("evidently evidence requires non-empty score rows")
    evidence_report = Report(metrics=[DataDriftPreset()])
    evidence_report.run(reference_data=reference, current_data=current)
    report_path = work_dir / "evidently_report.html"
    evidence_report.save_html(str(report_path))
    summary = evidence_report.as_dict()
    passed = not has_dataset_drift(summary)
    return {
        "passed": passed,
        "report_uri": str(profile.get("evidently_report_uri", report_path)),
    }


def read_jsonl(uri: str, storage_config: StorageConfig) -> list[dict[str, Any]]:
    raw = read_json_bytes(uri, storage_config).decode("utf-8")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped:
            rows.append(json.loads(stripped))
    return rows


def validate_evidence(name: str, evidence: dict[str, Any]) -> None:
    if "passed" not in evidence:
        raise RuntimeError(f"{name} evidence must include passed")


def has_dataset_drift(summary: dict[str, Any]) -> bool:
    for metric in summary.get("metrics", []):
        result = metric.get("result", {})
        if result.get("dataset_drift") is True:
            return True
    return False


def metric_deltas(candidate_metrics: dict[str, Any], champion_metrics: dict[str, Any]) -> dict[str, float]:
    deltas: dict[str, float] = {}
    if not champion_metrics:
        return deltas
    for name, value in candidate_metrics.items():
        champion_value = champion_metrics.get(name)
        if isinstance(value, (int, float)) and isinstance(champion_value, (int, float)):
            deltas[str(name)] = float(value) - float(champion_value)
    return deltas


if __name__ == "__main__":
    main()
