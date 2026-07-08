from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class TrainingArtifactManifest:
    training_run_id: str
    model_uri: str
    model_name: str
    model_version: str
    base_model: str
    artifact_format: str
    artifact_checksum: str
    artifact_size_bytes: int
    adapter_uri: str = ""
    serving_target: str = ""
    serving_model: str = ""
    serving_load_status: str = ""
    recipe_hash: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class EvaluationReportManifest:
    training_run_id: str
    report_uri: str
    passed: bool
    metrics: dict[str, float] = field(default_factory=dict)
    thresholds: dict[str, float] = field(default_factory=dict)
    evaluator_name: str = ""
    evaluator_version: str = ""
    metric_suite: str = ""
    eval_dataset_uri: str = ""
    eval_dataset_mode: str = ""
    judge_provider: str = ""
    judge_model: str = ""
    judge_template_version: str = ""
    deepchecks_passed: bool = False
    deepchecks_report_uri: str = ""
    evidently_passed: bool = False
    evidently_report_uri: str = ""
    score_rows_uri: str = ""
    failure_reason: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class PromotionReportManifest:
    user_id: str
    org_id: str
    model_id: str
    training_run_id: str
    promotion_report_uri: str
    deepchecks_passed: bool = False
    deepchecks_report_uri: str = ""
    evidently_passed: bool = False
    evidently_report_uri: str = ""
    deltas: dict[str, float] = field(default_factory=dict)
    failure_reason: str = ""

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":")).encode("utf-8")


def parse_profile(raw: str) -> dict[str, Any]:
    if not raw:
        return {}
    stripped = raw.strip()
    if not stripped:
        return {}
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return {"name": stripped}
