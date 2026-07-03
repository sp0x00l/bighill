from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any, get_args, get_origin, get_type_hints

from training_jobs import evaluate, manifest, train


def contract() -> dict[str, list[str] | str]:
    path = Path(__file__).resolve().parents[1] / "contracts" / "training_job_contract.json"
    return json.loads(path.read_text(encoding="utf-8"))


class TrainingJobContractTests(unittest.TestCase):
    def test_python_env_and_manifest_contract_matches_fixture(self) -> None:
        spec = contract()

        self.assertEqual(list(train.REQUIRED_ENV_KEYS), spec["python_training_required_env_keys"])
        self.assertEqual(list(train.OPTIONAL_ENV_KEYS), spec["python_training_optional_env_keys"])
        self.assertEqual(list(evaluate.REQUIRED_ENV_KEYS), spec["python_evaluation_required_env_keys"])
        self.assertEqual(list(evaluate.OPTIONAL_ENV_KEYS), spec["python_evaluation_optional_env_keys"])
        self.assertEqual(sorted(manifest.TrainingArtifactManifest.__dataclass_fields__.keys()), spec["training_manifest_keys"])
        self.assertEqual(sorted(manifest.EvaluationReportManifest.__dataclass_fields__.keys()), spec["evaluation_manifest_keys"])
        self.assertEqual(field_types(manifest.TrainingArtifactManifest), spec["training_manifest_field_types"])
        self.assertEqual(field_types(manifest.EvaluationReportManifest), spec["evaluation_manifest_field_types"])
        for key in list(train.REQUIRED_ENV_KEYS) + list(train.OPTIONAL_ENV_KEYS) + list(evaluate.REQUIRED_ENV_KEYS) + list(evaluate.OPTIONAL_ENV_KEYS):
            self.assertIn(key, spec["env_key_contract"])
            self.assertEqual(spec["env_key_contract"][key]["type"], "string")
            self.assertTrue(spec["env_key_contract"][key]["direction"])


def field_types(cls: type[Any]) -> dict[str, str]:
    hints = get_type_hints(cls)
    return {name: contract_type(hints[name]) for name in sorted(cls.__dataclass_fields__.keys())}


def contract_type(annotation: Any) -> str:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is dict and args == (str, float):
        return "map_string_float64"
    if annotation is str:
        return "string"
    if annotation is int:
        return "int64"
    if annotation is bool:
        return "bool"
    return str(annotation)


if __name__ == "__main__":
    unittest.main()
