from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from training_jobs import evaluate


class EnvPatch:
    def __init__(self, values: dict[str, str]):
        self.values = values
        self.previous: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, value in self.values.items():
            self.previous[key] = os.environ.get(key)
            os.environ[key] = value

    def __exit__(self, *_args: object) -> None:
        for key, value in self.previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class EvaluationJobTests(unittest.TestCase):
    def test_builtin_evaluator_uses_rag_style_metrics_and_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = root / "local_s3"
            model_dir = storage / "models" / "run-1"
            model_dir.mkdir(parents=True)
            (model_dir / "adapter_model.safetensors").write_text("weights", encoding="utf-8")
            eval_dataset = storage / "evals" / "run-1.jsonl"
            eval_dataset.parent.mkdir(parents=True)
            eval_dataset.write_text(
                json.dumps(
                    {
                        "answer": "refund policy allows returns",
                        "expected_answer": "refund policy",
                        "contexts": ["the refund policy allows returns for thirty days"],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            profile = json.dumps(
                {
                    "dataset_uri": "s3://evals/run-1.jsonl",
                    "min_faithfulness": 0.5,
                    "min_answer_relevancy": 0.5,
                    "min_context_precision": 0.5,
                }
            )
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "local-dev",
                    "TRAINING_RUN_ID": "run-1",
                    "TRAINING_MODEL_URI": "s3://models/run-1",
                    "TRAINING_EVALUATION_PROFILE": profile,
                    "TRAINING_EVALUATION_REPORT_URI": "s3://evals/reports/run-1.json",
                    "TRAINING_EVALUATION_MANIFEST_URI": "s3://evals/reports/run-1.json",
                    "TRAINING_JOB_WORK_DIR": str(root / "work"),
                }
            ):
                evaluate.main()

            report = json.loads((storage / "evals" / "reports" / "run-1.json").read_text(encoding="utf-8"))
            self.assertTrue(report["passed"])
            self.assertEqual(report["training_run_id"], "run-1")
            self.assertGreaterEqual(report["metrics"]["faithfulness"], 0.5)
            self.assertGreaterEqual(report["metrics"]["answer_relevancy"], 0.5)
            self.assertGreaterEqual(report["metrics"]["context_precision"], 0.5)

    def test_external_evaluator_report_is_validated_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = root / "local_s3"
            model_dir = storage / "models" / "run-2"
            model_dir.mkdir(parents=True)
            (model_dir / "adapter_model.safetensors").write_text("weights", encoding="utf-8")
            evaluator = root / "write_report.py"
            evaluator.write_text(
                "from pathlib import Path\n"
                "import os\n"
                "Path(os.environ['TRAINING_EVALUATION_REPORT_PATH']).write_text('{\"passed\": false, \"metrics\": {\"faithfulness\": 0.1}, \"thresholds\": {\"faithfulness\": 0.8}, \"failure_reason\": \"low faithfulness\"}', encoding='utf-8')\n",
                encoding="utf-8",
            )
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "local-dev",
                    "TRAINING_RUN_ID": "run-2",
                    "TRAINING_MODEL_URI": "s3://models/run-2",
                    "TRAINING_EVALUATION_PROFILE": "{}",
                    "TRAINING_EVALUATION_REPORT_URI": "s3://evals/reports/run-2.json",
                    "TRAINING_EVALUATION_MANIFEST_URI": "s3://evals/reports/run-2.json",
                    "TRAINING_EVALUATION_COMMAND": f"{sys.executable} {evaluator}",
                    "TRAINING_JOB_WORK_DIR": str(root / "work"),
                }
            ):
                evaluate.main()

            report = json.loads((storage / "evals" / "reports" / "run-2.json").read_text(encoding="utf-8"))
            self.assertFalse(report["passed"])
            self.assertEqual(report["failure_reason"], "low faithfulness")


if __name__ == "__main__":
    unittest.main()
