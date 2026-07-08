from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from training_jobs import evaluate, promotion_report


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
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
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
            self.assertEqual(report["score_rows_uri"], "s3://evals/reports/run-1.scores.jsonl")
            self.assertTrue((storage / "evals" / "reports" / "run-1.scores.jsonl").is_file())

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
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
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
            self.assertEqual(report["evaluator_name"], "external")

    def test_promotion_evidence_command_fields_are_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = root / "local_s3"
            model_dir = storage / "models" / "run-promotion"
            model_dir.mkdir(parents=True)
            (model_dir / "adapter_model.safetensors").write_text("weights", encoding="utf-8")
            eval_dataset = storage / "evals" / "run-promotion.jsonl"
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
            evidence = root / "write_evidence.py"
            evidence.write_text(
                "from pathlib import Path\n"
                "import json\n"
                "import sys\n"
                "out = Path(sys.argv[sys.argv.index('--output') + 1])\n"
                "out.write_text(json.dumps({'passed': True, 'report_uri': 's3://evals/reports/deepchecks.html'}), encoding='utf-8')\n",
                encoding="utf-8",
            )
            profile = json.dumps(
                {
                    "dataset_uri": "s3://evals/run-promotion.jsonl",
                    "min_faithfulness": 0.5,
                    "min_answer_relevancy": 0.5,
                    "min_context_precision": 0.5,
                    "promotion": {
                        "deepchecks_command": f"{sys.executable} {evidence} --output {{output}}",
                    },
                }
            )
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "TRAINING_RUN_ID": "run-promotion",
                    "TRAINING_MODEL_URI": "s3://models/run-promotion",
                    "TRAINING_EVALUATION_PROFILE": profile,
                    "TRAINING_EVALUATION_REPORT_URI": "s3://evals/reports/run-promotion.json",
                    "TRAINING_EVALUATION_MANIFEST_URI": "s3://evals/reports/run-promotion.json",
                    "TRAINING_JOB_WORK_DIR": str(root / "work"),
                }
            ):
                evaluate.main()

            report = json.loads((storage / "evals" / "reports" / "run-promotion.json").read_text(encoding="utf-8"))
            self.assertTrue(report["deepchecks_passed"])
            self.assertEqual(report["deepchecks_report_uri"], "s3://evals/reports/deepchecks.html")

    def test_promotion_report_job_persists_evidence_and_deltas(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = root / "local_s3"
            reports = storage / "evals" / "reports"
            reports.mkdir(parents=True)
            candidate = {
                "report_uri": "s3://evals/reports/candidate.json",
                "passed": True,
                "metrics": {"faithfulness": 0.9, "answer_relevancy": 0.8},
                "score_rows_uri": "s3://evals/reports/candidate.scores.jsonl",
            }
            champion = {
                "report_uri": "s3://evals/reports/champion.json",
                "passed": True,
                "metrics": {"faithfulness": 0.7, "answer_relevancy": 0.82},
                "score_rows_uri": "s3://evals/reports/champion.scores.jsonl",
            }
            (reports / "candidate.json").write_text(json.dumps(candidate), encoding="utf-8")
            (reports / "champion.json").write_text(json.dumps(champion), encoding="utf-8")
            (reports / "candidate.scores.jsonl").write_text(json.dumps({"faithfulness": 0.9}) + "\n", encoding="utf-8")
            (reports / "champion.scores.jsonl").write_text(json.dumps({"faithfulness": 0.7}) + "\n", encoding="utf-8")
            evidence = root / "write_evidence.py"
            evidence.write_text(
                "from pathlib import Path\n"
                "import json\n"
                "import sys\n"
                "out = Path(sys.argv[sys.argv.index('--output') + 1])\n"
                "out.write_text(json.dumps({'passed': True, 'report_uri': 's3://evals/reports/evidence.html'}), encoding='utf-8')\n",
                encoding="utf-8",
            )
            profile = json.dumps({"promotion": {"deepchecks_command": f"{sys.executable} {evidence} --output {{output}}"}})
            job_spec = {
                "user_id": "user-1",
                "org_id": "org-1",
                "model_id": "model-1",
                "training_run_id": "training-1",
                "candidate_report_uri": "s3://evals/reports/candidate.json",
                "candidate_metrics_metadata": json.dumps(candidate),
                "champion_report_uri": "s3://evals/reports/champion.json",
                "champion_metrics_metadata": json.dumps(champion),
                "promotion_profile": profile,
                "report_uri": "s3://evals/reports/promotion.json",
                "report_manifest_uri": "s3://evals/reports/promotion.json",
            }
            encoded = base64.urlsafe_b64encode(json.dumps(job_spec).encode("utf-8")).decode("ascii").rstrip("=")
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "TRAINING_JOB_WORK_DIR": str(root / "work"),
                }
            ):
                promotion_report.main(["--job-spec-b64", encoded])

            report = json.loads((reports / "promotion.json").read_text(encoding="utf-8"))
            self.assertTrue(report["deepchecks_passed"])
            self.assertEqual(report["deepchecks_report_uri"], "s3://evals/reports/evidence.html")
            self.assertAlmostEqual(report["deltas"]["faithfulness"], 0.2)
            self.assertAlmostEqual(report["deltas"]["answer_relevancy"], -0.02)

    def test_required_promotion_evidence_fails_loudly_when_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = root / "local_s3"
            model_dir = storage / "models" / "run-required"
            model_dir.mkdir(parents=True)
            (model_dir / "adapter_model.safetensors").write_text("weights", encoding="utf-8")
            profile = json.dumps(
                {
                    "promotion": {
                        "require_deepchecks": True,
                    },
                }
            )
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "TRAINING_RUN_ID": "run-required",
                    "TRAINING_MODEL_URI": "s3://models/run-required",
                    "TRAINING_EVALUATION_PROFILE": profile,
                    "TRAINING_EVALUATION_REPORT_URI": "s3://evals/reports/run-required.json",
                    "TRAINING_EVALUATION_MANIFEST_URI": "s3://evals/reports/run-required.json",
                    "TRAINING_JOB_WORK_DIR": str(root / "work"),
                }
            ):
                with self.assertRaises(RuntimeError):
                    evaluate.main()

    def test_ragas_evaluator_is_selected_by_profile_and_persists_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = root / "local_s3"
            model_dir = storage / "models" / "run-3"
            model_dir.mkdir(parents=True)
            (model_dir / "adapter_model.safetensors").write_text("weights", encoding="utf-8")
            eval_dataset = storage / "evals" / "run-3.jsonl"
            eval_dataset.parent.mkdir(parents=True)
            eval_dataset.write_text(
                json.dumps(
                    {
                        "question": "What is the refund policy?",
                        "answer": "Returns are allowed for thirty days.",
                        "contexts": ["The refund policy allows returns for thirty days."],
                        "ground_truth": "Returns are allowed for thirty days.",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            profile = json.dumps(
                {
                    "evaluator": "ragas",
                    "evaluator_version": "ragas-v1",
                    "metric_suite": "rag",
                    "dataset_uri": "s3://evals/run-3.jsonl",
                    "dataset_mode": "labeled",
                    "judge_provider": "openai",
                    "judge_model": "local-judge",
                    "judge_template_version": "judge-v1",
                    "metrics": ["faithfulness", "answer_relevancy"],
                    "thresholds": {"faithfulness": 0.8, "answer_relevancy": 0.7},
                }
            )
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "TRAINING_RUN_ID": "run-3",
                    "TRAINING_MODEL_URI": "s3://models/run-3",
                    "TRAINING_EVALUATION_PROFILE": profile,
                    "TRAINING_EVALUATION_REPORT_URI": "s3://evals/reports/run-3.json",
                    "TRAINING_EVALUATION_MANIFEST_URI": "s3://evals/reports/run-3.json",
                    "TRAINING_JOB_WORK_DIR": str(root / "work"),
                }
            ), mock.patch.object(evaluate, "ragas_dataset", return_value="dataset") as dataset_mock, mock.patch.object(
                evaluate, "ragas_metrics", return_value=["faithfulness", "answer_relevancy"]
            ), mock.patch.object(evaluate, "ragas_llm", return_value="judge"), mock.patch.object(
                evaluate, "call_ragas_evaluate", return_value={"faithfulness": 0.91, "answer_relevancy": 0.83}
            ):
                evaluate.main()

            dataset_mock.assert_called_once()
            report = json.loads((storage / "evals" / "reports" / "run-3.json").read_text(encoding="utf-8"))
            self.assertTrue(report["passed"])
            self.assertEqual(report["evaluator_name"], "ragas")
            self.assertEqual(report["evaluator_version"], "ragas-v1")
            self.assertEqual(report["metric_suite"], "rag")
            self.assertEqual(report["eval_dataset_uri"], "s3://evals/run-3.jsonl")
            self.assertEqual(report["eval_dataset_mode"], "labeled")
            self.assertEqual(report["judge_provider"], "openai")
            self.assertEqual(report["judge_model"], "local-judge")
            self.assertEqual(report["judge_template_version"], "judge-v1")
            self.assertEqual(report["metrics"]["faithfulness"], 0.91)
            self.assertEqual(report["thresholds"]["answer_relevancy"], 0.7)


if __name__ == "__main__":
    unittest.main()
