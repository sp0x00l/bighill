from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from training_jobs import train


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


class TrainingJobTests(unittest.TestCase):
    def test_recipe_rewrites_output_dir_for_local_batch_job(self) -> None:
        rendered = train.recipe_with_local_output("base_model: mistral\noutput_dir: s3://models/run\n", Path("/tmp/out"))

        self.assertIn("base_model: mistral", rendered)
        self.assertIn("output_dir: /tmp/out", rendered)
        self.assertNotIn("s3://models/run", rendered)

    def test_training_entrypoint_runs_command_uploads_adapter_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            command = root / "write_adapter.py"
            command.write_text(
                "from pathlib import Path\n"
                "import os\n"
                "out = Path(os.environ['TRAINING_OUTPUT_DIR'])\n"
                "out.mkdir(parents=True, exist_ok=True)\n"
                "(out / 'adapter_model.safetensors').write_text('weights', encoding='utf-8')\n"
                "(out / 'adapter_config.json').write_text('{\"r\":8}', encoding='utf-8')\n",
                encoding="utf-8",
            )
            storage = root / "local_s3"
            work_dir = root / "work"
            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(storage),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "TRAINING_RUN_ID": "run-1",
                    "TRAINING_DATASET_URI": "s3://features/dataset.parquet",
                    "TRAINING_MODEL_NAME": "ranker",
                    "TRAINING_MODEL_VERSION": "v1",
                    "TRAINING_BASE_MODEL": "mistral",
                    "TRAINING_MODEL_URI": "s3://models/run-1",
                    "TRAINING_ARTIFACT_MANIFEST_URI": "s3://models/run-1/artifact.json",
                    "TRAINING_RECIPE_YAML": "base_model: mistral\noutput_dir: s3://models/run-1\n",
                    "TRAINING_RECIPE_HASH": "abc123",
                    "TRAINING_SERVING_TARGET": "vllm-local",
                    "TRAINING_SERVING_MODEL": "rag-adapter-v1",
                    "TRAINING_SERVING_LOAD_STATUS": "LOADED",
                    "TRAINING_AXOLOTL_COMMAND": f"{sys.executable} {command}",
                    "TRAINING_JOB_WORK_DIR": str(work_dir),
                }
            ):
                train.main()

            self.assertTrue((storage / "models" / "run-1" / "adapter_model.safetensors").is_file())
            manifest = json.loads((storage / "models" / "run-1" / "artifact.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["training_run_id"], "run-1")
            self.assertEqual(manifest["model_uri"], "s3://models/run-1")
            self.assertEqual(manifest["artifact_format"], "HF_PEFT_ADAPTER")
            self.assertEqual(manifest["adapter_uri"], "s3://models/run-1")
            self.assertEqual(manifest["serving_target"], "vllm-local")
            self.assertEqual(manifest["serving_model"], "rag-adapter-v1")
            self.assertEqual(manifest["serving_load_status"], "LOADED")
            self.assertEqual(manifest["recipe_hash"], "abc123")
            self.assertGreater(manifest["artifact_size_bytes"], 0)
            self.assertTrue(manifest["artifact_checksum"].startswith("sha256:"))


if __name__ == "__main__":
    unittest.main()
