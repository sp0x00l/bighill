from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from training_jobs import model_onboard


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


class ModelOnboardTests(unittest.TestCase):
    def test_main_downloads_valid_snapshot_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_s3 = root / "local_s3"

            def fake_snapshot_download(*, repo_id: str, revision: str, token: str, local_dir: Path) -> str:
                self.assertEqual(repo_id, "org/model")
                self.assertEqual(revision, "abc123")
                self.assertEqual(token, "hf_test")
                local_dir.mkdir(parents=True)
                (local_dir / "config.json").write_text("{}", encoding="utf-8")
                (local_dir / "model.safetensors").write_bytes(b"weights")
                return str(local_dir)

            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(local_s3),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "local-dev",
                    "INGESTION_SERVICE_MODEL_RESOURCE_ID": "11111111-1111-1111-1111-111111111111",
                    "INGESTION_SERVICE_MODEL_NAME": "llama-test",
                    "INGESTION_SERVICE_MODEL_VERSION": "1",
                    "INGESTION_SERVICE_MODEL_BASE_MODEL": "org/model",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE": "BASE_MODEL",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT": "HF_MODEL",
                    "INGESTION_SERVICE_HUGGINGFACE_REPO_ID": "org/model",
                    "INGESTION_SERVICE_HUGGINGFACE_REVISION": "abc123",
                    "INGESTION_SERVICE_HUGGINGFACE_TOKEN": "hf_test",
                    "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI": "s3://bucket/models/huggingface",
                }
            ), mock.patch.object(
                model_onboard, "resolve_commit_sha", return_value="resolvedabc123"
            ), mock.patch.object(
                model_onboard, "snapshot_download", side_effect=fake_snapshot_download
            ), mock.patch("builtins.print") as printed:
                model_onboard.main()

            payload = json.loads(printed.call_args.args[0])
            self.assertEqual(payload["hf_repo_id"], "org/model")
            self.assertEqual(payload["hf_commit_sha"], "resolvedabc123")
            self.assertEqual(payload["artifact_type"], "BASE_MODEL")
            self.assertTrue((local_s3 / "bucket" / "models" / "huggingface" / payload["resource_id"] / "snapshot" / "config.json").is_file())
            manifest_path = local_s3 / "bucket" / "models" / "huggingface" / payload["resource_id"] / "manifest.json"
            self.assertTrue(manifest_path.is_file())

    def test_validate_snapshot_rejects_missing_weights(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp)
            (snapshot / "config.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "safetensors"):
                model_onboard.validate_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
