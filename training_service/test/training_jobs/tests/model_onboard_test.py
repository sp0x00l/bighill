from __future__ import annotations

import io
import json
import os
import hashlib
import tempfile
import types
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
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
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
                model_onboard, "validate_login", return_value="test-user"
            ) as validate_login, mock.patch.object(
                model_onboard, "resolve_commit_sha", return_value="resolvedabc123"
            ), mock.patch.object(
                model_onboard, "snapshot_download", side_effect=fake_snapshot_download
            ), mock.patch("builtins.print") as printed:
                model_onboard.main()

            validate_login.assert_called_once_with(token="hf_test")
            payload = json.loads(printed.call_args.args[0])
            self.assertEqual(payload["hf_repo_id"], "org/model")
            self.assertEqual(payload["hf_commit_sha"], "resolvedabc123")
            self.assertEqual(payload["artifact_type"], "BASE_MODEL")
            self.assertTrue((local_s3 / "bucket" / "models" / "huggingface" / payload["resource_id"] / "snapshot" / "config.json").is_file())
            manifest_path = local_s3 / "bucket" / "models" / "huggingface" / payload["resource_id"] / "manifest.json"
            self.assertTrue(manifest_path.is_file())

    def test_main_downloads_exact_gguf_file_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_s3 = root / "local_s3"
            gguf_bytes = b"real gguf bytes"
            gguf_sha = hashlib.sha256(gguf_bytes).hexdigest()

            def fake_file_download(*, repo_id: str, revision: str, token: str, filename: str, local_dir: Path) -> str:
                self.assertEqual(repo_id, "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF")
                self.assertEqual(revision, "main")
                self.assertEqual(token, "hf_test")
                self.assertEqual(filename, "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf")
                target = local_dir / filename
                target.parent.mkdir(parents=True)
                target.write_bytes(gguf_bytes)
                return str(target)

            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(local_s3),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "INGESTION_SERVICE_MODEL_RESOURCE_ID": "44444444-4444-4444-4444-444444444444",
                    "INGESTION_SERVICE_MODEL_NAME": "llama-gguf",
                    "INGESTION_SERVICE_MODEL_VERSION": "1",
                    "INGESTION_SERVICE_MODEL_BASE_MODEL": "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE": "BASE_MODEL",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT": "",
                    "INGESTION_SERVICE_HUGGINGFACE_REPO_ID": "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
                    "INGESTION_SERVICE_HUGGINGFACE_REVISION": "main",
                    "INGESTION_SERVICE_HUGGINGFACE_FILE": "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
                    "INGESTION_SERVICE_HUGGINGFACE_TOKEN": "hf_test",
                    "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI": "s3://bucket/models/huggingface",
                }
            ), mock.patch.object(model_onboard, "validate_login", return_value="test-user"), mock.patch.object(
                model_onboard, "resolve_commit_sha", return_value="resolvedabc123"
            ), mock.patch.object(
                model_onboard, "resolve_file_sha256", return_value=gguf_sha
            ), mock.patch.object(
                model_onboard, "file_download", side_effect=fake_file_download
            ), mock.patch.object(model_onboard, "validate_gguf_file") as validate_gguf, mock.patch("builtins.print") as printed:
                model_onboard.main()

            payload = json.loads(printed.call_args.args[0])
            self.assertEqual(payload["artifact_type"], "BASE_MODEL")
            self.assertEqual(payload["artifact_format"], "GGUF_MODEL")
            self.assertEqual(payload["hf_file"], "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf")
            self.assertEqual(payload["storage_location"], "s3://bucket/models/huggingface/44444444-4444-4444-4444-444444444444/Meta-Llama-3-8B-Instruct.Q4_K_M.gguf")
            self.assertTrue((local_s3 / "bucket" / "models" / "huggingface" / payload["resource_id"] / "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf").is_file())
            validate_gguf.assert_called_once()

    def test_main_rejects_exact_gguf_file_with_explicit_non_gguf_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(root / "local_s3"),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "INGESTION_SERVICE_MODEL_RESOURCE_ID": "55555555-5555-5555-5555-555555555555",
                    "INGESTION_SERVICE_MODEL_NAME": "llama-gguf",
                    "INGESTION_SERVICE_MODEL_VERSION": "1",
                    "INGESTION_SERVICE_MODEL_BASE_MODEL": "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE": "BASE_MODEL",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT": "HF_MODEL",
                    "INGESTION_SERVICE_HUGGINGFACE_REPO_ID": "QuantFactory/Meta-Llama-3-8B-Instruct-GGUF",
                    "INGESTION_SERVICE_HUGGINGFACE_REVISION": "main",
                    "INGESTION_SERVICE_HUGGINGFACE_FILE": "Meta-Llama-3-8B-Instruct.Q4_K_M.gguf",
                    "INGESTION_SERVICE_HUGGINGFACE_TOKEN": "hf_test",
                    "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI": "s3://bucket/models/huggingface",
                }
            ), mock.patch.object(model_onboard, "validate_login") as validate_login, mock.patch.object(
                model_onboard, "file_download"
            ) as download, mock.patch("sys.stderr", io.StringIO()) as stderr:
                with self.assertRaises(SystemExit) as raised:
                    model_onboard.main()

            self.assertEqual(raised.exception.code, 1)
            self.assertIn("GGUF Hugging Face files must use GGUF_MODEL or GGUF_LORA_ADAPTER", stderr.getvalue())
            validate_login.assert_not_called()
            download.assert_not_called()

    def test_validate_snapshot_rejects_missing_weights(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp)
            (snapshot / "config.json").write_text("{}", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "safetensors"):
                model_onboard.validate_snapshot(snapshot)

    def test_validate_login_uses_hugging_face_identity(self) -> None:
        api_cls = mock.Mock()
        api_cls.return_value.whoami.return_value = {"name": "hf-user"}

        with mock.patch.dict("sys.modules", {"huggingface_hub": types.SimpleNamespace(HfApi=api_cls)}):
            login = model_onboard.validate_login(token="hf_test")

        self.assertEqual(login, "hf-user")
        api_cls.return_value.whoami.assert_called_once_with(token="hf_test")

    def test_validate_login_rejects_empty_identity(self) -> None:
        api_cls = mock.Mock()
        api_cls.return_value.whoami.return_value = {}

        with mock.patch.dict("sys.modules", {"huggingface_hub": types.SimpleNamespace(HfApi=api_cls)}):
            with self.assertRaisesRegex(RuntimeError, "login validation failed"):
                model_onboard.validate_login(token="hf_test")

    def test_main_stops_before_download_when_login_validation_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            local_s3 = root / "local_s3"

            with EnvPatch(
                {
                    "BIGHILL_LOCAL_S3_STORAGE_DIR": str(local_s3),
                    "TRAINING_ARTIFACT_BUCKET_REGION": "eu-west-1",
                    "INGESTION_SERVICE_MODEL_RESOURCE_ID": "33333333-3333-3333-3333-333333333333",
                    "INGESTION_SERVICE_MODEL_NAME": "llama-test",
                    "INGESTION_SERVICE_MODEL_VERSION": "1",
                    "INGESTION_SERVICE_MODEL_BASE_MODEL": "org/model",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_TYPE": "BASE_MODEL",
                    "INGESTION_SERVICE_MODEL_ARTIFACT_FORMAT": "HF_MODEL",
                    "INGESTION_SERVICE_HUGGINGFACE_REPO_ID": "org/model",
                    "INGESTION_SERVICE_HUGGINGFACE_REVISION": "main",
                    "INGESTION_SERVICE_HUGGINGFACE_TOKEN": "hf_bad",
                    "INGESTION_SERVICE_HUGGINGFACE_OUTPUT_URI": "s3://bucket/models/huggingface",
                }
            ), mock.patch.object(
                model_onboard, "validate_login", side_effect=RuntimeError("bad Hugging Face token")
            ) as validate_login, mock.patch.object(
                model_onboard, "resolve_commit_sha"
            ) as resolve_commit, mock.patch.object(
                model_onboard, "snapshot_download"
            ) as download, mock.patch("sys.stderr", io.StringIO()) as stderr:
                with self.assertRaises(SystemExit) as raised:
                    model_onboard.main()

            self.assertEqual(raised.exception.code, 1)
            self.assertIn("bad Hugging Face token", stderr.getvalue())
            self.assertIn("ONBOARDING_FAILED", stderr.getvalue())
            validate_login.assert_called_once_with(token="hf_bad")
            resolve_commit.assert_not_called()
            download.assert_not_called()


if __name__ == "__main__":
    unittest.main()
