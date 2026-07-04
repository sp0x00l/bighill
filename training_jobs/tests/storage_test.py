from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path

from training_jobs import storage


class FakeS3NotFound(Exception):
    def __init__(self) -> None:
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, str]] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload_file(self, source: str, bucket: str, key: str) -> None:
        self.uploads.append((source, bucket, key))
        self.objects[(bucket, key)] = Path(source).read_bytes()

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def get_object(self, Bucket: str, Key: str) -> dict[str, object]:
        return {"Body": FakeBody(self.objects[(Bucket, Key)])}

    def head_object(self, Bucket: str, Key: str) -> dict[str, object]:
        payload = self.objects.get((Bucket, Key))
        if payload is None:
            raise FakeS3NotFound()
        return {"ContentLength": len(payload)}

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        prefix = str(kwargs["Prefix"])
        contents = [
            {"Key": key, "Size": len(payload)}
            for (candidate_bucket, key), payload in self.objects.items()
            if candidate_bucket == bucket and key.startswith(prefix)
        ]
        return {"Contents": contents}


class FakeBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload


class StorageTests(unittest.TestCase):
    def test_local_dev_s3_uses_repo_local_storage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "adapter"
            source.mkdir()
            (source / "adapter.bin").write_bytes(b"weights")
            config = storage.StorageConfig(
                artifact_bucket_region="eu-west-1",
                local_s3_storage_dir=root / "local_s3",
            )

            artifact = storage.upload_directory(source, "s3://bucket/models/run-1", config)

            self.assertEqual(artifact.uri, "s3://bucket/models/run-1")
            self.assertTrue((root / "local_s3" / "bucket" / "models" / "run-1" / "adapter.bin").is_file())

    def test_non_local_s3_uses_boto3(self) -> None:
        fake_client = FakeS3Client()
        fake_boto3 = types.SimpleNamespace(client=lambda service, region_name=None: fake_client)
        previous_boto3 = sys.modules.get("boto3")
        sys.modules["boto3"] = fake_boto3
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = root / "adapter"
                source.mkdir()
                (source / "adapter.bin").write_bytes(b"weights")
                config = storage.StorageConfig(artifact_bucket_region="eu-west-1")

                artifact = storage.upload_directory(source, "s3://bucket/models/run-1", config)
                storage.write_json_bytes("s3://bucket/models/run-1/artifact.json", b'{"ok":true}', config)
                payload = storage.read_json_bytes("s3://bucket/models/run-1/artifact.json", config)
                info = storage.artifact_info("s3://bucket/models/run-1", config)

            self.assertEqual(fake_client.uploads[0][1:], ("bucket", "models/run-1/adapter.bin"))
            self.assertEqual(payload, b'{"ok":true}')
            self.assertEqual(artifact.size_bytes, len(b"weights"))
            self.assertGreaterEqual(info.size_bytes, len(b"weights"))
        finally:
            if previous_boto3 is None:
                sys.modules.pop("boto3", None)
            else:
                sys.modules["boto3"] = previous_boto3


if __name__ == "__main__":
    unittest.main()
