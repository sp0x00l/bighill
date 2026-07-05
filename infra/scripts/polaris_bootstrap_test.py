from __future__ import annotations

import importlib.util
import pathlib
import unittest
from typing import Any


def load_bootstrap_module():
    path = pathlib.Path(__file__).with_name("polaris_bootstrap.py")
    spec = importlib.util.spec_from_file_location("polaris_bootstrap", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PolarisBootstrapTest(unittest.TestCase):
    def test_ensure_catalog_creates_s3_catalog_with_storage_identity(self) -> None:
        bootstrap = load_bootstrap_module()
        calls: list[tuple[str, str, Any | None, tuple[int, ...]]] = []

        def fake_request(method: str, path: str, token: str, payload: Any | None = None, accept: tuple[int, ...] = (200,)):
            calls.append((method, path, payload, accept))
            if method == "GET":
                return 404, {}
            return 201, {}

        bootstrap.request = fake_request

        bootstrap.ensure_catalog("token")

        self.assertEqual(calls[0][0:2], ("GET", "/api/management/v1/catalogs/bighill"))
        self.assertEqual(calls[1][0:2], ("POST", "/api/management/v1/catalogs"))
        catalog = calls[1][2]["catalog"]
        storage = catalog["storageConfigInfo"]
        self.assertEqual(catalog["name"], "bighill")
        self.assertEqual(catalog["type"], "INTERNAL")
        properties = catalog["properties"]
        self.assertEqual(properties["default-base-location"], "s3://bighill-mlops-lakehouse/")
        self.assertEqual(properties["s3.endpoint"], "http://polaris-object-store:9000")
        self.assertEqual(properties["s3.path-style-access"], "true")
        self.assertEqual(storage["storageType"], "S3")
        self.assertEqual(storage["allowedLocations"], ["s3://bighill-mlops-lakehouse/"])
        self.assertEqual(storage["roleArn"], "arn:aws:iam::000000000000:role/bighill-polaris")
        self.assertEqual(storage["externalId"], "bighill-local-polaris")
        self.assertEqual(storage["region"], "eu-west-1")

    def test_ensure_catalog_reconciles_storage_config_when_catalog_exists(self) -> None:
        bootstrap = load_bootstrap_module()
        calls: list[tuple[str, str, Any | None]] = []

        def fake_request(method: str, path: str, token: str, payload: Any | None = None, accept: tuple[int, ...] = (200,)):
            calls.append((method, path, payload))
            if method == "GET":
                return 200, {"catalog": {"entityVersion": 12, "storageConfigInfo": {"storageType": "S3"}}}
            return 200, {}

        bootstrap.request = fake_request

        bootstrap.ensure_catalog("token")

        self.assertEqual(calls[0][0:2], ("GET", "/api/management/v1/catalogs/bighill"))
        self.assertEqual(calls[1][0:2], ("PUT", "/api/management/v1/catalogs/bighill"))
        payload = calls[1][2]
        self.assertEqual(payload["currentEntityVersion"], 12)
        self.assertEqual(payload["properties"]["default-base-location"], "s3://bighill-mlops-lakehouse/")
        self.assertEqual(payload["properties"]["s3.endpoint"], "http://polaris-object-store:9000")
        self.assertEqual(payload["properties"]["s3.path-style-access"], "true")
        storage = payload["storageConfigInfo"]
        self.assertEqual(storage["roleArn"], "arn:aws:iam::000000000000:role/bighill-polaris")
        self.assertEqual(storage["region"], "eu-west-1")


if __name__ == "__main__":
    unittest.main()
