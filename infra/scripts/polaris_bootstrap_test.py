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
        self.assertEqual(catalog["properties"]["default-base-location"], "s3://bighill-mlops-lakehouse/")
        self.assertEqual(storage["storageType"], "S3")
        self.assertEqual(storage["allowedLocations"], ["s3://bighill-mlops-lakehouse/"])
        self.assertEqual(storage["roleArn"], "arn:aws:iam::000000000000:role/bighill-polaris")
        self.assertEqual(storage["userArn"], "arn:aws:iam::000000000000:user/bighill-polaris")
        self.assertEqual(storage["externalId"], "bighill-local-polaris")
        self.assertEqual(storage["endpoint"], "http://polaris-object-store:9000")
        self.assertTrue(storage["pathStyleAccess"])
        self.assertTrue(storage["stsUnavailable"])
        self.assertTrue(storage["kmsUnavailable"])

    def test_ensure_catalog_does_not_create_when_catalog_exists(self) -> None:
        bootstrap = load_bootstrap_module()
        calls: list[tuple[str, str]] = []

        def fake_request(method: str, path: str, token: str, payload: Any | None = None, accept: tuple[int, ...] = (200,)):
            calls.append((method, path))
            return 200, {}

        bootstrap.request = fake_request

        bootstrap.ensure_catalog("token")

        self.assertEqual(calls, [("GET", "/api/management/v1/catalogs/bighill")])


if __name__ == "__main__":
    unittest.main()
