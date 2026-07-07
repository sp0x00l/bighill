#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


BASE_URL = env("POLARIS_BASE_URL", "http://localhost:8181").rstrip("/")
TOKEN_URL = env("POLARIS_TOKEN_URL", BASE_URL + "/api/catalog/v1/oauth/tokens")
ROOT_CLIENT_ID = env("POLARIS_ROOT_CLIENT_ID", "root")
ROOT_CLIENT_SECRET = env("POLARIS_ROOT_CLIENT_SECRET", "s3cr3t")
CATALOG = env("POLARIS_CATALOG", "bighill")
WAREHOUSE = env("POLARIS_WAREHOUSE", "s3://bighill-mlops-lakehouse/")
STORAGE_ENDPOINT = env("POLARIS_STORAGE_ENDPOINT", "http://polaris-object-store:9000")
STORAGE_REGION = env("POLARIS_STORAGE_REGION", "eu-west-1")
STORAGE_PATH_STYLE = env("POLARIS_STORAGE_PATH_STYLE", "true").lower() in {"1", "true", "yes"}
STORAGE_ROLE_ARN = env("POLARIS_STORAGE_ROLE_ARN", "arn:aws:iam::000000000000:role/bighill-polaris")
STORAGE_EXTERNAL_ID = env("POLARIS_STORAGE_EXTERNAL_ID", "bighill-local-polaris")
SERVICE_PRINCIPAL = env("POLARIS_SERVICE_PRINCIPAL", "bighill-services")
PRINCIPAL_ROLE = env("POLARIS_PRINCIPAL_ROLE", "bighill-services")
CATALOG_ROLE = env("POLARIS_CATALOG_ROLE", "bighill-catalog-writer")
EXISTING_SERVICE_CREDENTIAL = env("POLARIS_SERVICE_CREDENTIAL", "")


def main() -> None:
    token = root_token()
    wait_for_polaris(token)
    ensure_catalog(token)
    credentials = ensure_principal(token)
    ensure_principal_role(token)
    ensure_catalog_role(token)
    assign_principal_role(token)
    assign_catalog_role(token)
    add_catalog_grants(token)
    print(json.dumps(credentials, sort_keys=True))


def root_token() -> str:
    payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": ROOT_CLIENT_ID,
            "client_secret": ROOT_CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        }
    ).encode("utf-8")
    req = urllib.request.Request(TOKEN_URL, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    token = str(body.get("access_token", "")).strip()
    if not token:
        raise RuntimeError("Polaris token response did not include access_token")
    return token


def wait_for_polaris(token: str) -> None:
    deadline = time.monotonic() + 120
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request("GET", "/api/management/v1/catalogs", token, accept=(200,))
            return
        except Exception as err:
            last_error = err
            time.sleep(2)
    raise RuntimeError(f"Polaris management API did not become ready: {last_error}")


def ensure_catalog(token: str) -> None:
    path = f"/api/management/v1/catalogs/{quote(CATALOG)}"
    status, body = request("GET", path, token, accept=(200, 404))
    if status == 200:
        reconcile_catalog(token, body)
        return
    request("POST", "/api/management/v1/catalogs", token, create_catalog_payload(), accept=(200, 201, 409))


def create_catalog_payload() -> dict[str, Any]:
    return {
        "catalog": {
            "type": "INTERNAL",
            "name": CATALOG,
            "properties": catalog_properties(),
            "storageConfigInfo": storage_config_payload(),
        }
    }


def catalog_properties() -> dict[str, str]:
    properties = {
        "default-base-location": WAREHOUSE,
        "s3.path-style-access": "true" if STORAGE_PATH_STYLE else "false",
    }
    if STORAGE_ENDPOINT:
        properties["s3.endpoint"] = STORAGE_ENDPOINT
    return properties


def storage_config_payload() -> dict[str, Any]:
    return {
        "storageType": "S3",
        "allowedLocations": [WAREHOUSE],
        "roleArn": STORAGE_ROLE_ARN,
        "externalId": STORAGE_EXTERNAL_ID,
        "region": STORAGE_REGION,
    }


def reconcile_catalog(token: str, body: Any) -> None:
    catalog = body.get("catalog", body) if isinstance(body, dict) else {}
    if not isinstance(catalog, dict):
        raise RuntimeError("Polaris catalog response did not include a catalog object")
    entity_version = catalog.get("entityVersion")
    payload: dict[str, Any] = {
        "properties": catalog_properties(),
        "storageConfigInfo": storage_config_payload(),
    }
    if entity_version is not None:
        payload["currentEntityVersion"] = entity_version
    request("PUT", f"/api/management/v1/catalogs/{quote(CATALOG)}", token, payload, accept=(200, 201, 409))


def ensure_principal(token: str) -> dict[str, str]:
    status, _ = request("GET", f"/api/management/v1/principals/{quote(SERVICE_PRINCIPAL)}", token, accept=(200, 404))
    if status == 200:
        if EXISTING_SERVICE_CREDENTIAL:
            return credential_payload(EXISTING_SERVICE_CREDENTIAL)
        return credential_payload(f"{ROOT_CLIENT_ID}:{ROOT_CLIENT_SECRET}")

    payload = {
        "principal": {
            "name": SERVICE_PRINCIPAL,
            "properties": {"service": "bighill"},
        },
        "credentialRotationRequired": False,
    }
    _, body = request("POST", "/api/management/v1/principals", token, payload, accept=(200, 201, 409))
    credentials = body.get("credentials") if isinstance(body, dict) else None
    if isinstance(credentials, dict):
        client_id = str(credentials.get("clientId", "")).strip()
        client_secret = str(credentials.get("clientSecret", "")).strip()
        if client_id and client_secret:
            return {
                "client_id": client_id,
                "client_secret": client_secret,
                "credential": f"{client_id}:{client_secret}",
            }
    if EXISTING_SERVICE_CREDENTIAL:
        return credential_payload(EXISTING_SERVICE_CREDENTIAL)
    return credential_payload(f"{ROOT_CLIENT_ID}:{ROOT_CLIENT_SECRET}")


def credential_payload(credential: str) -> dict[str, str]:
    client_id, _, client_secret = credential.partition(":")
    if not client_id or not client_secret:
        raise RuntimeError("Polaris service credential must use client_id:client_secret")
    return {"client_id": client_id, "client_secret": client_secret, "credential": credential}


def ensure_principal_role(token: str) -> None:
    status, _ = request("GET", f"/api/management/v1/principal-roles/{quote(PRINCIPAL_ROLE)}", token, accept=(200, 404))
    if status == 200:
        return
    request("POST", "/api/management/v1/principal-roles", token, {"principalRole": {"name": PRINCIPAL_ROLE}}, accept=(200, 201, 409))


def ensure_catalog_role(token: str) -> None:
    path = f"/api/management/v1/catalogs/{quote(CATALOG)}/catalog-roles/{quote(CATALOG_ROLE)}"
    status, _ = request("GET", path, token, accept=(200, 404))
    if status == 200:
        return
    request("POST", f"/api/management/v1/catalogs/{quote(CATALOG)}/catalog-roles", token, {"catalogRole": {"name": CATALOG_ROLE}}, accept=(200, 201, 409))


def assign_principal_role(token: str) -> None:
    request("PUT", f"/api/management/v1/principals/{quote(SERVICE_PRINCIPAL)}/principal-roles", token, {"principalRole": {"name": PRINCIPAL_ROLE}}, accept=(200, 201, 409))


def assign_catalog_role(token: str) -> None:
    request("PUT", f"/api/management/v1/principal-roles/{quote(PRINCIPAL_ROLE)}/catalog-roles/{quote(CATALOG)}", token, {"catalogRole": {"name": CATALOG_ROLE}}, accept=(200, 201, 409))


def add_catalog_grants(token: str) -> None:
    privileges = [
        "CATALOG_MANAGE_ACCESS",
        "CATALOG_MANAGE_CONTENT",
        "CATALOG_MANAGE_METADATA",
        "CATALOG_READ_PROPERTIES",
        "CATALOG_WRITE_PROPERTIES",
        "NAMESPACE_CREATE",
        "NAMESPACE_LIST",
        "NAMESPACE_FULL_METADATA",
        "TABLE_CREATE",
        "TABLE_LIST",
        "TABLE_READ_DATA",
        "TABLE_WRITE_DATA",
        "TABLE_FULL_METADATA",
        "TABLE_DROP",
    ]
    path = f"/api/management/v1/catalogs/{quote(CATALOG)}/catalog-roles/{quote(CATALOG_ROLE)}/grants"
    for privilege in privileges:
        request("PUT", path, token, {"grant": {"type": "catalog", "privilege": privilege}}, accept=(200, 201, 409))


def request(method: str, path: str, token: str, payload: Any | None = None, accept: tuple[int, ...] = (200,)) -> tuple[int, Any]:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(BASE_URL + path, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = decode_body(resp.read())
            if resp.status not in accept:
                raise RuntimeError(f"{method} {path} returned {resp.status}: {body}")
            return resp.status, body
    except urllib.error.HTTPError as err:
        body = decode_body(err.read())
        if err.code in accept:
            return err.code, body
        raise RuntimeError(f"{method} {path} returned {err.code}: {body}") from err


def decode_body(body: bytes) -> Any:
    if not body:
        return {}
    text = body.decode("utf-8").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Polaris bootstrap failed: {exc}", file=sys.stderr)
        sys.exit(1)
