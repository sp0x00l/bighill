from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True)
class ArtifactInfo:
    uri: str
    checksum: str
    size_bytes: int


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "shared_lib").exists():
            return candidate
    return current


def local_s3_storage_dir() -> Path:
    configured = os.environ.get("BIGHILL_LOCAL_S3_STORAGE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return find_repo_root() / "tmp" / "local_s3_storage"


def artifact_bucket_region() -> str:
    return (
        os.environ.get("TRAINING_ARTIFACT_BUCKET_REGION")
        or os.environ.get("BIGHILL_ARTIFACT_BUCKET_REGION")
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "local-dev"
    ).strip()


def use_local_s3() -> bool:
    return artifact_bucket_region() == "local-dev"


def uri_to_local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(parsed.path).expanduser().resolve()
    if parsed.scheme == "":
        return Path(uri).expanduser().resolve()
    if parsed.scheme == "s3":
        if not use_local_s3():
            raise ValueError("s3 uri is not local-dev backed")
        return (local_s3_storage_dir() / parsed.netloc / parsed.path.lstrip("/")).resolve()
    raise ValueError(f"unsupported storage uri scheme {parsed.scheme!r}")


def upload_directory(source_dir: Path, destination_uri: str) -> ArtifactInfo:
    source_dir = source_dir.resolve()
    if not source_dir.is_dir():
        raise FileNotFoundError(f"artifact directory does not exist: {source_dir}")
    if is_remote_s3(destination_uri):
        checksum, size = directory_digest(source_dir)
        upload_directory_s3(source_dir, destination_uri)
        return ArtifactInfo(uri=destination_uri, checksum=checksum, size_bytes=size)
    destination = uri_to_local_path(destination_uri)
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, destination)
    return artifact_info(destination_uri)


def write_json_bytes(destination_uri: str, payload: bytes) -> None:
    if is_remote_s3(destination_uri):
        parsed = parse_s3_uri(destination_uri)
        s3_client().put_object(Bucket=parsed[0], Key=parsed[1], Body=payload)
        return
    destination = uri_to_local_path(destination_uri)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)


def read_json_bytes(uri: str) -> bytes:
    if is_remote_s3(uri):
        parsed = parse_s3_uri(uri)
        response = s3_client().get_object(Bucket=parsed[0], Key=parsed[1])
        return response["Body"].read()
    return uri_to_local_path(uri).read_bytes()


def artifact_info(uri: str) -> ArtifactInfo:
    if is_remote_s3(uri):
        checksum, size = remote_s3_digest(uri)
        return ArtifactInfo(uri=uri, checksum=checksum, size_bytes=size)
    path = uri_to_local_path(uri)
    if path.is_file():
        checksum, size = file_digest(path)
        return ArtifactInfo(uri=uri, checksum=checksum, size_bytes=size)
    if path.is_dir():
        checksum, size = directory_digest(path)
        return ArtifactInfo(uri=uri, checksum=checksum, size_bytes=size)
    raise FileNotFoundError(f"artifact not found: {uri}")


def is_remote_s3(uri: str) -> bool:
    return urlparse(uri).scheme == "s3" and not use_local_s3()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"invalid s3 uri: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def s3_client():
    import boto3

    return boto3.client("s3", region_name=artifact_bucket_region())


def upload_directory_s3(source_dir: Path, destination_uri: str) -> None:
    bucket, prefix = parse_s3_uri(destination_uri)
    client = s3_client()
    for child in sorted(p for p in source_dir.rglob("*") if p.is_file()):
        key = f"{prefix.rstrip('/')}/{child.relative_to(source_dir).as_posix()}"
        client.upload_file(str(child), bucket, key)


def remote_s3_digest(uri: str) -> tuple[str, int]:
    bucket, key = parse_s3_uri(uri)
    client = s3_client()
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        size = int(head.get("ContentLength", 0))
        checksum = head.get("ChecksumSHA256")
        return ("sha256:" + checksum if checksum else "", size)
    except Exception as err:
        if not is_s3_not_found(err):
            raise
        prefix = key.rstrip("/") + "/"
        size = 0
        found = False
        continuation: str | None = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation:
                kwargs["ContinuationToken"] = continuation
            response = client.list_objects_v2(**kwargs)
            for item in response.get("Contents", []):
                found = True
                size += int(item.get("Size", 0))
            continuation = response.get("NextContinuationToken")
            if not continuation:
                break
        if not found:
            raise FileNotFoundError(f"artifact not found: {uri}")
        return "", size


def is_s3_not_found(err: Exception) -> bool:
    response = getattr(err, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    code = str(error.get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound", "NotFoundException"}


def file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
    return "sha256:" + digest.hexdigest(), size


def directory_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        relative = child.relative_to(path).as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\0")
        child_digest, child_size = file_digest(child)
        digest.update(child_digest.encode("ascii"))
        digest.update(b"\0")
        size += child_size
    if size == 0:
        raise ValueError(f"artifact directory is empty: {path}")
    return "sha256:" + digest.hexdigest(), size
