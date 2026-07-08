from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True)
class StorageConfig:
    artifact_bucket_region: str
    local_s3_storage_dir: Path | None = None

    def uses_local_s3(self) -> bool:
        return self.local_s3_storage_dir is not None


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


def uri_to_local_path(uri: str, config: StorageConfig) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return Path(parsed.path).expanduser().resolve()
    if parsed.scheme == "":
        return Path(uri).expanduser().resolve()
    if parsed.scheme == "s3":
        if not config.uses_local_s3():
            raise ValueError("s3 uri is not local-dev backed")
        return (config.local_s3_storage_dir / parsed.netloc / parsed.path.lstrip("/")).resolve()
    raise ValueError(f"unsupported storage uri scheme {parsed.scheme!r}")


def upload_directory(source_dir: Path, destination_uri: str, config: StorageConfig) -> ArtifactInfo:
    source_dir = source_dir.resolve()
    if not source_dir.is_dir():
        raise FileNotFoundError(f"artifact directory does not exist: {source_dir}")
    if is_remote_s3(destination_uri, config):
        checksum, size = directory_digest(source_dir)
        upload_directory_s3(source_dir, destination_uri, config)
        return ArtifactInfo(uri=destination_uri, checksum=checksum, size_bytes=size)
    destination = uri_to_local_path(destination_uri, config)
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, destination)
    return artifact_info(destination_uri, config)


def upload_file(source_file: Path, destination_uri: str, config: StorageConfig) -> ArtifactInfo:
    source_file = source_file.resolve()
    if not source_file.is_file():
        raise FileNotFoundError(f"artifact file does not exist: {source_file}")
    checksum, size = file_digest(source_file)
    if is_remote_s3(destination_uri, config):
        bucket, key = parse_s3_uri(destination_uri)
        s3_client(config).upload_file(str(source_file), bucket, key)
        return ArtifactInfo(uri=destination_uri, checksum=checksum, size_bytes=size)
    destination = uri_to_local_path(destination_uri, config)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_file, destination)
    return ArtifactInfo(uri=destination_uri, checksum=checksum, size_bytes=size)


def write_json_bytes(destination_uri: str, payload: bytes, config: StorageConfig) -> None:
    if is_remote_s3(destination_uri, config):
        parsed = parse_s3_uri(destination_uri)
        s3_client(config).put_object(Bucket=parsed[0], Key=parsed[1], Body=payload)
        return
    destination = uri_to_local_path(destination_uri, config)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)


def read_json_bytes(uri: str, config: StorageConfig) -> bytes:
    if is_remote_s3(uri, config):
        parsed = parse_s3_uri(uri)
        response = s3_client(config).get_object(Bucket=parsed[0], Key=parsed[1])
        return response["Body"].read()
    return uri_to_local_path(uri, config).read_bytes()


def artifact_info(uri: str, config: StorageConfig) -> ArtifactInfo:
    if is_remote_s3(uri, config):
        checksum, size = remote_s3_digest(uri, config)
        return ArtifactInfo(uri=uri, checksum=checksum, size_bytes=size)
    path = uri_to_local_path(uri, config)
    if path.is_file():
        checksum, size = file_digest(path)
        return ArtifactInfo(uri=uri, checksum=checksum, size_bytes=size)
    if path.is_dir():
        checksum, size = directory_digest(path)
        return ArtifactInfo(uri=uri, checksum=checksum, size_bytes=size)
    raise FileNotFoundError(f"artifact not found: {uri}")


def is_remote_s3(uri: str, config: StorageConfig) -> bool:
    return urlparse(uri).scheme == "s3" and not config.uses_local_s3()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"invalid s3 uri: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def s3_client(config: StorageConfig):
    import boto3

    return boto3.client("s3", region_name=config.artifact_bucket_region)


def upload_directory_s3(source_dir: Path, destination_uri: str, config: StorageConfig) -> None:
    bucket, prefix = parse_s3_uri(destination_uri)
    client = s3_client(config)
    for child in sorted(p for p in source_dir.rglob("*") if p.is_file()):
        key = f"{prefix.rstrip('/')}/{child.relative_to(source_dir).as_posix()}"
        client.upload_file(str(child), bucket, key)


def remote_s3_digest(uri: str, config: StorageConfig) -> tuple[str, int]:
    bucket, key = parse_s3_uri(uri)
    client = s3_client(config)
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
