"""Credentialed preparation/publication; never executed alongside a Dockerfile RUN.

The intervening rootless init container sees only the scratch data volume.
Configuration is mounted separately, read-only, by the trusted actuator.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from loom_bundle_checksum import sha256_of_dir

from loom.task_image_build_plan import derive_task_image_build_components
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    _parse_bundle_file_metadata,
    _validate_bundle_relative_path,
    bundle_file_metadata_sha256,
)
from loom_execution_actuator.task_image_oci import (
    NativeOCIArchiveError,
    validate_native_oci_archive,
)

_FILE_LIMIT = 2000
_BUNDLE_BYTES = 512 * 1024 * 1024
_CACHE_BYTES = 1024 * 1024 * 1024
_CACHE_TOTAL_BYTES = 4 * _CACHE_BYTES
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_KEY = re.compile(r"[0-9a-f]{64}\Z")


class BuildPreparationError(ValueError):
    pass


def load_claim(path: Path) -> dict[str, Any]:
    if path.stat().st_size > 256 * 1024:
        raise BuildPreparationError("build claim is too large")
    claim = json.loads(path.read_text())
    if not isinstance(claim, dict):
        raise BuildPreparationError("build claim must be an object")
    for key in ("task_checksum", "materialization_key"):
        if not isinstance(claim.get(key), str) or not _KEY.fullmatch(claim[key]):
            raise BuildPreparationError("invalid build identity")
    source = claim.get("task_source", "")
    expected = "s3://" + claim["source_bucket"] + "/"
    if not isinstance(source, str) or not source.startswith(expected):
        raise BuildPreparationError("task source is outside the configured source bucket")
    prefix = source[len(expected) :]
    if not prefix.endswith("/"):
        raise BuildPreparationError("task source must be a directory prefix")
    _validate_bundle_relative_path(prefix[:-1])
    repo = claim["registry_repository"]
    if not isinstance(repo, str) or not re.fullmatch(
        r"[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9/_.-]+", repo
    ):
        raise BuildPreparationError("invalid configured task image repository")
    if len(repo) > 200:
        raise BuildPreparationError("task image repository exceeds receipt size budget")
    if len(derive_task_image_build_components(claim["task_config"])) > 8:
        raise BuildPreparationError("native build supports at most eight image components")
    return claim


def _client(claim: dict[str, Any], secret: Path) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=claim["storage_endpoint"],
        region_name=claim["storage_region"],
        aws_access_key_id=(secret / "access-key").read_text().strip(),
        aws_secret_access_key=(secret / "secret-key").read_text().strip(),
        config=Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=30,
            retries={"mode": "standard", "max_attempts": 3},
        ),
    )


def _download(client: Any, *, bucket: str, key: str, destination: Path, limit: int) -> int:
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]
    size = 0
    try:
        if response.get("ContentLength", 0) > limit:
            raise BuildPreparationError("input exceeds its byte limit")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as output:
            while chunk := body.read(min(1024 * 1024, limit - size + 1)):
                size += len(chunk)
                if size > limit:
                    raise BuildPreparationError("input exceeds its byte limit")
                output.write(chunk)
    finally:
        body.close()
    return size


def download_bundle(claim: dict[str, Any], client: Any, directory: Path) -> None:
    prefix = claim["task_source"].split("/", 3)[3]
    objects: list[str] = []
    total = 0
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=claim["source_bucket"], Prefix=prefix
    ):
        for item in page.get("Contents", []):
            relative = item["Key"][len(prefix) :]
            if relative == BUNDLE_FILE_METADATA_NAME:
                continue
            _validate_bundle_relative_path(relative)
            total += item["Size"]
            objects.append(relative)
            if len(objects) > _FILE_LIMIT or total > _BUNDLE_BYTES:
                raise BuildPreparationError("task bundle exceeds native build limits")
    if not objects:
        raise BuildPreparationError("task bundle is empty")
    directory.mkdir(parents=True, exist_ok=False)
    downloaded = 0
    for relative in sorted(objects):
        downloaded += _download(
            client,
            bucket=claim["source_bucket"],
            key=prefix + relative,
            destination=directory / relative,
            limit=_BUNDLE_BYTES - downloaded,
        )
    metadata = directory / BUNDLE_FILE_METADATA_NAME
    _download(
        client,
        bucket=claim["source_bucket"],
        key=prefix + BUNDLE_FILE_METADATA_NAME,
        destination=metadata,
        limit=4 * 1024 * 1024,
    )
    modes = _parse_bundle_file_metadata(metadata.read_bytes(), expected_paths=set(objects))
    for relative, mode in modes.items():
        (directory / relative).chmod(mode)
    metadata.unlink()
    # This is the source transfer boundary; downstream phases use these bytes.
    if sha256_of_dir(directory) != claim["task_checksum"]:
        raise BuildPreparationError("task bundle content does not match its frozen revision")
    expected_modes = claim["task_source_provenance"].get("bundle_file_metadata_sha256")
    if expected_modes is not None and bundle_file_metadata_sha256(directory) != expected_modes:
        raise BuildPreparationError("task bundle file modes do not match its frozen revision")


def unpack_cache(archive: Path, directory: Path) -> None:
    """Cache is untrusted data: reject links, devices and path traversal."""
    directory.mkdir(parents=True, exist_ok=False)
    total = 0
    with tarfile.open(archive, "r:") as source:
        for index, member in enumerate(source):
            if index >= 10000 or member.size < 0:
                raise BuildPreparationError("build cache exceeds file limit")
            _validate_bundle_relative_path(member.name)
            total += member.size
            if total > _CACHE_BYTES or not (member.isdir() or member.isfile()):
                raise BuildPreparationError("build cache contains unsupported content")
            target = directory / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                stream = source.extractfile(member)
                assert stream is not None
                with stream, target.open("xb") as output:
                    while chunk := stream.read(1024 * 1024):
                        output.write(chunk)


def _cache_prefix(claim: dict[str, Any]) -> str:
    # Content-bound keys prevent a task from importing another task's private data.
    return f"task-build-cache/{claim['materialization_key']}/"


def prepare(claim: dict[str, Any], work: Path, secrets: Path) -> None:
    source = _client(claim, secrets / "source")
    try:
        download_bundle(claim, source, work / "context")
    finally:
        source.close()
    (work / "oci").mkdir(exist_ok=True)
    if not (secrets / "cache").is_dir():
        return
    cache = _client(claim, secrets / "cache")
    try:
        for index, _ in enumerate(derive_task_image_build_components(claim["task_config"])):
            with tempfile.TemporaryDirectory() as temporary:
                archive = Path(temporary) / "cache.tar"
                try:
                    _download(
                        cache,
                        bucket=claim["cache_bucket"],
                        key=_cache_prefix(claim) + f"{index}.tar",
                        destination=archive,
                        limit=_CACHE_BYTES,
                    )
                except ClientError as error:
                    if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
                        continue
                    raise
                cache_directory = work / "cache-in" / str(index)
                try:
                    unpack_cache(archive, cache_directory)
                except (BuildPreparationError, tarfile.TarError):
                    # Disposable cache must not permanently poison this source
                    # revision. No links are extracted by unpack_cache.
                    shutil.rmtree(cache_directory, ignore_errors=False)
                    print(
                        json.dumps(
                            {"cache": "miss", "reason": "invalid_archive", "component_index": index}
                        ),
                        flush=True,
                    )
    finally:
        cache.close()


def pack_cache(directory: Path, archive: Path) -> None:
    if not stat.S_ISDIR(directory.lstat().st_mode):
        raise BuildPreparationError("cache root must be a real directory")
    total = 0
    with tarfile.open(archive, "w:", dereference=True) as output:
        for index, path in enumerate(sorted(directory.rglob("*"))):
            mode = path.lstat().st_mode
            if index >= 10000 or not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
                raise BuildPreparationError("build cache contains unsupported content")
            if path.is_file():
                total += path.stat().st_size
                if total > _CACHE_BYTES:
                    raise BuildPreparationError("build cache exceeds byte limit")
            output.add(path, arcname=path.relative_to(directory).as_posix(), recursive=False)


def trim_cache(client: Any, bucket: str, incoming_bytes: int) -> None:
    """Bound disposable cache size and age without a second GC service."""
    items = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="task-build-cache/"
    ):
        for item in page.get("Contents", []):
            if re.fullmatch(r"task-build-cache/[0-9a-f]{64}/[0-7]\.tar", item["Key"]):
                items.append(item)
    total = sum(item["Size"] for item in items) + incoming_bytes
    cutoff = datetime.now(UTC) - timedelta(days=7)
    for item in sorted(items, key=lambda row: row["LastModified"]):
        if total <= _CACHE_TOTAL_BYTES and item["LastModified"] >= cutoff:
            break
        client.delete_object(Bucket=bucket, Key=item["Key"])
        total -= item["Size"]


def _output_path(work: Path, relative: str, *, directory: bool = False) -> Path:
    """No task-created ancestor may redirect a credentialed publisher's reads."""
    _validate_bundle_relative_path(relative)
    current = work
    parts = relative.split("/")
    for index, part in enumerate(parts):
        current = current / part
        mode = current.lstat().st_mode
        expected_directory = index < len(parts) - 1 or directory
        if not (stat.S_ISDIR(mode) if expected_directory else stat.S_ISREG(mode)):
            raise BuildPreparationError("build output contains a link or special file")
    return current


def publish(
    claim: dict[str, Any],
    work: Path,
    secrets: Path,
    *,
    receipt_path: Path = Path("/dev/termination-log"),
) -> dict[str, Any]:
    images: dict[str, str] = {}
    cache = _client(claim, secrets / "cache") if (secrets / "cache").is_dir() else None
    auth_directory = tempfile.TemporaryDirectory()
    try:
        registry_auth = secrets / "registry" / "config.json"
        credentials = secrets / "registry" / "credentials.json"
        if credentials.is_file():
            from loom.nebius_registry_auth import mint_registry_auth

            registry_auth = Path(auth_directory.name) / "config.json"
            mint_registry_auth(
                credentials, "/".join(claim["registry_repository"].split("/")[:2]), registry_auth
            )
        for index, component in enumerate(derive_task_image_build_components(claim["task_config"])):
            archive = _output_path(work, component.oci_output_path)
            if archive.stat().st_size > 3 * _CACHE_BYTES:
                raise BuildPreparationError("build output is not a bounded regular OCI archive")
            validate_native_oci_archive(archive)
            tag = f"{claim['registry_repository']}:{claim['materialization_key']}-{claim['lease_epoch']}-{index}"
            with tempfile.TemporaryDirectory() as temporary:
                digest_file = Path(temporary) / "digest"
                subprocess.run(
                    [
                        "skopeo",
                        "--tmpdir",
                        temporary,
                        "copy",
                        "--authfile",
                        str(registry_auth),
                        "--preserve-digests",
                        "--digestfile",
                        str(digest_file),
                        f"oci-archive:{archive}",
                        f"docker://{tag}",
                    ],
                    check=True,
                    timeout=300,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                digest = digest_file.read_text().strip()
                if not _DIGEST.fullmatch(digest):
                    raise BuildPreparationError("registry did not return an OCI digest")
                images[component.name] = f"{claim['registry_repository']}@{digest}"
                receipt_path.write_text(
                    json.dumps(
                        {
                            "materialization_id": claim["id"],
                            "lease_epoch": claim["lease_epoch"],
                            "registry_images": images,
                        }
                    )
                )
                cache_dir = work / "cache-out" / str(index)
                if cache is not None and (cache_dir.exists() or cache_dir.is_symlink()):
                    cache_dir = _output_path(work, f"cache-out/{index}", directory=True)
                    cache_archive = Path(temporary) / "cache.tar"
                    pack_cache(cache_dir, cache_archive)
                    trim_cache(cache, claim["cache_bucket"], cache_archive.stat().st_size)
                    cache.upload_file(
                        str(cache_archive),
                        claim["cache_bucket"],
                        _cache_prefix(claim) + f"{index}.tar",
                    )
    finally:
        if cache is not None:
            cache.close()
        auth_directory.cleanup()
    receipt = {
        "materialization_id": claim["id"],
        "lease_epoch": claim["lease_epoch"],
        "registry_images": images,
    }
    if len(json.dumps(receipt).encode()) > 4000:
        raise BuildPreparationError(
            "publication receipt exceeds Kubernetes termination message limit"
        )
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "publish"))
    parser.add_argument("--claim", type=Path, required=True)
    args = parser.parse_args()
    try:
        claim = load_claim(args.claim)
        work, secrets = Path("/loom/build"), Path("/var/run/loom-task-build")
        if args.phase == "prepare":
            prepare(claim, work, secrets)
        else:
            receipt = publish(claim, work, secrets)
            Path("/dev/termination-log").write_text(json.dumps(receipt))
    except Exception as error:
        # SDK/tool exceptions can carry credentials, endpoints or task-controlled text.
        reason = (
            str(error)
            if isinstance(error, (BuildPreparationError, NativeOCIArchiveError))
            else type(error).__name__
        )
        error_receipt: dict[str, Any] = {}
        try:
            error_receipt = json.loads(Path("/dev/termination-log").read_text())
        except (OSError, ValueError):
            pass
        error_receipt.update(phase=args.phase, error=reason[:500])
        Path("/dev/termination-log").write_text(json.dumps(error_receipt))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
