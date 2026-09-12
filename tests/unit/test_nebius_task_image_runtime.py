from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import subprocess
import tarfile
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import tomli_w
from botocore.exceptions import ClientError
from loom_bundle_checksum import sha256_of_dir

from loom.models.taskset import UserTaskSetManifest, bundle_object_key
from loom.taskset.materialize import _publish_service_execution_input_manifest, materialize_task_set
from loom.taskset.storage_bytes import taskset_root
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    bundle_file_metadata_body,
    bundle_file_metadata_sha256,
)
from loom_execution_actuator import task_image_runtime as runtime


def _write_native_oci(path: Path, *, platform: str = "linux/amd64") -> None:
    """Valid minimal local image, while subprocess tests retain a fake registry."""
    image_os, _, architecture = platform.partition("/")
    config = json.dumps(
        {"architecture": architecture, "os": image_os, "rootfs": {"type": "layers", "diff_ids": []}}
    ).encode()
    manifest = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:" + "c" * 64,
                "size": len(config),
            },
            "layers": [],
        }
    ).encode()
    index = json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:" + "d" * 64,
                    "size": len(manifest),
                    "platform": {"architecture": "amd64", "os": "linux"},
                }
            ],
        }
    ).encode()
    with tarfile.open(path, "w:") as archive:
        for name, body in {
            "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
            "index.json": index,
            "blobs/sha256/" + "c" * 64: config,
            "blobs/sha256/" + "d" * 64: manifest,
        }.items():
            member = tarfile.TarInfo(name)
            member.size = len(body)
            archive.addfile(member, io.BytesIO(body))


class FakeS3:
    def __init__(self, objects: dict[str, bytes], *, listing: list[dict[str, Any]] | None = None):
        self.objects = objects
        self.listing = listing
        self.gets: list[str] = []
        self.bodies: list[io.BytesIO] = []
        self.uploads: list[tuple[str, str, bytes]] = []
        self.closed = False

    def get_paginator(self, name: str) -> FakeS3:
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        prefix = kwargs["Prefix"]
        yield {
            "Contents": self.listing
            if self.listing is not None
            else [
                {"Key": key, "Size": len(body)}
                for key, body in self.objects.items()
                if key.startswith(prefix)
            ]
        }

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        self.gets.append(key)
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = io.BytesIO(self.objects[key])
        self.bodies.append(body)
        return {"Body": body, "ContentLength": len(self.objects[key])}

    def close(self) -> None:
        self.closed = True

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploads.append((bucket, key, Path(filename).read_bytes()))


@pytest.fixture
def source_bundle(tmp_path):
    original = tmp_path / "original"
    (original / "environment").mkdir(parents=True)
    (original / "environment/Dockerfile").write_text("FROM scratch\n")
    (original / "instruction.md").write_text("Run the task.\n")
    (original / "run.sh").write_text("#!/bin/sh\nexit 0\n")
    (original / "run.sh").chmod(0o755)
    for name in ("environment/Dockerfile", "instruction.md"):
        (original / name).chmod(0o644)
    claim = {
        "id": str(uuid4()),
        "task_checksum": sha256_of_dir(original),
        "materialization_key": "a" * 64,
        "task_source": "s3://source/tasks/revision/",
        "source_bucket": "source",
        "cache_bucket": "cache",
        "task_source_provenance": {
            "bundle_file_metadata_sha256": bundle_file_metadata_sha256(original)
        },
        "registry_repository": "registry.example/tasks",
        "lease_epoch": 3,
        "task_config": {
            "schema_version": "1",
            "task": {"id": "test", "name": "test"},
            "environment": {
                "os": "linux",
                "cpu_arch": "x86_64",
                "dockerfile": "environment/Dockerfile",
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
        },
    }
    objects = {
        "tasks/revision/" + path.relative_to(original).as_posix(): path.read_bytes()
        for path in original.rglob("*")
        if path.is_file()
    }
    objects["tasks/revision/" + BUNDLE_FILE_METADATA_NAME] = bundle_file_metadata_body(original)
    return claim, FakeS3(objects)


def test_download_restores_verified_source_bytes_and_executable_modes(
    source_bundle, tmp_path
) -> None:
    claim, source = source_bundle
    destination = tmp_path / "download"
    runtime.download_bundle(claim, source, destination)
    assert sha256_of_dir(destination) == claim["task_checksum"]
    assert (destination / "instruction.md").read_text() == "Run the task.\n"
    assert stat.S_IMODE((destination / "run.sh").stat().st_mode) == 0o755
    assert stat.S_IMODE((destination / "instruction.md").stat().st_mode) == 0o644
    assert not (destination / BUNDLE_FILE_METADATA_NAME).exists()
    assert source.bodies and all(body.closed for body in source.bodies)


def test_download_ordinary_uploaded_taskset_without_benchmark_sidecar(source_bundle, tmp_path):
    claim, source = source_bundle
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "native-input", "display_name": "Native input"},
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz", "subset": "tasks"},
    })
    buffer = io.BytesIO()
    files = {
        "task.toml": tomli_w.dumps(claim["task_config"]).encode(),
        "instruction.md": b"Run the task.\n",
        "environment/Dockerfile": b"FROM scratch\n",
    }
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, body in files.items():
            member = tarfile.TarInfo("tasks/test/" + name)
            member.size = len(body)
            member.mode = 0o644
            archive.addfile(member, io.BytesIO(body))
    source.objects = {
        bundle_object_key(
            prefix=taskset_root(team_id="team-test", slug=manifest.slug).removesuffix("/"),
            relative_path=manifest.source.locator,
        ): buffer.getvalue()
    }
    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team-test/native-input", owning_team_id="team-test",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None, minio_client=source,
        artifacts_bucket="source", upstream_cache_root=tmp_path / "upstream",
    )
    assert result.status == "ready", result.error_summary
    row, = result.task_rows
    assert "service_execution_input" in row.source_provenance
    assert not any(key.endswith(BUNDLE_FILE_METADATA_NAME) for key in source.objects)
    claim.update(task_source=row.source, task_checksum=row.checksum,
                 task_config=row.config, task_source_provenance=row.source_provenance)
    runtime.download_bundle(claim, source, tmp_path / "download")
    assert sha256_of_dir(tmp_path / "download") == row.checksum
    assert (tmp_path / "download/environment/Dockerfile").read_bytes() == b"FROM scratch\n"


@pytest.fixture
def input_manifest_bundle(source_bundle, tmp_path):
    claim, source = source_bundle
    del source.objects["tasks/revision/" + BUNDLE_FILE_METADATA_NAME]
    provenance, _ = _publish_service_execution_input_manifest(
        source, bucket="source", manifest_key="task-inputs/revision.json",
        bundle_dir=tmp_path / "original", task_checksum_value=claim["task_checksum"],
    )
    claim["task_source_provenance"].update(provenance)
    return claim, source


def test_input_manifest_restores_frozen_executable_modes(input_manifest_bundle, tmp_path):
    claim, source = input_manifest_bundle
    runtime.download_bundle(claim, source, tmp_path / "download")
    assert stat.S_IMODE((tmp_path / "download/run.sh").stat().st_mode) == 0o755
    assert stat.S_IMODE((tmp_path / "download/instruction.md").stat().st_mode) == 0o644
    assert not any(key.endswith(BUNDLE_FILE_METADATA_NAME) for key in source.gets)
    assert all(body.closed for body in source.bodies)


@pytest.mark.parametrize("change,match", [
    ("manifest_bytes", "frozen binding"),
    ("content", "content does not match"),
    ("missing_file", "frozen bundle"),
    ("extra_file", "frozen bundle"),
    ("revision", "frozen bundle"),
    ("count", "frozen bundle"),
    ("total", "frozen bundle"),
    ("unsafe_mode", "Input should be"),
    ("mode", "file modes do not match"),
    ("noncanonical", "frozen bundle"),
    ("outside_bucket", "outside the configured source bucket"),
])
def test_input_manifest_rejects_frozen_input_drift(input_manifest_bundle, tmp_path, change, match):
    claim, source = input_manifest_bundle
    key = "task-inputs/revision.json"
    binding = claim["task_source_provenance"]["service_execution_input"]
    if change == "manifest_bytes":
        source.objects[key] += b" "
    elif change == "content":
        source.objects["tasks/revision/instruction.md"] = b"changed bytes"
    elif change == "missing_file":
        del source.objects["tasks/revision/run.sh"]
    elif change == "extra_file":
        source.objects["tasks/revision/extra"] = b"unlisted"
    elif change == "outside_bucket":
        binding["manifest_uri"] = "s3://other/task-inputs/revision.json"
    elif change in {"count", "total"}:
        binding["file_count" if change == "count" else "total_bytes"] += 1
    else:
        manifest = json.loads(source.objects[key])
        if change == "revision":
            manifest["task_revision_sha256"] = "sha256:" + "0" * 64
        elif change in {"mode", "unsafe_mode"}:
            next(row for row in manifest["files"] if row["relative_path"] == "run.sh")[
                "mode"
            ] = "0644" if change == "mode" else "4755"
        body = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        if change == "noncanonical":
            body += b" "
        source.objects[key] = body
        binding["manifest_sha256"] = "sha256:" + hashlib.sha256(body).hexdigest()
    with pytest.raises(ValueError, match=match):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert all(body.closed for body in source.bodies)


def test_missing_bound_manifest_does_not_fall_back_to_sidecar(input_manifest_bundle, tmp_path):
    claim, source = input_manifest_bundle
    del source.objects["task-inputs/revision.json"]
    source.objects["tasks/revision/" + BUNDLE_FILE_METADATA_NAME] = bundle_file_metadata_body(
        tmp_path / "original"
    )
    with pytest.raises(ClientError, match="NoSuchKey"):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert source.gets == ["task-inputs/revision.json"]


@pytest.mark.parametrize(
    "change,match",
    [
        ("content", "content does not match"),
        ("mode", "file modes do not match"),
        ("unsafe_mode", "unsafe file mode"),
        ("missing_path", "paths do not exactly match"),
    ],
)
def test_download_rejects_source_or_metadata_drift(source_bundle, tmp_path, change, match) -> None:
    claim, source = source_bundle
    metadata_key = "tasks/revision/" + BUNDLE_FILE_METADATA_NAME
    if change == "content":
        source.objects["tasks/revision/instruction.md"] = b"changed source"
    else:
        metadata = json.loads(source.objects[metadata_key])
        if change == "missing_path":
            del metadata["files"]["run.sh"]
        else:
            metadata["files"]["run.sh"]["mode"] = "0644" if change == "mode" else "4755"
        source.objects[metadata_key] = json.dumps(metadata).encode()
    with pytest.raises(ValueError, match=match):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert all(body.closed for body in source.bodies)


@pytest.mark.parametrize(
    "relative", ["../outside", "/absolute", "nested/../../outside", "a\\outside"]
)
def test_download_rejects_nonrelative_listing_before_writing(
    source_bundle, tmp_path, relative
) -> None:
    claim, source = source_bundle
    source.listing = [{"Key": "tasks/revision/" + relative, "Size": 1}]
    with pytest.raises(ValueError):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert source.gets == []
    assert not (tmp_path / "download").exists()


@pytest.mark.parametrize("limit", ["files", "bytes"])
def test_download_rejects_listing_limits_before_getting_objects(
    source_bundle, tmp_path, monkeypatch, limit
) -> None:
    claim, source = source_bundle
    monkeypatch.setattr(runtime, "_FILE_LIMIT" if limit == "files" else "_BUNDLE_BYTES", 1)
    with pytest.raises(runtime.BuildPreparationError, match="limits"):
        runtime.download_bundle(claim, source, tmp_path / "download")
    assert source.gets == []


@pytest.mark.parametrize("reported", [0, 6])
def test_download_bounds_stream_even_when_content_length_underreports(tmp_path, reported) -> None:
    body = io.BytesIO(b"123456")

    class Client:
        def get_object(self, **_kwargs):
            return {"Body": body, "ContentLength": reported}

    with pytest.raises(runtime.BuildPreparationError, match="byte limit"):
        runtime._download(
            Client(), bucket="source", key="key", destination=tmp_path / "file", limit=4
        )
    assert body.closed
    assert not (tmp_path / "file").exists() or (tmp_path / "file").stat().st_size <= 4


def test_prepare_accepts_missing_cache_and_closes_both_clients(
    source_bundle, tmp_path, monkeypatch
) -> None:
    claim, source = source_bundle
    cache = FakeS3({})
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    work = tmp_path / "work"
    (tmp_path / "secrets/cache").mkdir(parents=True)
    runtime.prepare(claim, work, tmp_path / "secrets")
    assert (work / "context/run.sh").is_file() and (work / "oci").is_dir()
    assert cache.gets == ["task-build-cache/" + claim["materialization_key"] + "/0.tar"]
    assert source.closed and cache.closed


def test_prepare_without_cache_secret_never_creates_cache_client(
    source_bundle, tmp_path, monkeypatch
) -> None:
    claim, source = source_bundle
    clients = []

    def client(_claim, secret):
        clients.append(secret.name)
        assert secret.name == "source"
        return source

    monkeypatch.setattr(runtime, "_client", client)
    runtime.prepare(claim, tmp_path / "work", tmp_path / "secrets")
    assert clients == ["source"] and source.closed
    assert (tmp_path / "work/oci").is_dir()


def test_invalid_disposable_cache_becomes_a_cold_build(
    source_bundle, tmp_path, monkeypatch, capsys
) -> None:
    claim, source = source_bundle
    cache = FakeS3(
        {"task-build-cache/" + claim["materialization_key"] + "/0.tar": b"broken archive"}
    )
    monkeypatch.setattr(
        runtime, "_client", lambda _claim, secret: source if secret.name == "source" else cache
    )
    (tmp_path / "secrets/cache").mkdir(parents=True)
    runtime.prepare(claim, tmp_path / "work", tmp_path / "secrets")
    assert (tmp_path / "work/context/run.sh").is_file()
    assert not (tmp_path / "work/cache-in/0").exists()
    assert json.loads(capsys.readouterr().out)["reason"] == "invalid_archive"
    assert cache.closed and source.closed


def test_cache_round_trip_preserves_nested_regular_files(tmp_path) -> None:
    cache = tmp_path / "cache"
    (cache / "blobs/sha256").mkdir(parents=True)
    (cache / "index.json").write_bytes(b'{"schemaVersion":2}')
    (cache / "blobs/sha256/layer").write_bytes(b"cache layer")
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    restored = tmp_path / "restored"
    runtime.unpack_cache(archive, restored)
    assert (restored / "index.json").read_bytes() == (cache / "index.json").read_bytes()
    assert (restored / "blobs/sha256/layer").read_bytes() == b"cache layer"


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "traversal", "absolute", "device"])
def test_unpack_cache_rejects_links_devices_and_escaping_paths(tmp_path, kind) -> None:
    archive = tmp_path / "cache.tar"
    member = tarfile.TarInfo({"traversal": "../outside", "absolute": "/outside"}.get(kind, "entry"))
    if kind in {"symlink", "hardlink"}:
        member.type = tarfile.SYMTYPE if kind == "symlink" else tarfile.LNKTYPE
        member.linkname = "../credential"
    elif kind == "device":
        member.type = tarfile.CHRTYPE
    with tarfile.open(archive, "w:") as output:
        output.addfile(member)
    with pytest.raises(ValueError):
        runtime.unpack_cache(archive, tmp_path / "restored")
    assert not (tmp_path / "outside").exists()


@pytest.mark.parametrize("kind", ["root_symlink", "file_symlink", "directory_symlink"])
def test_pack_cache_cannot_archive_linked_credentials(tmp_path, kind) -> None:
    secret = tmp_path / "credential"
    secret.write_text("fake credential")
    cache = tmp_path / "cache"
    if kind == "root_symlink":
        cache.symlink_to(tmp_path, target_is_directory=True)
    else:
        cache.mkdir()
        target = cache / "entry"
        target.symlink_to(
            tmp_path if kind == "directory_symlink" else secret,
            target_is_directory=kind == "directory_symlink",
        )
    with pytest.raises(runtime.BuildPreparationError):
        runtime.pack_cache(cache, tmp_path / "cache.tar")


def test_pack_cache_normalizes_internal_hardlinks_to_regular_entries(tmp_path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "first").write_bytes(b"shared cache layer")
    os.link(cache / "first", cache / "second")
    archive = tmp_path / "cache.tar"
    runtime.pack_cache(cache, archive)
    with tarfile.open(archive) as packed:
        assert all(member.isfile() and not member.islnk() for member in packed)
    runtime.unpack_cache(archive, tmp_path / "restored")
    assert (
        (tmp_path / "restored/first").read_bytes()
        == (tmp_path / "restored/second").read_bytes()
        == b"shared cache layer"
    )


def test_cache_size_limits_apply_to_pack_and_unpack(tmp_path, monkeypatch) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "blob").write_bytes(b"12345")
    archive = tmp_path / "valid.tar"
    runtime.pack_cache(cache, archive)
    monkeypatch.setattr(runtime, "_CACHE_BYTES", 4)
    with pytest.raises(runtime.BuildPreparationError):
        runtime.pack_cache(cache, tmp_path / "oversized.tar")
    with pytest.raises(runtime.BuildPreparationError):
        runtime.unpack_cache(archive, tmp_path / "restored")


@pytest.mark.parametrize("path", ["oci", "oci/0000.tar", "cache-out", "cache-out/0"])
def test_publisher_output_path_rejects_task_created_credential_symlinks(tmp_path, path) -> None:
    work, secrets = tmp_path / "work", tmp_path / "secrets"
    work.mkdir()
    secrets.mkdir()
    (secrets / "config.json").write_text("fake credential")
    linked = work / path
    linked.parent.mkdir(parents=True, exist_ok=True)
    linked.symlink_to(secrets, target_is_directory=True)
    relative = "oci/0000.tar" if path.startswith("oci") else "cache-out/0"
    with pytest.raises(runtime.BuildPreparationError, match="link"):
        runtime._output_path(work, relative, directory=path.startswith("cache-out"))


@pytest.mark.parametrize("platform", ["linux/arm64", "windows/amd64", ""])
def test_publisher_rejects_wrong_architecture_before_copy_or_receipt(
    source_bundle, tmp_path, monkeypatch, platform
) -> None:
    claim, _ = source_bundle
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar", platform=platform)
    (tmp_path / "secrets/cache").mkdir(parents=True)
    cache = FakeS3({})
    calls = []
    monkeypatch.setattr(runtime, "_client", lambda *_args: cache)

    def run(argv, **kwargs):
        calls.append(argv)
        raise AssertionError("invalid OCI configuration must fail before Skopeo")

    monkeypatch.setattr(runtime.subprocess, "run", run)
    with pytest.raises(runtime.NativeOCIArchiveError, match="linux/amd64"):
        runtime.publish(claim, work, tmp_path / "secrets", receipt_path=tmp_path / "receipt.json")
    assert calls == [] and cache.closed
    assert not (tmp_path / "receipt.json").exists()


@pytest.fixture
def publisher(monkeypatch, tmp_path):
    (tmp_path / "secrets/cache").mkdir(parents=True)
    cache = FakeS3({})
    calls = []
    monkeypatch.setattr(runtime, "_client", lambda *_args: cache)

    def run(argv, **kwargs):
        calls.append(argv)
        assert argv[:2] == ["skopeo", "--tmpdir"] and argv[3] == "copy"
        assert Path(argv[2]).is_dir() and "--preserve-digests" in argv
        Path(argv[argv.index("--digestfile") + 1]).write_text("sha256:" + "b" * 64)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(runtime.subprocess, "run", run)
    return cache, calls


@pytest.mark.parametrize("cache_enabled", [True, False])
def test_publisher_records_registry_digest_with_optional_cache(
    source_bundle, tmp_path, publisher, cache_enabled
) -> None:
    claim, _ = source_bundle
    cache, calls = publisher
    if not cache_enabled:
        (tmp_path / "secrets/cache").rmdir()
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar")
    (work / "cache-out/0/blobs").mkdir(parents=True)
    (work / "cache-out/0/blobs/layer").write_bytes(b"cached layer")
    receipt_file = tmp_path / "receipt.json"
    result = runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt_file)
    assert (
        result
        == json.loads(receipt_file.read_text())
        == {
            "materialization_id": claim["id"],
            "lease_epoch": 3,
            "registry_images": {"task": "registry.example/tasks@sha256:" + "b" * 64},
        }
    )
    assert [call[3] for call in calls] == ["copy"]
    if not cache_enabled:
        assert cache.uploads == [] and not cache.closed
        return
    assert len(cache.uploads) == 1 and cache.closed
    bucket, key, body = cache.uploads[0]
    assert (bucket, key) == ("cache", "task-build-cache/" + claim["materialization_key"] + "/0.tar")
    with tarfile.open(fileobj=io.BytesIO(body)) as packed:
        assert packed.extractfile("blobs/layer").read() == b"cached layer"


@pytest.mark.parametrize("linked_directory", ["oci", "cache-out"])
def test_publisher_rejects_symlinked_output_ancestors_before_reading_or_uploading_secrets(
    source_bundle,
    tmp_path,
    publisher,
    linked_directory,
) -> None:
    claim, _ = source_bundle
    cache, calls = publisher
    work, secret = tmp_path / "work", tmp_path / "secrets/registry"
    work.mkdir()
    (secret / "0").mkdir(parents=True)
    (secret / "0000.tar").write_bytes(b"fake credential")
    (secret / "0/config.json").write_bytes(b"fake credential")
    (work / linked_directory).symlink_to(secret, target_is_directory=True)
    if linked_directory == "cache-out":
        (work / "oci").mkdir()
        _write_native_oci(work / "oci/0000.tar")
    receipt_file = tmp_path / "receipt.json"
    with pytest.raises(runtime.BuildPreparationError, match="link"):
        runtime.publish(claim, work, tmp_path / "secrets", receipt_path=receipt_file)
    assert cache.uploads == [] and cache.closed
    if linked_directory == "oci":
        assert calls == [] and not receipt_file.exists()
    else:
        # Keep publication evidence even when a later cache step is rejected.
        assert json.loads(receipt_file.read_text())["registry_images"]["task"].endswith("b" * 64)


@pytest.mark.parametrize(
    "field,value",
    [
        ("task_source", "s3://another-bucket/tasks/revision/"),
        ("task_source", "s3://source/tasks/../outside/"),
        ("task_source", "s3://source/tasks/revision"),
        ("materialization_key", "not-an-identity"),
        ("registry_repository", "registry.example/tasks;injected"),
    ],
)
def test_claim_rejects_out_of_scope_source_or_invalid_identity(
    source_bundle, tmp_path, field, value
) -> None:
    claim, _ = source_bundle
    claim[field] = value
    path = tmp_path / "claim.json"
    path.write_text(json.dumps(claim))
    with pytest.raises(ValueError):
        runtime.load_claim(path)


def test_native_publisher_mints_auth_outside_task_volume_and_removes_it(
    source_bundle, tmp_path, publisher, monkeypatch
) -> None:
    from loom import nebius_registry_auth

    claim, _ = source_bundle
    _, calls = publisher
    claim["registry_repository"] = "cr.eu-north1.nebius.cloud/test/task-images"
    secrets = tmp_path / "secrets"
    (secrets / "registry").mkdir()
    credentials = secrets / "registry/credentials.json"
    credentials.write_text("trusted-key-fixture")
    work = tmp_path / "work"
    (work / "oci").mkdir(parents=True)
    _write_native_oci(work / "oci/0000.tar")
    minted = []

    def mint(path, prefix, auth_file):
        assert path == credentials and prefix == "cr.eu-north1.nebius.cloud/test"
        assert not auth_file.is_relative_to(work)
        auth_file.write_text("temporary-auth-fixture")
        minted.append(auth_file)
        return {}

    monkeypatch.setattr(nebius_registry_auth, "mint_registry_auth", mint)
    runtime.publish(claim, work, secrets, receipt_path=tmp_path / "receipt.json")
    assert len(minted) == 1
    assert calls[0][calls[0].index("--authfile") + 1] == str(minted[0])
    assert not minted[0].exists()
