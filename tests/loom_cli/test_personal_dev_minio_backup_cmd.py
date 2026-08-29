from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import pytest

from loom.personal_dev_minio_backup import (
    PersonalDevMinioBackupError,
    PersonalDevMinioObject,
    build_personal_dev_minio_manifest,
    write_personal_dev_minio_manifest,
)
from loom_cli.personal_dev_minio_backup_cmd import (
    PersonalDevMinioCommandResult,
    capture_personal_dev_minio_backup,
    restore_personal_dev_minio_backup,
)


def _json_line(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("ascii") + b"\n"


def _bucket_record(bucket: str) -> dict[str, object]:
    return {
        "status": "success",
        "type": "folder",
        "lastModified": "2026-08-29T12:00:00Z",
        "size": 0,
        "key": f"{bucket}/",
        "etag": "",
        "url": "http://minio.internal/",
        "versionOrdinal": 1,
    }


def _list_record(
    *, key: str = "personal-dev/source/candidate.tar", size: int = 7
) -> dict[str, object]:
    return {
        "status": "success",
        "type": "file",
        "key": key,
        "size": size,
        "etag": "source-etag",
        "lastModified": "2026-08-29T12:00:00Z",
    }


def _stat_record(
    *, key: str = "personal-dev/source/candidate.tar", size: int = 7
) -> dict[str, object]:
    return {
        "status": "success",
        "type": "file",
        "name": key,
        "size": size,
        "etag": "source-etag",
        "lastModified": "2026-08-29T12:00:00Z",
        "metadata": {
            "Content-Type": "application/x-tar",
            "Cache-Control": "no-cache",
            "X-Amz-Meta-Archive-Sha256": "a" * 64,
        },
    }


def _version_disabled(alias: str, bucket: str) -> bytes:
    return _json_line(
        {
            "Op": "info",
            "status": "success",
            "url": f"{alias}/{bucket}",
            "versioning": {"status": "", "MFADelete": ""},
        }
    )


def _retention_absent() -> bytes:
    return _json_line(
        {
            "status": "error",
            "error": {
                "message": "Remote bucket `%s` does not support locking",
                "cause": {"message": "", "error": {}},
                "type": "fatal",
            },
        }
    )


def _encryption_absent(bucket: str) -> bytes:
    message = "The server side encryption configuration was not found"
    return _json_line(
        {
            "status": "error",
            "error": {
                "message": "Unable to get encryption info",
                "cause": {
                    "message": message,
                    "error": {
                        "Code": "ServerSideEncryptionConfigurationNotFoundError",
                        "Message": message,
                        "BucketName": bucket,
                        "Key": "",
                        "Resource": f"/{bucket}/",
                        "RequestID": "18D053008D73B431",
                        "HostID": "8c7504ac1df34cce90dc1e439b338de5",
                        "Region": "",
                        "Server": "MinIO",
                    },
                },
                "type": "fatal",
            },
        }
    )


def _tags_absent(target: str) -> bytes:
    return _json_line(
        {
            "status": "error",
            "error": {
                "message": f"No tags found  for http://credential-marker/{target}",
                "cause": {
                    "message": "check 'mc tag set --help' on how to set tags",
                    "error": {},
                },
                "type": "fatal",
            },
        }
    )


def _result(
    stdout: bytes = b"", *, stderr: bytes = b"", returncode: int = 0
) -> PersonalDevMinioCommandResult:
    return PersonalDevMinioCommandResult(
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


@dataclass(frozen=True)
class _Run:
    arguments: tuple[str, ...]
    result: PersonalDevMinioCommandResult


@dataclass(frozen=True)
class _Stream:
    arguments: tuple[str, ...]
    payload: bytes
    digest: str | None = None
    expected_size: int | None = None
    destination_none: bool = False


class _RecordingTransport:
    def __init__(self, actions: Sequence[_Run | _Stream]) -> None:
        self.actions = list(actions)
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def run(
        self,
        arguments: Sequence[str],
        *,
        maximum_stdout_bytes: int,
        timeout_seconds: int,
    ) -> PersonalDevMinioCommandResult:
        del maximum_stdout_bytes, timeout_seconds
        actual = tuple(arguments)
        self.calls.append(("run", actual))
        action = self.actions.pop(0)
        assert isinstance(action, _Run)
        assert actual == action.arguments
        return action.result

    def stream(
        self,
        arguments: Sequence[str],
        *,
        destination: BinaryIO | None,
        expected_size: int,
        timeout_seconds: int,
    ) -> str:
        del timeout_seconds
        actual = tuple(arguments)
        self.calls.append(("stream", actual))
        action = self.actions.pop(0)
        assert isinstance(action, _Stream)
        assert actual == action.arguments
        assert expected_size == (
            len(action.payload) if action.expected_size is None else action.expected_size
        )
        assert (destination is None) is action.destination_none
        if destination is not None:
            destination.write(action.payload)
        return action.digest or hashlib.sha256(action.payload).hexdigest()


def _capture_actions(
    *,
    payload: bytes = b"payload",
    version: bytes | None = None,
    retention: PersonalDevMinioCommandResult | None = None,
    encryption: PersonalDevMinioCommandResult | None = None,
    tags: PersonalDevMinioCommandResult | None = None,
) -> list[_Run | _Stream]:
    key = "personal-dev/source/candidate.tar"
    bucket_output = _json_line(_bucket_record("artifacts")) + _json_line(
        _bucket_record("trajectories")
    )
    listing = _json_line(_list_record(size=len(payload)))
    empty_listing = b""
    stat = _json_line(_stat_record(size=len(payload)))
    actions: list[_Run | _Stream] = [
        _Run(("ls", "--json", "local"), _result(bucket_output)),
    ]
    for bucket in ("artifacts", "trajectories"):
        actions.append(
            _Run(
                ("version", "info", "--json", f"local/{bucket}"),
                _result(version or _version_disabled("local", bucket)),
            )
        )
    for bucket in ("artifacts", "trajectories"):
        actions.append(
            _Run(
                ("retention", "info", "--json", f"local/{bucket}"),
                retention or _result(stderr=_retention_absent(), returncode=1),
            )
        )
    for bucket in ("artifacts", "trajectories"):
        actions.append(
            _Run(
                ("encrypt", "info", "--json", f"local/{bucket}"),
                encryption or _result(stderr=_encryption_absent(bucket), returncode=1),
            )
        )
    actions.extend(
        [
            _Run(
                ("ls", "--recursive", "--json", "local/artifacts"),
                _result(listing),
            ),
            _Run(
                ("ls", "--recursive", "--json", "local/trajectories"),
                _result(empty_listing),
            ),
            _Run(
                ("tag", "list", "--json", f"local/artifacts/{key}"),
                tags or _result(stderr=_tags_absent(f"artifacts/{key}"), returncode=1),
            ),
            _Run(("stat", "--json", f"local/artifacts/{key}"), _result(stat)),
            _Stream(("cat", f"local/artifacts/{key}"), payload),
            _Run(
                ("ls", "--recursive", "--json", "local/artifacts"),
                _result(listing),
            ),
            _Run(
                ("ls", "--recursive", "--json", "local/trajectories"),
                _result(empty_listing),
            ),
            _Run(("stat", "--json", f"local/artifacts/{key}"), _result(stat)),
        ]
    )
    return actions


def _capture_paths(tmp_path: Path) -> tuple[Path, Path]:
    backup_root = tmp_path / "backup"
    backup_root.mkdir(mode=0o700)
    return backup_root / "source.json", backup_root / "payloads"


def test_capture_uses_the_exact_read_only_order_and_publishes_canonical_authority(
    tmp_path: Path,
) -> None:
    actions = _capture_actions()
    transport = _RecordingTransport(actions)
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    manifest = capture_personal_dev_minio_backup(
        transport=transport,
        source_manifest_path=source_manifest_path,
        payload_root=payload_root,
    )

    assert not transport.actions
    assert transport.calls == [
        ("run", action.arguments) if isinstance(action, _Run) else ("stream", action.arguments)
        for action in actions
    ]
    assert manifest.object_count == 1
    assert manifest.total_payload_bytes == 7
    assert source_manifest_path.read_bytes() == manifest.canonical_bytes
    digest = hashlib.sha256(b"payload").hexdigest()
    assert (payload_root / digest).read_bytes() == b"payload"


@pytest.mark.parametrize(
    "override",
    [
        {
            "version": _json_line(
                {
                    "Op": "info",
                    "status": "success",
                    "url": "local/artifacts",
                    "versioning": {"status": "Enabled", "MFADelete": ""},
                }
            )
        },
        {"retention": _result(_json_line({"status": "success"}))},
        {"encryption": _result(_json_line({"status": "success"}))},
        {"tags": _result(_json_line({"status": "success"}))},
    ],
    ids=("versioning", "retention", "encryption", "tags"),
)
def test_capture_fails_closed_when_an_unsupported_bucket_feature_is_present(
    tmp_path: Path,
    override: dict[str, object],
) -> None:
    transport = _RecordingTransport(_capture_actions(**override))  # type: ignore[arg-type]
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert not source_manifest_path.exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "Suspended"),
        ("MFADelete", "Enabled"),
        ("extra", "unexpected"),
    ],
)
def test_capture_accepts_only_the_exact_disabled_version_shape(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    versioning = {"status": "", "MFADelete": ""}
    record: dict[str, object] = {
        "Op": "info",
        "status": "success",
        "url": "local/artifacts",
        "versioning": versioning,
    }
    if field in versioning:
        versioning[field] = value
    else:
        record[field] = value
    transport = _RecordingTransport(_capture_actions(version=_json_line(record)))
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert not source_manifest_path.exists()


def _replace_run(
    actions: list[_Run | _Stream],
    arguments: tuple[str, ...],
    result: PersonalDevMinioCommandResult,
    *,
    occurrence: int = 0,
) -> None:
    matches = [
        index
        for index, action in enumerate(actions)
        if isinstance(action, _Run) and action.arguments == arguments
    ]
    index = matches[occurrence]
    actions[index] = _Run(arguments, result)


@pytest.mark.parametrize("preexisting", ("manifest", "payload-root", "dangling-manifest"))
def test_capture_rejects_every_preexisting_output_path_before_transport(
    tmp_path: Path,
    preexisting: str,
) -> None:
    source_manifest_path, payload_root = _capture_paths(tmp_path)
    if preexisting == "manifest":
        source_manifest_path.write_bytes(b"already here")
    elif preexisting == "payload-root":
        payload_root.mkdir(mode=0o700)
    else:
        source_manifest_path.symlink_to(tmp_path / "missing-sensitive-marker")
    transport = _RecordingTransport(())

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert transport.calls == []
    assert payload_root.exists() is (preexisting == "payload-root")


@pytest.mark.parametrize("drift", ("addition", "removal", "size", "metadata"))
def test_capture_does_not_publish_when_live_authority_drifts(
    tmp_path: Path,
    drift: str,
) -> None:
    actions = _capture_actions()
    if drift == "addition":
        _replace_run(
            actions,
            ("ls", "--recursive", "--json", "local/trajectories"),
            _result(_json_line(_list_record(key="added", size=1))),
            occurrence=1,
        )
    elif drift == "removal":
        _replace_run(
            actions,
            ("ls", "--recursive", "--json", "local/artifacts"),
            _result(),
            occurrence=1,
        )
    elif drift == "size":
        _replace_run(
            actions,
            ("ls", "--recursive", "--json", "local/artifacts"),
            _result(_json_line(_list_record(size=8))),
            occurrence=1,
        )
    else:
        stat = _stat_record()
        stat["metadata"] = {"Content-Type": "application/octet-stream"}
        _replace_run(
            actions,
            ("stat", "--json", "local/artifacts/personal-dev/source/candidate.tar"),
            _result(_json_line(stat)),
            occurrence=1,
        )
    transport = _RecordingTransport(actions)
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert not source_manifest_path.exists()


@pytest.mark.parametrize("mismatch", ("digest", "size"))
def test_capture_requires_transport_and_filesystem_payload_integrity(
    tmp_path: Path,
    mismatch: str,
) -> None:
    actions = _capture_actions()
    stream_index = next(
        index for index, action in enumerate(actions) if isinstance(action, _Stream)
    )
    stream = actions[stream_index]
    assert isinstance(stream, _Stream)
    actions[stream_index] = (
        _Stream(stream.arguments, stream.payload, digest="0" * 64)
        if mismatch == "digest"
        else _Stream(stream.arguments, b"short", expected_size=7)
    )
    transport = _RecordingTransport(actions)
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert not source_manifest_path.exists()


@pytest.mark.parametrize("limit", ("count", "total"))
def test_capture_rejects_fixed_inventory_limits_before_streaming(
    tmp_path: Path,
    limit: str,
) -> None:
    actions = _capture_actions()
    if limit == "count":
        artifact_records = b"".join(
            _json_line(_list_record(key=f"object-{index:05d}", size=0)) for index in range(5_001)
        )
        trajectory_records = b"".join(
            _json_line(_list_record(key=f"object-{index:05d}", size=0)) for index in range(5_001)
        )
    else:
        maximum_object_bytes = 64 * 1024 * 1024 * 1024
        artifact_records = b"".join(
            _json_line(_list_record(key=f"object-{index:02d}", size=maximum_object_bytes))
            for index in range(9)
        )
        trajectory_records = b"".join(
            _json_line(_list_record(key=f"object-{index:02d}", size=maximum_object_bytes))
            for index in range(8)
        )
    _replace_run(
        actions,
        ("ls", "--recursive", "--json", "local/artifacts"),
        _result(artifact_records),
    )
    _replace_run(
        actions,
        ("ls", "--recursive", "--json", "local/trajectories"),
        _result(trajectory_records),
    )
    transport = _RecordingTransport(actions)
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert all(kind != "stream" for kind, _ in transport.calls)
    assert not source_manifest_path.exists()


@pytest.mark.parametrize("failure", ("excess-output", "nonzero", "sensitive-exception"))
def test_capture_sanitizes_transport_and_output_failures(
    tmp_path: Path,
    failure: str,
) -> None:
    marker = "credential-key-sensitive-marker"
    actions = _capture_actions()
    first = actions[0]
    assert isinstance(first, _Run)
    if failure == "excess-output":
        actions[0] = _Run(first.arguments, _result(b"x" * (1024 * 1024 + 1)))
        transport: _RecordingTransport = _RecordingTransport(actions)
    elif failure == "nonzero":
        internal = _result(marker.encode(), returncode=1)
        actions[0] = _Run(first.arguments, internal)
        transport = _RecordingTransport(actions)
        assert marker in internal.stdout.decode()
    else:

        class _SensitiveTransport(_RecordingTransport):
            def run(
                self,
                arguments: Sequence[str],
                *,
                maximum_stdout_bytes: int,
                timeout_seconds: int,
            ) -> PersonalDevMinioCommandResult:
                del arguments, maximum_stdout_bytes, timeout_seconds
                raise TimeoutError(marker)

        transport = _SensitiveTransport(actions)
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError) as raised:
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert str(raised.value) == "personal-dev MinIO backup is invalid"
    assert marker not in str(raised.value)
    assert raised.value.__cause__ is None
    assert not source_manifest_path.exists()


def test_failed_capture_root_is_owner_only_and_never_reusable(tmp_path: Path) -> None:
    actions = _capture_actions()
    actions[1] = _Run(actions[1].arguments, _result(b"not-json"))  # type: ignore[union-attr]
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=_RecordingTransport(actions),
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert payload_root.stat().st_mode & 0o777 == 0o700
    second_transport = _RecordingTransport(_capture_actions())
    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=second_transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )
    assert second_transport.calls == []


@pytest.mark.parametrize("spelling", ("same", "direct", "dotdot"))
def test_capture_never_places_the_manifest_inside_the_payload_inventory(
    tmp_path: Path,
    spelling: str,
) -> None:
    backup_root = tmp_path / "backup"
    backup_root.mkdir(mode=0o700)
    payload_root = backup_root / "payloads"
    source_manifest_path = {
        "same": payload_root,
        "direct": payload_root / "source.json",
        "dotdot": payload_root / ".." / "payloads" / "source.json",
    }[spelling]

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=_RecordingTransport(_capture_actions()),
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert not source_manifest_path.exists()


def test_capture_rejects_a_no_tags_message_without_its_bounded_target(
    tmp_path: Path,
) -> None:
    malformed = _json_line(
        {
            "status": "error",
            "error": {
                "message": "No tags found  for ",
                "cause": {
                    "message": "check 'mc tag set --help' on how to set tags",
                    "error": {},
                },
                "type": "fatal",
            },
        }
    )
    transport = _RecordingTransport(_capture_actions(tags=_result(stderr=malformed, returncode=1)))
    source_manifest_path, payload_root = _capture_paths(tmp_path)

    with pytest.raises(PersonalDevMinioBackupError):
        capture_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )

    assert not source_manifest_path.exists()


def _source_authority(
    tmp_path: Path,
) -> tuple[Path, Path, Path, PersonalDevMinioObject]:
    backup_root = tmp_path / "backup-authority"
    backup_root.mkdir(mode=0o700)
    payload_root = backup_root / "payloads"
    payload_root.mkdir(mode=0o700)
    payload = b"payload"
    digest = hashlib.sha256(payload).hexdigest()
    payload_path = payload_root / digest
    payload_path.write_bytes(payload)
    payload_path.chmod(0o600)
    object = PersonalDevMinioObject(
        bucket="artifacts",
        key="personal-dev/source/candidate.tar",
        payload_sha256=digest,
        size_bytes=len(payload),
        content_type="application/x-tar",
        cache_control="no-cache",
        metadata={"archive-sha256": "a" * 64},
    )
    manifest = build_personal_dev_minio_manifest((object,))
    source_manifest_path = backup_root / "source.json"
    write_personal_dev_minio_manifest(source_manifest_path, manifest)
    return source_manifest_path, payload_root, backup_root / "restored.json", object


def _restore_actions(
    *,
    payload_root: Path,
    object: PersonalDevMinioObject,
    stat_overrides: Mapping[str, object] | None = None,
    artifact_listing: bytes | None = None,
    trajectory_listing: bytes = b"",
    readback_digest: str | None = None,
) -> list[_Run | _Stream]:
    target = f"restore/{object.bucket}/{object.key}"
    stat = _stat_record(size=object.size_bytes)
    stat["etag"] = "different-restored-etag"
    stat["lastModified"] = "2030-01-01T00:00:00Z"
    if stat_overrides:
        stat.update(stat_overrides)
    bucket_output = _json_line(_bucket_record("artifacts")) + _json_line(
        _bucket_record("trajectories")
    )
    listing = artifact_listing
    if listing is None:
        restored_list = _list_record(size=object.size_bytes)
        restored_list["etag"] = "different-restored-etag"
        restored_list["lastModified"] = "2030-01-01T00:00:00Z"
        listing = _json_line(restored_list)
    attrs = (
        "Content-Type=application/x-tar;Cache-Control=no-cache;"
        f"X-Amz-Meta-archive-sha256={'a' * 64}"
    )
    actions: list[_Run | _Stream] = [
        _Run(("mb", "restore/artifacts"), _result()),
        _Run(("mb", "restore/trajectories"), _result()),
        _Run(
            (
                "cp",
                "--attr",
                attrs,
                str(payload_root / object.payload_sha256),
                target,
            ),
            _result(),
        ),
        _Run(("ls", "--json", "restore"), _result(bucket_output)),
    ]
    for bucket in ("artifacts", "trajectories"):
        actions.append(
            _Run(
                ("version", "info", "--json", f"restore/{bucket}"),
                _result(_version_disabled("restore", bucket)),
            )
        )
    for bucket in ("artifacts", "trajectories"):
        actions.append(
            _Run(
                ("retention", "info", "--json", f"restore/{bucket}"),
                _result(stderr=_retention_absent(), returncode=1),
            )
        )
    for bucket in ("artifacts", "trajectories"):
        actions.append(
            _Run(
                ("encrypt", "info", "--json", f"restore/{bucket}"),
                _result(stderr=_encryption_absent(bucket), returncode=1),
            )
        )
    actions.extend(
        [
            _Run(
                ("ls", "--recursive", "--json", "restore/artifacts"),
                _result(listing),
            ),
            _Run(
                ("ls", "--recursive", "--json", "restore/trajectories"),
                _result(trajectory_listing),
            ),
            _Run(
                ("tag", "list", "--json", target),
                _result(
                    stderr=_tags_absent(f"artifacts/{object.key}"),
                    returncode=1,
                ),
            ),
            _Run(("stat", "--json", target), _result(_json_line(stat))),
            _Stream(
                ("cat", target),
                b"payload",
                digest=readback_digest,
                expected_size=object.size_bytes,
                destination_none=True,
            ),
        ]
    )
    return actions


def test_restore_recreates_only_fixed_buckets_with_safe_copy_argv_and_readback(
    tmp_path: Path,
) -> None:
    source_manifest_path, payload_root, restored_manifest_path, object = _source_authority(tmp_path)
    actions = _restore_actions(payload_root=payload_root, object=object)
    transport = _RecordingTransport(actions)

    restored = restore_personal_dev_minio_backup(
        transport=transport,
        source_manifest_path=source_manifest_path,
        payload_root=payload_root,
        restored_manifest_path=restored_manifest_path,
    )

    assert not transport.actions
    assert transport.calls == [
        ("run", action.arguments) if isinstance(action, _Run) else ("stream", action.arguments)
        for action in actions
    ]
    cp = next(action for action in actions if action.arguments[0] == "cp")
    assert isinstance(cp, _Run)
    assert cp.arguments[-2] == str(payload_root / object.payload_sha256)
    assert cp.arguments[-1] == f"restore/{object.bucket}/{object.key}"
    assert restored.canonical_bytes == source_manifest_path.read_bytes()
    assert restored_manifest_path.read_bytes() == source_manifest_path.read_bytes()


@pytest.mark.parametrize(
    "mismatch",
    ("payload-sha", "size", "content-type", "cache-control", "metadata", "extra", "missing"),
)
def test_restore_rejects_every_restored_authority_mismatch(
    tmp_path: Path,
    mismatch: str,
) -> None:
    source_manifest_path, payload_root, restored_manifest_path, object = _source_authority(tmp_path)
    kwargs: dict[str, object] = {}
    if mismatch == "payload-sha":
        kwargs["readback_digest"] = "0" * 64
    elif mismatch == "size":
        kwargs["artifact_listing"] = _json_line(_list_record(size=8))
    elif mismatch in {"content-type", "cache-control", "metadata"}:
        metadata = dict(_stat_record()["metadata"])  # type: ignore[arg-type]
        if mismatch == "content-type":
            metadata["Content-Type"] = "application/octet-stream"
        elif mismatch == "cache-control":
            metadata["Cache-Control"] = "max-age=60"
        else:
            metadata["X-Amz-Meta-Archive-Sha256"] = "b" * 64
        kwargs["stat_overrides"] = {"metadata": metadata}
    elif mismatch == "extra":
        kwargs["trajectory_listing"] = _json_line(_list_record(key="unexpected", size=1))
    else:
        kwargs["artifact_listing"] = b""
    transport = _RecordingTransport(
        _restore_actions(payload_root=payload_root, object=object, **kwargs)  # type: ignore[arg-type]
    )

    with pytest.raises(PersonalDevMinioBackupError):
        restore_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
            restored_manifest_path=restored_manifest_path,
        )

    assert not restored_manifest_path.exists()


def test_restore_validates_all_retained_payloads_before_bucket_creation(tmp_path: Path) -> None:
    source_manifest_path, payload_root, restored_manifest_path, object = _source_authority(tmp_path)
    (payload_root / object.payload_sha256).write_bytes(b"corrupt")
    (payload_root / object.payload_sha256).chmod(0o600)
    transport = _RecordingTransport(())

    with pytest.raises(PersonalDevMinioBackupError):
        restore_personal_dev_minio_backup(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
            restored_manifest_path=restored_manifest_path,
        )

    assert transport.calls == []
    assert not restored_manifest_path.exists()


@pytest.mark.parametrize("spelling", ("direct", "dotdot"))
def test_restore_never_places_the_manifest_inside_the_payload_inventory(
    tmp_path: Path,
    spelling: str,
) -> None:
    source_manifest_path, payload_root, _, object = _source_authority(tmp_path)
    restored_manifest_path = (
        payload_root / "restored.json"
        if spelling == "direct"
        else payload_root / ".." / "payloads" / "restored.json"
    )

    with pytest.raises(PersonalDevMinioBackupError):
        restore_personal_dev_minio_backup(
            transport=_RecordingTransport(
                _restore_actions(payload_root=payload_root, object=object)
            ),
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
            restored_manifest_path=restored_manifest_path,
        )

    assert not restored_manifest_path.exists()


def test_restore_revalidates_retained_inventory_before_publication(tmp_path: Path) -> None:
    source_manifest_path, payload_root, restored_manifest_path, object = _source_authority(tmp_path)
    actions = _restore_actions(payload_root=payload_root, object=object)

    class _MutatingTransport(_RecordingTransport):
        def run(
            self,
            arguments: Sequence[str],
            *,
            maximum_stdout_bytes: int,
            timeout_seconds: int,
        ) -> PersonalDevMinioCommandResult:
            if tuple(arguments) == ("ls", "--json", "restore"):
                path = payload_root / object.payload_sha256
                path.write_bytes(b"corrupt")
                path.chmod(0o600)
            return super().run(
                arguments,
                maximum_stdout_bytes=maximum_stdout_bytes,
                timeout_seconds=timeout_seconds,
            )

    with pytest.raises(PersonalDevMinioBackupError):
        restore_personal_dev_minio_backup(
            transport=_MutatingTransport(actions),
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
            restored_manifest_path=restored_manifest_path,
        )

    assert not restored_manifest_path.exists()
