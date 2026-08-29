from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

import pytest

import loom.personal_dev_minio_backup as minio_backup
from loom.personal_dev_minio_backup import (
    PersonalDevMinioBackupError,
    PersonalDevMinioListedObject,
    PersonalDevMinioManifest,
    PersonalDevMinioObject,
    build_personal_dev_minio_manifest,
    install_personal_dev_minio_payload,
    load_personal_dev_minio_manifest,
    normalize_personal_dev_minio_object,
    parse_personal_dev_minio_listing,
    personal_dev_minio_restore_attributes,
    validate_personal_dev_minio_payload_root,
    write_personal_dev_minio_manifest,
)

_ERROR_PATTERN = r"^personal-dev MinIO backup is invalid$"


def _object(*, payload: bytes = b"archive payload") -> PersonalDevMinioObject:
    return PersonalDevMinioObject(
        bucket="artifacts",
        key="personal-dev/source/candidate.tar",
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        content_type="application/x-tar",
        cache_control=None,
        metadata={"archive-sha256": "a" * 64},
    )


def test_manifest_preserves_the_legacy_empty_canonical_bytes() -> None:
    empty = build_personal_dev_minio_manifest(())

    assert empty.canonical_bytes == b'{"buckets":["artifacts","trajectories"],"objects":[]}'
    assert empty.object_count == 0
    assert empty.total_payload_bytes == 0
    assert empty.payload_inventory_bytes == (
        b'{"payloads":[],"schema":"loom-personal-dev-minio-payload-inventory-v1"}'
    )


def test_manifest_canonicalizes_a_nonempty_object() -> None:
    payload = b"archive payload"
    nonempty = build_personal_dev_minio_manifest((_object(payload=payload),))

    assert json.loads(nonempty.canonical_bytes) == {
        "buckets": ["artifacts", "trajectories"],
        "objects": [
            {
                "bucket": "artifacts",
                "cache_control": None,
                "content_type": "application/x-tar",
                "key": "personal-dev/source/candidate.tar",
                "metadata": {"archive-sha256": "a" * 64},
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
        ],
        "schema": "loom-personal-dev-minio-backup-manifest-v1",
    }


def _manifest_bytes(objects: list[dict[str, object]], buckets: list[str]) -> bytes:
    return json.dumps(
        {
            "buckets": buckets,
            "objects": objects,
            "schema": "loom-personal-dev-minio-backup-manifest-v1",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


def test_manifest_loader_rejects_non_authoritative_shapes(tmp_path: Path) -> None:
    # A relaxed parser or canonicalizer accepting this payload would make this fail.
    objects = [
        {
            "bucket": "trajectories",
            "cache_control": None,
            "content_type": "text/plain",
            "key": "z",
            "metadata": {},
            "payload_sha256": "a" * 64,
            "size_bytes": 0,
        },
        {
            "bucket": "artifacts",
            "cache_control": None,
            "content_type": "text/plain",
            "key": "a",
            "metadata": {},
            "payload_sha256": "b" * 64,
            "size_bytes": 0,
        },
    ]
    path = tmp_path / "manifest.json"
    for payload in (
        _manifest_bytes(objects, ["artifacts", "trajectories"]),
        _manifest_bytes(
            [objects[1], {**objects[1], "payload_sha256": "a" * 64}], ["artifacts", "trajectories"]
        ),
        b'{"buckets":["artifacts","trajectories"],"objects":[],"objects":[]}',
        b'{"buckets":["artifacts","trajectories"],"objects":[{}]}',
        _manifest_bytes([], ["artifacts", "other"]),
    ):
        path.write_bytes(payload)
        with pytest.raises(
            PersonalDevMinioBackupError,
            match=_ERROR_PATTERN,
        ):
            load_personal_dev_minio_manifest(path)


def _list_record(
    *, key: object = "personal-dev/source/candidate.tar", size: object = 1
) -> dict[str, object]:
    return {
        "status": "success",
        "type": "file",
        "key": key,
        "size": size,
        "etag": "opaque-observation",
        "lastModified": "2026-08-29T00:00:00Z",
    }


def _stat_record(
    *,
    name: object = "personal-dev/source/candidate.tar",
    size: object = 1,
    metadata: object | None = None,
) -> dict[str, object]:
    return {
        "status": "success",
        "type": "file",
        "name": name,
        "size": size,
        "etag": "opaque-observation",
        "lastModified": "2026-08-29T00:00:00Z",
        "metadata": (
            {
                "Content-Type": "text/plain",
                "X-Amz-Meta-Archive-Sha256": "a" * 64,
            }
            if metadata is None
            else metadata
        ),
    }


def _json_line(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("ascii") + b"\n"


def test_listing_and_stat_normalization_preserve_only_restorable_authority(tmp_path: Path) -> None:
    payload = b"x"
    payload_path = tmp_path / "temporary"
    payload_path.write_bytes(payload)
    payload_path.chmod(0o600)
    listed = parse_personal_dev_minio_listing(_json_line(_list_record()), bucket="artifacts")

    assert listed == (
        PersonalDevMinioListedObject(
            bucket="artifacts",
            key="personal-dev/source/candidate.tar",
            size_bytes=1,
        ),
    )
    normalized = normalize_personal_dev_minio_object(
        listed=listed[0],
        stat_payload=_json_line(_stat_record()),
        payload_path=payload_path,
    )

    assert normalized == PersonalDevMinioObject(
        bucket="artifacts",
        key="personal-dev/source/candidate.tar",
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=1,
        content_type="text/plain",
        cache_control=None,
        metadata={"archive-sha256": "a" * 64},
    )
    assert personal_dev_minio_restore_attributes(normalized) == (
        "Content-Type=text/plain;X-Amz-Meta-archive-sha256=" + "a" * 64
    )


@pytest.mark.parametrize(
    "key",
    [
        "bad\x00key",
        "bad\\key",
        "/leading",
        "repeated//slash",
        "dot/./segment",
        "dot/../segment",
        "a" * 1025,
    ],
)
def test_listing_rejects_unsafe_or_oversized_keys(key: str) -> None:
    # Removing the key gate would allow a raw S3 identity to become unsafe authority.
    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        parse_personal_dev_minio_listing(_json_line(_list_record(key=key)), bucket="artifacts")


def test_listing_keeps_a_semicolon_in_a_safe_key_as_an_argv_identity() -> None:
    # Treating a key as an --attr value would incorrectly reject this safe positional identity.
    assert parse_personal_dev_minio_listing(
        _json_line(_list_record(key="personal-dev/source/candidate;v2.tar")),
        bucket="artifacts",
    ) == (
        PersonalDevMinioListedObject(
            bucket="artifacts",
            key="personal-dev/source/candidate;v2.tar",
            size_bytes=1,
        ),
    )


@pytest.mark.parametrize(
    "record",
    [
        {**_list_record(), "unexpected": "value"},
        {**_list_record(), "status": "error"},
        {**_list_record(), "type": "directory"},
        {**_list_record(), "versionId": "version"},
        _list_record(size=-1),
        _list_record(size=64 * 1024 * 1024 * 1024 + 1),
    ],
)
def test_listing_rejects_any_unpinned_surface_or_out_of_range_size(
    record: dict[str, object],
) -> None:
    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        parse_personal_dev_minio_listing(_json_line(record), bucket="artifacts")


@pytest.mark.parametrize(
    "metadata",
    [
        {"Content-Type": "text/plain", "X-Amz-Server-Side-Encryption": "AES256"},
        {"Content-Type": "text/plain", "X-Amz-Object-Lock-Mode": "GOVERNANCE"},
        {
            "Content-Type": "text/plain",
            **{f"X-Amz-Meta-Key-{index}": "value" for index in range(65)},
        },
        {"Content-Type": "text/plain", "X-Amz-Meta-" + "k" * 129: "value"},
        {"Content-Type": "text/plain", "X-Amz-Meta-Key": "v" * 2049},
        {"Content-Type": "text/plain", "X-Amz-Meta-Key": "unsafe;delimiter"},
        {"Content-Type": "text/plain; charset=utf-8"},
        {"Content-Type": "not-a-content-type"},
    ],
)
def test_stat_rejects_unsupported_or_unsafe_metadata(
    tmp_path: Path,
    metadata: dict[str, str],
) -> None:
    payload_path = tmp_path / "temporary"
    payload_path.write_bytes(b"x")
    listed = PersonalDevMinioListedObject("artifacts", "personal-dev/source/candidate.tar", 1)

    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        normalize_personal_dev_minio_object(
            listed=listed,
            stat_payload=_json_line(_stat_record(metadata=metadata)),
            payload_path=payload_path,
        )


@pytest.mark.parametrize(
    "record",
    [
        {**_stat_record(), "unexpected": "value"},
        {**_stat_record(), "versionId": "version"},
        _stat_record(size=-1),
        _stat_record(size=2),
        _stat_record(name="a-sensitive-marker-that-must-not-leak"),
    ],
)
def test_stat_rejection_has_only_the_stable_non_sensitive_error(
    tmp_path: Path,
    record: dict[str, object],
) -> None:
    payload_path = tmp_path / "temporary"
    payload_path.write_bytes(b"x")
    listed = PersonalDevMinioListedObject("artifacts", "personal-dev/source/candidate.tar", 1)

    with pytest.raises(PersonalDevMinioBackupError) as raised:
        normalize_personal_dev_minio_object(
            listed=listed,
            stat_payload=_json_line(record),
            payload_path=payload_path,
        )

    assert str(raised.value) == "personal-dev MinIO backup is invalid"
    assert "sensitive-marker" not in str(raised.value)


def _payload_manifest(
    payload: bytes = b"payload",
) -> tuple[PersonalDevMinioManifest, PersonalDevMinioObject]:
    object = _object(payload=payload)
    return build_personal_dev_minio_manifest((object,)), object


def _install_payload(
    tmp_path: Path,
    *,
    payload: bytes = b"payload",
) -> tuple[Path, PersonalDevMinioManifest, PersonalDevMinioObject]:
    manifest, object = _payload_manifest(payload)
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)
    temporary_path = payload_root / "temporary"
    temporary_path.write_bytes(payload)
    temporary_path.chmod(0o600)
    installed = install_personal_dev_minio_payload(
        temporary_path=temporary_path,
        payload_root=payload_root,
        object=object,
    )
    assert installed == payload_root / object.payload_sha256
    return payload_root, manifest, object


def test_install_creates_an_owner_only_content_addressed_payload_root(tmp_path: Path) -> None:
    payload_root, manifest, object = _install_payload(tmp_path)
    installed = payload_root / object.payload_sha256

    assert stat.S_IMODE(payload_root.stat().st_mode) == 0o700
    assert stat.S_IMODE(installed.stat().st_mode) == 0o600
    assert installed.stat().st_nlink == 1
    assert installed.read_bytes() == b"payload"
    assert validate_personal_dev_minio_payload_root(manifest, payload_root) == (
        b'{"payloads":[{"sha256":"'
        + object.payload_sha256.encode("ascii")
        + b'","size_bytes":7}],"schema":"loom-personal-dev-minio-payload-inventory-v1"}'
    )


def test_payload_install_accepts_zero_byte_objects(tmp_path: Path) -> None:
    payload_root, manifest, object = _install_payload(tmp_path, payload=b"")

    assert (payload_root / object.payload_sha256).read_bytes() == b""
    assert (
        validate_personal_dev_minio_payload_root(manifest, payload_root)
        == manifest.payload_inventory_bytes
    )


def test_payload_install_deduplicates_identical_content_without_retaining_a_hard_link(
    tmp_path: Path,
) -> None:
    payload_root, manifest, object = _install_payload(tmp_path)
    temporary_path = payload_root / "second-temporary"
    temporary_path.write_bytes(b"payload")
    temporary_path.chmod(0o600)

    installed = install_personal_dev_minio_payload(
        temporary_path=temporary_path,
        payload_root=payload_root,
        object=object,
    )

    assert installed == payload_root / object.payload_sha256
    assert not temporary_path.exists()
    assert installed.stat().st_nlink == 1
    assert (
        validate_personal_dev_minio_payload_root(manifest, payload_root)
        == manifest.payload_inventory_bytes
    )


def _replace_payload_with_symlink(root: Path, object: PersonalDevMinioObject) -> None:
    (root / object.payload_sha256).unlink()
    os.symlink("elsewhere", root / object.payload_sha256)


@pytest.mark.parametrize(
    ("name", "mutate"),
    [
        ("extra file", lambda root, object: (root / "extra").write_bytes(b"extra")),
        ("missing payload", lambda root, object: (root / object.payload_sha256).unlink()),
        (
            "wrong digest",
            lambda root, object: (root / object.payload_sha256).write_bytes(b"wrong!!"),
        ),
        ("wrong mode", lambda root, object: (root / object.payload_sha256).chmod(0o644)),
        (
            "hard link",
            lambda root, object: os.link(root / object.payload_sha256, root / ("f" * 64)),
        ),
        ("symlink", _replace_payload_with_symlink),
    ],
)
def test_payload_root_rejects_any_extra_missing_or_unsafe_payload_entry(
    tmp_path: Path,
    name: str,
    mutate: object,
) -> None:
    payload_root, manifest, object = _install_payload(tmp_path)
    mutate(payload_root, object)

    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        validate_personal_dev_minio_payload_root(manifest, payload_root)


def test_payload_root_rejects_wrong_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload_root, manifest, _ = _install_payload(tmp_path)
    original_geteuid = os.geteuid
    monkeypatch.setattr(minio_backup.os, "geteuid", lambda: original_geteuid() + 1)

    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        validate_personal_dev_minio_payload_root(manifest, payload_root)


def test_payload_root_rejects_root_or_payload_mutation_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload_root, manifest, object = _install_payload(tmp_path)
    original_read = minio_backup.os.read
    mutated = False

    def mutate_after_read(fd: int, amount: int) -> bytes:
        nonlocal mutated
        result = original_read(fd, amount)
        if result and not mutated:
            mutated = True
            (payload_root / object.payload_sha256).write_bytes(b"changed")
            (payload_root / "extra").write_bytes(b"extra")
        return result

    monkeypatch.setattr(minio_backup.os, "read", mutate_after_read)

    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        validate_personal_dev_minio_payload_root(manifest, payload_root)


def test_nonempty_manifest_rejects_an_empty_payload_root(tmp_path: Path) -> None:
    manifest, _ = _payload_manifest()
    payload_root = tmp_path / "payloads"
    payload_root.mkdir(mode=0o700)

    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        validate_personal_dev_minio_payload_root(manifest, payload_root)


def test_manifest_write_is_exclusive_and_owner_only(tmp_path: Path) -> None:
    manifest = build_personal_dev_minio_manifest(())
    path = tmp_path / "manifest.json"

    write_personal_dev_minio_manifest(path, manifest)

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.read_bytes() == manifest.canonical_bytes
    assert load_personal_dev_minio_manifest(path) == manifest
    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        write_personal_dev_minio_manifest(path, manifest)


def test_manifest_loader_rejects_non_owner_only_files_and_symlinks(tmp_path: Path) -> None:
    # Removing the owner-only no-follow check would let a substituted evidence file load.
    manifest = build_personal_dev_minio_manifest(())
    target = tmp_path / "target.json"
    write_personal_dev_minio_manifest(target, manifest)
    target.chmod(0o644)
    link = tmp_path / "link.json"
    os.symlink(target, link)

    for path in (target, link):
        with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
            load_personal_dev_minio_manifest(path)


def test_manifest_write_rejects_a_non_owner_only_parent_directory(tmp_path: Path) -> None:
    # Removing the parent-directory gate would place evidence in a writable location.
    unsafe_parent = tmp_path / "unsafe"
    unsafe_parent.mkdir(mode=0o755)
    unsafe_parent.chmod(0o755)

    with pytest.raises(PersonalDevMinioBackupError, match=_ERROR_PATTERN):
        write_personal_dev_minio_manifest(
            unsafe_parent / "manifest.json",
            build_personal_dev_minio_manifest(()),
        )


def test_public_parsers_reject_non_bytes_without_leaking_input() -> None:
    # Removing strict input typing would expose parser implementation failures to callers.
    with pytest.raises(PersonalDevMinioBackupError) as raised:
        parse_personal_dev_minio_listing("sensitive-marker", bucket="artifacts")  # type: ignore[arg-type]

    assert str(raised.value) == "personal-dev MinIO backup is invalid"
    assert "sensitive-marker" not in str(raised.value)
