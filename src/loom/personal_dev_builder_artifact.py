"""Strict verification for untrusted native personal-build artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tarfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevPlatform,
)

_DIGEST_PREFIX = "sha256:"
_OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
_OCI_INDEX_MEDIA_TYPE = "application/vnd.oci.image.index.v1+json"
_OCI_CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json"
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_OCI_MEMBERS = 100_000


class PersonalDevBuildArtifactError(RuntimeError):
    """A sandbox artifact is malformed, incomplete, or not exactly bound."""


@dataclass(frozen=True, slots=True)
class VerifiedPersonalDevImageArtifact:
    component: str
    platform: PersonalDevPlatform
    archive_path: Path
    archive_sha256: str
    archive_size_bytes: int
    manifest_digest: str


@dataclass(frozen=True, slots=True)
class VerifiedPersonalDevBuildArtifact:
    platform: PersonalDevPlatform
    manifest_sha256: str
    images: Mapping[str, VerifiedPersonalDevImageArtifact]


def _safe_member_name(name: str) -> PurePosixPath:
    value = PurePosixPath(name)
    if (
        not name
        or name == "."
        or value.is_absolute()
        or ".." in value.parts
        or any(part in {"", "."} for part in value.parts)
        or "\\" in name
        or len(name.encode("utf-8")) > 240
    ):
        raise PersonalDevBuildArtifactError("personal-dev build artifact has an unsafe member")
    return value


def _json_object(
    payload: bytes,
    *,
    label: str,
    require_canonical: bool,
) -> dict[str, Any]:
    if len(payload) > _MAX_MANIFEST_BYTES:
        raise PersonalDevBuildArtifactError(f"personal-dev {label} exceeds its limit")
    try:
        value = json.loads(payload)
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise PersonalDevBuildArtifactError(
            f"personal-dev {label} is not canonical JSON"
        ) from exc
    if (require_canonical and canonical != payload) or not isinstance(value, dict):
        raise PersonalDevBuildArtifactError(f"personal-dev {label} is not canonical JSON")
    return value


def _digest(value: object, *, label: str, prefixed: bool) -> str:
    if not isinstance(value, str):
        raise PersonalDevBuildArtifactError(f"personal-dev {label} digest is invalid")
    raw = value.removeprefix(_DIGEST_PREFIX) if prefixed else value
    if len(raw) != 64 or any(character not in "0123456789abcdef" for character in raw):
        raise PersonalDevBuildArtifactError(f"personal-dev {label} digest is invalid")
    if prefixed and not value.startswith(_DIGEST_PREFIX):
        raise PersonalDevBuildArtifactError(f"personal-dev {label} digest is invalid")
    return value


def _exact_positive_int(value: object, *, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise PersonalDevBuildArtifactError(f"personal-dev {label} is invalid")
    return value


def _read_tar_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    if member.size > _MAX_MANIFEST_BYTES:
        raise PersonalDevBuildArtifactError("personal-dev build manifest exceeds its limit")
    stream = archive.extractfile(member)
    if stream is None:
        raise PersonalDevBuildArtifactError("personal-dev build artifact member is unavailable")
    payload = stream.read(_MAX_MANIFEST_BYTES + 1)
    if len(payload) != member.size:
        raise PersonalDevBuildArtifactError("personal-dev build artifact member is truncated")
    return payload


def _stream_member_to_file(
    archive: tarfile.TarFile,
    member: tarfile.TarInfo,
    *,
    output_directory_fd: int,
    output_name: str,
) -> tuple[str, int]:
    stream = archive.extractfile(member)
    if stream is None:
        raise PersonalDevBuildArtifactError("personal-dev image archive is unavailable")
    descriptor = os.open(
        output_name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
        dir_fd=output_directory_fd,
    )
    observed = 0
    digest = hashlib.sha256()
    try:
        while chunk := stream.read(1024 * 1024):
            observed += len(chunk)
            if observed > member.size:
                raise PersonalDevBuildArtifactError(
                    "personal-dev image archive exceeded its binding"
                )
            digest.update(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if observed != member.size:
        raise PersonalDevBuildArtifactError("personal-dev image archive is truncated")
    return digest.hexdigest(), observed


def _oci_json(
    archive: tarfile.TarFile,
    members: Mapping[str, tarfile.TarInfo],
    name: str,
    *,
    label: str,
) -> dict[str, Any]:
    try:
        member = members[name]
    except KeyError:
        raise PersonalDevBuildArtifactError(f"personal-dev OCI {label} is missing") from None
    return _json_object(
        _read_tar_member(archive, member),
        label=f"OCI {label}",
        require_canonical=False,
    )


def _descriptor_digest(value: object, *, label: str) -> tuple[str, int]:
    if not isinstance(value, dict):
        raise PersonalDevBuildArtifactError(f"personal-dev OCI {label} is invalid")
    allowed = {"mediaType", "digest", "size", "annotations", "platform"}
    if not {"mediaType", "digest", "size"} <= set(value) or not set(value) <= allowed:
        raise PersonalDevBuildArtifactError(f"personal-dev OCI {label} is invalid")
    digest = _digest(value["digest"], label=f"OCI {label}", prefixed=True)
    size = _exact_positive_int(value["size"], label=f"OCI {label} size")
    return digest, size


def verify_personal_dev_oci_image_archive(
    path: Path,
    *,
    platform: PersonalDevPlatform,
    max_bytes: int,
    expected_manifest_digest: str | None = None,
) -> str:
    """Verify one native OCI layout archive and return its manifest digest."""
    if platform not in PERSONAL_DEV_PLATFORMS or max_bytes <= 0:
        raise ValueError("personal-dev OCI verification inputs are invalid")
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= max_bytes
        ):
            raise PersonalDevBuildArtifactError("personal-dev OCI archive authority is invalid")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            try:
                archive = tarfile.open(fileobj=stream, mode="r:")
            except tarfile.TarError as exc:
                raise PersonalDevBuildArtifactError(
                    "personal-dev OCI archive is not an uncompressed tar"
                ) from exc
            with archive:
                members: dict[str, tarfile.TarInfo] = {}
                total = 0
                for member in archive:
                    name = _safe_member_name(member.name).as_posix().rstrip("/")
                    if name in members:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI archive contains duplicate members"
                        )
                    if member.isdir():
                        if name not in {"blobs", "blobs/sha256"} or member.size != 0:
                            raise PersonalDevBuildArtifactError(
                                "personal-dev OCI archive contains an invalid directory"
                            )
                    elif not member.isreg():
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI archive contains a non-regular member"
                        )
                    else:
                        total += member.size
                    members[name] = member
                    if len(members) > _MAX_OCI_MEMBERS or total > max_bytes:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI archive exceeds its limits"
                        )

                regular = {name: member for name, member in members.items() if member.isreg()}
                if not {"oci-layout", "index.json"} <= set(regular):
                    raise PersonalDevBuildArtifactError("personal-dev OCI archive is incomplete")
                if any(
                    name not in {"oci-layout", "index.json"}
                    and (
                        not name.startswith("blobs/sha256/")
                        or len(name.removeprefix("blobs/sha256/")) != 64
                        or any(
                            character not in "0123456789abcdef"
                            for character in name.removeprefix("blobs/sha256/")
                        )
                    )
                    for name in regular
                ):
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI archive contains an unexpected member"
                    )
                layout = _oci_json(archive, regular, "oci-layout", label="layout")
                if layout != {"imageLayoutVersion": "1.0.0"}:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI layout version is invalid"
                    )
                index = _oci_json(archive, regular, "index.json", label="index")
                if (
                    set(index) != {"schemaVersion", "mediaType", "manifests"}
                    or index["schemaVersion"] != 2
                    or index["mediaType"] != _OCI_INDEX_MEDIA_TYPE
                    or not isinstance(index["manifests"], list)
                    or len(index["manifests"]) != 1
                ):
                    raise PersonalDevBuildArtifactError("personal-dev OCI index is invalid")
                image_descriptor = index["manifests"][0]
                manifest_digest, manifest_size = _descriptor_digest(
                    image_descriptor,
                    label="manifest descriptor",
                )
                architecture = platform.rsplit("/", 1)[1]
                if (
                    image_descriptor.get("mediaType") != _OCI_MANIFEST_MEDIA_TYPE
                    or image_descriptor.get("platform")
                    != {"architecture": architecture, "os": "linux"}
                    or (
                        expected_manifest_digest is not None
                        and manifest_digest != expected_manifest_digest
                    )
                ):
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI platform manifest binding is invalid"
                    )
                manifest_name = "blobs/sha256/" + manifest_digest.removeprefix(_DIGEST_PREFIX)
                manifest_member = regular.get(manifest_name)
                if manifest_member is None or manifest_member.size != manifest_size:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI platform manifest is unavailable"
                    )
                manifest = _oci_json(
                    archive,
                    regular,
                    manifest_name,
                    label="platform manifest",
                )
                if (
                    not {"schemaVersion", "mediaType", "config", "layers"} <= set(manifest)
                    or not set(manifest)
                    <= {"schemaVersion", "mediaType", "config", "layers", "annotations"}
                    or manifest["schemaVersion"] != 2
                    or manifest["mediaType"] != _OCI_MANIFEST_MEDIA_TYPE
                    or not isinstance(manifest["layers"], list)
                ):
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI platform manifest is invalid"
                    )
                config_digest, config_size = _descriptor_digest(
                    manifest["config"],
                    label="config descriptor",
                )
                if manifest["config"].get("mediaType") != _OCI_CONFIG_MEDIA_TYPE:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI config media type is invalid"
                    )
                referenced = {
                    manifest_digest: manifest_size,
                    config_digest: config_size,
                }
                for layer in manifest["layers"]:
                    layer_digest, layer_size = _descriptor_digest(
                        layer,
                        label="layer descriptor",
                    )
                    if (
                        layer_digest in referenced
                        and referenced[layer_digest] != layer_size
                    ):
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI descriptor sizes conflict"
                        )
                    referenced[layer_digest] = layer_size
                for digest, expected_size in referenced.items():
                    member_name = "blobs/sha256/" + digest.removeprefix(_DIGEST_PREFIX)
                    referenced_member = regular.get(member_name)
                    if referenced_member is None or referenced_member.size != expected_size:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI referenced blob is unavailable"
                        )
                    blob = archive.extractfile(referenced_member)
                    if blob is None:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI referenced blob is unavailable"
                        )
                    observed = hashlib.sha256()
                    count = 0
                    while chunk := blob.read(1024 * 1024):
                        count += len(chunk)
                        observed.update(chunk)
                    if (
                        count != referenced_member.size
                        or observed.hexdigest() != digest.removeprefix(_DIGEST_PREFIX)
                    ):
                        raise PersonalDevBuildArtifactError(
                            "personal-dev OCI blob digest is invalid"
                        )
                blob_digests = {
                    _DIGEST_PREFIX + name.removeprefix("blobs/sha256/")
                    for name in regular
                    if name.startswith("blobs/sha256/")
                }
                if blob_digests != set(referenced):
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI archive contains unreferenced blobs"
                    )
                config_name = "blobs/sha256/" + config_digest.removeprefix(_DIGEST_PREFIX)
                config_member = regular[config_name]
                if config_member.size != config_size:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI config size is invalid"
                    )
                config = _oci_json(archive, regular, config_name, label="image config")
                if config.get("os") != "linux" or config.get("architecture") != architecture:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev OCI image config platform is invalid"
                    )
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise PersonalDevBuildArtifactError("personal-dev OCI archive changed during verification")
    return manifest_digest


def verify_personal_dev_build_artifact(
    path: Path,
    registration: CandidateRegistration,
    *,
    platform: PersonalDevPlatform,
    output_directory: Path,
    max_artifact_bytes: int,
    max_image_archive_bytes: int,
) -> VerifiedPersonalDevBuildArtifact:
    """Verify and safely materialize one complete native OCI image bundle."""
    if platform not in PERSONAL_DEV_PLATFORMS:
        raise ValueError("personal-dev build artifact platform is unsupported")
    if (
        type(max_artifact_bytes) is not int
        or type(max_image_archive_bytes) is not int
        or max_artifact_bytes <= 0
        or not 0 < max_image_archive_bytes <= max_artifact_bytes
    ):
        raise ValueError("personal-dev build artifact limits are invalid")
    attempt = registration.build_attempt
    candidate = registration.candidate
    if attempt is None or attempt.candidate_id != candidate.id or attempt.lease_epoch <= 0:
        raise ValueError("personal-dev build artifact registration is unavailable")
    output_metadata = os.lstat(output_directory)
    if not stat.S_ISDIR(output_metadata.st_mode) or stat.S_ISLNK(output_metadata.st_mode):
        raise ValueError("personal-dev artifact output directory is invalid")
    output_fd = os.open(output_directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    extracted: list[Path] = []
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= max_artifact_bytes
        ):
            raise PersonalDevBuildArtifactError(
                "personal-dev build artifact authority is invalid"
            )
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            try:
                archive = tarfile.open(fileobj=stream, mode="r:")
            except tarfile.TarError as exc:
                raise PersonalDevBuildArtifactError(
                    "personal-dev build artifact is not an uncompressed tar"
                ) from exc
            with archive:
                members: dict[str, tarfile.TarInfo] = {}
                for member in archive:
                    name = _safe_member_name(member.name).as_posix()
                    if name in members:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev build artifact has duplicate members"
                        )
                    if not member.isreg() or member.mode != 0o644:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev build artifact has a noncanonical member"
                        )
                    members[name] = member
                manifest_member = members.get("manifest.json")
                if manifest_member is None:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev build artifact manifest is missing"
                    )
                manifest_payload = _read_tar_member(archive, manifest_member)
                manifest = _json_object(
                    manifest_payload,
                    label="build artifact manifest",
                    require_canonical=True,
                )
                expected_fields = {
                    "schema_version",
                    "attestation_scope",
                    "candidate_sha",
                    "source_sha256",
                    "archive_sha256",
                    "build_contract_sha256",
                    "attempt_id",
                    "lease_epoch",
                    "platform",
                    "components",
                }
                if set(manifest) != expected_fields:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev build artifact manifest fields are invalid"
                    )
                bindings = {
                    "schema_version": 1,
                    "attestation_scope": "personal-dev-only",
                    "candidate_sha": candidate.candidate_sha,
                    "source_sha256": candidate.source_sha256,
                    "archive_sha256": candidate.archive_sha256,
                    "build_contract_sha256": candidate.build_contract_sha256,
                    "attempt_id": str(attempt.id),
                    "lease_epoch": attempt.lease_epoch,
                    "platform": platform,
                }
                if any(manifest[key] != value for key, value in bindings.items()):
                    raise PersonalDevBuildArtifactError(
                        "personal-dev build artifact platform or attempt binding is invalid"
                    )
                components = manifest["components"]
                if not isinstance(components, dict) or set(components) != set(
                    PERSONAL_DEV_COMPONENTS
                ):
                    raise PersonalDevBuildArtifactError(
                        "personal-dev build artifact image set is incomplete"
                    )
                expected_members = {"manifest.json"}
                records: dict[str, tuple[str, str, int, str]] = {}
                for component in PERSONAL_DEV_COMPONENTS:
                    record = components[component]
                    if not isinstance(record, dict) or set(record) != {
                        "archive_path",
                        "archive_sha256",
                        "archive_size_bytes",
                        "manifest_digest",
                    }:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev build artifact image record is invalid"
                        )
                    expected_path = f"images/{component}.oci.tar"
                    archive_sha256 = _digest(
                        record["archive_sha256"],
                        label="image archive",
                        prefixed=False,
                    )
                    archive_size = _exact_positive_int(
                        record["archive_size_bytes"],
                        label="image archive size",
                    )
                    manifest_digest = _digest(
                        record["manifest_digest"],
                        label="image manifest",
                        prefixed=True,
                    )
                    if (
                        record["archive_path"] != expected_path
                        or archive_size > max_image_archive_bytes
                    ):
                        raise PersonalDevBuildArtifactError(
                            "personal-dev build artifact image binding is invalid"
                        )
                    expected_members.add(expected_path)
                    records[component] = (
                        expected_path,
                        archive_sha256,
                        archive_size,
                        manifest_digest,
                    )
                if set(members) != expected_members:
                    raise PersonalDevBuildArtifactError(
                        "personal-dev build artifact members are incomplete"
                    )
                verified: dict[str, VerifiedPersonalDevImageArtifact] = {}
                for component in PERSONAL_DEV_COMPONENTS:
                    member_name, expected_sha, expected_size, manifest_digest = records[
                        component
                    ]
                    member = members[member_name]
                    if member.size != expected_size:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev image archive size is invalid"
                        )
                    output_name = f"{component}.oci.tar"
                    observed_sha, observed_size = _stream_member_to_file(
                        archive,
                        member,
                        output_directory_fd=output_fd,
                        output_name=output_name,
                    )
                    extracted_path = output_directory / output_name
                    extracted.append(extracted_path)
                    if observed_sha != expected_sha or observed_size != expected_size:
                        raise PersonalDevBuildArtifactError(
                            "personal-dev image archive digest is invalid"
                        )
                    verify_personal_dev_oci_image_archive(
                        extracted_path,
                        platform=platform,
                        expected_manifest_digest=manifest_digest,
                        max_bytes=max_image_archive_bytes,
                    )
                    verified[component] = VerifiedPersonalDevImageArtifact(
                        component=component,
                        platform=platform,
                        archive_path=extracted_path,
                        archive_sha256=expected_sha,
                        archive_size_bytes=expected_size,
                        manifest_digest=manifest_digest,
                    )
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise PersonalDevBuildArtifactError(
                "personal-dev build artifact changed during verification"
            )
        return VerifiedPersonalDevBuildArtifact(
            platform=platform,
            manifest_sha256=hashlib.sha256(manifest_payload).hexdigest(),
            images=MappingProxyType(verified),
        )
    except Exception:
        for extracted_path in extracted:
            extracted_path.unlink(missing_ok=True)
        raise
    finally:
        os.close(descriptor)
        os.close(output_fd)


__all__ = [
    "PersonalDevBuildArtifactError",
    "VerifiedPersonalDevBuildArtifact",
    "VerifiedPersonalDevImageArtifact",
    "verify_personal_dev_build_artifact",
    "verify_personal_dev_oci_image_archive",
]
