#!/usr/bin/env python3
"""Assemble one exact trusted release for personal-development infrastructure."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

if __package__:
    from scripts.ci_image_release_evidence import (
        EvidenceError,
        validate_architecture_record,
        validate_manifest_subjects,
    )
else:  # pragma: no cover - direct workflow script entry point
    from ci_image_release_evidence import (
        EvidenceError,
        validate_architecture_record,
        validate_manifest_subjects,
    )

_MAX_INPUT_BYTES = 1024 * 1024
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_NUMERIC_ID = re.compile(r"[1-9][0-9]*")
_PLATFORMS = {"amd64": "linux/amd64", "arm64": "linux/arm64"}
_INTERNAL_IMAGES: dict[str, dict[str, str]] = {
    "service": {
        "release_key": "loom_service",
        "image_name": "loom-service",
        "dockerfile": "deploy/Dockerfile.service",
    },
    "personal-dev-builder": {
        "release_key": "personal_dev_builder",
        "image_name": "loom-personal-dev-builder",
        "dockerfile": "deploy/Dockerfile.personal-dev-builder",
    },
    "personal-dev-activation-agent": {
        "release_key": "personal_dev_activation_agent",
        "image_name": "loom-personal-dev-activation-agent",
        "dockerfile": "deploy/Dockerfile.personal-dev-activation-agent",
    },
}
_EXTERNAL_REPOSITORIES = {
    "postgres": "docker.io/library/postgres",
    "minio": "quay.io/minio/minio",
    "minio_client": "quay.io/minio/mc",
}


class TrustedReleaseError(ValueError):
    """The aggregate image evidence is incomplete or inconsistent."""


def _duplicate_rejecting_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise TrustedReleaseError(f"duplicate JSON key is forbidden: {key}")
        value[key] = item
    return value


def _reject_nonfinite(value: str) -> object:
    raise TrustedReleaseError(f"non-finite JSON value is forbidden: {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise TrustedReleaseError(f"non-finite JSON value is forbidden: {value}")
    return parsed


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_bounded_file(path: Path, label: str, limit: int) -> bytes:
    descriptor: int | None = None
    try:
        path_before = path.lstat()
        if (
            not stat.S_ISREG(path_before.st_mode)
            or stat.S_ISLNK(path_before.st_mode)
            or path_before.st_uid != os.geteuid()
            or path_before.st_nlink != 1
            or not 0 < path_before.st_size <= limit
        ):
            raise TrustedReleaseError(f"{label} is invalid")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(path_before):
            raise TrustedReleaseError(f"{label} is invalid")
        payload = bytearray()
        while len(payload) <= limit:
            chunk = os.read(descriptor, min(64 * 1024, limit + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or len(payload) > limit
            or _file_identity(os.fstat(descriptor)) != _file_identity(opened)
            or _file_identity(path.lstat()) != _file_identity(path_before)
        ):
            raise TrustedReleaseError(f"{label} is invalid")
        return bytes(payload)
    except TrustedReleaseError:
        raise
    except OSError:
        raise TrustedReleaseError(f"{label} is invalid") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _read_json(path: Path, label: str) -> tuple[object, bytes]:
    payload = _read_bounded_file(path, label, _MAX_INPUT_BYTES)
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=_reject_nonfinite,
            parse_float=_finite_float,
        )
    except TrustedReleaseError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise TrustedReleaseError(f"{label} is invalid") from None
    return value, payload


def _read_canonical_json(path: Path, label: str) -> tuple[object, bytes]:
    value, payload = _read_json(path, label)
    try:
        canonical = _canonical_json(value)
    except (TypeError, UnicodeError, ValueError):
        raise TrustedReleaseError(f"{label} is invalid") from None
    if payload != canonical + b"\n":
        raise TrustedReleaseError(f"{label} is not canonical")
    return value, payload


def _require_exact_files(directory: Path, names: set[str], label: str) -> None:
    try:
        entries = list(directory.iterdir())
    except OSError:
        raise TrustedReleaseError(f"{label} is invalid") from None
    if {entry.name for entry in entries} != names or any(
        entry.is_symlink() or not entry.is_file() for entry in entries
    ):
        raise TrustedReleaseError(f"{label} must contain exactly the expected files")


def _positive_integer(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise TrustedReleaseError(f"{label} is invalid")
    return value


def _exact_text(pattern: re.Pattern[str], value: str, label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise TrustedReleaseError(f"{label} is invalid")
    return value


def _external_images(value: object) -> dict[str, dict[str, object]]:
    if not isinstance(value, Mapping) or set(value) != {"schema_version", "images"}:
        raise TrustedReleaseError("external image binding is invalid")
    if value["schema_version"] != 1 or isinstance(value["schema_version"], bool):
        raise TrustedReleaseError("external image binding is invalid")
    images = value["images"]
    if not isinstance(images, Mapping) or set(images) != set(_EXTERNAL_REPOSITORIES):
        raise TrustedReleaseError("external image binding is invalid")
    validated: dict[str, dict[str, object]] = {}
    for key, repository in _EXTERNAL_REPOSITORIES.items():
        item = images[key]
        if not isinstance(item, Mapping) or set(item) != {"reference", "members"}:
            raise TrustedReleaseError("external image binding is invalid")
        reference = item["reference"]
        members = item["members"]
        if (
            not isinstance(reference, str)
            or not reference.startswith(f"{repository}@sha256:")
            or _HEX64.fullmatch(reference.removeprefix(f"{repository}@sha256:")) is None
            or reference.endswith("0" * 64)
            or not isinstance(members, Mapping)
            or set(members) != set(_PLATFORMS.values())
        ):
            raise TrustedReleaseError("external image binding is invalid")
        normalized_members: dict[str, str] = {}
        for platform in sorted(_PLATFORMS.values()):
            digest = members[platform]
            if (
                not isinstance(digest, str)
                or not digest.startswith("sha256:")
                or _HEX64.fullmatch(digest[7:]) is None
                or digest == "sha256:" + "0" * 64
            ):
                raise TrustedReleaseError("external image binding is invalid")
            normalized_members[platform] = digest
        validated[key] = {"reference": reference, "members": normalized_members}
    return validated


def _validate_external_manifest_subjects(
    payload: object,
    expected: Mapping[str, object],
) -> None:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("manifests"), list):
        raise TrustedReleaseError("external image manifest is invalid")
    manifests = payload["manifests"]
    if not 2 <= len(manifests) <= 128:
        raise TrustedReleaseError("external image manifest is invalid")
    observed: dict[str, str] = {}
    target_platforms = set(_PLATFORMS.values())
    for descriptor in manifests:
        if not isinstance(descriptor, Mapping):
            raise TrustedReleaseError("external image manifest is invalid")
        platform = descriptor.get("platform")
        digest = descriptor.get("digest")
        if (
            not isinstance(platform, Mapping)
            or not isinstance(platform.get("os"), str)
            or not platform["os"]
            or not isinstance(platform.get("architecture"), str)
            or not platform["architecture"]
            or not isinstance(digest, str)
            or not digest.startswith("sha256:")
            or _HEX64.fullmatch(digest[7:]) is None
            or digest == "sha256:" + "0" * 64
        ):
            raise TrustedReleaseError("external image manifest is invalid")
        identity = f"{platform['os']}/{platform['architecture']}"
        if identity in target_platforms:
            if identity in observed:
                raise TrustedReleaseError("external target platform is duplicated")
            observed[identity] = digest
    if observed != expected:
        raise TrustedReleaseError("external image manifest target members are invalid")


def assemble_personal_dev_trusted_release(
    *,
    records_dir: Path,
    manifests_dir: Path,
    external_images_file: Path,
    repository: str,
    ref_name: str,
    source_sha: str,
    source_tree: str,
    run_id: int,
    run_attempt: int,
    event_name: str,
    repository_id: str,
    repository_owner_id: str,
    runner_environment: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Validate six indexes and return canonical release and evidence values."""

    repository = _exact_text(_REPOSITORY, repository, "repository")
    if ref_name not in {"dev", "main"}:
        raise TrustedReleaseError("release ref is invalid")
    source_sha = _exact_text(_HEX40, source_sha, "source commit")
    source_tree = _exact_text(_HEX40, source_tree, "source tree")
    if source_sha == "0" * 40 or source_tree == "0" * 40:
        raise TrustedReleaseError("source identity is invalid")
    run_id = _positive_integer(run_id, "workflow run id")
    run_attempt = _positive_integer(run_attempt, "workflow run attempt")
    if event_name not in {"push", "workflow_dispatch"}:
        raise TrustedReleaseError("release event is invalid")
    repository_id = _exact_text(_NUMERIC_ID, repository_id, "repository id")
    repository_owner_id = _exact_text(
        _NUMERIC_ID, repository_owner_id, "repository owner id"
    )
    if runner_environment != "github-hosted":
        raise TrustedReleaseError("release runner environment is invalid")

    record_names = {
        f"{component}-{architecture}.json"
        for component in _INTERNAL_IMAGES
        for architecture in _PLATFORMS
    }
    manifest_names = {
        f"{key}.json" for key in (*_INTERNAL_IMAGES, *_EXTERNAL_REPOSITORIES)
    }
    _require_exact_files(records_dir, record_names, "architecture record directory")
    _require_exact_files(manifests_dir, manifest_names, "manifest directory")
    external_value, _external_bytes = _read_canonical_json(
        external_images_file, "external image binding"
    )
    external = _external_images(external_value)

    release_images: dict[str, str] = {}
    internal_evidence: dict[str, object] = {}
    owner = repository.partition("/")[0].lower()
    for component, contract in _INTERNAL_IMAGES.items():
        platforms: dict[str, object] = {}
        architecture_digests: dict[str, str] = {}
        subject_name = f"ghcr.io/{owner}/{contract['image_name']}"
        for architecture, platform in sorted(_PLATFORMS.items()):
            value, _record_bytes = _read_canonical_json(
                records_dir / f"{component}-{architecture}.json",
                "architecture record",
            )
            try:
                record = validate_architecture_record(
                    value,
                    repository=repository,
                    ref_name=ref_name,
                    head_sha=source_sha,
                    tree_sha=source_tree,
                    run_id=run_id,
                    run_attempt=run_attempt,
                    event_name=event_name,
                    repository_id=repository_id,
                    repository_owner_id=repository_owner_id,
                    runner_environment=runner_environment,
                    image=component,
                    image_name=contract["image_name"],
                    dockerfile=contract["dockerfile"],
                    build_context=".",
                    platform=platform,
                    architecture=architecture,
                )
            except EvidenceError as exc:
                raise TrustedReleaseError("architecture record is invalid") from exc
            subject = record["subject"]
            scan = record["scan"]
            if (
                not isinstance(subject, Mapping)
                or subject.get("name") != subject_name
                or not isinstance(scan, Mapping)
            ):
                raise TrustedReleaseError("architecture subjects are inconsistent")
            digest = subject["digest"]
            if not isinstance(digest, str):
                raise TrustedReleaseError("architecture subject digest is invalid")
            architecture_digests[platform] = digest
            platforms[platform] = {
                "subject_digest": digest,
                "scan_report_sha256": scan["report_sha256"],
                "build": record["build"],
            }

        manifest_value, manifest_bytes = _read_json(
            manifests_dir / f"{component}.json", "internal image manifest"
        )
        try:
            validate_manifest_subjects(
                manifest_value, architecture_digests=architecture_digests
            )
        except EvidenceError as exc:
            raise TrustedReleaseError("internal image manifest is invalid") from exc
        reference = f"{subject_name}@sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
        release_images[contract["release_key"]] = reference
        internal_evidence[component] = {
            "reference": reference,
            "platforms": platforms,
        }

    external_evidence: dict[str, object] = {}
    for key in _EXTERNAL_REPOSITORIES:
        item = external[key]
        manifest_value, manifest_bytes = _read_json(
            manifests_dir / f"{key}.json", "external image manifest"
        )
        members = item["members"]
        if not isinstance(members, Mapping):  # narrowed by _external_images
            raise TrustedReleaseError("external image binding is invalid")
        _validate_external_manifest_subjects(manifest_value, members)
        external_reference = item["reference"]
        if not isinstance(external_reference, str) or not external_reference.endswith(
            hashlib.sha256(manifest_bytes).hexdigest()
        ):
            raise TrustedReleaseError("external image index digest is invalid")
        release_images[key] = external_reference
        external_evidence[key] = {
            "reference": external_reference,
            "platforms": dict(sorted(members.items())),
        }

    index_digests = [reference.rpartition("@sha256:")[2] for reference in release_images.values()]
    if len(index_digests) != len(set(index_digests)):
        raise TrustedReleaseError("trusted image index digests must be distinct")

    evidence: dict[str, object] = {
        "schema_version": 1,
        "release": {
            "repository": repository,
            "ref": f"refs/heads/{ref_name}",
            "commit": source_sha,
            "tree": source_tree,
            "run_id": run_id,
            "run_attempt": run_attempt,
        },
        "internal_images": internal_evidence,
        "external_images": external_evidence,
    }
    release: dict[str, object] = {
        "schema_version": 1,
        "source_sha": source_sha,
        "source_tree": source_tree,
        "images": release_images,
        "release_evidence_sha256": hashlib.sha256(_canonical_json(evidence)).hexdigest(),
    }
    return release, evidence


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--records-dir", type=Path, required=True)
    parser.add_argument("--manifests-dir", type=Path, required=True)
    parser.add_argument("--external-images-file", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--ref-name", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-tree", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--repository-id", required=True)
    parser.add_argument("--repository-owner-id", required=True)
    parser.add_argument("--runner-environment", required=True)


def _common_values(arguments: argparse.Namespace) -> dict[str, Any]:
    return {
        name: getattr(arguments, name)
        for name in (
            "records_dir",
            "manifests_dir",
            "external_images_file",
            "repository",
            "ref_name",
            "source_sha",
            "source_tree",
            "run_id",
            "run_attempt",
            "event_name",
            "repository_id",
            "repository_owner_id",
            "runner_environment",
        )
    }


def _write_new_file(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        written = 0
        while written < len(payload):
            count = os.write(descriptor, payload[written:])
            if count <= 0:
                raise OSError("trusted release output write did not progress")
            written += count
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_outputs(output_dir: Path, release: object, evidence: object) -> None:
    output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    release_bytes = _canonical_json(release)
    _write_new_file(output_dir / "trusted-release.json", release_bytes)
    _write_new_file(output_dir / "trusted-release-evidence.json", _canonical_json(evidence))
    _write_new_file(
        output_dir / "trusted-release.sha256",
        (hashlib.sha256(release_bytes).hexdigest() + "\n").encode("ascii"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    operations = parser.add_subparsers(dest="operation", required=True)
    assemble = operations.add_parser("assemble")
    _common_arguments(assemble)
    assemble.add_argument("--output-dir", type=Path, required=True)
    validate = operations.add_parser("validate")
    _common_arguments(validate)
    validate.add_argument("--release-file", type=Path, required=True)
    validate.add_argument("--evidence-file", type=Path, required=True)
    validate.add_argument("--sha256-file", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        release, evidence = assemble_personal_dev_trusted_release(**_common_values(arguments))
        if arguments.operation == "assemble":
            _write_outputs(arguments.output_dir, release, evidence)
        else:
            observed_release, release_bytes = _read_json(
                arguments.release_file, "trusted release"
            )
            observed_evidence, evidence_bytes = _read_json(
                arguments.evidence_file, "trusted release evidence"
            )
            digest_bytes = _read_bounded_file(
                arguments.sha256_file, "trusted release digest", 65
            )
            expected_digest = hashlib.sha256(_canonical_json(release)).hexdigest()
            if (
                observed_release != release
                or observed_evidence != evidence
                or release_bytes != _canonical_json(release)
                or evidence_bytes != _canonical_json(evidence)
                or digest_bytes != (expected_digest + "\n").encode("ascii")
            ):
                raise TrustedReleaseError("trusted release output is invalid")
    except (OSError, TrustedReleaseError) as exc:
        sys.stderr.write(f"error: personal-dev trusted release validation failed: {exc}\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "TrustedReleaseError",
    "assemble_personal_dev_trusted_release",
]
