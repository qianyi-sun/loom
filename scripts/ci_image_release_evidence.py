#!/usr/bin/env python3
"""Create and validate exact SLSA evidence for release-image publication."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from scripts.write_trivy_release_policy import (
        TRIVY_CONFIG_SHA256,
        TRIVY_IGNORE_SHA256,
        TRIVY_SCANNER_NAME,
        TRIVY_VERSION,
    )
elif __package__:
    from scripts.write_trivy_release_policy import (
        TRIVY_CONFIG_SHA256,
        TRIVY_IGNORE_SHA256,
        TRIVY_SCANNER_NAME,
        TRIVY_VERSION,
    )
else:  # pragma: no cover - direct workflow script entry point
    from write_trivy_release_policy import (
        TRIVY_CONFIG_SHA256,
        TRIVY_IGNORE_SHA256,
        TRIVY_SCANNER_NAME,
        TRIVY_VERSION,
    )

SLSA_PREDICATE_TYPE = "https://slsa.dev/provenance/v1"
_STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
_WORKFLOW_PATH = ".github/workflows/images.yml"
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_COMPONENT = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
_IMAGE_NAME = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*")
_SUBJECT_NAME = re.compile(r"ghcr\.io/[a-z0-9](?:[a-z0-9-]{0,38})/[a-z0-9]+(?:[._-][a-z0-9]+)*")
_PLATFORMS = {"amd64": "linux/amd64", "arm64": "linux/arm64"}
_BUILD_MODES = {"trusted-rebuild"}
TRIVY_ACTION = "aquasecurity/trivy-action@ed142fd0673e97e23eac54620cfb913e5ce36c25"


class EvidenceError(ValueError):
    """Release evidence is incomplete, ambiguous, or inconsistent."""


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvidenceError(f"duplicate JSON key is forbidden: {key}")
        result[key] = value
    return result


def _reject_nonfinite_json(value: str) -> object:
    raise EvidenceError(f"non-finite JSON value is forbidden: {value}")


def _parse_finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise EvidenceError(f"non-finite JSON value is forbidden: {value}")
    return parsed


def _parse_json(value: str, label: str) -> object:
    try:
        return json.loads(
            value,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite_json,
            parse_float=_parse_finite_float,
        )
    except json.JSONDecodeError as exc:
        raise EvidenceError(f"{label} is invalid JSON: {exc}") from None


def _read_json(path: Path, label: str) -> object:
    return _parse_json(path.read_text(encoding="utf-8"), label)


def _type_strict_equal(observed: object, expected: object) -> bool:
    if type(observed) is not type(expected):
        return False
    if isinstance(observed, dict) and isinstance(expected, dict):
        return observed.keys() == expected.keys() and all(
            _type_strict_equal(observed[key], expected[key]) for key in observed
        )
    if isinstance(observed, list) and isinstance(expected, list):
        return len(observed) == len(expected) and all(
            _type_strict_equal(observed_item, expected_item)
            for observed_item, expected_item in zip(observed, expected, strict=True)
        )
    return observed == expected


def _exact(pattern: re.Pattern[str], value: str, label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise EvidenceError(f"{label} is invalid")
    return value


def _positive_integer(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise EvidenceError(f"{label} is invalid")
    return value


def _relative_path(value: str, label: str, *, directory: bool = False) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise EvidenceError(f"{label} is invalid")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or (not directory and value.endswith("/")):
        raise EvidenceError(f"{label} is invalid")
    return value


def _common_values(
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
) -> dict[str, object]:
    repository = _exact(_REPOSITORY, repository, "repository")
    if ref_name not in {"dev", "main"}:
        raise EvidenceError("release ref is invalid")
    head_sha = _exact(_HEX40, head_sha, "release commit")
    tree_sha = _exact(_HEX40, tree_sha, "release tree")
    image = _exact(_COMPONENT, image, "image component")
    image_name = _exact(_IMAGE_NAME, image_name, "image name")
    dockerfile = _relative_path(dockerfile, "Dockerfile")
    build_context = _relative_path(build_context, "build context", directory=True)
    run_id = _positive_integer(run_id, "workflow run id")
    run_attempt = _positive_integer(run_attempt, "workflow run attempt")
    workflow_identity = f"https://github.com/{repository}/{_WORKFLOW_PATH}@refs/heads/{ref_name}"
    return {
        "repository": repository,
        "ref_name": ref_name,
        "head_sha": head_sha,
        "tree_sha": tree_sha,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "image": image,
        "image_name": image_name,
        "dockerfile": dockerfile,
        "build_context": build_context,
        "workflow_identity": workflow_identity,
    }


def _source(common: Mapping[str, object]) -> dict[str, str]:
    return {
        "repository": str(common["repository"]),
        "ref": f"refs/heads/{common['ref_name']}",
        "commit": str(common["head_sha"]),
        "tree": str(common["tree_sha"]),
    }


def _require_release_subject(*, repository: str, image_name: str, subject_name: str) -> None:
    repository = _exact(_REPOSITORY, repository, "repository")
    image_name = _exact(_IMAGE_NAME, image_name, "image name")
    subject_name = _exact(_SUBJECT_NAME, subject_name, "attestation subject name")
    owner = repository.partition("/")[0].lower()
    if subject_name != f"ghcr.io/{owner}/{image_name}":
        raise EvidenceError("subject name does not identify the expected release image")


def _run_details(
    common: Mapping[str, object],
    byproducts: Sequence[dict[str, object]],
) -> dict[str, object]:
    repository = common["repository"]
    run_id = common["run_id"]
    run_attempt = common["run_attempt"]
    return {
        "builder": {"id": common["workflow_identity"]},
        "metadata": {
            "invocationId": (
                f"https://github.com/{repository}/actions/runs/{run_id}/attempts/{run_attempt}"
            )
        },
        "byproducts": list(byproducts),
    }


def _scan_policy_parameters() -> dict[str, object]:
    return {
        "action": TRIVY_ACTION,
        "scanner": {"name": TRIVY_SCANNER_NAME, "version": TRIVY_VERSION},
        "config_sha256": TRIVY_CONFIG_SHA256,
        "ignore_sha256": TRIVY_IGNORE_SHA256,
        "scan_type": "image",
        "vuln_type": ["os", "library"],
        "timeout": "10m0s",
        "severity": ["CRITICAL"],
        "exit_code": 1,
        "ignore_unfixed": False,
        "scanners": ["vuln"],
        "cache": False,
    }


def _scan_policy(report_sha256: str) -> dict[str, object]:
    return {
        **_scan_policy_parameters(),
        "report_sha256": _exact(_HEX64, report_sha256, "scan report digest"),
    }


def _build(
    *,
    build_mode: str,
) -> dict[str, object]:
    if build_mode not in _BUILD_MODES:
        raise EvidenceError("build mode is invalid")
    return {"mode": build_mode}


def _validated_build(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping) or "mode" not in value:
        raise EvidenceError("architecture build is invalid")
    mode = value["mode"]
    if mode == "trusted-rebuild":
        if set(value) != {"mode"}:
            raise EvidenceError("architecture build is invalid")
        return _build(build_mode="trusted-rebuild")
    raise EvidenceError("build mode is invalid")


def architecture_predicate(
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
    platform: str,
    architecture: str,
    scan_report_sha256: str,
    build_mode: str,
) -> dict[str, object]:
    """Return the canonical SLSA predicate for one published architecture."""

    common = _common_values(
        repository=repository,
        ref_name=ref_name,
        head_sha=head_sha,
        tree_sha=tree_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        image=image,
        image_name=image_name,
        dockerfile=dockerfile,
        build_context=build_context,
    )
    if architecture not in _PLATFORMS or _PLATFORMS[architecture] != platform:
        raise EvidenceError("image platform is invalid")
    scan = _scan_policy(scan_report_sha256)
    build = _build(build_mode=build_mode)
    return {
        "buildDefinition": {
            "buildType": common["workflow_identity"],
            "externalParameters": {
                "source": _source(common),
                "image": {
                    "component": common["image"],
                    "repository": common["image_name"],
                    "dockerfile": common["dockerfile"],
                    "context": common["build_context"],
                    "platform": platform,
                    "architecture": architecture,
                },
                "build": build,
                "scan": scan,
            },
            "internalParameters": {
                "github": {
                    "run_id": common["run_id"],
                    "run_attempt": common["run_attempt"],
                }
            },
            "resolvedDependencies": [
                {
                    "uri": f"git+https://github.com/{common['repository']}.git",
                    "digest": {
                        "gitCommit": common["head_sha"],
                        "gitTree": common["tree_sha"],
                    },
                }
            ],
        },
        "runDetails": _run_details(
            common,
            [
                {
                    "name": f"trivy-{platform}",
                    "digest": {"sha256": scan_report_sha256},
                }
            ],
        ),
    }


def _exact_platform_map(values: Mapping[str, str], label: str) -> dict[str, str]:
    if not isinstance(values, Mapping) or set(values) != set(_PLATFORMS.values()):
        raise EvidenceError(f"{label} must cover exactly both native platforms")
    return {platform: str(values[platform]) for platform in sorted(values)}


def _exact_build_map(values: Mapping[str, object], label: str) -> dict[str, dict[str, object]]:
    if not isinstance(values, Mapping) or set(values) != set(_PLATFORMS.values()):
        raise EvidenceError(f"{label} must cover exactly both native platforms")
    return {platform: _validated_build(values[platform]) for platform in sorted(values)}


def manifest_predicate(
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
    architecture_digests: Mapping[str, str],
    scan_report_digests: Mapping[str, str],
    architecture_builds: Mapping[str, object],
) -> dict[str, object]:
    """Return the canonical SLSA predicate for the joined native manifest."""

    common = _common_values(
        repository=repository,
        ref_name=ref_name,
        head_sha=head_sha,
        tree_sha=tree_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        image=image,
        image_name=image_name,
        dockerfile=dockerfile,
        build_context=build_context,
    )
    architecture_digests = _exact_platform_map(architecture_digests, "architecture digests")
    scan_report_digests = _exact_platform_map(scan_report_digests, "scan report digests")
    architecture_builds = _exact_build_map(architecture_builds, "architecture builds")
    for digest in architecture_digests.values():
        if not digest.startswith("sha256:") or _HEX64.fullmatch(digest[7:]) is None:
            raise EvidenceError("architecture digest is invalid")
    for digest in scan_report_digests.values():
        _exact(_HEX64, digest, "scan report digest")
    return {
        "buildDefinition": {
            "buildType": common["workflow_identity"],
            "externalParameters": {
                "source": _source(common),
                "image": {
                    "component": common["image"],
                    "repository": common["image_name"],
                    "dockerfile": common["dockerfile"],
                    "context": common["build_context"],
                    "platforms": sorted(_PLATFORMS.values()),
                },
                "architecture_subjects": architecture_digests,
                "architecture_builds": architecture_builds,
                "scan": {
                    **_scan_policy_parameters(),
                    "report_sha256": scan_report_digests,
                },
            },
            "internalParameters": {
                "github": {
                    "run_id": common["run_id"],
                    "run_attempt": common["run_attempt"],
                }
            },
            "resolvedDependencies": [
                {
                    "uri": f"git+https://github.com/{common['repository']}.git",
                    "digest": {
                        "gitCommit": common["head_sha"],
                        "gitTree": common["tree_sha"],
                    },
                }
            ],
        },
        "runDetails": _run_details(
            common,
            [
                {
                    "name": f"trivy-{platform}",
                    "digest": {"sha256": scan_report_digests[platform]},
                }
                for platform in sorted(scan_report_digests)
            ],
        ),
    }


def _verified_statements(payload: object) -> list[Mapping[str, object]]:
    if not isinstance(payload, list):
        raise EvidenceError("verified attestation output is invalid")
    statements: list[Mapping[str, object]] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise EvidenceError("verified attestation output is invalid")
        verification = item.get("verificationResult")
        if not isinstance(verification, Mapping):
            raise EvidenceError("verified attestation output is invalid")
        statement = verification.get("statement")
        if not isinstance(statement, Mapping):
            raise EvidenceError("verified attestation output is invalid")
        statements.append(statement)
    return statements


def _matching_predicates(
    payload: object,
    *,
    subject_name: str,
    subject_digest: str,
) -> list[Mapping[str, object]]:
    subject_name = _exact(_SUBJECT_NAME, subject_name, "attestation subject name")
    if not subject_digest.startswith("sha256:") or _HEX64.fullmatch(subject_digest[7:]) is None:
        raise EvidenceError("attestation subject digest is invalid")
    expected_subject = [{"name": subject_name, "digest": {"sha256": subject_digest[7:]}}]
    predicates: list[Mapping[str, object]] = []
    for statement in _verified_statements(payload):
        if (
            statement.get("_type") == _STATEMENT_TYPE
            and statement.get("subject") == expected_subject
            and statement.get("predicateType") == SLSA_PREDICATE_TYPE
            and isinstance(statement.get("predicate"), Mapping)
        ):
            predicates.append(statement["predicate"])  # type: ignore[arg-type]
    return predicates


def verify_architecture_attestation(
    payload: object,
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
    platform: str,
    architecture: str,
    subject_name: str,
    subject_digest: str,
    build_mode: str,
    scan_report_sha256: str | None = None,
) -> str:
    """Require exactly one canonical verified architecture attestation."""

    _require_release_subject(
        repository=repository, image_name=image_name, subject_name=subject_name
    )
    predicates = _matching_predicates(
        payload, subject_name=subject_name, subject_digest=subject_digest
    )
    matches: list[str] = []
    for predicate in predicates:
        try:
            external = predicate["buildDefinition"]["externalParameters"]  # type: ignore[index]
            observed_scan = external["scan"]["report_sha256"]
            expected = architecture_predicate(
                repository=repository,
                ref_name=ref_name,
                head_sha=head_sha,
                tree_sha=tree_sha,
                run_id=run_id,
                run_attempt=run_attempt,
                image=image,
                image_name=image_name,
                dockerfile=dockerfile,
                build_context=build_context,
                platform=platform,
                architecture=architecture,
                scan_report_sha256=observed_scan,
                build_mode=build_mode,
            )
        except (EvidenceError, KeyError, TypeError):
            continue
        if _type_strict_equal(predicate, expected) and (
            scan_report_sha256 is None or observed_scan == scan_report_sha256
        ):
            matches.append(observed_scan)
    if len(matches) != 1:
        raise EvidenceError(
            "verified architecture evidence must contain exactly one canonical attestation"
        )
    return matches[0]


def verify_manifest_attestation(
    payload: object,
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
    subject_name: str,
    subject_digest: str,
    architecture_digests: Mapping[str, str],
    scan_report_digests: Mapping[str, str],
    architecture_builds: Mapping[str, object],
) -> None:
    """Require exactly one canonical verified multi-architecture attestation."""

    _require_release_subject(
        repository=repository, image_name=image_name, subject_name=subject_name
    )
    expected = manifest_predicate(
        repository=repository,
        ref_name=ref_name,
        head_sha=head_sha,
        tree_sha=tree_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        image=image,
        image_name=image_name,
        dockerfile=dockerfile,
        build_context=build_context,
        architecture_digests=architecture_digests,
        scan_report_digests=scan_report_digests,
        architecture_builds=architecture_builds,
    )
    matches = [
        predicate
        for predicate in _matching_predicates(
            payload, subject_name=subject_name, subject_digest=subject_digest
        )
        if _type_strict_equal(predicate, expected)
    ]
    if len(matches) != 1:
        raise EvidenceError(
            "verified manifest evidence must contain exactly one canonical attestation"
        )


def architecture_record(
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
    platform: str,
    architecture: str,
    subject_name: str,
    subject_digest: str,
    scan_report_sha256: str,
    build_mode: str,
) -> dict[str, object]:
    """Return the canonical immutable handoff record for one architecture."""

    common = _common_values(
        repository=repository,
        ref_name=ref_name,
        head_sha=head_sha,
        tree_sha=tree_sha,
        run_id=run_id,
        run_attempt=run_attempt,
        image=image,
        image_name=image_name,
        dockerfile=dockerfile,
        build_context=build_context,
    )
    if architecture not in _PLATFORMS or _PLATFORMS[architecture] != platform:
        raise EvidenceError("image platform is invalid")
    subject_name = _exact(_SUBJECT_NAME, subject_name, "record subject name")
    _require_release_subject(
        repository=repository, image_name=image_name, subject_name=subject_name
    )
    if not subject_digest.startswith("sha256:") or _HEX64.fullmatch(subject_digest[7:]) is None:
        raise EvidenceError("record subject digest is invalid")
    scan = _scan_policy(scan_report_sha256)
    build = _build(build_mode=build_mode)
    return {
        "schema_version": 1,
        "subject": {"name": subject_name, "digest": subject_digest},
        "release": {
            "repository": common["repository"],
            "ref": f"refs/heads/{common['ref_name']}",
            "commit": common["head_sha"],
            "tree": common["tree_sha"],
            "run_id": common["run_id"],
            "run_attempt": common["run_attempt"],
        },
        "image": {
            "component": common["image"],
            "repository": common["image_name"],
            "dockerfile": common["dockerfile"],
            "context": common["build_context"],
            "platform": platform,
            "architecture": architecture,
        },
        "scan": scan,
        "build": build,
    }


def validate_architecture_record(
    payload: object,
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
    platform: str,
    architecture: str,
) -> dict[str, object]:
    """Validate one record against the release and image expected by the caller."""

    if not isinstance(payload, Mapping):
        raise EvidenceError("canonical architecture record is invalid")
    try:
        subject = payload["subject"]
        scan = payload["scan"]
        build = _validated_build(payload["build"])
        if not isinstance(subject, Mapping) or not isinstance(scan, Mapping):
            raise EvidenceError("canonical architecture record is invalid")
        expected = architecture_record(
            repository=repository,
            ref_name=ref_name,
            head_sha=head_sha,
            tree_sha=tree_sha,
            run_id=run_id,
            run_attempt=run_attempt,
            image=image,
            image_name=image_name,
            dockerfile=dockerfile,
            build_context=build_context,
            platform=platform,
            architecture=architecture,
            subject_name=subject["name"],
            subject_digest=subject["digest"],
            scan_report_sha256=scan["report_sha256"],
            build_mode=cast(str, build["mode"]),
        )
    except (KeyError, TypeError):
        raise EvidenceError("canonical architecture record is invalid") from None
    if not _type_strict_equal(payload, expected):
        raise EvidenceError("canonical architecture record is invalid")
    return expected


def validate_architecture_records(
    records_dir: Path,
    *,
    repository: str,
    ref_name: str,
    head_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
    image: str,
    image_name: str,
    dockerfile: str,
    build_context: str,
) -> dict[str, object]:
    """Validate the exact AMD64/ARM64 handoff set for one release image."""

    expected_directories = set(_PLATFORMS)
    directories = list(records_dir.iterdir())
    if {entry.name for entry in directories} != expected_directories or any(
        not entry.is_dir() or entry.is_symlink() for entry in directories
    ):
        raise EvidenceError("record directory must contain exactly the expected architecture files")
    architectures: dict[str, object] = {}
    subject_names: set[str] = set()
    for architecture, platform in sorted(_PLATFORMS.items()):
        directory = records_dir / architecture
        expected_name = f"{image}-{architecture}.json"
        entries = list(directory.iterdir())
        if {entry.name for entry in entries} != {expected_name} or any(
            not entry.is_file() or entry.is_symlink() for entry in entries
        ):
            raise EvidenceError(
                "record directory must contain exactly the expected architecture files"
            )
        path = directory / expected_name
        payload = _read_json(path, "architecture record")
        record = validate_architecture_record(
            payload,
            repository=repository,
            ref_name=ref_name,
            head_sha=head_sha,
            tree_sha=tree_sha,
            run_id=run_id,
            run_attempt=run_attempt,
            image=image,
            image_name=image_name,
            dockerfile=dockerfile,
            build_context=build_context,
            platform=platform,
            architecture=architecture,
        )
        subject = record["subject"]
        scan = record["scan"]
        if not isinstance(subject, Mapping) or not isinstance(scan, Mapping):
            raise EvidenceError("canonical architecture record is invalid")
        subject_names.add(str(subject["name"]))
        architectures[architecture] = {
            "platform": platform,
            "subject_digest": subject["digest"],
            "scan_report_sha256": scan["report_sha256"],
            "build": record["build"],
        }
    if len(subject_names) != 1:
        raise EvidenceError("architecture record subjects are inconsistent")
    return {"subject_name": subject_names.pop(), "architectures": architectures}


def validate_manifest_subjects(
    payload: object,
    *,
    architecture_digests: Mapping[str, str],
) -> None:
    """Validate the platform subjects in one published manifest index."""

    expected = _exact_platform_map(architecture_digests, "architecture digests")
    for digest in expected.values():
        if not digest.startswith("sha256:") or _HEX64.fullmatch(digest[7:]) is None:
            raise EvidenceError("architecture digest is invalid")
    if not isinstance(payload, Mapping) or not isinstance(payload.get("manifests"), list):
        raise EvidenceError("published manifest is invalid")
    manifests = payload["manifests"]
    if len(manifests) != 2:
        raise EvidenceError("published manifest must contain exactly two descriptors")
    subjects: dict[str, str] = {}
    for item in manifests:
        if not isinstance(item, Mapping):
            raise EvidenceError("published manifest descriptor is invalid")
        platform = item.get("platform")
        observed_digest = item.get("digest")
        if (
            not isinstance(platform, Mapping)
            or not isinstance(platform.get("os"), str)
            or not platform["os"]
            or not isinstance(platform.get("architecture"), str)
            or not platform["architecture"]
            or not isinstance(observed_digest, str)
            or not observed_digest.startswith("sha256:")
            or _HEX64.fullmatch(observed_digest[7:]) is None
        ):
            raise EvidenceError("published manifest descriptor is invalid")
        identity = f"{platform['os']}/{platform['architecture']}"
        if identity in subjects:
            raise EvidenceError("published manifest platform is duplicated")
        subjects[identity] = observed_digest
    if not _type_strict_equal(subjects, expected):
        raise EvidenceError("published manifest subjects are invalid")


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True)
    parser.add_argument("--ref-name", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--tree-sha", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-name", required=True)
    parser.add_argument("--dockerfile", required=True)
    parser.add_argument("--build-context", required=True)


def _common_namespace(arguments: argparse.Namespace) -> dict[str, Any]:
    return {
        key: getattr(arguments, key)
        for key in (
            "repository",
            "ref_name",
            "head_sha",
            "tree_sha",
            "run_id",
            "run_attempt",
            "image",
            "image_name",
            "dockerfile",
            "build_context",
        )
    }


def _platform_map(values: Sequence[str], label: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        platform, separator, digest = value.partition("=")
        if not separator or platform in parsed:
            raise EvidenceError(f"{label} is invalid")
        parsed[platform] = digest
    return parsed


def _platform_build_map(values: Sequence[str]) -> dict[str, object]:
    parsed: dict[str, object] = {}
    for value in values:
        platform, separator, raw_build = value.partition("=")
        if not separator or platform in parsed:
            raise EvidenceError("architecture build is invalid")
        parsed[platform] = _parse_json(raw_build, "architecture build")
    return parsed


def _build_mode_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--build-mode", required=True)


def _build_namespace(arguments: argparse.Namespace) -> dict[str, Any]:
    return {"build_mode": arguments.build_mode}


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    operations = parser.add_subparsers(dest="operation", required=True)
    for name in (
        "predicate-architecture",
        "verify-architecture",
        "record-architecture",
    ):
        operation = operations.add_parser(name)
        _common_arguments(operation)
        operation.add_argument("--platform", required=True)
        operation.add_argument("--architecture", required=True)
        operation.add_argument("--scan-report-sha256")
        _build_mode_arguments(operation)
        if name == "verify-architecture":
            operation.add_argument("--verification", type=Path, required=True)
            operation.add_argument("--subject-name", required=True)
            operation.add_argument("--subject-digest", required=True)
        elif name == "record-architecture":
            operation.add_argument("--subject-name", required=True)
            operation.add_argument("--subject-digest", required=True)
        operation.add_argument("--output", type=Path, required=True)
    for name in ("predicate-manifest", "verify-manifest"):
        operation = operations.add_parser(name)
        _common_arguments(operation)
        operation.add_argument("--architecture-digest", action="append", required=True)
        operation.add_argument("--scan-report-digest", action="append", required=True)
        operation.add_argument("--architecture-build", action="append", required=True)
        if name.startswith("verify"):
            operation.add_argument("--verification", type=Path, required=True)
            operation.add_argument("--subject-name", required=True)
            operation.add_argument("--subject-digest", required=True)
        operation.add_argument("--output", type=Path)
    validate_records = operations.add_parser("validate-architecture-records")
    _common_arguments(validate_records)
    validate_records.add_argument("--records-dir", type=Path, required=True)
    validate_records.add_argument("--output", type=Path)
    validate_record = operations.add_parser("validate-architecture-record")
    _common_arguments(validate_record)
    validate_record.add_argument("--platform", required=True)
    validate_record.add_argument("--architecture", required=True)
    validate_record.add_argument("--record", type=Path, required=True)
    validate_record.add_argument("--output", type=Path)
    validate_manifest = operations.add_parser("validate-manifest")
    validate_manifest.add_argument("--manifest", type=Path, required=True)
    validate_manifest.add_argument("--architecture-digest", action="append", required=True)
    validate_manifest.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    try:
        if arguments.operation == "validate-manifest":
            payload = _read_json(arguments.manifest, "published manifest")
            validate_manifest_subjects(
                payload,
                architecture_digests=_platform_map(
                    arguments.architecture_digest, "architecture digest"
                ),
            )
            if arguments.output is not None:
                _write_json(arguments.output, {"status": "validated"})
            return
        common = _common_namespace(arguments)
        if arguments.operation == "predicate-architecture":
            if arguments.scan_report_sha256 is None:
                raise EvidenceError("scan report digest is required")
            predicate = architecture_predicate(
                **common,
                platform=arguments.platform,
                architecture=arguments.architecture,
                scan_report_sha256=arguments.scan_report_sha256,
                **_build_namespace(arguments),
            )
            _write_json(arguments.output, predicate)
        elif arguments.operation == "verify-architecture":
            payload = _read_json(arguments.verification, "architecture verification")
            scan_digest = verify_architecture_attestation(
                payload,
                **common,
                platform=arguments.platform,
                architecture=arguments.architecture,
                subject_name=arguments.subject_name,
                subject_digest=arguments.subject_digest,
                scan_report_sha256=arguments.scan_report_sha256,
                **_build_namespace(arguments),
            )
            _write_json(arguments.output, {"scan_report_sha256": scan_digest})
        elif arguments.operation == "record-architecture":
            if arguments.scan_report_sha256 is None:
                raise EvidenceError("scan report digest is required")
            record = architecture_record(
                **common,
                platform=arguments.platform,
                architecture=arguments.architecture,
                subject_name=arguments.subject_name,
                subject_digest=arguments.subject_digest,
                scan_report_sha256=arguments.scan_report_sha256,
                **_build_namespace(arguments),
            )
            _write_json(arguments.output, record)
        elif arguments.operation == "validate-architecture-records":
            validated = validate_architecture_records(
                arguments.records_dir,
                **common,
            )
            if arguments.output is not None:
                _write_json(arguments.output, validated)
        elif arguments.operation == "validate-architecture-record":
            payload = _read_json(arguments.record, "architecture record")
            validated = validate_architecture_record(
                payload,
                **common,
                platform=arguments.platform,
                architecture=arguments.architecture,
            )
            if arguments.output is not None:
                _write_json(arguments.output, validated)
        else:
            architecture_digests = _platform_map(
                arguments.architecture_digest, "architecture digest"
            )
            scan_report_digests = _platform_map(arguments.scan_report_digest, "scan report digest")
            architecture_builds = _platform_build_map(arguments.architecture_build)
            if arguments.operation == "predicate-manifest":
                if arguments.output is None:
                    raise EvidenceError("predicate output is required")
                predicate = manifest_predicate(
                    **common,
                    architecture_digests=architecture_digests,
                    scan_report_digests=scan_report_digests,
                    architecture_builds=architecture_builds,
                )
                _write_json(arguments.output, predicate)
            else:
                payload = _read_json(arguments.verification, "manifest verification")
                verify_manifest_attestation(
                    payload,
                    **common,
                    subject_name=arguments.subject_name,
                    subject_digest=arguments.subject_digest,
                    architecture_digests=architecture_digests,
                    scan_report_digests=scan_report_digests,
                    architecture_builds=architecture_builds,
                )
                if arguments.output is not None:
                    _write_json(arguments.output, {"status": "verified"})
    except (EvidenceError, OSError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"error: release evidence validation failed: {exc}\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "SLSA_PREDICATE_TYPE",
    "TRIVY_ACTION",
    "EvidenceError",
    "architecture_predicate",
    "architecture_record",
    "manifest_predicate",
    "validate_architecture_record",
    "validate_architecture_records",
    "validate_manifest_subjects",
    "verify_architecture_attestation",
    "verify_manifest_attestation",
]
