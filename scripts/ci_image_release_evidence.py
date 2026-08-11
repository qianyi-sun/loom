#!/usr/bin/env python3
"""Create and validate exact SLSA evidence for release-image publication."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

SLSA_PREDICATE_TYPE = "https://slsa.dev/provenance/v1"
_STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
_WORKFLOW_PATH = ".github/workflows/images.yml"
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_COMPONENT = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
_IMAGE_NAME = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*")
_SUBJECT_NAME = re.compile(
    r"ghcr\.io/[a-z0-9](?:[a-z0-9-]{0,38})/[a-z0-9]+(?:[._-][a-z0-9]+)*"
)
_PLATFORMS = {"amd64": "linux/amd64", "arm64": "linux/arm64"}


class EvidenceError(ValueError):
    """Release evidence is incomplete, ambiguous, or inconsistent."""


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
                f"https://github.com/{repository}/actions/runs/{run_id}/attempts/"
                f"{run_attempt}"
            )
        },
        "byproducts": list(byproducts),
    }


def _scan_policy(report_sha256: str) -> dict[str, object]:
    report_sha256 = _exact(_HEX64, report_sha256, "scan report digest")
    return {
        "scanner": "trivy",
        "report_sha256": report_sha256,
        "severity": ["HIGH", "CRITICAL"],
        "ignore_unfixed": False,
        "scanners": ["vuln"],
    }


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
    architecture_digests = _exact_platform_map(
        architecture_digests, "architecture digests"
    )
    scan_report_digests = _exact_platform_map(
        scan_report_digests, "scan report digests"
    )
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
                "scan": {
                    "scanner": "trivy",
                    "report_sha256": scan_report_digests,
                    "severity": ["HIGH", "CRITICAL"],
                    "ignore_unfixed": False,
                    "scanners": ["vuln"],
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
    expected_subject = [
        {"name": subject_name, "digest": {"sha256": subject_digest[7:]}}
    ]
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
    scan_report_sha256: str | None = None,
) -> str:
    """Require exactly one canonical verified architecture attestation."""

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
            )
        except (EvidenceError, KeyError, TypeError):
            continue
        if predicate == expected and (
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
) -> None:
    """Require exactly one canonical verified multi-architecture attestation."""

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
    )
    matches = [
        predicate
        for predicate in _matching_predicates(
            payload, subject_name=subject_name, subject_digest=subject_digest
        )
        if predicate == expected
    ]
    if len(matches) != 1:
        raise EvidenceError(
            "verified manifest evidence must contain exactly one canonical attestation"
        )


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


def _write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    operations = parser.add_subparsers(dest="operation", required=True)
    for name in ("predicate-architecture", "verify-architecture"):
        operation = operations.add_parser(name)
        _common_arguments(operation)
        operation.add_argument("--platform", required=True)
        operation.add_argument("--architecture", required=True)
        operation.add_argument("--scan-report-sha256")
        if name.startswith("verify"):
            operation.add_argument("--verification", type=Path, required=True)
            operation.add_argument("--subject-name", required=True)
            operation.add_argument("--subject-digest", required=True)
        operation.add_argument("--output", type=Path, required=True)
    for name in ("predicate-manifest", "verify-manifest"):
        operation = operations.add_parser(name)
        _common_arguments(operation)
        operation.add_argument("--architecture-digest", action="append", required=True)
        operation.add_argument("--scan-report-digest", action="append", required=True)
        if name.startswith("verify"):
            operation.add_argument("--verification", type=Path, required=True)
            operation.add_argument("--subject-name", required=True)
            operation.add_argument("--subject-digest", required=True)
        operation.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    common = _common_namespace(arguments)
    try:
        if arguments.operation == "predicate-architecture":
            if arguments.scan_report_sha256 is None:
                raise EvidenceError("scan report digest is required")
            predicate = architecture_predicate(
                **common,
                platform=arguments.platform,
                architecture=arguments.architecture,
                scan_report_sha256=arguments.scan_report_sha256,
            )
            _write_json(arguments.output, predicate)
        elif arguments.operation == "verify-architecture":
            payload = json.loads(arguments.verification.read_text(encoding="utf-8"))
            scan_digest = verify_architecture_attestation(
                payload,
                **common,
                platform=arguments.platform,
                architecture=arguments.architecture,
                subject_name=arguments.subject_name,
                subject_digest=arguments.subject_digest,
                scan_report_sha256=arguments.scan_report_sha256,
            )
            _write_json(arguments.output, {"scan_report_sha256": scan_digest})
        else:
            architecture_digests = _platform_map(
                arguments.architecture_digest, "architecture digest"
            )
            scan_report_digests = _platform_map(
                arguments.scan_report_digest, "scan report digest"
            )
            if arguments.operation == "predicate-manifest":
                if arguments.output is None:
                    raise EvidenceError("predicate output is required")
                predicate = manifest_predicate(
                    **common,
                    architecture_digests=architecture_digests,
                    scan_report_digests=scan_report_digests,
                )
                _write_json(arguments.output, predicate)
            else:
                payload = json.loads(arguments.verification.read_text(encoding="utf-8"))
                verify_manifest_attestation(
                    payload,
                    **common,
                    subject_name=arguments.subject_name,
                    subject_digest=arguments.subject_digest,
                    architecture_digests=architecture_digests,
                    scan_report_digests=scan_report_digests,
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
    "EvidenceError",
    "architecture_predicate",
    "manifest_predicate",
    "verify_architecture_attestation",
    "verify_manifest_attestation",
]
