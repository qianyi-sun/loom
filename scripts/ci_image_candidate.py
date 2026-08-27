#!/usr/bin/env python3
"""Create and verify reusable PR image-archive provenance.

The image workflow builds same-repository PR images without registry authority.
This helper records the resulting local Docker archives, aggregates the
per-platform records, and verifies an exact candidate index before a trusted
push imports and publishes any archive.
"""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

SCHEMA = 1
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
ARCHIVE_SHA_PATTERN = re.compile(r"[0-9a-f]{64}")
ARCHITECTURE_PLATFORMS = {
    "amd64": "linux/amd64",
    "arm64": "linux/arm64",
}
MATRIX_KEYS = (
    "image",
    "image_name",
    "dockerfile",
    "context",
    "architecture",
    "platform",
)
RECORD_KEYS = {
    "schema",
    "repository",
    "pull_request",
    "head_sha",
    "base_sha",
    "tree_sha",
    "run_id",
    "run_attempt",
    *MATRIX_KEYS,
    "artifact_name",
    "archive_sha256",
    "archive_size",
}
INDEX_KEYS = {
    "schema",
    "repository",
    "pull_request",
    "head_sha",
    "base_sha",
    "tree_sha",
    "run_id",
    "run_attempt",
    "builds",
}


class CandidateError(ValueError):
    """Raised when candidate provenance is incomplete or ambiguous."""


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CandidateError(f"cannot read JSON from {path}: {exc}") from exc


def _require_sha(value: object, field: str) -> str:
    if not isinstance(value, str) or SHA_PATTERN.fullmatch(value) is None:
        raise CandidateError(f"{field} must be a lowercase 40-character SHA")
    return value


def _require_positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CandidateError(f"{field} must be a positive integer")
    return value


def _require_string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        raise CandidateError(f"{field} must be a non-empty single-line string")
    return value


def _matrix_rows(raw: object) -> list[dict[str, str]]:
    if not isinstance(raw, list) or not raw:
        raise CandidateError("expected image matrix must be a non-empty JSON array")
    rows: list[dict[str, str]] = []
    identities: set[tuple[str, str]] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise CandidateError(f"matrix row {index} must be an object")
        if set(item) != set(MATRIX_KEYS):
            raise CandidateError(
                f"matrix row {index} differs from schema: "
                f"missing={sorted(set(MATRIX_KEYS) - set(item))}, "
                f"extra={sorted(set(item) - set(MATRIX_KEYS))}"
            )
        row = {key: _require_string(item.get(key), f"matrix[{index}].{key}") for key in MATRIX_KEYS}
        architecture = row["architecture"]
        if ARCHITECTURE_PLATFORMS.get(architecture) != row["platform"]:
            raise CandidateError(f"matrix row {index} has inconsistent architecture/platform")
        identity = (row["image"], architecture)
        if identity in identities:
            raise CandidateError(f"duplicate matrix identity: {identity}")
        identities.add(identity)
        rows.append(row)
    return rows


def parse_matrix(value: str) -> list[dict[str, str]]:
    try:
        raw = json.loads(value)
    except json.JSONDecodeError as exc:
        raise CandidateError(f"invalid expected matrix JSON: {exc}") from exc
    return _matrix_rows(raw)


def _validate_common(payload: Mapping[str, object]) -> dict[str, object]:
    repository = _require_string(payload.get("repository"), "repository")
    if repository.count("/") != 1:
        raise CandidateError("repository must use owner/name form")
    return {
        "repository": repository,
        "pull_request": _require_positive_integer(payload.get("pull_request"), "pull_request"),
        "head_sha": _require_sha(payload.get("head_sha"), "head_sha"),
        "base_sha": _require_sha(payload.get("base_sha"), "base_sha"),
        "tree_sha": _require_sha(payload.get("tree_sha"), "tree_sha"),
        "run_id": _require_positive_integer(payload.get("run_id"), "run_id"),
        "run_attempt": _require_positive_integer(payload.get("run_attempt"), "run_attempt"),
    }


def _expected_artifact_name(image: str, architecture: str, run_attempt: int) -> str:
    return f"image-candidate-archive-{image}-{architecture}-attempt-{run_attempt}"


def _artifact_attempt(artifact_name: str, image: str, architecture: str) -> int:
    prefix = f"image-candidate-archive-{image}-{architecture}-attempt-"
    suffix = artifact_name.removeprefix(prefix)
    if not artifact_name.startswith(prefix) or not suffix.isdigit():
        raise CandidateError("candidate artifact name does not match its image")
    return _require_positive_integer(int(suffix), "candidate artifact run attempt")


def validate_record(payload: object) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        raise CandidateError("candidate record must be an object")
    if set(payload) != RECORD_KEYS:
        raise CandidateError(
            "candidate record keys differ from schema: "
            f"missing={sorted(RECORD_KEYS - set(payload))}, "
            f"extra={sorted(set(payload) - RECORD_KEYS)}"
        )
    if payload.get("schema") != SCHEMA:
        raise CandidateError("unsupported candidate record schema")
    common = _validate_common(payload)
    row = {key: _require_string(payload.get(key), key) for key in MATRIX_KEYS}
    if ARCHITECTURE_PLATFORMS.get(row["architecture"]) != row["platform"]:
        raise CandidateError("candidate architecture/platform mismatch")
    artifact_name = _require_string(payload.get("artifact_name"), "artifact_name")
    if artifact_name != _expected_artifact_name(
        row["image"],
        row["architecture"],
        _require_positive_integer(common.get("run_attempt"), "run_attempt"),
    ):
        raise CandidateError("candidate artifact name does not match its image")
    archive_sha256 = _require_string(payload.get("archive_sha256"), "archive_sha256")
    if ARCHIVE_SHA_PATTERN.fullmatch(archive_sha256) is None:
        raise CandidateError("archive_sha256 must be a lowercase SHA-256")
    archive_size = _require_positive_integer(payload.get("archive_size"), "archive_size")
    return {
        "schema": SCHEMA,
        **common,
        **row,
        "artifact_name": artifact_name,
        "archive_sha256": archive_sha256,
        "archive_size": archive_size,
    }


def build_record(args: argparse.Namespace) -> dict[str, object]:
    return validate_record(
        {
            "schema": SCHEMA,
            "repository": args.repository,
            "pull_request": args.pull_request,
            "head_sha": args.head_sha,
            "base_sha": args.base_sha,
            "tree_sha": args.tree_sha,
            "run_id": args.run_id,
            "run_attempt": args.run_attempt,
            "image": args.image,
            "image_name": args.image_name,
            "dockerfile": args.dockerfile,
            "context": args.context,
            "architecture": args.architecture,
            "platform": args.platform,
            "artifact_name": args.artifact_name,
            "archive_sha256": args.archive_sha256,
            "archive_size": args.archive_size,
        }
    )


def _run_identity(payload: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(
        payload[key]
        for key in (
            "repository",
            "pull_request",
            "head_sha",
            "base_sha",
            "tree_sha",
            "run_id",
        )
    )


def aggregate_records(
    record_paths: Sequence[Path], expected_matrix: list[dict[str, str]]
) -> dict[str, object]:
    if not record_paths:
        raise CandidateError("candidate record directory is empty")
    records = [validate_record(_load_json(path)) for path in record_paths]
    identities = {_run_identity(record) for record in records}
    if len(identities) != 1:
        raise CandidateError("candidate records do not share one source identity")
    actual: dict[tuple[str, str], dict[str, object]] = {}
    attempts: set[tuple[str, str, int]] = set()
    for record in records:
        identity = (str(record["image"]), str(record["architecture"]))
        attempt = _require_positive_integer(record.get("run_attempt"), "run_attempt")
        attempt_identity = (*identity, attempt)
        if attempt_identity in attempts:
            raise CandidateError("candidate records contain duplicate image architecture attempts")
        attempts.add(attempt_identity)
        previous = actual.get(identity)
        if previous is None or attempt > _require_positive_integer(
            previous.get("run_attempt"), "run_attempt"
        ):
            actual[identity] = record
    expected = {(row["image"], row["architecture"]): row for row in expected_matrix}
    if set(actual) != set(expected):
        raise CandidateError(
            "candidate records differ from expected matrix: "
            f"missing={sorted(set(expected) - set(actual))}, "
            f"extra={sorted(set(actual) - set(expected))}"
        )
    for identity, row in expected.items():
        observed = actual[identity]
        if any(observed[key] != row[key] for key in MATRIX_KEYS):
            raise CandidateError(f"candidate matrix fields changed for {identity}")
    first = next(iter(actual.values()))
    latest_attempt = max(
        _require_positive_integer(record.get("run_attempt"), "run_attempt")
        for record in actual.values()
    )
    builds = [
        {
            **row,
            "candidate_artifact": str(actual[(row["image"], row["architecture"])]["artifact_name"]),
            "archive_sha256": str(actual[(row["image"], row["architecture"])]["archive_sha256"]),
            "archive_size": _require_positive_integer(
                actual[(row["image"], row["architecture"])].get("archive_size"),
                "archive_size",
            ),
        }
        for row in expected_matrix
    ]
    return {
        "schema": SCHEMA,
        **{
            key: first[key]
            for key in (
                "repository",
                "pull_request",
                "head_sha",
                "base_sha",
                "tree_sha",
                "run_id",
            )
        },
        "run_attempt": latest_attempt,
        "builds": builds,
    }


def validate_index(
    payload: object,
    *,
    expected_matrix: list[dict[str, str]],
    repository: str,
    pull_request: int,
    head_sha: str,
    base_sha: str,
    tree_sha: str,
    run_id: int,
    run_attempt: int,
) -> dict[str, object]:
    if not isinstance(payload, Mapping) or set(payload) != INDEX_KEYS:
        raise CandidateError("candidate index does not match the exact schema")
    if payload.get("schema") != SCHEMA:
        raise CandidateError("unsupported candidate index schema")
    common = _validate_common(payload)
    expected_common = {
        "repository": repository,
        "pull_request": pull_request,
        "head_sha": head_sha,
        "base_sha": base_sha,
        "tree_sha": tree_sha,
        "run_id": run_id,
        "run_attempt": run_attempt,
    }
    if common != expected_common:
        raise CandidateError("candidate index source identity does not match the merge")
    builds = payload.get("builds")
    if not isinstance(builds, list):
        raise CandidateError("candidate index builds must be an array")
    expected = {(row["image"], row["architecture"]): row for row in expected_matrix}
    validated: dict[tuple[str, str], dict[str, object]] = {}
    candidate_attempts: list[int] = []
    for index, item in enumerate(builds):
        if not isinstance(item, Mapping):
            raise CandidateError(f"candidate index build {index} must be an object")
        allowed = {
            *MATRIX_KEYS,
            "candidate_artifact",
            "archive_sha256",
            "archive_size",
        }
        if set(item) != allowed:
            raise CandidateError(f"candidate index build {index} has invalid keys")
        row = {key: _require_string(item.get(key), f"builds[{index}].{key}") for key in MATRIX_KEYS}
        identity = (row["image"], row["architecture"])
        if identity in validated or identity not in expected:
            raise CandidateError(f"unexpected candidate index identity: {identity}")
        if any(row[key] != expected[identity][key] for key in MATRIX_KEYS):
            raise CandidateError(f"candidate index matrix fields changed for {identity}")
        candidate_artifact = _require_string(
            item.get("candidate_artifact"), f"builds[{index}].candidate_artifact"
        )
        candidate_attempt = _artifact_attempt(candidate_artifact, *identity)
        if candidate_attempt > _require_positive_integer(
            common.get("run_attempt"), "run_attempt"
        ):
            raise CandidateError(f"candidate artifact is invalid for {identity}")
        candidate_attempts.append(candidate_attempt)
        archive_sha256 = _require_string(
            item.get("archive_sha256"), f"builds[{index}].archive_sha256"
        )
        if ARCHIVE_SHA_PATTERN.fullmatch(archive_sha256) is None:
            raise CandidateError(f"candidate archive SHA is invalid for {identity}")
        archive_size = _require_positive_integer(
            item.get("archive_size"), f"builds[{index}].archive_size"
        )
        validated[identity] = {
            **row,
            "candidate_artifact": candidate_artifact,
            "archive_sha256": archive_sha256,
            "archive_size": archive_size,
        }
    if set(validated) != set(expected):
        raise CandidateError("candidate index does not cover the expected matrix")
    if max(candidate_attempts) != common["run_attempt"]:
        raise CandidateError("candidate index run attempt does not match its newest build")
    return {
        "schema": SCHEMA,
        **common,
        "builds": [validated[(row["image"], row["architecture"])] for row in expected_matrix],
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_github_output(path: Path, values: Mapping[str, object]) -> None:
    with path.open("a", encoding="utf-8") as output:
        for key, value in values.items():
            rendered = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
            output.write(f"{key}={rendered}\n")


def _add_common_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True)
    parser.add_argument("--pull-request", type=int, required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--tree-sha", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--run-attempt", type=int, required=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    record_parser = subparsers.add_parser("record")
    _add_common_identity_arguments(record_parser)
    for argument in MATRIX_KEYS:
        record_parser.add_argument(f"--{argument.replace('_', '-')}", required=True)
    record_parser.add_argument("--artifact-name", required=True)
    record_parser.add_argument("--archive-sha256", required=True)
    record_parser.add_argument("--archive-size", type=int, required=True)
    record_parser.add_argument("--output", type=Path, required=True)

    aggregate_parser = subparsers.add_parser("aggregate")
    aggregate_parser.add_argument("--records-dir", type=Path, required=True)
    aggregate_parser.add_argument("--expected-matrix-json", required=True)
    aggregate_parser.add_argument("--output", type=Path, required=True)

    verify_parser = subparsers.add_parser("verify-index")
    verify_parser.add_argument("--index", type=Path, required=True)
    verify_parser.add_argument("--expected-matrix-json", required=True)
    _add_common_identity_arguments(verify_parser)
    verify_parser.add_argument("--github-output", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "record":
        _write_json(args.output, build_record(args))
    elif args.command == "aggregate":
        expected = parse_matrix(args.expected_matrix_json)
        paths = sorted(args.records_dir.glob("*.json"))
        _write_json(args.output, aggregate_records(paths, expected))
    elif args.command == "verify-index":
        expected = parse_matrix(args.expected_matrix_json)
        index = validate_index(
            _load_json(args.index),
            expected_matrix=expected,
            repository=args.repository,
            pull_request=args.pull_request,
            head_sha=args.head_sha,
            base_sha=args.base_sha,
            tree_sha=args.tree_sha,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
        )
        _write_github_output(
            args.github_output,
            {
                "available": "true",
                "source_head": index["head_sha"],
                "source_base": index["base_sha"],
                "source_tree": index["tree_sha"],
                "source_run_id": index["run_id"],
                "source_run_attempt": index["run_attempt"],
                "builds": index["builds"],
            },
        )
    else:  # pragma: no cover - argparse enforces the command set.
        raise AssertionError(args.command)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CandidateError as exc:
        raise SystemExit(f"FAIL: {exc}") from exc
