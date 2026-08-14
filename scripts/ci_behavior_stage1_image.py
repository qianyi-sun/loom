#!/usr/bin/env python3
"""Create and validate canonical evidence for the Stage 1 simulator image."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import stat
import sys
from pathlib import Path
from typing import Any


class Stage1ImageEvidenceError(ValueError):
    """Stage 1 image evidence is incomplete, ambiguous, or inconsistent."""


_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_RECORD_KEYS = {
    "build",
    "component",
    "evidence",
    "platform",
    "schema_version",
    "source",
    "subject",
}


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise Stage1ImageEvidenceError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _reject_nonfinite(value: str) -> object:
    raise Stage1ImageEvidenceError(f"non-finite JSON value: {value}")


def _parse(path: Path, *, label: str, max_bytes: int) -> dict[str, Any]:
    observed = path.lstat()
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_nlink != 1
        or observed.st_size <= 0
        or observed.st_size > max_bytes
    ):
        raise Stage1ImageEvidenceError(f"{label} is not a bounded private regular file")
    try:
        value = json.loads(
            path.read_bytes(),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite,
            parse_float=lambda raw: _finite_float(raw, label),
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Stage1ImageEvidenceError(f"{label} is invalid JSON: {exc}") from exc
    after = path.lstat()
    if (observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise Stage1ImageEvidenceError(f"{label} changed while reading")
    if not isinstance(value, dict):
        raise Stage1ImageEvidenceError(f"{label} must be an object")
    return value


def _finite_float(raw: str, label: str) -> float:
    value = float(raw)
    if not math.isfinite(value):
        raise Stage1ImageEvidenceError(f"{label} contains a non-finite number")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        + b"\n"
    )


def _exact(value: str, pattern: re.Pattern[str], label: str) -> str:
    if pattern.fullmatch(value) is None:
        raise Stage1ImageEvidenceError(f"{label} is invalid")
    return value


def _positive(value: int, label: str) -> int:
    if isinstance(value, bool) or value <= 0:
        raise Stage1ImageEvidenceError(f"{label} must be positive")
    return value


def _canonical_document(path: Path, *, label: str) -> dict[str, Any]:
    value = _parse(path, label=label, max_bytes=4 * 1024 * 1024)
    if path.read_bytes() != _canonical(value):
        raise Stage1ImageEvidenceError(f"{label} is not canonical JCS plus LF")
    return value


def _record(args: argparse.Namespace) -> dict[str, object]:
    if args.mode == "candidate" and args.event_name not in {
        "merge_group",
        "pull_request",
        "workflow_dispatch",
    }:
        raise Stage1ImageEvidenceError("candidate event is invalid")
    if args.mode == "trusted-publish":
        if args.event_name not in {"push", "workflow_dispatch"}:
            raise Stage1ImageEvidenceError("trusted publish event is invalid")
        if args.ref_name not in {"dev", "main"}:
            raise Stage1ImageEvidenceError("trusted publish ref is invalid")
    if args.image != "behavior-stage1-sim":
        raise Stage1ImageEvidenceError("Stage 1 component identity drift")
    if args.image_name != "loom-behavior-stage1-sim":
        raise Stage1ImageEvidenceError("Stage 1 release image identity drift")
    if args.dockerfile != "deploy/Dockerfile.behavior-stage1-sim" or args.context != ".":
        raise Stage1ImageEvidenceError("Stage 1 build input drift")
    if args.platform != "linux/amd64":
        raise Stage1ImageEvidenceError("Stage 1 platform drift")

    source_evidence = _canonical_document(args.source_evidence, label="source evidence")
    compatibility = _canonical_document(
        args.compatibility_manifest,
        label="compatibility manifest",
    )
    if compatibility.get("build_sha") != args.head_sha:
        raise Stage1ImageEvidenceError("compatibility manifest build SHA drift")
    if compatibility.get("build_tree_sha") != args.tree_sha:
        raise Stage1ImageEvidenceError("compatibility manifest build tree drift")
    if compatibility.get("source_evidence_sha256") != _sha256(args.source_evidence):
        raise Stage1ImageEvidenceError("compatibility manifest source evidence drift")
    if source_evidence.get("schema_version") != ("loom.behavior-stage1-image-source-evidence.v1"):
        raise Stage1ImageEvidenceError("source evidence schema drift")

    scan = _parse(args.scan_report, label="Trivy report", max_bytes=256 * 1024 * 1024)
    sbom = _parse(args.sbom, label="SPDX SBOM", max_bytes=256 * 1024 * 1024)
    if not isinstance(scan.get("Results"), list):
        raise Stage1ImageEvidenceError("Trivy report has no Results array")
    if sbom.get("spdxVersion") != "SPDX-2.3" or not isinstance(sbom.get("packages"), list):
        raise Stage1ImageEvidenceError("SPDX SBOM contract drift")

    return {
        "build": {
            "mode": args.mode,
            "run_attempt": _positive(args.run_attempt, "run attempt"),
            "run_id": _positive(args.run_id, "run id"),
        },
        "component": {
            "context": args.context,
            "dockerfile": args.dockerfile,
            "id": args.image,
            "image_name": args.image_name,
        },
        "evidence": {
            "compatibility_manifest_sha256": _sha256(args.compatibility_manifest),
            "preflight_sha256": _sha256(args.preflight),
            "sbom_sha256": _sha256(args.sbom),
            "scan_report_sha256": _sha256(args.scan_report),
            "source_evidence_sha256": _sha256(args.source_evidence),
        },
        "platform": args.platform,
        "schema_version": "loom.behavior-stage1-image-child-release.v1",
        "source": {
            "event_name": args.event_name,
            "head_sha": _exact(args.head_sha, _GIT_SHA, "head SHA"),
            "ref_name": args.ref_name,
            "repository": _exact(args.repository, _REPOSITORY, "repository"),
            "tree_sha": _exact(args.tree_sha, _GIT_SHA, "tree SHA"),
        },
        "subject": {
            "digest": _exact(args.subject_digest, _DIGEST, "subject digest"),
            "name": args.subject_name,
        },
    }


def _write(path: Path, value: object) -> None:
    path.write_bytes(_canonical(value))


def _validate_record(path: Path, *, mode: str) -> None:
    value = _canonical_document(path, label="Stage 1 child release record")
    if set(value) != _RECORD_KEYS:
        raise Stage1ImageEvidenceError("Stage 1 child release record keys drifted")
    if value.get("schema_version") != "loom.behavior-stage1-image-child-release.v1":
        raise Stage1ImageEvidenceError("Stage 1 child release record schema drift")
    build = value.get("build")
    if not isinstance(build, dict) or build.get("mode") != mode:
        raise Stage1ImageEvidenceError("Stage 1 child release record mode drift")
    subject = value.get("subject")
    if not isinstance(subject, dict):
        raise Stage1ImageEvidenceError("Stage 1 child release subject is invalid")
    _exact(str(subject.get("digest")), _DIGEST, "recorded subject digest")


def _record_index(args: argparse.Namespace) -> dict[str, object]:
    _validate_record(args.child_record, mode="trusted-publish")
    child = _canonical_document(args.child_record, label="Stage 1 child release record")
    manifest = _parse(args.index_manifest, label="OCI index manifest", max_bytes=4 * 1024 * 1024)
    if manifest.get("mediaType") != "application/vnd.oci.image.index.v1+json":
        raise Stage1ImageEvidenceError("Stage 1 release subject is not an OCI index")
    descriptors = manifest.get("manifests")
    child_subject = child["subject"]
    if not isinstance(child_subject, dict):
        raise Stage1ImageEvidenceError("Stage 1 child subject drift")
    expected_descriptor = {
        "architecture": "amd64",
        "digest": child_subject["digest"],
        "os": "linux",
    }
    if not isinstance(descriptors, list) or len(descriptors) != 1:
        raise Stage1ImageEvidenceError("Stage 1 OCI index must contain exactly one child")
    descriptor = descriptors[0]
    if (
        not isinstance(descriptor, dict)
        or descriptor.get("digest") != expected_descriptor["digest"]
    ):
        raise Stage1ImageEvidenceError("Stage 1 OCI child digest drift")
    platform = descriptor.get("platform")
    if not isinstance(platform, dict) or {
        "architecture": platform.get("architecture"),
        "os": platform.get("os"),
    } != {"architecture": "amd64", "os": "linux"}:
        raise Stage1ImageEvidenceError("Stage 1 OCI child platform drift")
    _parse(
        args.provenance_verification,
        label="provenance verification",
        max_bytes=4 * 1024 * 1024,
    )
    _parse(
        args.sbom_verification,
        label="SBOM verification",
        max_bytes=4 * 1024 * 1024,
    )
    return {
        "attestations": {
            "child_provenance_verification_sha256": _sha256(args.provenance_verification),
            "child_sbom_verification_sha256": _sha256(args.sbom_verification),
        },
        "child": child,
        "index": {
            "manifest_sha256": _sha256(args.index_manifest),
            "subject": {
                "digest": _exact(args.subject_digest, _DIGEST, "index subject digest"),
                "name": args.subject_name,
            },
        },
        "schema_version": "loom.behavior-stage1-image-index-release.v1",
    }


def _validate_index_record(path: Path) -> None:
    value = _canonical_document(path, label="Stage 1 index release record")
    if set(value) != {"attestations", "child", "index", "schema_version"}:
        raise Stage1ImageEvidenceError("Stage 1 index release record keys drifted")
    if value.get("schema_version") != "loom.behavior-stage1-image-index-release.v1":
        raise Stage1ImageEvidenceError("Stage 1 index release record schema drift")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record-child")
    record.add_argument("--mode", choices=("candidate", "trusted-publish"), required=True)
    record.add_argument("--repository", required=True)
    record.add_argument("--ref-name", required=True)
    record.add_argument("--head-sha", required=True)
    record.add_argument("--tree-sha", required=True)
    record.add_argument("--run-id", type=int, required=True)
    record.add_argument("--run-attempt", type=int, required=True)
    record.add_argument("--event-name", required=True)
    record.add_argument("--image", required=True)
    record.add_argument("--image-name", required=True)
    record.add_argument("--dockerfile", required=True)
    record.add_argument("--context", required=True)
    record.add_argument("--platform", required=True)
    record.add_argument("--subject-name", required=True)
    record.add_argument("--subject-digest", required=True)
    record.add_argument("--scan-report", type=Path, required=True)
    record.add_argument("--sbom", type=Path, required=True)
    record.add_argument("--source-evidence", type=Path, required=True)
    record.add_argument("--compatibility-manifest", type=Path, required=True)
    record.add_argument("--preflight", type=Path, required=True)
    record.add_argument("--output", type=Path, required=True)
    validate = subparsers.add_parser("validate-child")
    validate.add_argument("--record", type=Path, required=True)
    validate.add_argument("--mode", choices=("candidate", "trusted-publish"), required=True)
    index = subparsers.add_parser("record-index")
    index.add_argument("--child-record", type=Path, required=True)
    index.add_argument("--index-manifest", type=Path, required=True)
    index.add_argument("--subject-name", required=True)
    index.add_argument("--subject-digest", required=True)
    index.add_argument("--provenance-verification", type=Path, required=True)
    index.add_argument("--sbom-verification", type=Path, required=True)
    index.add_argument("--output", type=Path, required=True)
    validate_index = subparsers.add_parser("validate-index")
    validate_index.add_argument("--record", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "record-child":
            _write(args.output, _record(args))
        elif args.command == "validate-child":
            _validate_record(args.record, mode=args.mode)
        elif args.command == "record-index":
            _write(args.output, _record_index(args))
        else:
            _validate_index_record(args.record)
        return 0
    except (OSError, Stage1ImageEvidenceError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
