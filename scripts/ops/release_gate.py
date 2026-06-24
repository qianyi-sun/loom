#!/usr/bin/env python3
"""Validate release-promotion gate evidence for production deploys.

The heavy staging gate includes live cluster, API, benchmark, provider, worker,
and rollback checks that are partly operator-driven. This script validates the
structured evidence manifest those checks produce so a production deploy can
machine-reject missing evidence or leaked secrets.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REQUIRED_IMAGE_DIGESTS = (
    "loom-control-plane",
    "loom-llm-gateway",
    "loom-service",
    "loom-worker",
    "loom-web",
)

REQUIRED_CHECKS: dict[str, tuple[str, ...]] = {
    "repository_ci": ("url",),
    "image_build": ("url",),
    "cluster_render_audit": ("url", "staging_config", "production_config"),
    "migration_dry_run": ("url", "db_recovery_point"),
    "public_api_spa_smoke": ("url", "batch_id", "trial_id", "artifact_url"),
    "secret_redaction": ("url",),
    "provider_smoke": ("url", "provider_path"),
    "benchmark_reward_gate": ("url", "batch_id", "benchmarks"),
    "benchmark_score_alignment": ("url", "manifest", "benchmarks"),
    "worker_capacity_smoke": ("url", "batch_id", "k8s_workers", "oldlab_workers"),
    "rollback_plan": (
        "previous_production_image_digest",
        "rendered_manifest",
        "db_recovery_point",
    ),
    "release_owner_approval": ("owner", "url"),
}

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"(^|@)sha256:[0-9a-f]{64}$")
URL_RE = re.compile(r"^https://[^\s]+$")
FORBIDDEN_PATTERNS = (
    re.compile(r"authorization:\s*bearer", re.IGNORECASE),
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"[?&](X-Amz-Signature|AWSAccessKeyId|Signature)=", re.IGNORECASE),
    re.compile(r"\bloom://", re.IGNORECASE),
    re.compile(r"\bgithub-environment:", re.IGNORECASE),
    re.compile(r"https?://loom-(minio|postgres|llm-gateway|control-plane)([:/.]|$)", re.IGNORECASE),
    re.compile(r"\.svc\.cluster\.local\b", re.IGNORECASE),
    re.compile(r"\bhost\.docker\.internal\b", re.IGNORECASE),
)


def _load_manifest(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("manifest root must be a JSON object")
    return raw


def _iter_strings(value: Any, path: str) -> list[tuple[str, str]]:
    if isinstance(value, str):
        return [(path, value)]
    if isinstance(value, dict):
        pairs: list[tuple[str, str]] = []
        for key, child in value.items():
            pairs.extend(_iter_strings(child, f"{path}.{key}" if path else str(key)))
        return pairs
    if isinstance(value, list):
        pairs = []
        for index, child in enumerate(value):
            pairs.extend(_iter_strings(child, f"{path}[{index}]"))
        return pairs
    return []


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_top_level(
    manifest: dict[str, Any],
    *,
    candidate_sha: str | None,
    image_tag: str | None,
) -> list[str]:
    errors: list[str] = []
    if manifest.get("schema_version") != 1:
        errors.append("schema_version must be 1")

    manifest_sha = manifest.get("candidate_sha")
    if not isinstance(manifest_sha, str) or not SHA_RE.fullmatch(manifest_sha):
        errors.append("candidate_sha must be a 40-character lowercase git SHA")
    if candidate_sha and manifest_sha != candidate_sha:
        errors.append(f"candidate_sha mismatch: manifest={manifest_sha!r} expected={candidate_sha!r}")

    manifest_image_tag = manifest.get("image_tag")
    if not _is_non_empty_string(manifest_image_tag):
        errors.append("image_tag must be a non-empty string")
    if image_tag and manifest_image_tag != image_tag:
        errors.append(f"image_tag mismatch: manifest={manifest_image_tag!r} expected={image_tag!r}")

    staging_url = manifest.get("staging_url")
    if not isinstance(staging_url, str) or not URL_RE.fullmatch(staging_url):
        errors.append("staging_url must be an https URL")

    digests = manifest.get("image_digests")
    if not isinstance(digests, dict):
        errors.append("image_digests must be an object")
        return errors
    for image_name in REQUIRED_IMAGE_DIGESTS:
        digest = digests.get(image_name)
        if not isinstance(digest, str) or not DIGEST_RE.search(digest):
            errors.append(f"image_digests.{image_name} must end with @sha256:<64 hex>")
    return errors


def _validate_checks(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    checks = manifest.get("checks")
    if not isinstance(checks, dict):
        return ["checks must be an object"]

    for check_name, required_fields in REQUIRED_CHECKS.items():
        check = checks.get(check_name)
        if not isinstance(check, dict):
            errors.append(f"missing required check '{check_name}'")
            continue
        if check.get("status") != "pass":
            errors.append(f"{check_name}.status must be 'pass'")
        for field in required_fields:
            value = check.get(field)
            if field in {"k8s_workers", "oldlab_workers"}:
                if not isinstance(value, int) or value < 0:
                    errors.append(f"{check_name}.{field} must be a non-negative integer")
                continue
            if field == "benchmarks":
                if not isinstance(value, list) or not value or not all(
                    _is_non_empty_string(item) for item in value
                ):
                    errors.append(f"{check_name}.benchmarks must be a non-empty string list")
                continue
            if not _is_non_empty_string(value):
                errors.append(f"{check_name}.{field} must be a non-empty string")

        if check_name == "worker_capacity_smoke":
            errors.extend(_validate_worker_capacity_smoke(check))

    return errors


def _validate_worker_capacity_smoke(check: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    runtime_seconds = check.get("runtime_seconds")
    if not isinstance(runtime_seconds, int | float) or runtime_seconds < 0:
        errors.append("worker_capacity_smoke.runtime_seconds must be a non-negative number")
    failures = check.get("failures")
    if not isinstance(failures, int) or failures < 0:
        errors.append("worker_capacity_smoke.failures must be a non-negative integer")

    oldlab_workers = check.get("oldlab_workers")
    if not isinstance(oldlab_workers, int) or oldlab_workers <= 0:
        return errors

    records = check.get("oldlab_worker_records")
    if not isinstance(records, list) or len(records) < oldlab_workers:
        errors.append(
            "worker_capacity_smoke.oldlab_worker_records must include one "
            "record per OLDLAB worker",
        )
        return errors

    required_text_fields = ("node_name", "slurm_job_id", "worker_id")
    required_int_fields = ("concurrency", "trials_claimed")
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            errors.append(f"worker_capacity_smoke.oldlab_worker_records[{index}] must be an object")
            continue
        for field in required_text_fields:
            if not _is_non_empty_string(record.get(field)):
                errors.append(
                    f"worker_capacity_smoke.oldlab_worker_records[{index}].{field} "
                    "must be a non-empty string",
                )
        for field in required_int_fields:
            value = record.get(field)
            minimum = 1 if field == "concurrency" else 0
            if not isinstance(value, int) or value < minimum:
                errors.append(
                    f"worker_capacity_smoke.oldlab_worker_records[{index}].{field} "
                    f"must be an integer >= {minimum}",
                )
    return errors


def _validate_no_leaks(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for path, value in _iter_strings(manifest, ""):
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(value):
                errors.append(f"forbidden evidence value at {path}")
                break
    return errors


def validate_manifest(
    manifest: dict[str, Any],
    *,
    candidate_sha: str | None = None,
    image_tag: str | None = None,
) -> list[str]:
    errors = _validate_top_level(manifest, candidate_sha=candidate_sha, image_tag=image_tag)
    errors.extend(_validate_checks(manifest))
    errors.extend(_validate_no_leaks(manifest))
    return errors


def _evidence_report(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "pass",
        "candidate_sha": manifest["candidate_sha"],
        "image_tag": manifest["image_tag"],
        "staging_url": manifest["staging_url"],
        "image_digests": manifest["image_digests"],
        "checks": manifest["checks"],
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Release Gate Evidence",
        "",
        f"- Candidate SHA: `{report['candidate_sha']}`",
        f"- Image tag: `{report['image_tag']}`",
        f"- Staging URL: {report['staging_url']}",
        "",
        "## Image Digests",
        "",
    ]
    for image_name in REQUIRED_IMAGE_DIGESTS:
        lines.append(f"- `{image_name}`: `{report['image_digests'][image_name]}`")

    lines.extend(["", "## Checks", "", "| Check | Status | Evidence |", "| --- | --- | --- |"])
    for check_name in REQUIRED_CHECKS:
        check = report["checks"][check_name]
        evidence = check.get("url") or check.get("rendered_manifest") or check.get("owner") or ""
        lines.append(f"| `{check_name}` | `{check['status']}` | {evidence} |")
    lines.append("")
    return "\n".join(lines)


def _write_outputs(
    *,
    manifest: dict[str, Any],
    output_json: Path | None,
    output_markdown: Path | None,
) -> None:
    report = _evidence_report(manifest)
    if output_json is not None:
        output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if output_markdown is not None:
        output_markdown.write_text(_render_markdown(report), encoding="utf-8")


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--image-tag", required=True)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate and render gate evidence.")
    _add_common_args(validate)
    validate.add_argument("--output-json", type=Path)
    validate.add_argument("--output-markdown", type=Path)

    verify = subparsers.add_parser(
        "verify-production",
        help="Validate gate evidence before production deploy.",
    )
    _add_common_args(verify)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        manifest = _load_manifest(args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Release gate validation: FAIL\n- failed to read manifest: {exc}", file=sys.stderr)
        return 1

    errors = validate_manifest(
        manifest,
        candidate_sha=args.candidate_sha,
        image_tag=args.image_tag,
    )
    if errors:
        print("Release gate validation: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    if args.command == "validate":
        _write_outputs(
            manifest=manifest,
            output_json=args.output_json,
            output_markdown=args.output_markdown,
        )

    print("Release gate validation: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
