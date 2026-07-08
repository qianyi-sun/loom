#!/usr/bin/env python3
"""Layer-1 benchmark score-alignment manifest gate.

Layer 1 is model-independent: it checks that every v1.0-supported benchmark has
an explicit canonical scoring reference, score-semantics contract, parity
decision, and at least one same-output replay/golden case definition. It does
not call provider APIs and does not judge live model quality.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from loom.benchmark_readiness import V1_SUPPORTED_BENCHMARK_IDS
except ImportError:  # pragma: no cover - direct script fallback.
    V1_SUPPORTED_BENCHMARK_IDS = frozenset(
        {
            "aime-24",
            "aime-25",
            "humaneval",
            "livecodebench",
            "mbpp",
            "mmlu-pro",
            "math-500",
            "gpqa",
            "skillflow",
            "skilllearnbench",
            "swe-bench-verified",
            "terminal-bench-2",
        }
    )


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "docs" / "score-alignment" / "manifest.json"

REFERENCE_REQUIRED = (
    "source_type",
    "title",
    "url",
    "version_or_revision",
    "justification",
)
HARBOR_REQUIRED = ("status", "parity_target", "decision")
SCORE_REQUIRED = (
    "task_set",
    "denominator",
    "task_reward",
    "aggregation",
    "displayed_metric",
    "partial_credit",
)
EVIDENCE_REQUIRED = ("status", "cases")
CASE_REQUIRED = (
    "case_id",
    "input_kind",
    "reference",
    "expected_reward",
    "replay_method",
)
HARBOR_STATUSES = frozenset({"supported", "not_supported", "partial", "unknown"})


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    status: str
    detail: str
    remediation: str


class ManifestError(RuntimeError):
    """Raised when the manifest cannot be loaded as JSON object data."""


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ManifestError(f"{path}: cannot read manifest: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ManifestError(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ManifestError(f"{path}: top-level manifest must be a JSON object")
    return payload


def manifest_benchmark_ids(manifest: dict[str, Any]) -> list[str]:
    rows = manifest.get("benchmarks")
    if not isinstance(rows, list):
        return []
    ids: list[str] = []
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("benchmark_id"), str):
            ids.append(row["benchmark_id"])
    return sorted(ids)


def _missing_string_fields(row: dict[str, Any], fields: tuple[str, ...]) -> list[str]:
    missing: list[str] = []
    for field in fields:
        value = row.get(field)
        if not isinstance(value, str) or not value.strip():
            missing.append(field)
    return missing


def _row_errors(row: dict[str, Any], index: int) -> list[str]:
    benchmark_id = row.get("benchmark_id")
    label = benchmark_id if isinstance(benchmark_id, str) and benchmark_id else f"row[{index}]"
    errors: list[str] = []
    if not isinstance(benchmark_id, str) or not benchmark_id.strip():
        errors.append(f"{label}: missing benchmark_id")

    reference = row.get("canonical_reference")
    if not isinstance(reference, dict):
        errors.append(f"{label}: missing canonical_reference")
    else:
        for field in _missing_string_fields(reference, REFERENCE_REQUIRED):
            errors.append(f"{label}: canonical_reference.{field} is required")

    harbor = row.get("harbor_support")
    if not isinstance(harbor, dict):
        errors.append(f"{label}: missing harbor_support")
    else:
        for field in _missing_string_fields(harbor, HARBOR_REQUIRED):
            errors.append(f"{label}: harbor_support.{field} is required")
        status = harbor.get("status")
        if isinstance(status, str) and status not in HARBOR_STATUSES:
            errors.append(f"{label}: harbor_support.status {status!r} is invalid")
        for field_name in ("decision", "parity_target"):
            value = harbor.get(field_name)
            if isinstance(value, str) and "coder-harbor-cloud" in value:
                errors.append(
                    f"{label}: harbor_support.{field_name} references "
                    f"coder-harbor-cloud (Huawei platform); use "
                    f"harbor-framework/harbor instead"
                )

    score = row.get("score_semantics")
    if not isinstance(score, dict):
        errors.append(f"{label}: missing score_semantics")
    else:
        for field in _missing_string_fields(score, SCORE_REQUIRED):
            errors.append(f"{label}: score_semantics.{field} is required")

    evidence = row.get("layer1_evidence")
    if not isinstance(evidence, dict):
        errors.append(f"{label}: missing layer1_evidence")
    else:
        for field in EVIDENCE_REQUIRED:
            if field not in evidence:
                errors.append(f"{label}: layer1_evidence.{field} is required")
        status = evidence.get("status")
        if not isinstance(status, str) or not status.strip():
            errors.append(f"{label}: layer1_evidence.status is required")
        cases = evidence.get("cases")
        if not isinstance(cases, list) or not cases:
            errors.append(f"{label}: missing layer1 evidence case")
        else:
            for case_index, case in enumerate(cases):
                if not isinstance(case, dict):
                    errors.append(f"{label}: layer1_evidence.cases[{case_index}] must be an object")
                    continue
                for field in CASE_REQUIRED:
                    value = case.get(field)
                    if field == "expected_reward":
                        if not isinstance(value, (int, float)) or isinstance(value, bool):
                            errors.append(
                                f"{label}: cases[{case_index}].expected_reward must be numeric"
                            )
                    elif not isinstance(value, str) or not value.strip():
                        errors.append(f"{label}: cases[{case_index}].{field} is required")
    return errors


def _harbor_reference_errors(manifest: dict[str, Any]) -> list[str]:
    ref = manifest.get("harbor_reference")
    if not isinstance(ref, dict):
        return [
            "harbor_reference block is required at the top level "
            "(must pin harbor-framework/harbor at a 40-char hex commit sha)"
        ]
    errors: list[str] = []
    repo = ref.get("repo")
    if repo != "harbor-framework/harbor":
        errors.append(
            f"harbor_reference.repo must be 'harbor-framework/harbor', got {repo!r}"
        )
    url = ref.get("url")
    if url != "https://github.com/harbor-framework/harbor":
        errors.append(
            "harbor_reference.url must be "
            "'https://github.com/harbor-framework/harbor', "
            f"got {url!r}"
        )
    pinned = ref.get("pinned_commit")
    if not (
        isinstance(pinned, str)
        and len(pinned) == 40
        and all(c in "0123456789abcdef" for c in pinned)
    ):
        errors.append(
            "harbor_reference.pinned_commit must be a 40-char hex sha"
        )
    return errors


def check_manifest(manifest: dict[str, Any]) -> list[CheckResult]:
    failures: list[str] = []
    if manifest.get("schema_version") != 1:
        failures.append("schema_version must be 1")

    failures.extend(_harbor_reference_errors(manifest))

    rows = manifest.get("benchmarks")
    if not isinstance(rows, list):
        failures.append("benchmarks must be a list")
        rows = []

    seen: set[str] = set()
    duplicates: set[str] = set()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            failures.append(f"row[{index}] must be an object")
            continue
        benchmark_id = row.get("benchmark_id")
        if isinstance(benchmark_id, str):
            if benchmark_id in seen:
                duplicates.add(benchmark_id)
            seen.add(benchmark_id)
        failures.extend(_row_errors(row, index))

    expected = set(V1_SUPPORTED_BENCHMARK_IDS)
    missing = sorted(expected - seen)
    unexpected = sorted(seen - expected)
    if missing:
        failures.append("missing v1 benchmarks: " + ", ".join(missing))
    if unexpected:
        failures.append("unexpected benchmarks: " + ", ".join(unexpected))
    if duplicates:
        failures.append("duplicate benchmarks: " + ", ".join(sorted(duplicates)))

    if failures:
        detail = "; ".join(failures[:25])
        if len(failures) > 25:
            detail += f"; ... +{len(failures) - 25} more"
        return [
            CheckResult(
                check_id="benchmark_score_alignment.layer1_manifest",
                status="fail",
                detail=detail,
                remediation=(
                    "Update docs/score-alignment/manifest.json so every "
                    "v1.0 benchmark has a canonical reference, score semantics, "
                    "Harbor/upstream decision, and at least one replay case."
                ),
            )
        ]

    return [
        CheckResult(
            check_id="benchmark_score_alignment.layer1_manifest",
            status="pass",
            detail=f"{len(rows)} benchmark score-alignment entries cover the v1.0 allowlist",
            remediation="",
        )
    ]


def _print_results(results: list[CheckResult]) -> int:
    failed = False
    for result in results:
        print(f"{result.check_id}: {result.status} - {result.detail}")
        if result.status != "pass":
            failed = True
            if result.remediation:
                print(f"  remediation: {result.remediation}")
    return 1 if failed else 0


def _run_manifest(args: argparse.Namespace) -> int:
    try:
        manifest = load_manifest(args.manifest)
    except ManifestError as exc:
        print(f"manifest load failed: {exc}", file=sys.stderr)
        return 2
    return _print_results(check_manifest(manifest))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check Layer-1 benchmark score-alignment metadata.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    manifest = sub.add_parser("manifest")
    manifest.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="Path to score-alignment/manifest.json.",
    )
    manifest.set_defaults(func=_run_manifest)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
