"""Fail-closed installed entrypoint for isolated preflight rehearsal actions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.operator.backup import VerifiedBackup
from loom_cli.rollout.operator.checkpoint_lease import inspect_critical_checkpoint
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.rehearsal_action_source import RehearsalPlan
from loom_cli.rollout.rehearsal_executor import IsolatedRehearsalExecutor
from loom_cli.rollout.rehearsal_journal_backend import RehearsalStepOutcome
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_PLAN_BYTES = 64 * 1024


def _strict_json_object(payload: bytes) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("rehearsal helper plan JSON is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("rehearsal helper plan JSON is invalid")
    return value


def _load_plan(path: Path, expected_digest: str) -> RehearsalPlan:
    if _SHA256_RE.fullmatch(expected_digest) is None:
        raise ValueError("rehearsal helper plan digest is invalid")
    trusted = read_trusted_file(
        path,
        service_uid=os.geteuid(),
        private=True,
        max_bytes=_MAX_PLAN_BYTES,
        require_nonempty=True,
    )
    payload = trusted.payload.rstrip(b"\n")
    if hashlib.sha256(payload).hexdigest() != expected_digest:
        raise ValueError("rehearsal helper plan digest drifted")
    plan = RehearsalPlan.from_record(_strict_json_object(payload))
    if plan.plan_digest != expected_digest:
        raise ValueError("rehearsal helper plan identity drifted")
    expected_parent = Path("/var/lib/loom-staging-rollout/rehearsals") / plan.resources.namespace
    if path != expected_parent / "plan.json":
        raise ValueError("rehearsal helper plan path escaped its authority")
    _verify_checkpoint(plan)
    _verify_artifact_publication(plan)
    return plan


def _verify_checkpoint(plan: RehearsalPlan) -> None:
    checkpoint = inspect_critical_checkpoint(
        VerifiedBackup(
            manifest_path=plan.checkpoint_manifest_path,
            manifest_sha256=plan.checkpoint_manifest_sha256,
        ),
        request_id=plan.checkpoint_request_id,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.geteuid(),
        now=datetime.now(UTC),
    )
    if (
        checkpoint.evidence_digest != plan.checkpoint_evidence_sha256
        or checkpoint.manifest_path != plan.checkpoint_manifest_path
        or checkpoint.manifest_sha256 != plan.checkpoint_manifest_sha256
        or checkpoint.mutation_epoch != plan.mutation_epoch
        or checkpoint.db_snapshot_identity != plan.db_snapshot_identity
        or checkpoint.object_inventory_root != plan.object_inventory_root
        or checkpoint.schema_revision != plan.schema_revision
    ):
        raise ValueError("rehearsal helper checkpoint identity drifted")


def _verify_artifact_publication(plan: RehearsalPlan) -> None:
    state_root = plan.artifact_descriptor_path.parents[2]
    publication = PreflightArtifactStore(
        state_root,
        service_uid=os.geteuid(),
    ).read(plan.artifact_bundle_sha256)
    if (
        publication.candidate_sha != plan.candidate_sha
        or publication.candidate_tree != plan.candidate_tree
        or publication.mutation_epoch != plan.mutation_epoch
        or publication.descriptor_path != plan.artifact_descriptor_path
        or publication.rendered_manifest_path != plan.rendered_manifest_path
        or publication.image_artifact_sha256 != plan.image_artifact_sha256
        or publication.manifest_artifact_sha256 != plan.manifest_artifact_sha256
        or publication.rendered_manifest_sha256 != plan.rendered_manifest_sha256
        or publication.migration_plan_sha256 != plan.migration_plan_sha256
        or publication.migration_target_revision != plan.migration_target_revision
        or publication.browser_report_schema_sha256 != plan.browser_report_schema_sha256
    ):
        raise ValueError("rehearsal helper artifact publication drifted")


def _record(
    *,
    check_id: str,
    plan: RehearsalPlan,
    passed: bool,
    details: Mapping[str, str],
    blockers: Mapping[str, str],
    cleanup_verified: bool = False,
) -> dict[str, object]:
    return {
        "blockers": dict(blockers),
        "check_id": check_id,
        "cleanup_verified": cleanup_verified,
        "details": dict(details),
        "passed": passed,
        "plan_digest": plan.plan_digest,
        "schema_version": 1,
    }


def _execute(check_id: str, plan: RehearsalPlan) -> RehearsalStepOutcome:
    return IsolatedRehearsalExecutor().execute(check_id, plan)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loom-staging-rollout-rehearsal")
    subparsers = parser.add_subparsers(dest="command", required=True)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--check-id", choices=REHEARSAL_CHECK_IDS, required=True)
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--plan-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        plan = _load_plan(args.plan, args.plan_sha256)
        outcome = _execute(args.check_id, plan)
        record = _record(
            check_id=args.check_id,
            plan=plan,
            passed=outcome.passed,
            details=outcome.details,
            blockers=outcome.blockers,
            cleanup_verified=outcome.cleanup_verified,
        )
    except (OSError, ValueError):
        return 2
    sys.stdout.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    return 0 if record["passed"] is True else 1


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())


__all__ = ["main"]
