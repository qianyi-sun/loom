"""Fail-closed installed entrypoint for attested protected final actions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from loom_capacity_executor.runtime import ActivationRuntimeDocumentV2
from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    PROTECTED_MUTATION_CHECK_IDS,
    FinalGateResult,
)
from loom_cli.rollout.operator.backup import VerifiedBackup
from loom_cli.rollout.operator.checkpoint_lease import inspect_critical_checkpoint
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.model import DriverEnvelope
from loom_cli.rollout.operator.protected_native_delivery_material import NativeDeliveryMaterial
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.preflight_contract import CheckOperation

_STATE_ROOT = Path("/var/lib/loom-staging-rollout")
_MAX_PLAN_BYTES = 2 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FinalGateExecute = Callable[[str, CheckOperation, FinalGatePlan], FinalGateResult]


def _strict_json_object(payload: bytes) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key")
            value[key] = item
        return value

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("final gate helper plan JSON is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("final gate helper plan JSON is invalid")
    return value


def _load_plan(path: Path, expected_digest: str, *, verify_checkpoint: bool = True) -> FinalGatePlan:
    if _SHA256_RE.fullmatch(expected_digest) is None:
        raise ValueError("final gate helper plan digest is invalid")
    trusted = read_trusted_file(
        path,
        service_uid=os.geteuid(),
        private=True,
        max_bytes=_MAX_PLAN_BYTES,
        require_nonempty=True,
    )
    plan = FinalGatePlan.from_dict(_strict_json_object(trusted.payload.rstrip(b"\n")))
    expected_path = (
        _STATE_ROOT
        / "requests"
        / plan.request_id
        / "attempts"
        / str(plan.attempt_number)
        / "final-gate-plan.json"
    )
    if path != expected_path or plan.plan_digest != expected_digest:
        raise ValueError("final gate helper plan path or identity drifted")
    _verify_artifacts(plan)
    if verify_checkpoint:
        _verify_checkpoint(plan)
    _verify_driver_envelope(plan)
    return plan


def _verify_driver_envelope(plan: FinalGatePlan) -> None:
    path = (
        _STATE_ROOT
        / "requests"
        / plan.request_id
        / "attempts"
        / str(plan.attempt_number)
        / "envelope.json"
    )
    trusted = read_trusted_file(
        path,
        service_uid=os.geteuid(),
        private=True,
        max_bytes=_MAX_PLAN_BYTES,
        require_nonempty=True,
    )
    envelope = DriverEnvelope.from_dict(_strict_json_object(trusted.payload.rstrip(b"\n")))
    if (
        hashlib.sha256(trusted.payload).hexdigest() != plan.request_envelope_sha256
        or envelope.request_id != plan.request_id
        or envelope.rollout_id != plan.rollout_id
        or envelope.attempt_number != plan.attempt_number
        or envelope.resolved_sha != plan.candidate_sha
        # A resolved tree, when present, must pin the exact candidate tree. A
        # merged-dev candidate now carries that derivable tree (see the
        # DriverEnvelope/CandidateBinding model), so it must NOT be rejected for
        # being populated; only sealed-cumulative additionally *requires* it.
        or (envelope.resolved_tree is not None and envelope.resolved_tree != plan.candidate_tree)
        or (plan.source_mode == "sealed-cumulative" and envelope.resolved_tree is None)
        or envelope.runner_config_sha256 != plan.runner_config_hash
        or envelope.preflight_attestation_sha256 != plan.attestation_digest
        or envelope.service_token_source != plan.service_token_source
    ):
        raise ValueError("final gate helper driver envelope drifted")


def _verify_artifacts(plan: FinalGatePlan) -> None:
    descriptor = Path(plan.artifact_descriptor_path)
    state_root = descriptor.parents[2]
    if state_root != _STATE_ROOT:
        raise ValueError("final gate helper artifact store escaped authority")
    publication = PreflightArtifactStore(state_root, service_uid=os.geteuid()).read(
        plan.artifact_bundle_digest
    )
    if (
        publication.candidate_sha != plan.candidate_sha
        or publication.candidate_tree != plan.candidate_tree
        or publication.mutation_epoch != plan.starting_mutation_epoch
        or str(publication.descriptor_path) != plan.artifact_descriptor_path
        or str(publication.rendered_manifest_path) != plan.rendered_manifest_path
        or publication.rendered_manifest_sha256 != plan.rendered_manifest_sha256
        or str(publication.migration_manifest_path) != plan.migration_manifest_path
        or publication.migration_manifest_sha256 != plan.migration_manifest_sha256
        or publication.migration_manifest_artifact_sha256 != plan.migration_manifest_artifact_sha256
        or str(publication.production_defaults_path) != plan.production_defaults_path
        or publication.production_defaults_sha256 != plan.production_defaults_sha256
        or publication.migration_job_name != plan.migration_job_name
        or publication.migration_image_id != plan.migration_image_id
        or publication.migration_plan_sha256 != plan.migration_plan_digest
        or publication.migration_target_revision != plan.migration_target_revision
        or publication.browser_report_schema_sha256 != plan.browser_report_schema
    ):
        raise ValueError("final gate helper artifact publication drifted")


def _verify_checkpoint(plan: FinalGatePlan) -> None:
    checkpoint = inspect_critical_checkpoint(
        VerifiedBackup(
            manifest_path=Path(plan.backup_manifest_path),
            manifest_sha256=plan.backup_manifest_sha256,
        ),
        request_id=plan.backup_source_request_id,
        environment=plan.environment,
        namespace=plan.namespace,
        expected_owner_uid=os.geteuid(),
        now=datetime.now(UTC),
    )
    if (
        checkpoint.manifest_sha256 != plan.backup_manifest_sha256
        or checkpoint.mutation_epoch != plan.starting_mutation_epoch
        or checkpoint.db_snapshot_identity != plan.db_snapshot_identity
        or checkpoint.object_inventory_root != plan.object_inventory_root
        or checkpoint.schema_revision != plan.schema_revision
    ):
        raise ValueError("final gate helper checkpoint identity drifted")


def _record(result: FinalGateResult) -> Mapping[str, object]:
    return {
        "attestation_digest": result.attestation_digest,
        "blockers": dict(result.blockers),
        "candidate_sha": result.candidate_sha,
        "check_id": result.check_id,
        "evidence_digest": result.evidence_digest,
        "observed_epoch": result.observed_epoch,
        "operation": result.operation.value,
        "protected_mutation": result.protected_mutation,
        "schema_version": 1,
    }


class ActivationExecute(Protocol):
    def __call__(self, plan: FinalGatePlan, *,
                 documents: Mapping[str, ActivationRuntimeDocumentV2] | None = None,
                 native_material: Mapping[str, NativeDeliveryMaterial] | None = None) -> ExecutionContextV2: ...


def _load_activation_documents(path: Path | None, expected_digest: str | None, *,
                               plan_path: Path) -> Mapping[str, ActivationRuntimeDocumentV2] | None:
    if path is None and expected_digest is None:
        return None
    if (path != plan_path.with_name("execution-activation-documents.json")
            or expected_digest is None or _SHA256_RE.fullmatch(expected_digest) is None):
        raise ValueError("activation document path or digest is invalid")
    assert path is not None
    payload = read_trusted_file(path, service_uid=os.geteuid(), private=True,
        max_bytes=256 * 1024, require_nonempty=True).payload
    if hashlib.sha256(payload).hexdigest() != expected_digest:
        raise ValueError("activation document digest changed")
    documents = _strict_json_object(payload)
    if set(documents) != {"gb10", "oldlab"}:
        raise ValueError("activation documents must cover both pools")
    return {pool: ActivationRuntimeDocumentV2.model_validate_json(json.dumps(value))
        for pool, value in documents.items()}


def _load_native_material(path: Path | None, expected_digest: str | None, *, plan_path: Path,
                          documents: Mapping[str, ActivationRuntimeDocumentV2] | None,
                          ) -> Mapping[str, NativeDeliveryMaterial] | None:
    if path is None and expected_digest is None:
        if documents is not None and any(document.native_delivery is not None for document in documents.values()):
            raise ValueError("native activation requires bound private material")
        return None
    if (documents is None or path != plan_path.with_name("execution-activation-native-material.json")
            or expected_digest is None or _SHA256_RE.fullmatch(expected_digest) is None):
        raise ValueError("native material path or digest is invalid")
    assert path is not None
    payload = read_trusted_file(path, service_uid=os.geteuid(), private=True,
        max_bytes=1024 * 1024, require_nonempty=True).payload
    if hashlib.sha256(payload).hexdigest() != expected_digest:
        raise ValueError("native material digest changed")
    values = _strict_json_object(payload)
    if not values or set(values) != {pool for pool, document in documents.items() if document.native_delivery is not None}:
        raise ValueError("native material must cover exactly the native pools")
    material = {pool: NativeDeliveryMaterial.from_dict(value) for pool, value in values.items()}
    for pool, entry in material.items():
        entry.files(documents[pool])
    return material


def _activate(args: argparse.Namespace, execute: ActivationExecute | None) -> int:
    # The immutable plan/envelope/artifacts remain mandatory. Fresh checkpoint
    # admission lives inside the installed forward guard, so its failure after
    # activation cannot prevent compensation using the retained exact authority.
    plan = _load_plan(args.plan, args.plan_sha256, verify_checkpoint=False)
    documents = _load_activation_documents(args.documents, args.documents_sha256, plan_path=args.plan)
    native_material = _load_native_material(args.native_material, args.native_material_sha256,
        plan_path=args.plan, documents=documents)
    if execute is None:
        from loom_cli.rollout.operator.installed_final_gate_executor import (
            build_installed_final_gate_executor,
        )

        execute = build_installed_final_gate_executor().activate_prepared_execution
    result = (execute(plan, documents=documents, native_material=native_material)
        if native_material is not None else execute(plan, documents=documents))
    result = ExecutionContextV2.model_validate_json(result.model_dump_json())
    if result.execution_state not in {"active", "drain-only"}:
        raise ValueError("activation result is not a completed transition")
    sys.stdout.write(json.dumps({"schema_version": 1, "plan_digest": plan.plan_digest,
        "execution": result.model_dump(mode="json")}, sort_keys=True, separators=(",", ":")) + "\n")
    return 0 if result.execution_state == "active" else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loom-staging-rollout-final-gate")
    commands = parser.add_subparsers(dest="command", required=True)
    execute = commands.add_parser("execute")
    execute.add_argument("--check-id", choices=FINAL_CHECK_IDS, required=True)
    execute.add_argument("--operation", choices=("apply", "verify"), required=True)
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--plan-sha256", required=True)
    activation = commands.add_parser("activate-prepared")
    activation.add_argument("--plan", type=Path, required=True)
    activation.add_argument("--plan-sha256", required=True)
    activation.add_argument("--documents", type=Path)
    activation.add_argument("--documents-sha256")
    activation.add_argument("--native-material", type=Path)
    activation.add_argument("--native-material-sha256")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    execute: FinalGateExecute | None = None,
    activate: ActivationExecute | None = None,
) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "activate-prepared":
            return _activate(args, activate)
        operation = CheckOperation(args.operation)
        expected = (
            CheckOperation.APPLY
            if args.check_id in PROTECTED_MUTATION_CHECK_IDS
            else CheckOperation.VERIFY
        )
        if operation is not expected:
            raise ValueError("final gate helper operation is invalid")
        plan = _load_plan(args.plan, args.plan_sha256)
        if execute is None:
            from loom_cli.rollout.operator.installed_final_gate_executor import (
                build_installed_final_gate_executor,
            )

            execute = build_installed_final_gate_executor()
        result = execute(args.check_id, operation, plan)
        successful_apply = bool(
            args.check_id in PROTECTED_MUTATION_CHECK_IDS and not result.blockers
        )
        if (
            result.check_id != args.check_id
            or result.operation is not operation
            or result.candidate_sha != plan.candidate_sha
            or result.attestation_digest != plan.attestation_digest
            or result.observed_epoch < plan.starting_mutation_epoch
            or (
                successful_apply
                and (
                    not result.protected_mutation
                    or result.observed_epoch != plan.starting_mutation_epoch + 1
                )
            )
            or (args.check_id not in PROTECTED_MUTATION_CHECK_IDS and result.protected_mutation)
        ):
            raise ValueError("final gate helper result drifted")
        record = _record(result)
    except (OSError, RuntimeError, ValueError):
        return 2
    sys.stdout.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    return 0 if not result.blockers else 1


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())


__all__ = ["FinalGateExecute", "main"]
