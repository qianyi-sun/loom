"""Fail-closed persistence for one separately authorized Stage 1 live smoke."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import Field, field_validator, model_validator
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Artifact,
    ArtifactUploadSession,
    PipelineBudgetLedger,
    PipelineInputImport,
    PipelineLivePreviewFrame,
    PipelineLivePreviewGeneration,
    PipelineRun,
    PipelineRunGpuBackendSelection,
    PipelineScopedPolicyActivation,
    PipelineStage1SmokeAuthorization,
    PipelineStage1SmokeEvent,
    SlurmWorkerJob,
    Worker,
)
from loom.pipeline.gpu_backend import PipelineRunGpuBackendSelectionV1
from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom.pipeline.spec import Digest, PipelineModel, RunGraphSpecV1, reject_secret_literals
from loom.pipeline.stage1_smoke import (
    Stage1SmokeAuthorizationV1,
    Stage1SmokeCandidateV1,
    Stage1SmokeCleanupV1,
    Stage1SmokePreflightV1,
    build_stage1_smoke_graph,
    validate_stage1_smoke_authorization,
)
from loom.system_identities import PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID

OFFICIAL_SUBMISSION_KIND = "behavior_stage1_smoke_v1"
_SIGNATURE_RE = re.compile(r"[0-9a-f]{128}")
_KEY_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}")


class Stage1SmokeServiceError(RuntimeError):
    def __init__(self, status_code: int, reason_code: str) -> None:
        super().__init__(reason_code)
        self.status_code = status_code
        self.reason_code = reason_code


class Stage1SmokeEvidenceV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-smoke-evidence.v1"]
    authorization_id: UUID
    candidate_sha256: Digest
    pipeline_run_id: UUID
    result_kind: Literal["success", "terminal"]
    evidence: dict[str, Any] = Field(min_length=1, max_length=64)
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("evidence timestamp must include a timezone")
        return value

    @model_validator(mode="after")
    def no_secret_literals(self) -> Stage1SmokeEvidenceV1:
        reject_secret_literals(self)
        return self


class Stage1SmokeSignatureVerifier:
    """Verify operator documents while never holding signing authority."""

    def __init__(self, *, keys: Mapping[str, bytes], max_age_seconds: int = 300) -> None:
        if (
            not keys
            or max_age_seconds <= 0
            or any(
                _KEY_ID_RE.fullmatch(key_id) is None or not isinstance(key, bytes) or len(key) != 32
                for key_id, key in keys.items()
            )
        ):
            raise ValueError("Stage 1 smoke verification keys are invalid")
        self._keys = MappingProxyType(
            {key_id: Ed25519PublicKey.from_public_bytes(key) for key_id, key in keys.items()}
        )
        self._max_age = timedelta(seconds=max_age_seconds)

    def verify(
        self,
        *,
        key_id: str,
        payload: bytes,
        signature: str,
        observed_at: datetime,
        now: datetime,
    ) -> str:
        if observed_at.tzinfo is None or now.tzinfo is None:
            raise ValueError("Stage 1 smoke verification timestamps must be aware")
        normalized_now = now.astimezone(UTC)
        observed = observed_at.astimezone(UTC)
        if observed < normalized_now - self._max_age or observed > normalized_now + timedelta(
            seconds=30
        ):
            raise ValueError("Stage 1 smoke signature freshness window expired")
        if _SIGNATURE_RE.fullmatch(signature) is None:
            raise ValueError("Stage 1 smoke signature is invalid")
        try:
            key = self._keys[key_id]
        except KeyError:
            raise ValueError("Stage 1 smoke signature key is unknown") from None
        raw_signature = bytes.fromhex(signature)
        try:
            key.verify(raw_signature, payload)
        except InvalidSignature:
            raise ValueError("Stage 1 smoke signature is invalid") from None
        return digest_bytes(raw_signature)


class Stage1SmokeCleanupAuthorityV1(Protocol):
    """Independent readback for residue not owned by Loom's relational DB."""

    async def verify_cleanup(
        self,
        *,
        authorization: PipelineStage1SmokeAuthorization,
        cleanup: Stage1SmokeCleanupV1,
    ) -> None: ...


class Stage1SmokeExecutionPreflightAuthorityV1(Protocol):
    """Independent readback required before the one live mutation."""

    async def verify_preflight(
        self,
        *,
        session: AsyncSession,
        candidate: Stage1SmokeCandidateV1,
        authorization: Stage1SmokeAuthorizationV1,
        preflight: Stage1SmokePreflightV1,
        graph: RunGraphSpecV1,
    ) -> None: ...


class Stage1SmokeEvidenceAuthorityV1(Protocol):
    """Independent terminal, Artifact, preview, viewer, and lineage readback."""

    async def verify_evidence(
        self,
        *,
        session: AsyncSession,
        authorization: PipelineStage1SmokeAuthorization,
        evidence: Stage1SmokeEvidenceV1,
    ) -> None: ...


def load_stage1_smoke_signature_verifier(
    key_file: Path, *, key_id: str, max_age_seconds: int
) -> Stage1SmokeSignatureVerifier:
    """Load one exact raw public key without following or racing links."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(key_file, flags)
    except OSError:
        raise RuntimeError("Stage 1 smoke public key must be an available regular file") from None
    try:
        before = os.fstat(descriptor)
        mode = stat.S_IMODE(before.st_mode)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or bool(mode & 0o133)
            or not mode & stat.S_IRUSR
            or before.st_size != 32
        ):
            raise RuntimeError("Stage 1 smoke public key must be an owner-held read-only file")
        key = os.read(descriptor, 33)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if len(key) != 32 or (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise RuntimeError("Stage 1 smoke public key changed while reading")
    return Stage1SmokeSignatureVerifier(keys={key_id: key}, max_age_seconds=max_age_seconds)


def execute_signature_payload(
    *,
    candidate: Stage1SmokeCandidateV1,
    authorization: Stage1SmokeAuthorizationV1,
    preflight: Stage1SmokePreflightV1,
    idempotency_key: str,
) -> bytes:
    return canonical_document(
        {
            "action": "execute",
            "authorization": authorization.model_dump(mode="json", exclude_none=False),
            "candidate": candidate.model_dump(mode="json", exclude_none=False),
            "idempotency_key": idempotency_key,
            "preflight": preflight.model_dump(mode="json", exclude_none=False),
            "schema_version": "loom.behavior-stage1-smoke-signed-request.v1",
        }
    )


def evidence_signature_payload(evidence: Stage1SmokeEvidenceV1) -> bytes:
    return canonical_document(
        {
            "action": "record_evidence",
            "evidence": evidence.model_dump(mode="json", exclude_none=False),
        }
    )


def cleanup_signature_payload(cleanup: Stage1SmokeCleanupV1) -> bytes:
    return canonical_document(
        {
            "action": "cleanup",
            "cleanup": cleanup.model_dump(mode="json", exclude_none=False),
        }
    )


def render_stage1_smoke_candidate(
    candidate: Stage1SmokeCandidateV1, *, repo_root: Path
) -> dict[str, Any]:
    """Resolve the candidate without database or external mutation."""

    graph = build_stage1_smoke_graph(candidate, repo_root=repo_root)
    return {
        "candidate_sha256": candidate.candidate_sha256,
        "candidate_bytes": candidate.canonical_bytes,
        "graph": graph,
        "graph_sha256": canonical_digest(graph),
    }


def stage1_smoke_request_digest(
    *,
    candidate: Stage1SmokeCandidateV1,
    authorization: Stage1SmokeAuthorizationV1,
    preflight: Stage1SmokePreflightV1,
    signature_key_id: str,
) -> Digest:
    return canonical_digest(
        {
            "authorization": authorization.model_dump(mode="json", exclude_none=False),
            "candidate": candidate.model_dump(mode="json", exclude_none=False),
            "preflight": preflight.model_dump(mode="json", exclude_none=False),
            "signature_key_id": signature_key_id,
        }
    )


def _validate_bindings(
    candidate: Stage1SmokeCandidateV1,
    authorization: Stage1SmokeAuthorizationV1,
    preflight: Stage1SmokePreflightV1,
) -> None:
    input_descriptor_set_sha256 = canonical_digest(candidate.inputs)
    try:
        validate_stage1_smoke_authorization(candidate, authorization)
    except ValueError as exc:
        raise Stage1SmokeServiceError(409, "stage1_smoke_authority_drift") from exc
    expected_gpu_contract = {
        "oldlab-rtx5080-2gpu": [
            (0, "NVIDIA GeForce RTX 5080", "sim"),
            (1, "NVIDIA GeForce RTX 5080", "vla"),
        ],
        "gb10-shared-1gpu": [(0, "NVIDIA GB10", "sim_and_vla")],
    }[candidate.backend_variant_id]
    if (
        preflight.candidate_sha256 != candidate.candidate_sha256
        or preflight.authorization_id != authorization.authorization_id
        or preflight.authorization_sha256 != authorization.authorization_sha256
        or preflight.policy_activation_epoch != candidate.policy_activation_epoch
        or preflight.platform_child_digest != candidate.platform_child_digest
        or preflight.image_runtime_contract_sha256 != candidate.image_runtime_contract_sha256
        or preflight.input_descriptor_set_sha256 != input_descriptor_set_sha256
        or [(item.logical_index, item.model, item.role) for item in preflight.gpu_devices]
        != expected_gpu_contract
    ):
        raise Stage1SmokeServiceError(409, "stage1_smoke_authority_drift")


async def _validate_inputs(
    session: AsyncSession, candidate: Stage1SmokeCandidateV1
) -> dict[UUID, Artifact]:
    rows = list(
        (
            await session.execute(
                select(Artifact).where(
                    Artifact.id.in_([item.artifact_id for item in candidate.inputs]),
                    Artifact.team_id == candidate.team_id,
                )
            )
        ).scalars()
    )
    by_id = {row.id: row for row in rows}
    for item in candidate.inputs:
        artifact = by_id.get(item.artifact_id)
        if (
            artifact is None
            or artifact.artifact_type != item.artifact_type
            or artifact.manifest_sha256 != item.manifest_sha256
            or artifact.content_hash != item.content_sha256
            or artifact.stored_size_bytes is None
            or artifact.unpacked_size_bytes is None
            or artifact.file_count is None
            or artifact.stored_size_bytes != item.stored_size_bytes
            or artifact.unpacked_size_bytes != item.unpacked_size_bytes
            or artifact.file_count != item.file_count
            or (
                artifact.safety_state not in {"verified_internal", "verified"}
                and not await _trusted_unknown_input(session, artifact, item.name)
            )
        ):
            raise Stage1SmokeServiceError(409, "stage1_smoke_input_drift")
    return by_id


async def _trusted_unknown_input(
    session: AsyncSession, artifact: Artifact, input_name: str
) -> bool:
    if (
        input_name not in {"dataset", "policy"}
        or artifact.safety_state != "unknown"
        or artifact.producer_kind != "input_import"
        or artifact.pipeline_input_import_id is None
        or artifact.artifact_upload_session_id is None
    ):
        return False
    imported = await session.get(PipelineInputImport, artifact.pipeline_input_import_id)
    upload = await session.get(ArtifactUploadSession, artifact.artifact_upload_session_id)
    return bool(
        imported is not None
        and upload is not None
        and imported.team_id == artifact.team_id
        and imported.kind == input_name
        and imported.target_artifact_type == artifact.artifact_type
        and imported.trust_class == "internal_trusted"
        and imported.state == "committed"
        and imported.committed_artifact_id == artifact.id
        and upload.state == "committed"
        and upload.manifest_sha256 == artifact.provenance.get("root_manifest_sha256")
        and upload.committed_marker_sha256 == artifact.provenance.get("marker_sha256")
    )


async def _validate_worker(
    session: AsyncSession,
    *,
    candidate: Stage1SmokeCandidateV1,
    preflight: Stage1SmokePreflightV1,
) -> None:
    worker = await session.get(Worker, preflight.worker_id, with_for_update=True)
    if worker is None:
        raise Stage1SmokeServiceError(409, "stage1_smoke_worker_missing")
    evidence = worker.slurm_gpu_allocation_evidence_json or {}
    capability = worker.capability_snapshot_json or {}
    expected_arch = {"linux/amd64": "x86_64", "linux/arm64": "arm64"}[candidate.platform]
    expected_device_uuids = [item.device_uuid for item in preflight.gpu_devices]
    capabilities_by_uuid = {
        item.get("device_uuid"): item
        for item in capability.get("gpu_devices", [])
        if isinstance(item, dict)
    }
    if (
        worker.lease_epoch != preflight.worker_lease_epoch
        or worker.capability_snapshot_digest != preflight.worker_capability_snapshot_sha256
        or worker.pool_name != candidate.policy_id
        or capability.get("cpu_arch") != expected_arch
        or evidence.get("slurm_cluster_id") != candidate.slurm_cluster_id
        or evidence.get("allocation_id") != preflight.slurm_allocation_id
        or evidence.get("variant_id") != candidate.backend_variant_id
        # Allocation and capability snapshots canonicalize UUIDs bytewise.  They
        # prove exact membership, while preflight preserves CUDA logical order.
        or evidence.get("device_uuids") != sorted(expected_device_uuids, key=str.encode)
        or any(
            capabilities_by_uuid.get(item.device_uuid, {}).get("model") != item.model
            for item in preflight.gpu_devices
        )
    ):
        raise Stage1SmokeServiceError(409, "stage1_smoke_worker_drift")


def _row_projection(row: PipelineStage1SmokeAuthorization) -> dict[str, Any]:
    try:
        candidate = Stage1SmokeCandidateV1.model_validate_json(row.candidate_bytes)
        authorization = Stage1SmokeAuthorizationV1.model_validate_json(row.authorization_bytes)
        preflight = Stage1SmokePreflightV1.model_validate_json(row.preflight_bytes)
    except ValueError as exc:
        raise Stage1SmokeServiceError(500, "stage1_smoke_persisted_document_invalid") from exc
    if (
        canonical_document(row.candidate_json) != row.candidate_bytes
        or canonical_document(row.authorization_json) != row.authorization_bytes
        or canonical_document(row.preflight_json) != row.preflight_bytes
        or candidate.canonical_bytes != row.candidate_bytes
        or candidate.candidate_sha256 != row.candidate_sha256
        or canonical_document(authorization.model_dump(mode="json", exclude_none=False))
        != row.authorization_bytes
        or authorization.authorization_sha256 != row.authorization_sha256
        or canonical_document(preflight.model_dump(mode="json", exclude_none=False))
        != row.preflight_bytes
        or canonical_digest(preflight.model_dump(mode="json", exclude_none=False))
        != row.preflight_sha256
        or authorization.authorization_id != row.authorization_id
        or candidate.team_id != row.team_id
    ):
        raise Stage1SmokeServiceError(500, "stage1_smoke_persisted_document_drift")
    return {
        "authorization_id": str(row.authorization_id),
        "candidate_sha256": row.candidate_sha256,
        "pipeline_run_id": str(row.pipeline_run_id),
        "policy_activation_id": str(row.policy_activation_id),
        "state": row.state,
        "evidence_sha256": row.evidence_sha256,
        "cleanup_sha256": row.cleanup_sha256,
    }


async def get_stage1_smoke_replay(
    session: AsyncSession,
    *,
    team_id: UUID,
    idempotency_key: str,
    request_digest: Digest,
) -> dict[str, Any] | None:
    row = (
        await session.execute(
            select(PipelineStage1SmokeAuthorization).where(
                PipelineStage1SmokeAuthorization.team_id == team_id,
                PipelineStage1SmokeAuthorization.idempotency_key == idempotency_key,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    if row.request_digest != request_digest:
        raise Stage1SmokeServiceError(409, "stage1_smoke_idempotency_conflict")
    return _row_projection(row)


async def execute_stage1_smoke(
    session: AsyncSession,
    *,
    candidate: Stage1SmokeCandidateV1,
    authorization: Stage1SmokeAuthorizationV1,
    preflight: Stage1SmokePreflightV1,
    idempotency_key: str,
    signature_key_id: str,
    signature_sha256: str,
    preflight_authority: Stage1SmokeExecutionPreflightAuthorityV1,
    repo_root: Path,
    now: datetime,
) -> tuple[dict[str, Any], bool]:
    _validate_bindings(candidate, authorization, preflight)
    if now > authorization.expires_at or now > candidate.start_by:
        raise Stage1SmokeServiceError(409, "stage1_smoke_authorization_expired")
    graph = build_stage1_smoke_graph(candidate, repo_root=repo_root)
    request_digest = stage1_smoke_request_digest(
        candidate=candidate,
        authorization=authorization,
        preflight=preflight,
        signature_key_id=signature_key_id,
    )
    existing = (
        await session.execute(
            select(PipelineStage1SmokeAuthorization)
            .where(
                PipelineStage1SmokeAuthorization.team_id == candidate.team_id,
                PipelineStage1SmokeAuthorization.idempotency_key == idempotency_key,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.request_digest != request_digest:
            raise Stage1SmokeServiceError(409, "stage1_smoke_idempotency_conflict")
        return _row_projection(existing), True
    artifacts = await _validate_inputs(session, candidate)
    await _validate_worker(session, candidate=candidate, preflight=preflight)
    try:
        await preflight_authority.verify_preflight(
            session=session,
            candidate=candidate,
            authorization=authorization,
            preflight=preflight,
            graph=graph,
        )
    except ValueError as exc:
        raise Stage1SmokeServiceError(409, "stage1_smoke_preflight_unverified") from exc
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1362))"),
        {
            "scope": canonical_digest(
                {"environment": candidate.environment, "policy_id": candidate.policy_id}
            )
        },
    )
    latest_epoch = (
        await session.execute(
            select(
                func.coalesce(func.max(PipelineScopedPolicyActivation.activation_epoch), 0)
            ).where(
                PipelineScopedPolicyActivation.environment == candidate.environment,
                PipelineScopedPolicyActivation.policy_id == candidate.policy_id,
            )
        )
    ).scalar_one()
    if candidate.policy_activation_epoch <= latest_epoch:
        raise Stage1SmokeServiceError(409, "stage1_smoke_activation_epoch_stale")

    run_id = uuid4()
    activation_id = uuid4()
    resolved_inputs = []
    for item in candidate.inputs:
        artifact = artifacts[item.artifact_id]
        resolved_inputs.append(
            {
                "input_name": item.name,
                "artifact_id": str(item.artifact_id),
                "artifact_type": item.artifact_type,
                "content_sha256": item.content_sha256,
                "manifest_sha256": item.manifest_sha256,
                "stored_size_bytes": artifact.stored_size_bytes,
                "unpacked_size_bytes": artifact.unpacked_size_bytes,
                "file_count": artifact.file_count,
            }
        )
    run = PipelineRun(
        id=run_id,
        team_id=candidate.team_id,
        created_by_user_id=PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID,
        display_name="BEHAVIOR Stage 1 live smoke",
        submission_policy="ordinary",
        official_submission_kind=OFFICIAL_SUBMISSION_KIND,
        official_submission_authority_id=authorization.authorization_id,
        official_submission_authority_snapshot_digest=authorization.authorization_sha256,
        official_submission_identity_digest=request_digest,
        recipe_name=graph.recipe.name,
        recipe_version=graph.recipe.version,
        recipe_digest=graph.recipe.digest,
        graph_spec_json=graph.model_dump(mode="json", exclude_none=False),
        graph_spec_digest=canonical_digest(graph),
        parameters_json=candidate.parameters,
        parameters_digest=canonical_digest(candidate.parameters),
        resolved_inputs_json=resolved_inputs,
        control_binding_snapshots_json=[],
        control_binding_snapshots_digest=canonical_digest([]),
        budget_json=candidate.run_budget.model_dump(mode="json"),
        request_digest=request_digest,
        idempotency_key=f"stage1_smoke:{idempotency_key}",
        state="submitted",
        created_at=now,
    )
    activation = PipelineScopedPolicyActivation(
        id=activation_id,
        environment=candidate.environment,
        policy_id=candidate.policy_id,
        policy_config_sha256=candidate.policy_config_sha256,
        authority_kind="acceptance",
        authority_id=authorization.authorization_id,
        activation_epoch=candidate.policy_activation_epoch,
        state="active",
        desired_slots=1,
        activated_at=now,
        updated_at=now,
    )
    selection = PipelineRunGpuBackendSelectionV1(
        pipeline_run_id=run_id,
        scope="all_gpu_nodes",
        variant_id=candidate.backend_variant_id,
        policy_id=candidate.policy_id,
        selection_source="acceptance_authority",
        selected_at=now,
    )
    selection_json = selection.model_dump(mode="json")
    ledger = PipelineBudgetLedger(
        pipeline_run_id=run_id,
        provider_limit_microusd=int(
            Decimal(candidate.run_budget.max_provider_cost_usd) * 1_000_000
        ),
        gpu_limit_seconds=candidate.run_budget.max_gpu_seconds,
        artifact_limit_bytes=candidate.run_budget.max_artifact_bytes,
        stage_run_limit=candidate.run_budget.max_stage_runs,
        attempt_limit=candidate.run_budget.max_attempts_total,
        wall_deadline_at=now + timedelta(seconds=candidate.run_budget.max_wall_seconds),
    )
    row = PipelineStage1SmokeAuthorization(
        authorization_id=authorization.authorization_id,
        team_id=candidate.team_id,
        operator_user_id=candidate.operator_user_id,
        environment=candidate.environment,
        candidate_json=candidate.model_dump(mode="json"),
        candidate_bytes=candidate.canonical_bytes,
        candidate_sha256=candidate.candidate_sha256,
        authorization_json=authorization.model_dump(mode="json"),
        authorization_bytes=canonical_document(
            authorization.model_dump(mode="json", exclude_none=False)
        ),
        authorization_sha256=authorization.authorization_sha256,
        preflight_json=preflight.model_dump(mode="json"),
        preflight_bytes=canonical_document(preflight.model_dump(mode="json", exclude_none=False)),
        preflight_sha256=canonical_digest(preflight.model_dump(mode="json", exclude_none=False)),
        nonce_sha256=authorization.nonce_sha256,
        idempotency_key=idempotency_key,
        request_digest=request_digest,
        signature_key_id=signature_key_id,
        signature_sha256=signature_sha256,
        policy_activation_id=activation_id,
        pipeline_run_id=run_id,
        state="submitted",
        authorized_at=authorization.authorized_at,
        expires_at=authorization.expires_at,
        start_by=candidate.start_by,
        cleanup_deadline=candidate.cleanup_deadline,
        consumed_at=now,
        created_at=now,
        updated_at=now,
    )
    event_payload = {
        "authorization_sha256": authorization.authorization_sha256,
        "candidate_sha256": candidate.candidate_sha256,
        "pipeline_run_id": str(run_id),
        "policy_activation_id": str(activation_id),
        "preflight_sha256": canonical_digest(preflight.model_dump(mode="json", exclude_none=False)),
        "request_digest": request_digest,
    }
    # Establish the parent row before its independently mapped FK children.
    # Both flushes remain inside the caller-owned transaction, so activation,
    # budget authority, GPU selection, and Stage 1 consumption are still atomic.
    session.add(run)
    await session.flush()
    session.add_all(
        [
            ledger,
            activation,
            PipelineRunGpuBackendSelection(
                id=uuid4(),
                pipeline_run_id=run_id,
                scope=selection.scope,
                variant_id=selection.variant_id,
                policy_id=selection.policy_id,
                selection_source=selection.selection_source,
                selected_at=selection.selected_at,
                selection_json=selection_json,
                selection_bytes=canonical_document(selection_json),
                gpu_backend_selection_sha256=selection.gpu_backend_selection_sha256,
            ),
            row,
            PipelineStage1SmokeEvent(
                authorization_id=authorization.authorization_id,
                seq=1,
                event_kind="live_action_consumed",
                payload_json=event_payload,
                payload_bytes=canonical_document(event_payload),
                payload_sha256=canonical_digest(event_payload),
                observed_at=now,
            ),
        ]
    )
    await session.flush()
    return _row_projection(row), False


async def record_stage1_smoke_evidence(
    session: AsyncSession,
    *,
    evidence: Stage1SmokeEvidenceV1,
    authority: Stage1SmokeEvidenceAuthorityV1,
    now: datetime,
) -> tuple[dict[str, Any], bool]:
    row = await session.get(
        PipelineStage1SmokeAuthorization, evidence.authorization_id, with_for_update=True
    )
    if row is None:
        raise Stage1SmokeServiceError(404, "stage1_smoke_not_found")
    if (
        row.candidate_sha256 != evidence.candidate_sha256
        or row.pipeline_run_id != evidence.pipeline_run_id
    ):
        raise Stage1SmokeServiceError(409, "stage1_smoke_evidence_drift")
    digest = canonical_digest(evidence.model_dump(mode="json", exclude_none=False))
    if row.evidence_sha256 is not None:
        if row.evidence_sha256 != digest:
            raise Stage1SmokeServiceError(409, "stage1_smoke_evidence_conflict")
        return _row_projection(row), True
    try:
        await authority.verify_evidence(
            session=session,
            authorization=row,
            evidence=evidence,
        )
    except ValueError as exc:
        raise Stage1SmokeServiceError(409, "stage1_smoke_evidence_unverified") from exc
    payload = evidence.model_dump(mode="json")
    row.evidence_sha256 = digest
    row.state = "cleanup_required"
    row.updated_at = now
    row.version += 1
    session.add(
        PipelineStage1SmokeEvent(
            authorization_id=row.authorization_id,
            seq=2,
            event_kind="evidence_recorded",
            payload_json=payload,
            payload_bytes=canonical_document(payload),
            payload_sha256=digest,
            observed_at=evidence.observed_at,
        )
    )
    await session.flush()
    return _row_projection(row), False


async def cleanup_stage1_smoke(
    session: AsyncSession,
    *,
    cleanup: Stage1SmokeCleanupV1,
    authority: Stage1SmokeCleanupAuthorityV1,
    now: datetime,
) -> tuple[dict[str, Any], bool]:
    row = (
        await session.execute(
            select(PipelineStage1SmokeAuthorization)
            .where(PipelineStage1SmokeAuthorization.pipeline_run_id == cleanup.pipeline_run_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if row is None:
        raise Stage1SmokeServiceError(404, "stage1_smoke_not_found")
    if row.candidate_sha256 != cleanup.candidate_sha256:
        raise Stage1SmokeServiceError(409, "stage1_smoke_cleanup_drift")
    digest = canonical_digest(cleanup.model_dump(mode="json", exclude_none=False))
    if row.cleanup_sha256 is not None:
        if row.cleanup_sha256 != digest:
            raise Stage1SmokeServiceError(409, "stage1_smoke_cleanup_conflict")
        return _row_projection(row), True
    if row.evidence_sha256 is None:
        raise Stage1SmokeServiceError(409, "stage1_smoke_evidence_missing")
    run = await session.get(PipelineRun, row.pipeline_run_id, with_for_update=True)
    activation = await session.get(
        PipelineScopedPolicyActivation, row.policy_activation_id, with_for_update=True
    )
    if (
        run is None
        or activation is None
        or run.state != "finished"
        or activation.state != "active"
        or activation.desired_slots != 1
    ):
        raise Stage1SmokeServiceError(409, "stage1_smoke_not_terminal")
    preview_count = (
        await session.execute(
            select(func.count())
            .select_from(PipelineLivePreviewGeneration)
            .where(
                PipelineLivePreviewGeneration.pipeline_run_id == row.pipeline_run_id,
                PipelineLivePreviewGeneration.purged_at.is_(None),
            )
        )
    ).scalar_one()
    frame_count = (
        await session.execute(
            select(func.count())
            .select_from(PipelineLivePreviewFrame)
            .join(
                PipelineLivePreviewGeneration,
                PipelineLivePreviewGeneration.execution_attempt_id
                == PipelineLivePreviewFrame.execution_attempt_id,
            )
            .where(PipelineLivePreviewGeneration.pipeline_run_id == row.pipeline_run_id)
        )
    ).scalar_one()
    upload_count = (
        await session.execute(
            select(func.count())
            .select_from(ArtifactUploadSession)
            .where(
                ArtifactUploadSession.pipeline_run_id == row.pipeline_run_id,
                ArtifactUploadSession.state.not_in(["committed", "aborted"]),
            )
        )
    ).scalar_one()
    slurm_count = (
        await session.execute(
            select(func.count())
            .select_from(SlurmWorkerJob)
            .where(
                SlurmWorkerJob.environment == row.environment,
                SlurmWorkerJob.pool_name == activation.policy_id,
                SlurmWorkerJob.state.in_(["pending", "running"]),
            )
        )
    ).scalar_one()
    other_active_policy_slots = (
        await session.execute(
            select(func.coalesce(func.sum(PipelineScopedPolicyActivation.desired_slots), 0)).where(
                PipelineScopedPolicyActivation.environment == row.environment,
                PipelineScopedPolicyActivation.policy_id == activation.policy_id,
                PipelineScopedPolicyActivation.state == "active",
                PipelineScopedPolicyActivation.id != activation.id,
            )
        )
    ).scalar_one()
    if preview_count or frame_count or upload_count or slurm_count or other_active_policy_slots:
        raise Stage1SmokeServiceError(409, "stage1_smoke_cleanup_incomplete")
    try:
        await authority.verify_cleanup(authorization=row, cleanup=cleanup)
    except ValueError as exc:
        raise Stage1SmokeServiceError(409, "stage1_smoke_cleanup_unverified") from exc
    activation.state = "disabled"
    activation.desired_slots = 0
    activation.updated_at = now
    payload = cleanup.model_dump(mode="json")
    row.cleanup_sha256 = digest
    accepted = run.result == "succeeded"
    row.state = "accepted" if accepted else "rejected"
    row.finished_at = now
    row.updated_at = now
    row.version += 1
    session.add_all(
        [
            PipelineStage1SmokeEvent(
                authorization_id=row.authorization_id,
                seq=3,
                event_kind="cleanup_complete",
                payload_json=payload,
                payload_bytes=canonical_document(payload),
                payload_sha256=digest,
                observed_at=cleanup.cleaned_at,
            ),
            PipelineStage1SmokeEvent(
                authorization_id=row.authorization_id,
                seq=4,
                event_kind="accepted" if accepted else "rejected",
                payload_json={"pipeline_result": run.result},
                payload_bytes=canonical_document({"pipeline_result": run.result}),
                payload_sha256=canonical_digest({"pipeline_result": run.result}),
                observed_at=now,
            ),
        ]
    )
    await session.flush()
    return _row_projection(row), False


__all__ = [
    "OFFICIAL_SUBMISSION_KIND",
    "Stage1SmokeCleanupAuthorityV1",
    "Stage1SmokeEvidenceAuthorityV1",
    "Stage1SmokeEvidenceV1",
    "Stage1SmokeExecutionPreflightAuthorityV1",
    "Stage1SmokeServiceError",
    "Stage1SmokeSignatureVerifier",
    "cleanup_signature_payload",
    "cleanup_stage1_smoke",
    "evidence_signature_payload",
    "execute_signature_payload",
    "execute_stage1_smoke",
    "get_stage1_smoke_replay",
    "load_stage1_smoke_signature_verifier",
    "record_stage1_smoke_evidence",
    "render_stage1_smoke_candidate",
    "stage1_smoke_request_digest",
]
