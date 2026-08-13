"""Read-only candidate composition and independent Stage 1 readback authorities.

The preparation surface accepts identities and bounded run inputs only.  Every
execution-bearing value is re-derived from the relational registry or the
checked-in immutable registries.  The live-observation boundary is injected;
``loom-service`` deliberately does not install it by default.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, NamedTuple, Protocol
from uuid import UUID

from pydantic import Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import (
    Artifact,
    ArtifactUploadSession,
    ExecutionAttempt,
    PipelineInputImport,
    PipelineLivePreviewFrame,
    PipelineLivePreviewGeneration,
    PipelineRun,
    PipelineScopedPolicyActivation,
    PipelineStage1SmokeAuthorization,
    PipelineStageRun,
    SlurmWorkerJob,
    Worker,
)
from loom.integrations.behavior.contracts import BehaviorRolloutParametersV1
from loom.pipeline.image_runtime import ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest, digest_bytes
from loom.pipeline.policy_config import PolicyConfigRegistry
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.spec import PipelineModel, RunBudgetV1, StageBudgetV1
from loom.pipeline.stage1_smoke import (
    STAGE1_SMOKE_RESOURCE_PROFILE,
    Stage1SmokeAuthorizationV1,
    Stage1SmokeCandidateV1,
    Stage1SmokeCleanupV1,
    Stage1SmokeEvidenceV1,
    Stage1SmokeInputV1,
    Stage1SmokeOutputV1,
    Stage1SmokePreflightV1,
    Stage1SmokePreviewPolicyV1,
    Stage1SmokeTerminalEvidenceV1,
    load_behavior_renderer_lock,
    stage1_smoke_recipe_digest,
)
from loom_service.pipeline_stage1_smoke_service import (
    OFFICIAL_SUBMISSION_KIND,
    Stage1SmokeCapacityPreflightAuthorityV1,
)

_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
_IMAGE_BUILD_SHA_PATH = Path("/opt/loom/build-sha")
_SCHEMA_PATH = Path("src/loom/pipeline/renderers/schemas/behavior.stage-request.v1.json")
_EXPECTED_INPUTS = (
    ("task_instance", "behavior_task_instance.v1"),
    ("dataset", "behavior_dataset_snapshot.v1"),
    ("policy", "behavior_policy_checkpoint.v1"),
)


class _BackendBinding(NamedTuple):
    platform: Literal["linux/amd64", "linux/arm64"]
    policy_id: Literal["behavior-gpu-oldlab", "behavior-gpu-gb10"]
    cluster_id: Literal["oldlab", "gb10"]


_BACKENDS: dict[Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"], _BackendBinding] = {
    "oldlab-rtx5080-2gpu": _BackendBinding("linux/amd64", "behavior-gpu-oldlab", "oldlab"),
    "gb10-shared-1gpu": _BackendBinding("linux/arm64", "behavior-gpu-gb10", "gb10"),
}


class Stage1CandidateAuthorityError(ValueError):
    def __init__(self, status_code: int, reason_code: str) -> None:
        super().__init__(reason_code)
        self.status_code = status_code
        self.reason_code = reason_code


class Stage1CandidateSelectionV1(PipelineModel):
    """Operator selections; no digest, image child, policy, or profile authority."""

    team_id: UUID
    backend_variant_id: Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"]
    image_index_digest: str = Field(
        pattern=(
            r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
            r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
        )
    )
    task_instance_artifact_id: UUID
    dataset_artifact_id: UUID
    policy_artifact_id: UUID
    parameters: BehaviorRolloutParametersV1
    run_budget: RunBudgetV1
    stage_budget: StageBudgetV1
    expected_domain_outcome: Literal["rollout_success", "rollout_failure"]
    start_by: datetime
    cleanup_deadline: datetime


class Stage1CandidateInventoryV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-smoke-inventory.v1"]
    loom_commit_sha: str
    environment: str
    team_id: UUID
    backend_variants: list[dict[str, Any]]
    images: list[dict[str, Any]]
    inputs: list[dict[str, Any]]


class RepositoryStage1CandidateAuthority:
    """Compose candidates from current DB rows and immutable repository files."""

    def __init__(self, *, repo_root: Path, loom_commit_sha: str, environment: str) -> None:
        if _COMMIT_RE.fullmatch(loom_commit_sha) is None or loom_commit_sha == "0" * 40:
            raise ValueError("Stage 1 candidate commit identity is invalid")
        if not environment or environment != environment.strip():
            raise ValueError("Stage 1 candidate environment is invalid")
        self._root = repo_root.resolve()
        self._commit = loom_commit_sha
        self._environment = environment

    def _registries(
        self,
    ) -> tuple[ResourceProfileRegistry, ImageRuntimeRegistry, PolicyConfigRegistry]:
        profiles = ResourceProfileRegistry.load(self._root / "config/resource-profiles.toml")
        images = ImageRuntimeRegistry.load(self._root / "config/image-runtime-contracts.toml")
        policies = PolicyConfigRegistry.load(
            resource_profiles=profiles,
            image_runtime_contracts=images,
            path=self._root / "config/pipeline-policy-config.toml",
            repository_root=self._root,
        )
        return profiles, images, policies

    async def inventory(
        self, session: AsyncSession, *, team_id: UUID
    ) -> Stage1CandidateInventoryV1:
        profiles, images, policies = self._registries()
        profile = profiles.get(STAGE1_SMOKE_RESOURCE_PROFILE)
        image_items: list[dict[str, Any]] = []
        for record in images.list():
            compatible = [
                variant
                for variant, (platform, policy_id, _cluster) in _BACKENDS.items()
                if platform == record.contract.platform
                and set(profile.profile.required_image_features)
                <= set(record.contract.application_features)
                and policies.get(policy_id).snapshot.policy_id == policy_id
            ]
            if compatible:
                image_items.append(
                    {
                        "image_index_digest": record.contract.image_index_digest,
                        "platform": record.contract.platform,
                        "platform_child_digest": record.contract.platform_manifest_digest,
                        "image_runtime_contract_sha256": record.snapshot_sha256,
                        "backend_variant_ids": compatible,
                    }
                )
        rows = list(
            (
                await session.execute(
                    select(Artifact)
                    .where(
                        Artifact.team_id == team_id,
                        Artifact.artifact_type.in_([item[1] for item in _EXPECTED_INPUTS]),
                        Artifact.manifest_sha256.is_not(None),
                    )
                    .order_by(Artifact.artifact_type, Artifact.created_at.desc(), Artifact.id)
                    .limit(300)
                )
            ).scalars()
        )
        return Stage1CandidateInventoryV1(
            schema_version="loom.behavior-stage1-smoke-inventory.v1",
            loom_commit_sha=self._commit,
            environment=self._environment,
            team_id=team_id,
            backend_variants=[
                {
                    "backend_variant_id": variant,
                    "platform": platform,
                    "policy_id": policy_id,
                    "slurm_cluster_id": cluster,
                }
                for variant, (platform, policy_id, cluster) in _BACKENDS.items()
            ],
            images=image_items,
            inputs=[_artifact_inventory_item(row) for row in rows],
        )

    async def prepare(
        self,
        session: AsyncSession,
        *,
        operator_user_id: UUID,
        selection: Stage1CandidateSelectionV1,
    ) -> Stage1SmokeCandidateV1:
        profiles, images, policies = self._registries()
        platform, policy_id, cluster_id = _BACKENDS[selection.backend_variant_id]
        try:
            image = images.get(selection.image_index_digest, platform)
        except ValueError as exc:
            raise Stage1CandidateAuthorityError(
                404, "stage1_candidate_selection_not_found"
            ) from exc
        profile = profiles.get(STAGE1_SMOKE_RESOURCE_PROFILE)
        policy = policies.get(policy_id)
        if set(profile.profile.required_image_features) > set(
            image.contract.application_features
        ) or selection.backend_variant_id not in {
            variant.variant_id for variant in profile.profile.execution_variants
        }:
            raise Stage1CandidateAuthorityError(404, "stage1_candidate_selection_not_found")

        artifact_ids = (
            selection.task_instance_artifact_id,
            selection.dataset_artifact_id,
            selection.policy_artifact_id,
        )
        rows = list(
            (
                await session.execute(
                    select(Artifact).where(
                        Artifact.id.in_(artifact_ids),
                        Artifact.team_id == selection.team_id,
                    )
                )
            ).scalars()
        )
        by_id = {row.id: row for row in rows}
        inputs: list[Stage1SmokeInputV1] = []
        for artifact_id, (name, artifact_type) in zip(artifact_ids, _EXPECTED_INPUTS, strict=True):
            row = by_id.get(artifact_id)
            if row is None or row.artifact_type != artifact_type:
                raise Stage1CandidateAuthorityError(404, "stage1_candidate_input_not_found")
            if not await _artifact_is_candidate_ready(session, row, name=name):
                raise Stage1CandidateAuthorityError(409, "stage1_candidate_input_not_ready")
            assert row.manifest_sha256 is not None
            assert row.stored_size_bytes is not None
            assert row.unpacked_size_bytes is not None
            assert row.file_count is not None
            inputs.append(
                Stage1SmokeInputV1(
                    name=name,
                    artifact_type=artifact_type,
                    required=True,
                    artifact_id=row.id,
                    manifest_sha256=row.manifest_sha256,
                    content_sha256=row.content_hash,
                    stored_size_bytes=row.stored_size_bytes,
                    unpacked_size_bytes=row.unpacked_size_bytes,
                    file_count=row.file_count,
                )
            )

        active = (
            await session.execute(
                select(func.count())
                .select_from(PipelineScopedPolicyActivation)
                .where(
                    PipelineScopedPolicyActivation.environment == self._environment,
                    PipelineScopedPolicyActivation.policy_id == policy_id,
                    PipelineScopedPolicyActivation.state.in_(["active", "draining"]),
                )
            )
        ).scalar_one()
        if active:
            raise Stage1CandidateAuthorityError(409, "stage1_candidate_policy_busy")
        latest_epoch = (
            await session.execute(
                select(
                    func.coalesce(func.max(PipelineScopedPolicyActivation.activation_epoch), 0)
                ).where(
                    PipelineScopedPolicyActivation.environment == self._environment,
                    PipelineScopedPolicyActivation.policy_id == policy_id,
                )
            )
        ).scalar_one()
        renderer_digest = canonical_digest(load_behavior_renderer_lock(self._root))
        schema_digest = digest_bytes((self._root / _SCHEMA_PATH).read_bytes())
        compatibility_digest = canonical_digest(
            {
                "image_runtime_contract_sha256": image.snapshot_sha256,
                "policy_config_sha256": policy.policy_config_sha256,
                "resource_profile_sha256": profile.snapshot_sha256,
                "slurm_cluster_config_sha256": policy.snapshot.slurm_cluster_config_sha256,
            }
        )
        recipe_digest = stage1_smoke_recipe_digest(
            renderer_lock_sha256=renderer_digest,
            stage_request_schema_sha256=schema_digest,
            resource_profile_sha256=profile.snapshot_sha256,
            image_index_digest=image.contract.image_index_digest,
            platform_child_digest=image.contract.platform_manifest_digest,
            compatibility_manifest_sha256=compatibility_digest,
        )
        return Stage1SmokeCandidateV1(
            schema_version="loom.behavior-stage1-smoke-candidate.v1",
            loom_commit_sha=self._commit,
            environment=self._environment,
            team_id=selection.team_id,
            operator_user_id=operator_user_id,
            backend_variant_id=selection.backend_variant_id,
            slurm_cluster_id=cluster_id,
            slurm_cluster_config_sha256=policy.snapshot.slurm_cluster_config_sha256,
            policy_id=policy_id,
            policy_config_sha256=policy.policy_config_sha256,
            policy_activation_epoch=latest_epoch + 1,
            image_index_digest=image.contract.image_index_digest,
            platform=platform,
            platform_child_digest=image.contract.platform_manifest_digest,
            image_runtime_contract_sha256=image.snapshot_sha256,
            resource_profile_sha256=profile.snapshot_sha256,
            renderer_lock_sha256=renderer_digest,
            stage_request_schema_sha256=schema_digest,
            compatibility_manifest_sha256=compatibility_digest,
            recipe_digest=recipe_digest,
            inputs=inputs,
            parameters=selection.parameters.model_dump(mode="json"),
            run_budget=selection.run_budget,
            stage_budget=selection.stage_budget,
            expected_outputs=[
                Stage1SmokeOutputV1(
                    name="rollout",
                    artifact_type="behavior_rollout_bundle.v1",
                    producer="container",
                    required=True,
                    max_bytes=selection.stage_budget.final_output_bytes_limit,
                )
            ],
            expected_domain_outcome=selection.expected_domain_outcome,
            preview_policy=Stage1SmokePreviewPolicyV1(
                schema_version="loom.behavior-stage1-preview-policy.v1",
                min_interval_ms=500,
                ttl_seconds=300,
                max_frame_bytes=524_288,
                max_frames_per_attempt=64,
                max_total_bytes_per_attempt=33_554_432,
                width=672,
                height=448,
                media_type="image/jpeg",
                label="LIVE / UNVERIFIED",
            ),
            start_by=selection.start_by,
            cleanup_deadline=selection.cleanup_deadline,
        )


async def _artifact_is_candidate_ready(
    session: AsyncSession, artifact: Artifact, *, name: str
) -> bool:
    if (
        artifact.manifest_sha256 is None
        or artifact.stored_size_bytes is None
        or artifact.unpacked_size_bytes is None
        or artifact.file_count is None
        or artifact.file_count < 1
    ):
        return False
    if artifact.safety_state in {"verified", "verified_internal"}:
        return True
    if (
        name not in {"dataset", "policy"}
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
        and imported.kind == name
        and imported.target_artifact_type == artifact.artifact_type
        and imported.trust_class == "internal_trusted"
        and imported.state == "committed"
        and imported.committed_artifact_id == artifact.id
        and upload.state == "committed"
        and upload.manifest_sha256 == artifact.provenance.get("root_manifest_sha256")
        and upload.committed_marker_sha256 == artifact.provenance.get("marker_sha256")
    )


def _artifact_inventory_item(row: Artifact) -> dict[str, Any]:
    return {
        "artifact_id": str(row.id),
        "artifact_type": row.artifact_type,
        "content_sha256": row.content_hash,
        "manifest_sha256": row.manifest_sha256,
        "stored_size_bytes": row.stored_size_bytes,
        "unpacked_size_bytes": row.unpacked_size_bytes,
        "file_count": row.file_count,
        "safety_state": row.safety_state,
    }


class Stage1PreflightObservationV1(PipelineModel):
    ancestry_ok: Literal[True]
    image_platform_ok: Literal[True]
    worker_capability_ok: Literal[True]
    slurm_config_ok: Literal[True]
    gpu_topology_ok: Literal[True]
    cas_capacity_ok: Literal[True]
    scratch_capacity_ok: Literal[True]
    input_markers_ok: Literal[True]
    observed_at: datetime


class Stage1CapacityObservationV1(PipelineModel):
    ancestry_ok: Literal[True]
    image_platform_ok: Literal[True]
    slurm_config_ok: Literal[True]
    cas_capacity_ok: Literal[True]
    scratch_capacity_ok: Literal[True]
    input_markers_ok: Literal[True]
    existing_pipeline_runs: Literal[0]
    existing_attempts: Literal[0]
    existing_upload_sessions: Literal[0]
    existing_slurm_jobs: Literal[0]
    observed_at: datetime


class Stage1EvidenceObservationV1(PipelineModel):
    viewer_ready: Literal[True]
    screenshot_captured: Literal[True]
    handoff_verified: Literal[True]
    unauthorized_viewer_denied: Literal[True]
    public_artifact_denied: Literal[True]
    evidence: Stage1SmokeTerminalEvidenceV1
    observed_at: datetime


class Stage1CleanupObservationV1(PipelineModel):
    active_allocations: Literal[0]
    active_input_leases: Literal[0]
    active_worker_fences: Literal[0]
    unexpected_processes: Literal[0]
    unexpected_mounts: Literal[0]
    observed_at: datetime


class Stage1SmokeObservationSourceV1(Protocol):
    """Read-only protected-environment observer; it grants no mutation."""

    async def observe_capacity_preflight(
        self, *, candidate: Stage1SmokeCandidateV1
    ) -> Stage1CapacityObservationV1: ...

    async def observe_preflight(
        self, *, candidate: Stage1SmokeCandidateV1, preflight: Stage1SmokePreflightV1
    ) -> Stage1PreflightObservationV1: ...

    async def observe_evidence(
        self, *, authorization: PipelineStage1SmokeAuthorization
    ) -> Stage1EvidenceObservationV1: ...

    async def observe_cleanup(
        self, *, authorization: PipelineStage1SmokeAuthorization
    ) -> Stage1CleanupObservationV1: ...


class SqlStage1SmokePreflightAuthority:
    def __init__(self, observation: Stage1SmokeObservationSourceV1) -> None:
        self._observation = observation

    async def verify_preflight(
        self,
        *,
        session: AsyncSession,
        candidate: Stage1SmokeCandidateV1,
        authorization: Stage1SmokeAuthorizationV1,
        preflight: Stage1SmokePreflightV1,
        graph: object,
    ) -> None:
        del graph
        worker = await session.get(Worker, preflight.worker_id)
        slurm = (
            await session.execute(
                select(SlurmWorkerJob).where(
                    SlurmWorkerJob.worker_id == preflight.worker_id,
                    SlurmWorkerJob.slurm_cluster_id == candidate.slurm_cluster_id,
                    SlurmWorkerJob.environment == candidate.environment,
                    SlurmWorkerJob.pool_name == candidate.policy_id,
                    SlurmWorkerJob.job_id == preflight.slurm_allocation_id,
                    SlurmWorkerJob.state == "running",
                )
            )
        ).scalar_one_or_none()
        existing_runs = (
            await session.execute(
                select(func.count())
                .select_from(PipelineRun)
                .where(
                    PipelineRun.team_id == candidate.team_id,
                    PipelineRun.official_submission_kind == OFFICIAL_SUBMISSION_KIND,
                    PipelineRun.official_submission_authority_id == authorization.authorization_id,
                )
            )
        ).scalar_one()
        existing_attempts = (
            await session.execute(
                select(func.count())
                .select_from(ExecutionAttempt)
                .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
                .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
                .where(
                    PipelineRun.official_submission_kind == OFFICIAL_SUBMISSION_KIND,
                    PipelineRun.official_submission_authority_id == authorization.authorization_id,
                )
            )
        ).scalar_one()
        existing_uploads = (
            await session.execute(
                select(func.count())
                .select_from(ArtifactUploadSession)
                .where(ArtifactUploadSession.pipeline_run_id.is_not(None))
                .join(PipelineRun, PipelineRun.id == ArtifactUploadSession.pipeline_run_id)
                .where(
                    PipelineRun.official_submission_kind == OFFICIAL_SUBMISSION_KIND,
                    PipelineRun.official_submission_authority_id == authorization.authorization_id,
                )
            )
        ).scalar_one()
        existing_slurm_jobs = (
            await session.execute(
                select(func.count())
                .select_from(SlurmWorkerJob)
                .where(
                    SlurmWorkerJob.environment == candidate.environment,
                    SlurmWorkerJob.pool_name == candidate.policy_id,
                    SlurmWorkerJob.candidate_sha == candidate.loom_commit_sha,
                    SlurmWorkerJob.id != (slurm.id if slurm is not None else None),
                    SlurmWorkerJob.state.in_(["pending", "running"]),
                )
            )
        ).scalar_one()
        intent = await session.get(PipelineStage1SmokeAuthorization, authorization.authorization_id)
        activation = (
            await session.execute(
                select(PipelineScopedPolicyActivation).where(
                    PipelineScopedPolicyActivation.id
                    == (intent.policy_activation_id if intent is not None else None),
                    PipelineScopedPolicyActivation.authority_id == authorization.authorization_id,
                    PipelineScopedPolicyActivation.environment == candidate.environment,
                    PipelineScopedPolicyActivation.policy_id == candidate.policy_id,
                    PipelineScopedPolicyActivation.policy_config_sha256
                    == candidate.policy_config_sha256,
                    PipelineScopedPolicyActivation.activation_epoch
                    == candidate.policy_activation_epoch,
                    PipelineScopedPolicyActivation.state == "active",
                    PipelineScopedPolicyActivation.desired_slots == 1,
                )
            )
        ).scalar_one_or_none()
        if (
            worker is None
            or worker.status != "active"
            or worker.lease_epoch != preflight.worker_lease_epoch
            or intent is None
            or intent.state != "capacity_pending"
            or intent.candidate_sha256 != candidate.candidate_sha256
            or intent.authorization_sha256 != authorization.authorization_sha256
            or intent.pipeline_run_id is not None
            or activation is None
            or slurm is None
            or slurm.candidate_sha != candidate.loom_commit_sha
            or existing_runs != preflight.existing_pipeline_runs
            or existing_attempts != preflight.existing_attempts
            or existing_uploads != preflight.existing_upload_sessions
            or existing_slurm_jobs != preflight.existing_slurm_jobs
            or any(
                value != 0
                for value in (
                    existing_runs,
                    existing_attempts,
                    existing_uploads,
                    existing_slurm_jobs,
                )
            )
        ):
            raise ValueError("Stage 1 authoritative preflight DB readback failed")
        observed = await self._observation.observe_preflight(
            candidate=candidate, preflight=preflight
        )
        if observed.observed_at != preflight.observed_at:
            raise ValueError("Stage 1 preflight observation timestamp drifted")


class SqlStage1SmokeCapacityPreflightAuthority(Stage1SmokeCapacityPreflightAuthorityV1):
    def __init__(self, observation: Stage1SmokeObservationSourceV1) -> None:
        self._observation = observation

    async def verify_capacity_preflight(
        self,
        *,
        session: AsyncSession,
        candidate: Stage1SmokeCandidateV1,
        authorization: Stage1SmokeAuthorizationV1,
        graph: object,
    ) -> None:
        del graph
        existing_runs = (
            await session.execute(
                select(func.count())
                .select_from(PipelineRun)
                .join(
                    PipelineStage1SmokeAuthorization,
                    PipelineStage1SmokeAuthorization.pipeline_run_id == PipelineRun.id,
                )
                .where(
                    PipelineStage1SmokeAuthorization.environment == candidate.environment,
                    PipelineStage1SmokeAuthorization.state.in_(
                        ["submitted", "running", "cleanup_required", "cleanup_draining"]
                    ),
                )
            )
        ).scalar_one()
        existing_attempts = (
            await session.execute(
                select(func.count())
                .select_from(ExecutionAttempt)
                .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
                .join(PipelineRun, PipelineRun.id == PipelineStageRun.pipeline_run_id)
                .join(
                    PipelineStage1SmokeAuthorization,
                    PipelineStage1SmokeAuthorization.pipeline_run_id == PipelineRun.id,
                )
                .where(
                    PipelineStage1SmokeAuthorization.environment == candidate.environment,
                    PipelineStage1SmokeAuthorization.state.in_(
                        ["submitted", "running", "cleanup_required", "cleanup_draining"]
                    ),
                )
            )
        ).scalar_one()
        existing_uploads = (
            await session.execute(
                select(func.count())
                .select_from(ArtifactUploadSession)
                .join(PipelineRun, PipelineRun.id == ArtifactUploadSession.pipeline_run_id)
                .join(
                    PipelineStage1SmokeAuthorization,
                    PipelineStage1SmokeAuthorization.pipeline_run_id == PipelineRun.id,
                )
                .where(
                    PipelineStage1SmokeAuthorization.environment == candidate.environment,
                    PipelineStage1SmokeAuthorization.state.in_(
                        ["submitted", "running", "cleanup_required", "cleanup_draining"]
                    ),
                )
            )
        ).scalar_one()
        existing_slurm = (
            await session.execute(
                select(func.count())
                .select_from(SlurmWorkerJob)
                .where(
                    SlurmWorkerJob.environment == candidate.environment,
                    SlurmWorkerJob.pool_name == candidate.policy_id,
                    SlurmWorkerJob.candidate_sha == candidate.loom_commit_sha,
                    SlurmWorkerJob.state.in_(["pending", "running"]),
                )
            )
        ).scalar_one()
        if any((existing_runs, existing_attempts, existing_uploads, existing_slurm)):
            raise ValueError("Stage 1 capacity preflight found existing candidate resources")
        observed = await self._observation.observe_capacity_preflight(candidate=candidate)
        if (
            observed.existing_pipeline_runs != existing_runs
            or observed.existing_attempts != existing_attempts
            or observed.existing_upload_sessions != existing_uploads
            or observed.existing_slurm_jobs != existing_slurm
            or observed.observed_at < authorization.authorized_at
            or observed.observed_at > authorization.expires_at
        ):
            raise ValueError("Stage 1 capacity observation drifted")


class SqlStage1SmokeEvidenceAuthority:
    def __init__(self, observation: Stage1SmokeObservationSourceV1) -> None:
        self._observation = observation

    async def verify_evidence(
        self,
        *,
        session: AsyncSession,
        authorization: PipelineStage1SmokeAuthorization,
        evidence: Stage1SmokeEvidenceV1,
    ) -> None:
        run = await session.get(PipelineRun, authorization.pipeline_run_id)
        stages = list(
            (
                await session.execute(
                    select(PipelineStageRun).where(
                        PipelineStageRun.pipeline_run_id == authorization.pipeline_run_id
                    )
                )
            ).scalars()
        )
        attempts = list(
            (
                await session.execute(
                    select(ExecutionAttempt)
                    .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
                    .where(PipelineStageRun.pipeline_run_id == authorization.pipeline_run_id)
                )
            ).scalars()
        )
        outputs = list(
            (
                await session.execute(
                    select(Artifact).where(
                        Artifact.pipeline_run_id == authorization.pipeline_run_id,
                        Artifact.name == "rollout",
                        Artifact.artifact_type == "behavior_rollout_bundle.v1",
                        Artifact.producer_kind == "container",
                    )
                )
            ).scalars()
        )
        preview = (
            await session.execute(
                select(PipelineLivePreviewGeneration).where(
                    PipelineLivePreviewGeneration.pipeline_run_id == authorization.pipeline_run_id
                )
            )
        ).scalar_one_or_none()
        frames = (
            list(
                (
                    await session.execute(
                        select(PipelineLivePreviewFrame)
                        .join(
                            PipelineLivePreviewGeneration,
                            PipelineLivePreviewGeneration.execution_attempt_id
                            == PipelineLivePreviewFrame.execution_attempt_id,
                        )
                        .where(
                            PipelineLivePreviewGeneration.pipeline_run_id
                            == authorization.pipeline_run_id
                        )
                        .order_by(PipelineLivePreviewFrame.sequence)
                    )
                ).scalars()
            )
            if preview is not None
            else []
        )
        candidate = Stage1SmokeCandidateV1.model_validate_json(authorization.candidate_bytes)
        preflight = Stage1SmokePreflightV1.model_validate(authorization.preflight_json)
        evidence_attempt_ids = [item.attempt_id for item in evidence.evidence.attempts]
        database_attempt_ids = [
            item.id for item in sorted(attempts, key=lambda item: item.attempt_number)
        ]
        if (
            run is None
            or run.state != "finished"
            or evidence.result_kind != ("success" if run.result == "succeeded" else "terminal")
            or len(stages) != 1
            or stages[0].state not in {"succeeded", "failed"}
            or not 1 <= len(attempts) <= 3
            or any(
                item.state not in {"succeeded", "failed", "cancelled", "lost"} for item in attempts
            )
            or len(outputs) != (1 if run.result == "succeeded" else 0)
            or any(item.manifest_sha256 is None for item in outputs)
            or preview is None
            or preview.frame_count < 3
            or len(frames) != preview.frame_count
            or preview.state not in {"handoff", "ended"}
            or evidence.evidence.stage_run_id != stages[0].id
            or evidence.evidence.stage_state != stages[0].state
            or evidence.evidence.domain_outcome != stages[0].domain_outcome
            or evidence.evidence.backend_variant_id != candidate.backend_variant_id
            or evidence.evidence.platform_child_digest != candidate.platform_child_digest
            or evidence.evidence.input_descriptor_set_sha256 != canonical_digest(candidate.inputs)
            or evidence.evidence.gpu_devices != preflight.gpu_devices
            or evidence_attempt_ids != database_attempt_ids
            or evidence.evidence.preview is None
            or evidence.evidence.preview.generation_id != preview.generation
            or evidence.evidence.preview.attempt_id != preview.execution_attempt_id
            or [item.sequence for item in evidence.evidence.preview.frames]
            != [item.sequence for item in frames]
            or [item.step_idx for item in evidence.evidence.preview.frames]
            != [int(item.step_idx) for item in frames]
            or [item.jpeg_sha256 for item in evidence.evidence.preview.frames]
            != [item.jpeg_sha256 for item in frames]
            or [item.received_at for item in evidence.evidence.preview.frames]
            != [item.received_at for item in frames]
        ):
            raise ValueError("Stage 1 terminal DB evidence is incomplete")
        for declared, actual in zip(
            evidence.evidence.attempts,
            sorted(attempts, key=lambda item: item.attempt_number),
            strict=True,
        ):
            spec = actual.stage_request_json or {}
            resolved_child = (
                spec.get("resolved_image_manifest_digest") if isinstance(spec, dict) else None
            )
            if (
                declared.attempt_number != actual.attempt_number
                or declared.state != actual.state
                or declared.worker_id != actual.worker_id
                or declared.input_view_digest != actual.input_view_digest
                or declared.cleanup_proof_digest != actual.cleanup_proof_digest
                or declared.platform_child_digest != resolved_child
            ):
                raise ValueError("Stage 1 Attempt evidence drifted")
        if run.result == "succeeded":
            assert len(outputs) == 1
            output = outputs[0]
            upload = await session.get(ArtifactUploadSession, output.artifact_upload_session_id)
            declared_output = evidence.evidence.output
            if (
                declared_output is None
                or upload is None
                or declared_output.artifact_id != output.id
                or declared_output.upload_session_id != upload.id
                or declared_output.artifact_type != output.artifact_type
                or declared_output.manifest_sha256 != output.manifest_sha256
                or declared_output.committed_marker_sha256 != upload.committed_marker_sha256
                or declared_output.content_sha256 != output.content_hash
                or declared_output.stage_result_sha256 != upload.stage_result_digest
                or declared_output.stored_size_bytes != output.stored_size_bytes
                or declared_output.unpacked_size_bytes != output.unpacked_size_bytes
                or declared_output.file_count != output.file_count
            ):
                raise ValueError("Stage 1 Artifact evidence drifted")
        observed = await self._observation.observe_evidence(authorization=authorization)
        if observed.observed_at != evidence.observed_at or observed.evidence != evidence.evidence:
            raise ValueError("Stage 1 external evidence timestamp drifted")


class SqlStage1SmokeCleanupAuthority:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        observation: Stage1SmokeObservationSourceV1,
    ) -> None:
        self._session_factory = session_factory
        self._observation = observation

    async def verify_cleanup(
        self,
        *,
        authorization: PipelineStage1SmokeAuthorization,
        cleanup: Stage1SmokeCleanupV1,
    ) -> None:
        async with self._session_factory() as session:
            attempts = (
                await session.execute(
                    select(func.count())
                    .select_from(ExecutionAttempt)
                    .join(PipelineStageRun, PipelineStageRun.id == ExecutionAttempt.stage_run_id)
                    .where(
                        PipelineStageRun.pipeline_run_id == authorization.pipeline_run_id,
                        ExecutionAttempt.state.in_(["claimed", "running"]),
                    )
                )
            ).scalar_one()
            previews = (
                await session.execute(
                    select(func.count())
                    .select_from(PipelineLivePreviewGeneration)
                    .where(
                        PipelineLivePreviewGeneration.pipeline_run_id
                        == authorization.pipeline_run_id,
                        PipelineLivePreviewGeneration.purged_at.is_(None),
                    )
                )
            ).scalar_one()
            frames = (
                await session.execute(
                    select(func.count())
                    .select_from(PipelineLivePreviewFrame)
                    .join(
                        PipelineLivePreviewGeneration,
                        PipelineLivePreviewGeneration.execution_attempt_id
                        == PipelineLivePreviewFrame.execution_attempt_id,
                    )
                    .where(
                        PipelineLivePreviewGeneration.pipeline_run_id
                        == authorization.pipeline_run_id
                    )
                )
            ).scalar_one()
            if attempts or previews or frames:
                raise ValueError("Stage 1 cleanup DB readback found active residue")
        observed = await self._observation.observe_cleanup(authorization=authorization)
        if (
            observed.observed_at != cleanup.cleaned_at
            or observed.active_allocations != cleanup.active_allocations
            or observed.active_input_leases != cleanup.active_input_leases
            or observed.active_worker_fences != cleanup.active_worker_fences
            or observed.unexpected_processes != cleanup.unexpected_processes
            or observed.unexpected_mounts != cleanup.unexpected_mounts
        ):
            raise ValueError("Stage 1 external cleanup evidence drifted")


def build_stage1_candidate_authority_from_environment(
    *, repo_root: Path
) -> RepositoryStage1CandidateAuthority | None:
    try:
        commit = _IMAGE_BUILD_SHA_PATH.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        commit = ""
    environment = os.environ.get("LOOM_ENV", "").strip()
    if _COMMIT_RE.fullmatch(commit) is None or commit == "0" * 40 or not environment:
        return None
    return RepositoryStage1CandidateAuthority(
        repo_root=repo_root, loom_commit_sha=commit, environment=environment
    )


def install_stage1_smoke_authority_adapters(
    *,
    app: Any,
    session_factory: async_sessionmaker[AsyncSession],
    observation: Stage1SmokeObservationSourceV1,
) -> None:
    """Explicitly install the external-observation-backed authorities."""

    app.state.pipeline_stage1_capacity_preflight_authority = (
        SqlStage1SmokeCapacityPreflightAuthority(observation)
    )
    app.state.pipeline_stage1_execution_preflight_authority = SqlStage1SmokePreflightAuthority(
        observation
    )
    app.state.pipeline_stage1_evidence_authority = SqlStage1SmokeEvidenceAuthority(observation)
    app.state.pipeline_stage1_cleanup_authority = SqlStage1SmokeCleanupAuthority(
        session_factory, observation
    )


__all__ = [
    "RepositoryStage1CandidateAuthority",
    "SqlStage1SmokeCapacityPreflightAuthority",
    "SqlStage1SmokeCleanupAuthority",
    "SqlStage1SmokeEvidenceAuthority",
    "SqlStage1SmokePreflightAuthority",
    "Stage1CandidateAuthorityError",
    "Stage1CandidateInventoryV1",
    "Stage1CandidateSelectionV1",
    "Stage1CapacityObservationV1",
    "Stage1CleanupObservationV1",
    "Stage1EvidenceObservationV1",
    "Stage1PreflightObservationV1",
    "Stage1SmokeObservationSourceV1",
    "build_stage1_candidate_authority_from_environment",
    "install_stage1_smoke_authority_adapters",
]
