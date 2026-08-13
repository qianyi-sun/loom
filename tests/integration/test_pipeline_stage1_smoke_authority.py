from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    Artifact,
    PipelineBudgetLedger,
    PipelineRun,
    PipelineRunGpuBackendSelection,
    PipelineScopedPolicyActivation,
    PipelineStage1SmokeAuthorization,
    PipelineStage1SmokeEvent,
    Team,
    User,
)
from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import RunGraphSpecV1
from loom.pipeline.stage1_smoke import (
    Stage1SmokeAuthorizationV1,
    Stage1SmokeCandidateV1,
    Stage1SmokeCleanupV1,
    Stage1SmokeGpuDeviceV1,
    Stage1SmokePreflightV1,
)
from loom_pipeline_orchestrator.repository import PipelineRepository
from loom_service import pipeline_stage1_smoke_service as service
from loom_service.pipeline_stage1_smoke_service import Stage1SmokeEvidenceV1
from tests.unit.test_pipeline_stage1_smoke import _candidate

REPO_ROOT = Path(__file__).resolve().parents[2]


async def test_execute_persists_claimable_budgeted_official_run_atomically(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    team_id = uuid4()
    operator_id = uuid4()
    candidate = _candidate(
        environment=f"stage1-test-{uuid4().hex}",
        team_id=team_id,
        operator_user_id=operator_id,
        policy_activation_epoch=1,
        start_by=now + timedelta(minutes=5),
        cleanup_deadline=now + timedelta(hours=1),
    )
    authorization = Stage1SmokeAuthorizationV1(
        schema_version="loom.behavior-stage1-smoke-authorization.v1",
        action="stage1",
        authorization_id=uuid4(),
        candidate_sha256=candidate.candidate_sha256,
        operator_user_id=operator_id,
        team_id=team_id,
        environment=candidate.environment,
        loom_commit_sha=candidate.loom_commit_sha,
        recipe_digest=candidate.recipe_digest,
        image_index_digest=candidate.image_index_digest,
        platform=candidate.platform,
        platform_child_digest=candidate.platform_child_digest,
        backend_variant_id=candidate.backend_variant_id,
        policy_id=candidate.policy_id,
        policy_config_sha256=candidate.policy_config_sha256,
        policy_activation_epoch=candidate.policy_activation_epoch,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        run_budget_sha256=canonical_digest(candidate.run_budget),
        start_by=candidate.start_by,
        cleanup_deadline=candidate.cleanup_deadline,
        live_mutation_authorized=True,
        authorized_at=now,
        expires_at=now + timedelta(minutes=5),
        nonce_sha256="sha256:" + "9" * 64,
    )
    preflight = Stage1SmokePreflightV1(
        schema_version="loom.behavior-stage1-smoke-preflight.v1",
        candidate_sha256=candidate.candidate_sha256,
        authorization_id=authorization.authorization_id,
        authorization_sha256=authorization.authorization_sha256,
        worker_id=uuid4(),
        worker_lease_epoch=1,
        worker_capability_snapshot_sha256="sha256:" + "8" * 64,
        slurm_allocation_id="oldlab:test-allocation",
        gpu_devices=[
            Stage1SmokeGpuDeviceV1(
                logical_index=0,
                device_uuid="GPU-Z",
                model="NVIDIA GeForce RTX 5080",
                role="sim",
            ),
            Stage1SmokeGpuDeviceV1(
                logical_index=1,
                device_uuid="GPU-A",
                model="NVIDIA GeForce RTX 5080",
                role="vla",
            ),
        ],
        policy_activation_epoch=candidate.policy_activation_epoch,
        platform_child_digest=candidate.platform_child_digest,
        image_runtime_contract_sha256=candidate.image_runtime_contract_sha256,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        ancestry_ok=True,
        image_platform_ok=True,
        worker_capability_ok=True,
        slurm_config_ok=True,
        gpu_topology_ok=True,
        cas_capacity_ok=True,
        scratch_capacity_ok=True,
        input_markers_ok=True,
        existing_pipeline_runs=0,
        existing_attempts=0,
        existing_upload_sessions=0,
        existing_slurm_jobs=0,
        observed_at=now,
    )

    class PreflightAuthority:
        called = 0

        async def verify_preflight(
            self,
            *,
            session: AsyncSession,
            candidate: Stage1SmokeCandidateV1,
            authorization: Stage1SmokeAuthorizationV1,
            preflight: Stage1SmokePreflightV1,
            graph: RunGraphSpecV1,
        ) -> None:
            assert session is not None
            assert authorization.candidate_sha256 == candidate.candidate_sha256
            assert preflight.candidate_sha256 == candidate.candidate_sha256
            assert graph.recipe.digest == candidate.recipe_digest
            self.called += 1

    preflight_authority = PreflightAuthority()
    frozen_artifacts = {
        item.artifact_id: Artifact(
            id=item.artifact_id,
            artifact_type=item.artifact_type,
            name=item.name,
            team_id=team_id,
            manifest_sha256=item.manifest_sha256,
            stored_size_bytes=item.stored_size_bytes,
            unpacked_size_bytes=item.unpacked_size_bytes,
            file_count=item.file_count,
            content_hash=item.content_sha256,
            artifact_upload_session_id=uuid4(),
            safety_state="verified_internal",
        )
        for index, item in enumerate(candidate.inputs)
    }

    async def fake_inputs(*_args: object, **_kwargs: object) -> dict[UUID, Artifact]:
        return frozen_artifacts

    async def fake_worker(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(service, "_validate_inputs", fake_inputs)
    monkeypatch.setattr(service, "_validate_worker", fake_worker)
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    run_id = None
    activation_id = None
    try:
        async with sessions() as session:
            session.add_all(
                [
                    Team(id=team_id, name=f"stage1-{team_id}"),
                    User(
                        id=operator_id,
                        username=f"stage1-{operator_id}",
                        username_normalized=f"stage1-{operator_id}",
                        status="active",
                    ),
                ]
            )
            await session.commit()

        class RejectingPreflightAuthority:
            async def verify_preflight(
                self,
                *,
                session: AsyncSession,
                candidate: Stage1SmokeCandidateV1,
                authorization: Stage1SmokeAuthorizationV1,
                preflight: Stage1SmokePreflightV1,
                graph: RunGraphSpecV1,
            ) -> None:
                del session, candidate, authorization, preflight, graph
                raise ValueError("contracts are unavailable")

        async with sessions() as session:
            with pytest.raises(
                service.Stage1SmokeServiceError,
                match="stage1_smoke_preflight_unverified",
            ):
                await service.execute_stage1_smoke(
                    session,
                    candidate=candidate,
                    authorization=authorization,
                    preflight=preflight,
                    idempotency_key="stage1-integration",
                    signature_key_id="stage1-test",
                    signature_sha256="sha256:" + "7" * 64,
                    preflight_authority=RejectingPreflightAuthority(),
                    repo_root=REPO_ROOT,
                    now=now,
                )
            assert not session.new and not session.dirty and not session.deleted
            await session.rollback()
        async with sessions() as session:
            authority_count = (
                await session.execute(
                    select(func.count())
                    .select_from(PipelineStage1SmokeAuthorization)
                    .where(PipelineStage1SmokeAuthorization.team_id == team_id)
                )
            ).scalar_one()
            run_count = (
                await session.execute(
                    select(func.count())
                    .select_from(PipelineRun)
                    .where(PipelineRun.team_id == team_id)
                )
            ).scalar_one()
            assert (authority_count, run_count) == (0, 0)
        async with sessions() as session:
            body, replay = await service.execute_stage1_smoke(
                session,
                candidate=candidate,
                authorization=authorization,
                preflight=preflight,
                idempotency_key="stage1-integration",
                signature_key_id="stage1-test",
                signature_sha256="sha256:" + "7" * 64,
                preflight_authority=preflight_authority,
                repo_root=REPO_ROOT,
                now=now,
            )
            await session.commit()
            assert not replay
            run_id = body["pipeline_run_id"]
            activation_id = body["policy_activation_id"]
        async with sessions() as session:
            replay_body, replay = await service.execute_stage1_smoke(
                session,
                candidate=candidate,
                authorization=authorization,
                preflight=preflight,
                idempotency_key="stage1-integration",
                signature_key_id="stage1-test",
                signature_sha256="sha256:" + "7" * 64,
                preflight_authority=preflight_authority,
                repo_root=REPO_ROOT,
                now=now,
            )
            await session.commit()
            assert replay
            assert replay_body == body
            assert preflight_authority.called == 1
        leases = await PipelineRepository(sessions).claim_runs(
            controller_id="stage1-integration-controller"
        )
        assert [lease.pipeline_run_id for lease in leases] == [UUID(run_id)]
        async with sessions() as session:
            row = await session.get(
                PipelineStage1SmokeAuthorization, authorization.authorization_id
            )
            assert row is not None
            assert row.candidate_bytes == candidate.canonical_bytes
            assert canonical_digest(row.candidate_json) == candidate.candidate_sha256
            assert [item["device_uuid"] for item in row.preflight_json["gpu_devices"]] == [
                "GPU-Z",
                "GPU-A",
            ]
            run = await session.get(PipelineRun, row.pipeline_run_id)
            ledger = await session.get(PipelineBudgetLedger, row.pipeline_run_id)
            selection = (
                await session.execute(
                    select(PipelineRunGpuBackendSelection).where(
                        PipelineRunGpuBackendSelection.pipeline_run_id == row.pipeline_run_id
                    )
                )
            ).scalar_one()
            assert run is not None and ledger is not None
            assert run.official_submission_kind == service.OFFICIAL_SUBMISSION_KIND
            assert run.acceptance_authorization_id is None
            assert run.acceptance_candidate_sha256 is None
            assert ledger.stage_run_limit == 1
            assert ledger.wall_deadline_at == now + timedelta(
                seconds=candidate.run_budget.max_wall_seconds
            )
            assert [item["stored_size_bytes"] for item in run.resolved_inputs_json] == [
                100,
                200,
                300,
            ]
            assert selection.selection_source == "acceptance_authority"
        async with sessions() as session:
            terminal_run = await session.get(PipelineRun, UUID(run_id), with_for_update=True)
            assert terminal_run is not None
            terminal_run.state = "finished"
            terminal_run.result = "succeeded"
            terminal_run.finished_at = now
            await session.commit()
        evidence = Stage1SmokeEvidenceV1(
            schema_version="loom.behavior-stage1-smoke-evidence.v1",
            authorization_id=authorization.authorization_id,
            candidate_sha256=candidate.candidate_sha256,
            pipeline_run_id=UUID(run_id),
            result_kind="success",
            evidence={"result_sha256": "sha256:" + "6" * 64},
            observed_at=now,
        )

        class EvidenceAuthority:
            called = False

            async def verify_evidence(
                self,
                *,
                session: AsyncSession,
                authorization: PipelineStage1SmokeAuthorization,
                evidence: Stage1SmokeEvidenceV1,
            ) -> None:
                assert session is not None
                assert authorization.pipeline_run_id == evidence.pipeline_run_id
                self.called = True

        evidence_authority = EvidenceAuthority()

        class RejectingEvidenceAuthority:
            async def verify_evidence(
                self,
                *,
                session: AsyncSession,
                authorization: PipelineStage1SmokeAuthorization,
                evidence: Stage1SmokeEvidenceV1,
            ) -> None:
                del session, authorization, evidence
                raise ValueError("evidence contracts are unavailable")

        async with sessions() as session:
            with pytest.raises(
                service.Stage1SmokeServiceError,
                match="stage1_smoke_evidence_unverified",
            ):
                await service.record_stage1_smoke_evidence(
                    session,
                    evidence=evidence,
                    authority=RejectingEvidenceAuthority(),
                    now=now,
                )
            assert not session.new and not session.dirty and not session.deleted
            await session.rollback()
        async with sessions() as session:
            unchanged = await session.get(
                PipelineStage1SmokeAuthorization, authorization.authorization_id
            )
            assert unchanged is not None
            assert (unchanged.state, unchanged.evidence_sha256) == ("submitted", None)
        async with sessions() as session:
            evidence_body, replay = await service.record_stage1_smoke_evidence(
                session,
                evidence=evidence,
                authority=evidence_authority,
                now=now,
            )
            await session.commit()
            assert not replay
            assert evidence_body["state"] == "cleanup_required"
        assert evidence_authority.called

        class CleanupAuthority:
            called = False

            async def verify_cleanup(
                self,
                *,
                authorization: PipelineStage1SmokeAuthorization,
                cleanup: Stage1SmokeCleanupV1,
            ) -> None:
                assert authorization.candidate_sha256 == cleanup.candidate_sha256
                self.called = True

        cleanup_authority = CleanupAuthority()
        cleanup = Stage1SmokeCleanupV1(
            schema_version="loom.behavior-stage1-smoke-cleanup.v1",
            candidate_sha256=candidate.candidate_sha256,
            pipeline_run_id=UUID(run_id),
            preview_generation_count=0,
            preview_frame_count=0,
            active_policy_slots=0,
            active_upload_sessions=0,
            active_input_leases=0,
            active_worker_fences=0,
            active_slurm_jobs=0,
            active_allocations=0,
            unexpected_processes=0,
            unexpected_mounts=0,
            cleaned_at=now,
        )
        async with sessions() as session:
            cleanup_body, replay = await service.cleanup_stage1_smoke(
                session,
                cleanup=cleanup,
                authority=cleanup_authority,
                now=now,
            )
            await session.commit()
            assert not replay
            assert cleanup_body["state"] == "accepted"
        assert cleanup_authority.called
        async with sessions() as session:
            activation = await session.get(PipelineScopedPolicyActivation, UUID(activation_id))
            events = list(
                (
                    await session.execute(
                        select(PipelineStage1SmokeEvent)
                        .where(
                            PipelineStage1SmokeEvent.authorization_id
                            == authorization.authorization_id
                        )
                        .order_by(PipelineStage1SmokeEvent.seq)
                    )
                ).scalars()
            )
            assert activation is not None
            assert (activation.state, activation.desired_slots) == ("disabled", 0)
            assert [event.event_kind for event in events] == [
                "live_action_consumed",
                "evidence_recorded",
                "cleanup_complete",
                "accepted",
            ]
    finally:
        async with sessions() as session:
            await session.execute(
                delete(PipelineStage1SmokeEvent).where(
                    PipelineStage1SmokeEvent.authorization_id == authorization.authorization_id
                )
            )
            await session.execute(
                delete(PipelineStage1SmokeAuthorization).where(
                    PipelineStage1SmokeAuthorization.authorization_id
                    == authorization.authorization_id
                )
            )
            if run_id is not None:
                await session.execute(delete(PipelineRun).where(PipelineRun.id == run_id))
            if activation_id is not None:
                await session.execute(
                    delete(PipelineScopedPolicyActivation).where(
                        PipelineScopedPolicyActivation.id == activation_id
                    )
                )
            await session.execute(delete(Team).where(Team.id == team_id))
            await session.execute(delete(User).where(User.id == operator_id))
            await session.commit()
        await engine.dispose()
