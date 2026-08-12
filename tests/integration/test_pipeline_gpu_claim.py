from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.models.worker_capabilities import (
    GpuDeviceCapabilityV1,
    SlurmGpuAllocationEvidenceV1,
    WorkerCapabilitySnapshotV1,
)
from loom.pipeline.gpu_backend import (
    PipelineGpuSelectionError,
    PipelineRunGpuBackendSelectionV1,
    recipe_hash_variant,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.resource_profiles import load_resource_profiles
from loom.pipeline.work_protocol import ImageRuntimeContractV1
from loom_control_plane.pipeline_gpu_selection import (
    ensure_ordinary_gpu_backend_selection,
    persist_gpu_backend_selection,
)
from loom_control_plane.scheduler.claim import claim_work

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/behavior-sim@sha256:" + "b" * 64


def _gb10_capability() -> tuple[WorkerCapabilitySnapshotV1, SlurmGpuAllocationEvidenceV1]:
    allocation = SlurmGpuAllocationEvidenceV1(
        allocation_id="gb10:91",
        slurm_cluster_id="gb10",
        job_id="91",
        node_name="trt-gb10-9",
        partition="gb10",
        gpu_tres="gpu:gb10:1",
        allocated_device_ids=[0],
        device_uuids=["GPU-GB10"],
        variant_id="gb10-shared-1gpu",
    )
    device = GpuDeviceCapabilityV1(
        allocation_id=allocation.allocation_id,
        device_uuid="GPU-GB10",
        vendor="nvidia",
        model="NVIDIA GB10",
        memory_kind="unified",
        memory_mb=None,
        unified_memory_mb=124_000,
        nvidia_driver_version="580.12.0",
        mig_mode="not_supported",
    )
    capability = WorkerCapabilitySnapshotV1(
        schema_version="loom.worker-capabilities.v1",
        cpu_arch="arm64",
        cpu_cores=20,
        memory_bytes=128 << 30,
        scratch_bytes=200 << 30,
        network_profiles=["none"],
        container_runtime_features=["egl", "nvidia-container-runtime"],
        gpu_devices=[device],
        input_cache_capacity_bytes=1_649_267_441_664,
        input_cache_reserved_bytes=0,
        input_cache_ready_bytes=0,
    )
    return capability, allocation


def _image_contract() -> ImageRuntimeContractV1:
    return ImageRuntimeContractV1.model_validate(
        {
            "image_index_digest": IMAGE,
            "platform": "linux/arm64",
            "platform_manifest_digest": DIGEST,
            "cpu_arch": "arm64",
            "gpu_vendor": "nvidia",
            "cuda_userspace_version": "13.0",
            "min_nvidia_driver_version": "580.1",
            "application_features": ["isaac-sim-5.1", "omnigibson-3.8"],
            "provider_assets": [],
            "preflight_argv": ["/opt/loom/gpu-preflight"],
            "preflight_digest": DIGEST,
            "sbom_digest": DIGEST,
            "attestation_digest": DIGEST,
        }
    )


def _run_id_for_variant(variant_id: str) -> UUID:
    while True:
        run_id = uuid4()
        if recipe_hash_variant(recipe_digest=DIGEST, pipeline_run_id=run_id) == variant_id:
            return run_id


def test_official_authority_may_pin_one_all_gpu_scope() -> None:
    selection = PipelineRunGpuBackendSelectionV1(
        pipeline_run_id=uuid4(),
        scope="all_gpu_nodes",
        variant_id="gb10-shared-1gpu",
        policy_id="behavior-gpu-gb10",
        selection_source="profile_calibration_authority",
        selected_at=datetime.now(UTC),
    )
    assert selection.scope == "all_gpu_nodes"


async def _insert_run(
    session: AsyncSession,
    *,
    team_id: UUID,
    run_id: UUID,
    authority_id: UUID | None = None,
) -> None:
    await session.execute(
        text("""
            INSERT INTO pipeline_runs (
                id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                resolved_inputs_json,budget_json,request_digest,idempotency_key,
                official_submission_kind,official_submission_authority_id,
                official_submission_authority_snapshot_digest,
                official_submission_identity_digest
            ) VALUES (
                :id,:team,'ordinary','behavior-recovery',1,:digest,'{}'::jsonb,:digest,
                '{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,:digest,:key,
                CASE WHEN CAST(:authority AS uuid) IS NULL THEN NULL
                     ELSE 'behavior_profile_calibration_run_v1' END,
                CAST(:authority AS uuid),
                CASE WHEN CAST(:authority AS uuid) IS NULL THEN NULL ELSE :digest END,
                CASE WHEN CAST(:authority AS uuid) IS NULL THEN NULL ELSE :digest END
            )
        """),
        {
            "id": run_id,
            "team": team_id,
            "digest": DIGEST,
            "key": f"gpu-{run_id}",
            "authority": authority_id,
        },
    )


async def test_recipe_hash_selection_is_stable_persisted_and_not_overwritable(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, run_id = uuid4(), uuid4()
    selected_at = datetime.now(UTC).replace(microsecond=0)
    async with sessions() as session, session.begin():
        await session.execute(
            text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"gpu-selection-{team_id}"},
        )
        await _insert_run(session, team_id=team_id, run_id=run_id)
        selection = await ensure_ordinary_gpu_backend_selection(
            session,
            pipeline_run_id=run_id,
            recipe_digest=DIGEST,
            selected_at=selected_at,
        )
        replay = await ensure_ordinary_gpu_backend_selection(
            session,
            pipeline_run_id=run_id,
            recipe_digest=DIGEST,
            selected_at=selected_at + timedelta(seconds=30),
        )
        assert replay == selection
        assert selection.variant_id == recipe_hash_variant(
            recipe_digest=DIGEST, pipeline_run_id=run_id
        )
        other_variant = (
            "oldlab-rtx5080-2gpu"
            if selection.variant_id == "gb10-shared-1gpu"
            else "gb10-shared-1gpu"
        )
        other = PipelineRunGpuBackendSelectionV1(
            pipeline_run_id=run_id,
            scope="all_gpu_nodes",
            variant_id=other_variant,
            policy_id=(
                "behavior-gpu-oldlab"
                if other_variant == "oldlab-rtx5080-2gpu"
                else "behavior-gpu-gb10"
            ),
            selection_source="recipe_hash",
            selected_at=selected_at,
        )
        with pytest.raises(PipelineGpuSelectionError, match="different evidence"):
            await persist_gpu_backend_selection(session, other)
    async with engine.begin() as connection:
        await connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
        await connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
    await engine.dispose()


async def test_gpu_claim_requires_frozen_variant_policy_and_allocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    (
        team_id,
        worker_id,
        policy_row_id,
        activation_id,
        authority_id,
        stage_id,
        attempt_id,
        wrong_worker_id,
        job_row_id,
    ) = (
        uuid4() for _ in range(9)
    )
    run_id = _run_id_for_variant("gb10-shared-1gpu")
    token_hash = b"g" * 32
    capability, allocation = _gb10_capability()
    profile = load_resource_profiles().get("behavior-sim-local-none@1").profile
    runtime = _image_contract()
    selected_at = datetime.now(UTC).replace(microsecond=0)
    async with sessions() as session, session.begin():
        await session.execute(
            text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"gpu-claim-{team_id}"},
        )
        await session.execute(
            text("INSERT INTO team_quotas(team_id) VALUES (:id)"), {"id": team_id}
        )
        await _insert_run(
            session, team_id=team_id, run_id=run_id, authority_id=authority_id
        )
        selection = PipelineRunGpuBackendSelectionV1(
            pipeline_run_id=run_id,
            scope="all_gpu_nodes",
            variant_id="gb10-shared-1gpu",
            policy_id="behavior-gpu-gb10",
            selection_source="profile_calibration_authority",
            selected_at=selected_at,
        )
        selection = await persist_gpu_backend_selection(session, selection)
        await session.execute(
            text("""
                INSERT INTO worker_pool_autoscaler_policies (
                    id,environment,pool_name,actuator,enabled,min_slots,max_slots,
                    actuator_config
                ) VALUES (
                    :id,'test','behavior-gpu-gb10','slurm',true,0,1,
                    jsonb_build_object(
                        'policy_id', 'behavior-gpu-gb10',
                        'policy_config_sha256', CAST(:digest AS text),
                        'slurm_cluster_config_sha256', CAST(:digest AS text),
                        'slurm_cluster_id', 'gb10',
                        'allowed_nodes', jsonb_build_array('trt-gb10-9')
                    )
                )
            """),
            {"id": policy_row_id, "digest": DIGEST},
        )
        worker_values = {
            "caps": json.dumps(
                [{
                    "os": "linux",
                    "cpu_arch": "arm64",
                    "gpu_vendor": "nvidia",
                    "network_policies": ["no-network"],
                }]
            ),
            "snapshot": json.dumps(capability.model_dump(mode="json")),
            "evidence": json.dumps(allocation.model_dump(mode="json")),
            "capability_digest": capability.digest,
            "evidence_digest": allocation.digest,
            "token": token_hash,
        }
        for current_worker_id, pool_name in (
            (worker_id, "behavior-gpu-gb10"),
            (wrong_worker_id, "behavior-gpu-oldlab"),
        ):
            await session.execute(
                text("""
                    INSERT INTO workers (
                        id,hostname,version,capabilities,supported_work_kinds,
                        capability_snapshot_digest,capability_snapshot_json,
                        slurm_gpu_allocation_evidence_json,
                        slurm_gpu_allocation_evidence_digest,auth_token_hash,max_concurrent,
                        pool_name,registered_at,last_seen_at,status
                    ) VALUES (
                        :id,:hostname,'test',CAST(:caps AS jsonb),
                        ARRAY['trial','execution_attempt']::text[], :capability_digest,
                        CAST(:snapshot AS jsonb),CAST(:evidence AS jsonb),:evidence_digest,
                        :token,1,:pool,now(),now(),'active'
                    )
                """),
                {
                    **worker_values,
                    "id": current_worker_id,
                    "hostname": (
                        "trt-gb10-9"
                        if current_worker_id == worker_id
                        else "trt-eai-oldlab-1"
                    ),
                    "pool": pool_name,
                },
            )
        await session.execute(
            text("""
                INSERT INTO slurm_worker_jobs (
                    id,slurm_cluster_id,environment,pool_name,nodelist,
                    requested_cpus,requested_memory_mib,requested_gpu_tres,
                    requested_gpus,requested_concurrency,job_id,slurm_state,state,
                    worker_id,redacted_env
                ) VALUES (
                    :id,'gb10','test','behavior-gpu-gb10','trt-gb10-9',
                    16,120000,'gpu:gb10:1',1,1,'91','RUNNING','running',
                    :worker,'{}'::jsonb
                )
            """),
            {"id": job_row_id, "worker": worker_id},
        )
        execution_spec = {
            "execution_variant_id": selection.variant_id,
            "gpu_backend_selection_sha256": selection.gpu_backend_selection_sha256,
        }
        await session.execute(
            text("""
                INSERT INTO pipeline_stage_runs (
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resolved_execution_spec_json,resolved_execution_spec_bytes,
                    execution_spec_digest,resource_profile_json,resource_profile_digest,
                    image_runtime_contract_json,image_runtime_contract_digest,
                    resolved_input_bindings_json,resolved_input_bindings_digest,
                    failure_policy,ready_at
                ) VALUES (
                    :id,:run,'sim','singleton','container','queued',CAST(:spec AS jsonb),
                    :spec_bytes,:spec_digest,CAST(:profile AS jsonb),:profile_digest,
                    CAST(:runtime AS jsonb),:runtime_digest,'[]'::jsonb,:bindings_digest,
                    'fail_run',now()
                )
            """),
            {
                "id": stage_id,
                "run": run_id,
                "spec": json.dumps(execution_spec),
                "spec_bytes": canonical_document(execution_spec),
                "spec_digest": canonical_digest(execution_spec),
                "profile": json.dumps(profile.model_dump(mode="json")),
                "profile_digest": canonical_digest(profile),
                "runtime": json.dumps(runtime.model_dump(mode="json")),
                "runtime_digest": canonical_digest(runtime),
                "bindings_digest": canonical_digest([]),
            },
        )
        await session.execute(
            text("""
                INSERT INTO execution_attempts(id,stage_run_id,attempt_number,state,queued_at)
                VALUES (:id,:stage,1,'queued',now())
            """),
            {"id": attempt_id, "stage": stage_id},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_budget_ledgers (
                    pipeline_run_id,provider_limit_microusd,gpu_limit_seconds,
                    artifact_limit_bytes,stage_run_limit,stage_runs_created,
                    attempt_limit,attempts_created,wall_deadline_at
                ) VALUES (:run,1000,14400,1000000,10,1,10,1,:deadline)
            """),
            {"run": run_id, "deadline": datetime.now(UTC) + timedelta(hours=1)},
        )
    async with sessions() as session:
        assert await claim_work(
            session,
            worker_id=worker_id,
            capability_snapshot_digest=capability.digest,
            worker_token_hash=token_hash,
            supported_work_kinds=["trial", "execution_attempt"],
            free_slots=1,
            worker_os=["linux"],
            worker_cpu_arches=["arm64"],
            worker_gpu_vendors=["nvidia"],
            worker_network_policies=["no-network"],
        ) is None
    async with sessions() as session, session.begin():
        await session.execute(
            text("""
                INSERT INTO pipeline_scoped_policy_activations (
                    id,environment,policy_id,policy_config_sha256,authority_kind,
                    authority_id,activation_epoch,state,desired_slots
                ) VALUES (
                    :id,'test','behavior-gpu-gb10',:digest,'profile_calibration',
                    :authority,7,'active',1
                )
            """),
            {"id": activation_id, "digest": DIGEST, "authority": authority_id},
        )
    async with sessions() as session:
        wrong = await claim_work(
            session,
            worker_id=wrong_worker_id,
            capability_snapshot_digest=capability.digest,
            worker_token_hash=token_hash,
            supported_work_kinds=["trial", "execution_attempt"],
            free_slots=1,
            worker_os=["linux"],
            worker_cpu_arches=["arm64"],
            worker_gpu_vendors=["nvidia"],
            worker_network_policies=["no-network"],
        )
        assert wrong is None
    async with sessions() as session:
        diagnostics = (
            await session.execute(
                text("""
                    SELECT
                      w.pool_name = 'behavior-gpu-gb10' AS pool_ok,
                      w.capability_snapshot_json->>'cpu_arch' = 'arm64' AS arch_ok,
                      (w.capability_snapshot_json->'network_profiles') ? 'none' AS network_ok,
                      (s.resource_profile_json->'required_host_runtime_features') <@
                        (w.capability_snapshot_json->'container_runtime_features') AS runtime_ok,
                      (s.resource_profile_json->'required_image_features') <@
                        (s.image_runtime_contract_json->'application_features') AS image_ok,
                      EXISTS (
                        SELECT 1 FROM pipeline_run_gpu_backend_selections selection
                         WHERE selection.pipeline_run_id = r.id
                           AND selection.variant_id = 'gb10-shared-1gpu'
                           AND selection.policy_id = w.pool_name
                           AND selection.gpu_backend_selection_sha256 =
                             s.resolved_execution_spec_json->>'gpu_backend_selection_sha256'
                      ) AS selection_ok,
                      EXISTS (
                        SELECT 1 FROM slurm_worker_jobs job
                         WHERE job.worker_id = w.id
                           AND job.slurm_cluster_id = 'gb10'
                           AND job.job_id = '91'
                           AND job.pool_name = w.pool_name
                           AND job.nodelist = 'trt-gb10-9'
                           AND job.requested_gpu_tres = 'gpu:gb10:1'
                           AND job.requested_gpus = 1
                           AND job.requested_concurrency = 1
                           AND job.state = 'running'
                      ) AS job_ok,
                      EXISTS (
                        SELECT 1 FROM worker_pool_autoscaler_policies policy
                         WHERE policy.pool_name = w.pool_name
                           AND policy.enabled
                           AND policy.actuator_config->>'policy_id' = w.pool_name
                           AND policy.actuator_config->>'slurm_cluster_id' = 'gb10'
                      ) AS policy_ok
                    FROM workers w
                    JOIN pipeline_stage_runs s ON s.id = :stage
                    JOIN pipeline_runs r ON r.id = s.pipeline_run_id
                    WHERE w.id = :worker
                """),
                {"worker": worker_id, "stage": stage_id},
            )
        ).mappings().one()
        assert all(diagnostics.values()), diagnostics
        claimed = await claim_work(
            session,
            worker_id=worker_id,
            capability_snapshot_digest=capability.digest,
            worker_token_hash=token_hash,
            supported_work_kinds=["trial", "execution_attempt"],
            free_slots=1,
            worker_os=["linux"],
            worker_cpu_arches=["arm64"],
            worker_gpu_vendors=["nvidia"],
            worker_network_policies=["no-network"],
        )
        assert claimed is not None and claimed[0]["work_kind"] == "execution_attempt"
        await session.commit()
    async with engine.begin() as connection:
        await connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
        await connection.execute(
            text("DELETE FROM slurm_worker_jobs WHERE id=:id"), {"id": job_row_id}
        )
        await connection.execute(
            text("DELETE FROM workers WHERE id IN (:one,:two)"),
            {"one": worker_id, "two": wrong_worker_id},
        )
        await connection.execute(
            text("DELETE FROM pipeline_scoped_policy_activations WHERE id=:id"),
            {"id": activation_id},
        )
        await connection.execute(
            text("DELETE FROM worker_pool_autoscaler_policies WHERE id=:id"),
            {"id": policy_row_id},
        )
        await connection.execute(text("DELETE FROM team_quotas WHERE team_id=:id"), {"id": team_id})
        await connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
    await engine.dispose()
