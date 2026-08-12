from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.pipeline.keys import canonical_digest, canonical_document
from loom_control_plane.scheduler.claim import WorkClaimConflictError, claim_work


def _contracts() -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    image = "registry.example.com/loom/pipeline@sha256:" + "4" * 64
    profile: dict[str, object] = {
        "name": "behavior-offline-none",
        "version": 1,
        "execution_variants": [{
            "variant_id": "cpu-data-x86_64", "cpu_arch": "x86_64",
            "gpu_count_exact": 0, "gpu_vendor": None, "allowed_gpu_models": [],
            "gpu_memory_kind": None, "gpu_memory_mb_min": None,
            "gpu_unified_memory_mb_min": None, "memory_accounting_kind": "separate",
            "container_memory_bytes_override": None, "same_gpu_model_required": False,
            "pool_class": "behavior-cpu-data", "device_roles": None,
        }],
        "cpu_cores": 8,
        "memory_bytes": 17_179_869_184,
        "scratch_bytes": 53_687_091_200,
        "timeout_seconds_max": 3_600,
        "required_host_runtime_features": [],
        "required_image_features": ["behavior-cpu-data"],
        "network_profile": "none",
        "input_cache_capacity_bytes_min": 1_649_267_441_664,
    }
    runtime: dict[str, object] = {
        "image_index_digest": image,
        "platform": "linux/amd64",
        "platform_manifest_digest": "sha256:" + "3" * 64,
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "cuda_userspace_version": None,
        "min_nvidia_driver_version": None,
        "application_features": ["behavior-cpu-data"],
        "provider_assets": [],
        "preflight_argv": ["/opt/loom/preflight"],
        "preflight_digest": "sha256:" + "0" * 64,
        "sbom_digest": "sha256:" + "1" * 64,
        "attestation_digest": "sha256:" + "2" * 64,
    }
    bindings: list[dict[str, object]] = []
    spec: dict[str, object] = {
        "schema_version": "loom.execution-spec.v1",
        "recipe_digest": "sha256:" + "0" * 64,
        "run_graph_digest": "sha256:" + "1" * 64,
        "node_key": "prepare",
        "shard_key": "singleton",
        "container_node": {
            "node_kind": "container", "node_key": "prepare", "image": image,
            "argv": ["python", "-m", "approved.prepare"], "workdir": "/workspace",
            "resource_profile": "behavior-offline-none@1", "network_profile": "none",
            "needs": [],
            "inputs": [], "outputs": [], "request_renderer": None, "checkpoint": None,
            "fanout": None, "fanout_commit": None, "timeout_seconds": 120,
            "max_attempts": 2, "failure_policy": "fail_run",
        },
        "image_runtime_contract_digest": canonical_digest(runtime),
        "resource_profile_digest": canonical_digest(profile),
        "execution_variant_id": "cpu-data-x86_64",
        "gpu_backend_selection_sha256": None,
        "resolved_image_manifest_digest": "sha256:" + "3" * 64,
        "network_profile": "none",
        "resolved_input_bindings_digest": canonical_digest(bindings),
        "fanout_source_manifest_digest": None,
        "fanout_item_digest": None,
        "fanout_parameters_digest": None,
        "request_renderer_lock_digest": None,
        "control_binding_snapshots": [],
    }
    return spec, profile, [runtime]


async def test_attempt_and_trial_share_one_server_authoritative_slot(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    (
        team_id,
        worker_id,
        policy_id,
        activation_id,
        authority_id,
        job_row_id,
        run_id,
        stage_id,
        attempt_id,
    ) = (uuid4() for _ in range(9))
    token_hash = b"w" * 32
    capability_digest = "sha256:" + "c" * 64
    spec, profile, runtime_list = _contracts()
    runtime = runtime_list[0]
    async with sessions() as session, session.begin():
        await session.execute(
            text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"work-claim-{team_id}"},
        )
        await session.execute(
            text("INSERT INTO team_quotas(team_id) VALUES (:id)"), {"id": team_id}
        )
        await session.execute(
            text("""
                INSERT INTO workers (
                    id,hostname,version,capabilities,supported_work_kinds,
                    capability_snapshot_digest,capability_snapshot_json,
                    auth_token_hash,max_concurrent,pool_name,
                    registered_at,last_seen_at,status
                ) VALUES (
                    :id,'worker','test',CAST(:caps AS jsonb),
                    ARRAY['trial','execution_attempt']::text[], :capability,
                    CAST(:snapshot AS jsonb),:token,1,'behavior-cpu-data',
                    now(),now(),'active'
                )
            """),
            {
                "id": worker_id,
                "caps": '[{"os":"linux","gpu_vendor":"none",'
                '"network_policies":["public","no-network"]}]',
                "capability": capability_digest,
                "snapshot": __import__("json").dumps({
                    "schema_version": "loom.worker-capabilities.v1",
                    "cpu_arch": "x86_64",
                    "cpu_cores": 16,
                    "memory_bytes": 68_719_476_736,
                    "scratch_bytes": 824_633_720_832,
                    "network_profiles": ["none"],
                    "container_runtime_features": [],
                    "gpu_devices": [],
                    "input_cache_capacity_bytes": 1_649_267_441_664,
                    "input_cache_reserved_bytes": 0,
                    "input_cache_ready_bytes": 0,
                }),
                "token": token_hash,
            },
        )
        await session.execute(
            text("""
                INSERT INTO worker_pool_autoscaler_policies (
                    id,environment,pool_name,actuator,enabled,min_slots,max_slots,
                    actuator_config
                ) VALUES (
                    :id,'test','behavior-cpu-data','slurm',true,0,1,
                    jsonb_build_object(
                        'policy_id', 'behavior-cpu-data',
                        'policy_config_sha256', CAST(:digest AS text),
                        'slurm_cluster_config_sha256', CAST(:digest AS text),
                        'slurm_cluster_id', 'oldlab',
                        'allowed_nodes', jsonb_build_array('worker')
                    )
                )
            """),
            {"id": policy_id, "digest": "sha256:" + "d" * 64},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_scoped_policy_activations (
                    id,environment,policy_id,policy_config_sha256,authority_kind,
                    authority_id,activation_epoch,state,desired_slots
                ) VALUES (
                    :id,'test','behavior-cpu-data',:digest,'profile_calibration',
                    :authority,1,'active',1
                )
            """),
            {
                "id": activation_id,
                "digest": "sha256:" + "d" * 64,
                "authority": authority_id,
            },
        )
        await session.execute(
            text("""
                INSERT INTO slurm_worker_jobs (
                    id,slurm_cluster_id,environment,pool_name,nodelist,
                    requested_gpus,requested_concurrency,job_id,slurm_state,state,
                    worker_id,redacted_env
                ) VALUES (
                    :id,'oldlab','test','behavior-cpu-data','worker',0,1,
                    :job_id,'RUNNING','running',:worker,'{}'::jsonb
                )
            """),
            {"id": job_row_id, "job_id": str(job_row_id.int), "worker": worker_id},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key,
                    official_submission_kind,official_submission_authority_id,
                    official_submission_authority_snapshot_digest,
                    official_submission_identity_digest
                    ) VALUES (:id,:team,'ordinary','claim-test',1,:recipe,'{}'::jsonb,:graph,
                              '{}'::jsonb,:recipe,'[]'::jsonb,'{}'::jsonb,:recipe,:key,
                              'behavior_profile_calibration_run_v1',:authority,:recipe,:recipe)
                """),
            {
                "id": run_id,
                "team": team_id,
                "recipe": spec["recipe_digest"],
                "graph": spec["run_graph_digest"],
                "key": f"claim-{run_id}",
                "authority": authority_id,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_stage_runs (
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resolved_execution_spec_json,resolved_execution_spec_bytes,execution_spec_digest,
                    resource_profile_json,resource_profile_digest,
                    image_runtime_contract_json,image_runtime_contract_digest,
                    resolved_input_bindings_json,resolved_input_bindings_digest,
                    failure_policy,ready_at
                ) VALUES (
                    :id,:run,'prepare','singleton','container','queued',CAST(:spec AS jsonb),
                    :spec_bytes,:spec_digest,CAST(:profile AS jsonb),:profile_digest,
                    CAST(:runtime AS jsonb),:runtime_digest,'[]'::jsonb,:bindings_digest,
                    'fail_run',now()
                )
            """),
            {
                "id": stage_id,
                "run": run_id,
                "spec": __import__("json").dumps(spec),
                "spec_bytes": canonical_document(spec),
                "spec_digest": canonical_digest(spec),
                "profile": __import__("json").dumps(profile),
                "profile_digest": canonical_digest(profile),
                "runtime": __import__("json").dumps(runtime),
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
                ) VALUES (:run,1000,0,1000000,10,1,10,1,:deadline)
            """),
            {"run": run_id, "deadline": datetime.now(UTC) + timedelta(hours=1)},
        )
    async with sessions() as session:
        claimed = await claim_work(
            session,
            worker_id=worker_id,
            capability_snapshot_digest=capability_digest,
            worker_token_hash=token_hash,
            supported_work_kinds=["trial", "execution_attempt"],
            free_slots=1,
            worker_os=["linux"],
            worker_cpu_arches=["x86_64"],
            worker_gpu_vendors=["none"],
            worker_network_policies=["public"],
        )
        assert claimed is not None and claimed[0]["work_kind"] == "execution_attempt"
        assert claimed[1] is not None
        await session.commit()
    async with sessions() as session:
        with pytest.raises(WorkClaimConflictError, match="worker_capacity_exhausted"):
            await claim_work(
                session,
                worker_id=worker_id,
                capability_snapshot_digest=capability_digest,
                worker_token_hash=token_hash,
                supported_work_kinds=["trial", "execution_attempt"],
                free_slots=1,
                worker_os=["linux"],
                worker_cpu_arches=["x86_64"],
                worker_gpu_vendors=["none"],
                worker_network_policies=["public"],
            )
    async with engine.begin() as connection:
        await connection.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": run_id})
        await connection.execute(
            text("DELETE FROM slurm_worker_jobs WHERE id=:id"), {"id": job_row_id}
        )
        await connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
        await connection.execute(
            text("DELETE FROM pipeline_scoped_policy_activations WHERE id=:id"),
            {"id": activation_id},
        )
        await connection.execute(
            text("DELETE FROM worker_pool_autoscaler_policies WHERE id=:id"),
            {"id": policy_id},
        )
        await connection.execute(text("DELETE FROM team_quotas WHERE team_id=:id"), {"id": team_id})
        await connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
    await engine.dispose()
