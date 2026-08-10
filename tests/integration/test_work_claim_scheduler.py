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
        "name": "cpu_small",
        "version": 1,
        "execution_variants": [{
            "variant_id": "linux_amd64", "cpu_arch": "x86_64", "gpu_count_exact": 0,
            "gpu_vendor": "none", "allowed_gpu_models": [], "gpu_memory_kind": "none",
            "gpu_memory_mb_min": 0, "gpu_unified_memory_mb_min": 0,
            "memory_accounting_kind": "container", "container_memory_bytes_override": None,
            "same_gpu_model_required": False, "pool_class": "cpu", "device_roles": None,
        }],
        "cpu_cores": 2,
        "memory_bytes": 1_073_741_824,
        "scratch_bytes": 1_073_741_824,
        "timeout_seconds_max": 120,
        "required_host_runtime_features": ["docker-v1"],
        "required_image_features": ["nonroot-v1"],
        "network_profile": "none",
        "input_cache_capacity_bytes_min": 0,
    }
    runtime: dict[str, object] = {
        "image_index_digest": image,
        "platform": "linux/amd64",
        "platform_manifest_digest": "sha256:" + "3" * 64,
        "cpu_arch": "x86_64",
        "gpu_vendor": "none",
        "cuda_userspace_version": None,
        "min_nvidia_driver_version": None,
        "application_features": ["nonroot-v1"],
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
            "resource_profile": "cpu_small@1", "network_profile": "none", "needs": [],
            "inputs": [], "outputs": [], "request_renderer": None, "checkpoint": None,
            "fanout": None, "fanout_commit": None, "timeout_seconds": 120,
            "max_attempts": 2, "failure_policy": "fail_run",
        },
        "image_runtime_contract_digest": canonical_digest(runtime),
        "resource_profile_digest": canonical_digest(profile),
        "execution_variant_id": None,
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
    team_id, worker_id, run_id, stage_id, attempt_id = (
        uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    )
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
                    capability_snapshot_digest,auth_token_hash,max_concurrent,
                    registered_at,last_seen_at,status
                ) VALUES (
                    :id,'worker','test',CAST(:caps AS jsonb),
                    ARRAY['trial','execution_attempt']::text[], :capability,:token,1,
                    now(),now(),'active'
                )
            """),
            {
                "id": worker_id,
                "caps": '[{"os":"linux","gpu_vendor":"none",'
                '"network_policies":["public","no-network"]}]',
                "capability": capability_digest,
                "token": token_hash,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key
                ) VALUES (:id,:team,'ordinary','claim-test',1,:recipe,'{}'::jsonb,:graph,
                          '{}'::jsonb,:recipe,'[]'::jsonb,'{}'::jsonb,:recipe,:key)
            """),
            {
                "id": run_id,
                "team": team_id,
                "recipe": spec["recipe_digest"],
                "graph": spec["run_graph_digest"],
                "key": f"claim-{run_id}",
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
        await connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
        await connection.execute(text("DELETE FROM team_quotas WHERE team_id=:id"), {"id": team_id})
        await connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})
    await engine.dispose()
