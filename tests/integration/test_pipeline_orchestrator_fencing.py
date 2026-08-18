from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import FanoutManifestV1, RunGraphSpecV1
from loom_pipeline_orchestrator.repository import StaleControllerLeaseError

if TYPE_CHECKING:
    from tests.integration.pipeline_orchestrator_fixtures import OrchestratorSeed


@pytest.mark.asyncio
async def test_two_controllers_claim_one_run_and_epoch_fences_stale_writer(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    first, second = await asyncio.gather(
        seed.repository.claim_runs(controller_id="controller-a"),
        seed.repository.claim_runs(controller_id="controller-b"),
    )
    leases = [*first, *second]
    assert len(leases) == 1
    lease = leases[0]
    assert lease.lease_epoch == 1

    stale = type(lease)(
        pipeline_run_id=lease.pipeline_run_id,
        claimed_by=lease.claimed_by,
        lease_epoch=lease.lease_epoch + 1,
        lease_expires_at=lease.lease_expires_at,
    )
    with pytest.raises(StaleControllerLeaseError):
        await seed.repository.release(stale)
    await seed.repository.release(lease)


@pytest.mark.asyncio
@pytest.mark.parametrize("item_count", [0, 1, 3])
async def test_atomic_zero_one_many_fanout_and_mirrored_gates(
    orchestrator_seed: OrchestratorSeed,
    item_count: int,
) -> None:
    from tests.integration.pipeline_orchestrator_fixtures import DIGEST, container

    seed = orchestrator_seed
    source_id = uuid4()
    item_ids = [uuid4() for _ in range(item_count)]
    node = container("fanout")
    node["inputs"] = [
        {
            "source": "fanout_item",
            "binding_name": "current_case",
            "artifact_type": "behavior.case.v1",
        }
    ]
    node["fanout"] = {
        "source": "run_input",
        "manifest_input_name": "cases",
        "items_pointer": "/items",
        "shard_key_pointer": "/shard_key",
        "item_binding_name": "current_case",
        "item_artifact_type": "behavior.case.v1",
        "parameters_contract": {"name": "case_params", "version": 1, "digest": DIGEST},
        "max_items": 3,
    }
    node["outputs"] = [
        {
            "name": "result",
            "artifact_type": "behavior.result.v1",
            "required": True,
            "role": "artifact",
            "producer": "container",
            "max_bytes": 1024,
        }
    ]
    finalize = container("finalize", needs=["fanout"])
    finalize["inputs"] = [
        {
            "source": "terminal_outputs",
            "binding_name": "results",
            "artifact_type": "behavior.result.v1",
            "stage_keys": ["fanout"],
            "output_name": "result",
            "match_outcomes": ["selected"],
        }
    ]
    finalize["request_renderer"] = {
        "name": "fanout_finalize",
        "version": 1,
        "digest": DIGEST,
        "max_bytes": 65_536,
        "terminal_stage_keys": ["fanout"],
    }
    graph_value = {
        "schema_version": "loom.run-graph.v1",
        "recipe": {"name": "fanout-fixture", "version": 1, "digest": DIGEST},
        "inputs": [{"name": "cases", "artifact_type": "loom.fanout-manifest.v1", "required": True}],
        "parameters": {},
        "budget": {
            "max_provider_cost_usd": "0",
            "max_gpu_seconds": 0,
            "max_wall_seconds": 600,
            "max_artifact_bytes": 1000,
            "max_stage_runs": 10,
            "max_attempts_total": 10,
        },
        "nodes": [
            node,
            {
                "node_kind": "gate",
                "gate_kind": "outcome",
                "node_key": "route",
                "shard_mode": "subject",
                "needs": ["fanout"],
                "subject_stage_key": "fanout",
                "match_outcomes": ["selected"],
                "matched_targets": [],
                "unmatched_targets": [],
            },
            finalize,
        ],
    }
    graph = RunGraphSpecV1.model_validate(graph_value)
    items = [
        {
            "artifact_bindings": [
                {
                    "artifact_id": item_id,
                    "artifact_type": "behavior.case.v1",
                    "name": "current_case",
                }
            ],
            "parameters": {"index": index},
            "shard_key": f"s{index:02d}",
        }
        for index, item_id in enumerate(item_ids)
    ]
    manifest = FanoutManifestV1.model_validate(
        {"schema_version": "loom.fanout-manifest.v1", "items": items}
    )
    manifest_digest = canonical_digest(manifest)
    async with seed.sessions() as session, session.begin():
        await session.execute(
            text("""
                UPDATE pipeline_runs SET recipe_name='fanout-fixture',
                    graph_spec_json=CAST(:graph AS jsonb), graph_spec_digest=:graph_digest,
                    budget_json=CAST(:budget AS jsonb)
                 WHERE id=:run_id
            """),
            {
                "run_id": seed.run_id,
                "graph": json.dumps(graph.model_dump(mode="json", exclude_none=False)),
                "graph_digest": canonical_digest(graph),
                "budget": json.dumps(graph.model_dump(mode="json")["budget"]),
            },
        )
        await session.execute(
            text("""
                INSERT INTO artifacts (id,artifact_type,name,team_id,content_hash)
                VALUES (:id,'loom.fanout-manifest.v1','cases',:team,:digest)
            """),
            {"id": source_id, "team": seed.team_id, "digest": manifest_digest},
        )
        for index, item_id in enumerate(item_ids):
            await session.execute(
                text("""
                    INSERT INTO artifacts (id,artifact_type,name,team_id,content_hash)
                    VALUES (:id,'behavior.case.v1',:name,:team,:digest)
                """),
                {
                    "id": item_id,
                    "name": f"case-{index}",
                    "team": seed.team_id,
                    "digest": DIGEST,
                },
            )
    lease = (await seed.repository.claim_runs(controller_id="fanout-controller"))[0]
    try:
        assert await seed.repository.initialize_run(lease) == 1
        assert await seed.repository.expand_fanout(
            lease,
            node_key="fanout",
            source_kind="run_input",
            source_artifact_id=source_id,
            source_manifest_digest=manifest_digest,
            manifest=manifest,
            run_input_parameters_validated=True,
        ) == item_count * 2
        assert await seed.repository.expand_fanout(
            lease,
            node_key="fanout",
            source_kind="run_input",
            source_artifact_id=source_id,
            source_manifest_digest=manifest_digest,
            manifest=manifest,
            run_input_parameters_validated=True,
        ) == 0
        async with seed.sessions() as session:
            values = (
                await session.execute(
                    text("""
                        SELECT (SELECT count(*) FROM pipeline_fanout_expansions
                                 WHERE pipeline_run_id=:id),
                               (SELECT count(*) FROM pipeline_stage_runs
                                 WHERE pipeline_run_id=:id),
                               (SELECT stage_runs_created FROM pipeline_budget_ledgers
                                 WHERE pipeline_run_id=:id),
                               (SELECT count(*) FROM pipeline_stage_dependencies d
                                  JOIN pipeline_stage_runs downstream
                                    ON downstream.id=d.downstream_stage_run_id
                                 WHERE d.pipeline_run_id=:id
                                   AND downstream.node_key='finalize'
                                   AND d.dependency_kind='terminal_barrier')
                    """),
                    {"id": seed.run_id},
                )
            ).one()
        assert values == (1, item_count * 2 + 1, item_count * 2 + 1, item_count)
    finally:
        await seed.repository.release(lease)


@pytest.mark.asyncio
async def test_initialization_is_idempotent_and_counts_static_rows_once(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    lease = (await seed.repository.claim_runs(controller_id="controller-a"))[0]
    assert await seed.repository.initialize_run(lease) == 1
    assert await seed.repository.initialize_run(lease) == 0
    async with seed.sessions() as session:
        counts = (
            await session.execute(
                text("""
                    SELECT (SELECT count(*) FROM pipeline_stage_runs WHERE pipeline_run_id=:id),
                           (SELECT stage_runs_created FROM pipeline_budget_ledgers
                             WHERE pipeline_run_id=:id),
                           (SELECT count(*) FROM pipeline_events WHERE pipeline_run_id=:id)
                """),
                {"id": seed.run_id},
            )
        ).one()
    assert counts == (1, 1, 1)
    await seed.repository.release(lease)
