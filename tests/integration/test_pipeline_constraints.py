"""Exercise Pipeline schema constraints against real PostgreSQL."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import IntegrityError


@dataclass(frozen=True)
class PipelineSeed:
    engine: Engine
    team_id: UUID
    other_team_id: UUID
    run_id: UUID
    other_run_id: UUID
    subject_stage_id: UUID
    downstream_stage_id: UUID
    other_stage_id: UUID
    source_artifact_id: UUID
    worker_id: UUID


def _insert_run_sql() -> str:
    return """
        INSERT INTO pipeline_runs (
            id, team_id, submission_policy, recipe_name, recipe_version, recipe_digest,
            graph_spec_json, graph_spec_digest, parameters_json, parameters_digest,
            resolved_inputs_json, budget_json, request_digest, idempotency_key
        ) VALUES (
            :id, :team_id, 'ordinary', 'pipeline-constraint-fixture', 1, :digest,
            '{}'::jsonb, :digest, '{}'::jsonb, :digest,
            '[]'::jsonb, '{}'::jsonb, :request_digest, :idempotency_key
        )
    """


def _insert_container_stage_sql() -> str:
    return """
        INSERT INTO pipeline_stage_runs (
            id, pipeline_run_id, node_key, shard_key, node_kind, state,
            resource_profile_json, resource_profile_digest, failure_policy
        ) VALUES (
            :id, :run_id, :node_key, :shard_key, 'container', :state,
            '{}'::jsonb, :digest, 'fail_run'
        )
    """


def _insert_retry_run_sql() -> str:
    return """
        INSERT INTO pipeline_runs (
            id, team_id, submission_policy, recipe_name, recipe_version, recipe_digest,
            graph_spec_json, graph_spec_digest, parameters_json, parameters_digest,
            resolved_inputs_json, budget_json, request_digest, idempotency_key,
            retry_of_pipeline_run_id, retry_from_stage_run_id
        ) VALUES (
            :id, :team_id, 'ordinary', 'pipeline-constraint-fixture', 1, :digest,
            '{}'::jsonb, :digest, '{}'::jsonb, :digest,
            '[]'::jsonb, '{}'::jsonb, :request_digest, :idempotency_key,
            :retry_of, :retry_from
        )
    """


@pytest.fixture
def pipeline_seed(postgres_url: str) -> Iterator[PipelineSeed]:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    other_team_id = uuid4()
    run_id = uuid4()
    other_run_id = uuid4()
    subject_stage_id = uuid4()
    downstream_stage_id = uuid4()
    other_stage_id = uuid4()
    source_artifact_id = uuid4()
    worker_id = uuid4()
    digest = "sha256:" + "a" * 64

    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            [
                {"id": team_id, "name": f"pipeline-constraints-{team_id}"},
                {"id": other_team_id, "name": f"pipeline-constraints-{other_team_id}"},
            ],
        )
        conn.execute(
            text(_insert_run_sql()),
            [
                {
                    "id": run_id,
                    "team_id": team_id,
                    "digest": digest,
                    "request_digest": digest,
                    "idempotency_key": f"run-{run_id}",
                },
                {
                    "id": other_run_id,
                    "team_id": other_team_id,
                    "digest": digest,
                    "request_digest": digest,
                    "idempotency_key": f"run-{other_run_id}",
                },
            ],
        )
        conn.execute(
            text(_insert_container_stage_sql()),
            [
                {
                    "id": subject_stage_id,
                    "run_id": run_id,
                    "node_key": "subject",
                    "shard_key": "singleton",
                    "state": "blocked",
                    "digest": digest,
                },
                {
                    "id": downstream_stage_id,
                    "run_id": run_id,
                    "node_key": "downstream",
                    "shard_key": "singleton",
                    "state": "blocked",
                    "digest": digest,
                },
                {
                    "id": other_stage_id,
                    "run_id": other_run_id,
                    "node_key": "subject",
                    "shard_key": "singleton",
                    "state": "blocked",
                    "digest": digest,
                },
            ],
        )
        conn.execute(
            text(
                "INSERT INTO artifacts "
                "(id, artifact_type, name, team_id, content_hash) "
                "VALUES (:id, 'fixture.manifest.v1', 'source-manifest', :team_id, :digest)"
            ),
            {"id": source_artifact_id, "team_id": team_id, "digest": digest},
        )
        conn.execute(
            text(
                "INSERT INTO workers "
                "(id, hostname, version, capabilities, registered_at, last_seen_at, status) "
                "VALUES (:id, :hostname, 'test', '[]'::jsonb, now(), now(), 'active')"
            ),
            {"id": worker_id, "hostname": f"pipeline-constraints-{worker_id}"},
        )

    seed = PipelineSeed(
        engine=engine,
        team_id=team_id,
        other_team_id=other_team_id,
        run_id=run_id,
        other_run_id=other_run_id,
        subject_stage_id=subject_stage_id,
        downstream_stage_id=downstream_stage_id,
        other_stage_id=other_stage_id,
        source_artifact_id=source_artifact_id,
        worker_id=worker_id,
    )
    try:
        yield seed
    finally:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM pipeline_runs "
                    "WHERE team_id IN (:team_id, :other_team_id) "
                    "AND retry_of_pipeline_run_id IS NOT NULL"
                ),
                {"team_id": team_id, "other_team_id": other_team_id},
            )
            conn.execute(
                text("DELETE FROM pipeline_runs WHERE id IN (:run_id, :other_run_id)"),
                {"run_id": run_id, "other_run_id": other_run_id},
            )
            conn.execute(
                text("DELETE FROM artifacts WHERE id = :id"),
                {"id": source_artifact_id},
            )
            conn.execute(text("DELETE FROM workers WHERE id = :id"), {"id": worker_id})
            conn.execute(
                text("DELETE FROM teams WHERE id IN (:team_id, :other_team_id)"),
                {"team_id": team_id, "other_team_id": other_team_id},
            )
        engine.dispose()


def _assert_rejected(engine: Engine, statement: str, parameters: dict[str, object]) -> None:
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(text(statement), parameters)


def test_pipeline_run_idempotency_and_retry_linkage(pipeline_seed: PipelineSeed) -> None:
    seed = pipeline_seed
    digest = "sha256:" + "b" * 64
    duplicate = {
        "id": uuid4(),
        "team_id": seed.team_id,
        "digest": digest,
        "request_digest": digest,
        "idempotency_key": f"run-{seed.run_id}",
    }
    _assert_rejected(seed.engine, _insert_run_sql(), duplicate)

    valid_retry_id = uuid4()
    with seed.engine.begin() as conn:
        conn.execute(
            text(_insert_retry_run_sql()),
            {
                "id": valid_retry_id,
                "team_id": seed.team_id,
                "digest": digest,
                "request_digest": digest,
                "idempotency_key": f"retry-{valid_retry_id}",
                "retry_of": seed.run_id,
                "retry_from": seed.subject_stage_id,
            },
        )

    invalid_retry_id = uuid4()
    _assert_rejected(
        seed.engine,
        _insert_retry_run_sql(),
        {
            "id": invalid_retry_id,
            "team_id": seed.team_id,
            "digest": digest,
            "request_digest": digest,
            "idempotency_key": f"retry-{invalid_retry_id}",
            "retry_of": seed.run_id,
            "retry_from": seed.other_stage_id,
        },
    )


def test_stage_identity_frozen_fields_and_gate_same_shard(pipeline_seed: PipelineSeed) -> None:
    seed = pipeline_seed
    digest = "sha256:" + "c" * 64
    _assert_rejected(
        seed.engine,
        _insert_container_stage_sql(),
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "node_key": "subject",
            "shard_key": "singleton",
            "state": "blocked",
            "digest": digest,
        },
    )
    _assert_rejected(
        seed.engine,
        _insert_container_stage_sql(),
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "node_key": "unfrozen",
            "shard_key": "singleton",
            "state": "ready",
            "digest": digest,
        },
    )

    gate_sql = """
        INSERT INTO pipeline_stage_runs (
            id, pipeline_run_id, node_key, shard_key, node_kind, state,
            gate_subject_stage_run_id
        ) VALUES (
            :id, :run_id, :node_key, :shard_key, 'gate', 'blocked', :subject_id
        )
    """
    with seed.engine.begin() as conn:
        conn.execute(
            text(gate_sql),
            {
                "id": uuid4(),
                "run_id": seed.run_id,
                "node_key": "same_shard_gate",
                "shard_key": "singleton",
                "subject_id": seed.subject_stage_id,
            },
        )
    _assert_rejected(
        seed.engine,
        gate_sql,
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "node_key": "cross_shard_gate",
            "shard_key": "shard-b",
            "subject_id": seed.subject_stage_id,
        },
    )


def test_attempt_event_dependency_and_fanout_uniqueness(pipeline_seed: PipelineSeed) -> None:
    seed = pipeline_seed
    attempt_id = uuid4()
    claim_id = uuid4()
    with seed.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO execution_attempts "
                "(id, stage_run_id, attempt_number, state, worker_id, claim_id, "
                " lease_token_digest, lease_expires_at, queued_at) "
                "VALUES (:id, :stage_id, 1, 'claimed', :worker_id, :claim_id, "
                "        :digest, now() + interval '5 minutes', now())"
            ),
            {
                "id": attempt_id,
                "stage_id": seed.subject_stage_id,
                "worker_id": seed.worker_id,
                "claim_id": claim_id,
                "digest": "sha256:" + "d" * 64,
            },
        )
        conn.execute(
            text(
                "INSERT INTO pipeline_events "
                "(pipeline_run_id, seq, stage_run_id, execution_attempt_id, "
                " event_type, actor_kind, payload_json) "
                "VALUES (:run_id, 1, :stage_id, :attempt_id, 'attempt_queued', "
                "        'controller', '{}'::jsonb)"
            ),
            {
                "run_id": seed.run_id,
                "stage_id": seed.subject_stage_id,
                "attempt_id": attempt_id,
            },
        )
        conn.execute(
            text(
                "INSERT INTO pipeline_stage_dependencies "
                "(pipeline_run_id, upstream_stage_run_id, downstream_stage_run_id, "
                " dependency_kind) VALUES (:run_id, :upstream, :downstream, 'required')"
            ),
            {
                "run_id": seed.run_id,
                "upstream": seed.subject_stage_id,
                "downstream": seed.downstream_stage_id,
            },
        )
        conn.execute(
            text(
                "INSERT INTO pipeline_fanout_expansions "
                "(id, pipeline_run_id, node_key, source_kind, source_artifact_id, "
                " source_manifest_digest, fanout_spec_digest, item_count) "
                "VALUES (:id, :run_id, 'fanout', 'run_input', :artifact_id, "
                "        :digest, :digest, 0)"
            ),
            {
                "id": uuid4(),
                "run_id": seed.run_id,
                "artifact_id": seed.source_artifact_id,
                "digest": "sha256:" + "d" * 64,
            },
        )

    _assert_rejected(
        seed.engine,
        "INSERT INTO execution_attempts "
        "(id, stage_run_id, attempt_number, state, queued_at) "
        "VALUES (:id, :stage_id, 1, 'queued', now())",
        {"id": uuid4(), "stage_id": seed.subject_stage_id},
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO execution_attempts "
        "(id, stage_run_id, attempt_number, state, worker_id, claim_id, "
        " lease_token_digest, lease_expires_at, queued_at) "
        "VALUES (:id, :stage_id, 1, 'claimed', :worker_id, :claim_id, "
        "        :digest, now() + interval '5 minutes', now())",
        {
            "id": uuid4(),
            "stage_id": seed.downstream_stage_id,
            "worker_id": seed.worker_id,
            "claim_id": claim_id,
            "digest": "sha256:" + "d" * 64,
        },
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_events "
        "(pipeline_run_id, seq, event_type, actor_kind, payload_json) "
        "VALUES (:run_id, 1, 'duplicate', 'controller', '{}'::jsonb)",
        {"run_id": seed.run_id},
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_stage_dependencies "
        "(pipeline_run_id, upstream_stage_run_id, downstream_stage_run_id, dependency_kind) "
        "VALUES (:run_id, :upstream, :downstream, 'required')",
        {
            "run_id": seed.run_id,
            "upstream": seed.subject_stage_id,
            "downstream": seed.downstream_stage_id,
        },
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_stage_dependencies "
        "(pipeline_run_id, upstream_stage_run_id, downstream_stage_run_id, dependency_kind) "
        "VALUES (:run_id, :upstream, :downstream, 'terminal_barrier')",
        {
            "run_id": seed.run_id,
            "upstream": seed.other_stage_id,
            "downstream": seed.downstream_stage_id,
        },
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_fanout_expansions "
        "(id, pipeline_run_id, node_key, source_kind, source_artifact_id, "
        " source_manifest_digest, fanout_spec_digest, item_count) "
        "VALUES (:id, :run_id, 'fanout', 'run_input', :artifact_id, :digest, :digest, 0)",
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "artifact_id": seed.source_artifact_id,
            "digest": "sha256:" + "e" * 64,
        },
    )


def test_artifact_pipeline_producer_output_unique_per_stage(pipeline_seed: PipelineSeed) -> None:
    seed = pipeline_seed
    digest = "sha256:" + "f" * 64
    artifact_sql = """
        INSERT INTO artifacts (
            id, artifact_type, name, team_id, content_hash,
            pipeline_run_id, pipeline_stage_run_id, execution_attempt_id, producer_kind
        ) VALUES (
            :id, 'fixture.output.v1', :name, :team_id, :digest,
            :run_id, :stage_id, :attempt_id, :producer_kind
        )
    """
    attempt_id = uuid4()
    with seed.engine.begin() as conn:
        conn.execute(
            text(
                    "INSERT INTO execution_attempts "
                    "(id, stage_run_id, attempt_number, state, queued_at) "
                    "VALUES (:id, :stage_id, 1, 'queued', now())"
            ),
            {"id": attempt_id, "stage_id": seed.subject_stage_id},
        )
        conn.execute(
            text(artifact_sql),
            {
                "id": uuid4(),
                "name": "result",
                "team_id": seed.team_id,
                "digest": digest,
                "run_id": seed.run_id,
                "stage_id": seed.subject_stage_id,
                "attempt_id": attempt_id,
                "producer_kind": "container",
            },
        )

    _assert_rejected(
        seed.engine,
        artifact_sql,
        {
            "id": uuid4(),
            "name": "result",
            "team_id": seed.team_id,
            "digest": digest,
            "run_id": seed.run_id,
            "stage_id": seed.subject_stage_id,
            "attempt_id": attempt_id,
            "producer_kind": "platform",
        },
    )
    with seed.engine.begin() as conn:
        conn.execute(
            text(artifact_sql),
            {
                "id": uuid4(),
                "name": "checkpoint-000000000001",
                "team_id": seed.team_id,
                "digest": digest,
                "run_id": seed.run_id,
                "stage_id": seed.subject_stage_id,
                "attempt_id": attempt_id,
                "producer_kind": "checkpoint",
            },
        )


def test_gate_attempt_fanout_limits_and_acceptance_prerequisite_fences(
    pipeline_seed: PipelineSeed,
) -> None:
    seed = pipeline_seed
    digest = "sha256:" + "9" * 64
    gate_id = uuid4()
    with seed.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_stage_runs "
                "(id, pipeline_run_id, node_key, shard_key, node_kind, state, "
                " gate_subject_stage_run_id) "
                "VALUES (:id, :run_id, 'gate', 'singleton', 'gate', 'blocked', :subject)"
            ),
            {"id": gate_id, "run_id": seed.run_id, "subject": seed.subject_stage_id},
        )

    _assert_rejected(
        seed.engine,
        "INSERT INTO execution_attempts "
        "(id, stage_run_id, attempt_number, state, queued_at) "
        "VALUES (:id, :stage_id, 1, 'queued', now())",
        {"id": uuid4(), "stage_id": gate_id},
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_stage_runs "
        "(id, pipeline_run_id, node_key, shard_key, node_kind, state, "
        " gate_subject_stage_run_id) "
        "VALUES (:id, :run_id, 'gate_two', 'singleton', 'gate', 'blocked', :subject)",
        {"id": uuid4(), "run_id": seed.run_id, "subject": gate_id},
    )
    _assert_rejected(
        seed.engine,
        _insert_container_stage_sql().replace(
            "failure_policy\n        )", "failure_policy, attempt_count\n        )"
        ).replace(
            "'fail_run'\n        )", "'fail_run', 4\n        )"
        ),
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "node_key": "too_many_attempts",
            "shard_key": "singleton",
            "state": "blocked",
            "digest": digest,
        },
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_fanout_expansions "
        "(id, pipeline_run_id, node_key, source_kind, source_artifact_id, "
        " source_manifest_digest, fanout_spec_digest, item_count) "
        "VALUES (:id, :run_id, 'too_many', 'run_input', :artifact, :digest, :digest, 5001)",
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "artifact": seed.source_artifact_id,
            "digest": digest,
        },
    )
    _assert_rejected(
        seed.engine,
        "INSERT INTO pipeline_fanout_expansions "
        "(id, pipeline_run_id, node_key, source_kind, source_stage_run_id, "
        " source_artifact_id, source_manifest_digest, fanout_spec_digest, item_count) "
        "VALUES (:id, :run_id, 'wrong_source', 'stage_output', :stage_id, "
        " :artifact, :digest, :digest, 0)",
        {
            "id": uuid4(),
            "run_id": seed.run_id,
            "stage_id": seed.subject_stage_id,
            "artifact": seed.source_artifact_id,
            "digest": digest,
        },
    )

    acceptance_id = uuid4()
    authorization_id = uuid4()
    with seed.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pipeline_runs ("
                "id, team_id, submission_policy, acceptance_authorization_id, "
                "acceptance_candidate_sha256, recipe_name, recipe_version, recipe_digest, "
                "graph_spec_json, graph_spec_digest, parameters_json, parameters_digest, "
                "resolved_inputs_json, budget_json, request_digest, idempotency_key) "
                "VALUES (:id, :team_id, 'acceptance_authorization_only', :authorization_id, "
                ":digest, 'behavior-recovery-acceptance-preflight', 1, :digest, "
                "'{}'::jsonb, :digest, '{}'::jsonb, :digest, '[]'::jsonb, '{}'::jsonb, "
                ":digest, :key)"
            ),
            {
                "id": acceptance_id,
                "team_id": seed.team_id,
                "authorization_id": authorization_id,
                "digest": digest,
                "key": f"acceptance-{acceptance_id}",
            },
        )
        conn.execute(
            text(
                "INSERT INTO pipeline_acceptance_preflight_prerequisites ("
                "pipeline_run_id, authorization_id, candidate_sha256, "
                "sealed_input_descriptor_set_sha256) "
                "VALUES (:run_id, :authorization_id, :digest, :digest)"
            ),
            {
                "run_id": acceptance_id,
                "authorization_id": authorization_id,
                "digest": digest,
            },
        )

    _assert_rejected(
        seed.engine,
        "UPDATE pipeline_acceptance_preflight_prerequisites "
        "SET worker_id = :worker_id WHERE pipeline_run_id = :run_id",
        {"worker_id": seed.worker_id, "run_id": acceptance_id},
    )
    _assert_rejected(
        seed.engine,
        "DELETE FROM pipeline_acceptance_preflight_prerequisites WHERE pipeline_run_id = :run_id",
        {"run_id": acceptance_id},
    )
    with seed.engine.begin() as conn:
        conn.execute(
            text("DELETE FROM pipeline_runs WHERE id = :run_id"),
            {"run_id": acceptance_id},
        )
