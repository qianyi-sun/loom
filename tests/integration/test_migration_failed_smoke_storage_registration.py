"""Migration 0110 registers exact legacy-bucket objects from one failed smoke batch."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, Engine, create_engine, text

TEAM_ID = "bbce1c49-8d6b-429c-a338-de37a6b533b7"
BATCH_ID = "5c79cb94-4892-4e73-9c72-b4276eb4f440"
RUN_AUTHORITY_ID = "0572462f-a7f0-4baa-aee6-a9a63683b407"
UNRELATED_AUTHORITY_ID = "11111111-1111-4111-8111-111111111110"
TASK_ID = "loom-smoke/gb10-oracle-hello-world"


@dataclass(frozen=True)
class Trial:
    trial_id: str
    trial_authority_id: str
    event_authority_id: str
    submitted_at: str
    finished_at: str
    sample_idx: int
    autoscaler_pool_name: str | None
    autoscaler_pool_assigned_at: str | None


TRIALS = (
    Trial(
        trial_id="9f71f6c2-4979-48d2-a7a6-2690622786c4",
        trial_authority_id="05189a12-6e53-4b89-ae9f-26a14863225d",
        event_authority_id="c87781df-dda6-423e-861f-9d6c439c96f0",
        submitted_at="2026-08-25T14:05:19.391987+00:00",
        finished_at="2026-08-25T14:45:04.723460+00:00",
        sample_idx=0,
        autoscaler_pool_name="gb10",
        autoscaler_pool_assigned_at="2026-08-25T14:05:44.346889+00:00",
    ),
    Trial(
        trial_id="b321c1fa-63b5-4187-9a9a-2d6e43495846",
        trial_authority_id="b977ff87-446d-4bed-ab30-402acc3cfe8c",
        event_authority_id="ac508ce0-825d-4803-b15f-acf450a9e4e4",
        submitted_at="2026-08-25T14:05:19.535689+00:00",
        finished_at="2026-08-25T14:46:10.889995+00:00",
        sample_idx=1,
        autoscaler_pool_name=None,
        autoscaler_pool_assigned_at=None,
    ),
)


@dataclass(frozen=True)
class LifecycleObject:
    trial_id: str
    bucket: str
    suffix: str
    content_sha256: str
    size_bytes: int
    created_at: str

    @property
    def object_key(self) -> str:
        return f"{TEAM_ID}/{self.trial_id}/{self.suffix}"

    @property
    def authority_id(self) -> str:
        return next(trial.trial_authority_id for trial in TRIALS if trial.trial_id == self.trial_id)


OBJECTS = (
    LifecycleObject(
        TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "80165612bc8ec1f98ef4e298e1e5316f8b648acef313f1a1bdfb545b174a8a72",
        704,
        "2026-08-25T14:06:47+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "799c084917dea6bfadc6bb05abf6def1f2d82deb50390ff7277b475136f65a23",
        1282,
        "2026-08-25T14:06:47+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "24708329dc23ac1d9a746503c1c2116c3110a056ceae69f0468797d9de5eda56",
        704,
        "2026-08-25T14:19:47+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "215ceebb90aad3f7a6e8564c59d65b9d7b15859d82b3d148f4e637a9c8f5084e",
        1283,
        "2026-08-25T14:19:47+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "24708329dc23ac1d9a746503c1c2116c3110a056ceae69f0468797d9de5eda56",
        704,
        "2026-08-25T14:32:37+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "eb2b8d768d4c05ab8a7eb5e9aa011d45231a735bf980c891740009935747d8bc",
        1282,
        "2026-08-25T14:32:37+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "b56416cb3a60a8fbdd4eaab778902b7014d9dc34aec0bbc8956a4fc4d45b0226",
        336,
        "2026-08-25T14:32:37+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T14:32:37+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "6b7906963347db7cb48ee1ec94c8610cfca6b9bd64dd7a535c2747bcb136ab45",
        328,
        "2026-08-25T14:32:37+00:00",
    ),
    LifecycleObject(
        TRIALS[0].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T14:32:37+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "c79c95cc26bf7ea9c21114eb2e8af21399e5174f03e6d0472df976c5a036d240",
        704,
        "2026-08-25T14:06:50+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "7fde081ad9ee853e8621c709748fea35a58468b99874da848eab6fc6677aa8b0",
        1282,
        "2026-08-25T14:06:49+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "ab6b296452e4390ef4c65eddf62886366099f465ca8aef45f28f21d2b12941aa",
        704,
        "2026-08-25T14:20:40+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "5e1fe2ce09c061a0763d7199f811d57af3fbb189ec059ef71fd2ccaa8cb2aea3",
        1283,
        "2026-08-25T14:20:40+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "ab6b296452e4390ef4c65eddf62886366099f465ca8aef45f28f21d2b12941aa",
        704,
        "2026-08-25T14:33:48+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "ef570b6e0a73a97ebc8e399e57a0329a700fc64df78cf417077b6eebda0fce2f",
        1282,
        "2026-08-25T14:33:48+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "2b1d33d7dc80387db9d8198df1711d33c24252d8d2cf6bb8f7fcce23a3b151da",
        336,
        "2026-08-25T14:33:48+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T14:33:48+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "c8c3a65969413e8f71ab6d3e7818998b4169ceac23ed499b88a0079faf9b45c2",
        327,
        "2026-08-25T14:33:48+00:00",
    ),
    LifecycleObject(
        TRIALS[1].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T14:33:48+00:00",
    ),
)


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def _at_0109(postgres_url: str) -> tuple[Config, Engine]:
    config = _config(postgres_url)
    command.downgrade(config, "0109")
    return config, create_engine(postgres_url)


def _authority_values(
    *,
    authority_id: str,
    data_class: str,
    owner_kind: str,
    owner_id: str,
    created_at: str,
) -> dict[str, object]:
    created = datetime.fromisoformat(created_at)
    return {
        "id": authority_id,
        "team_id": TEAM_ID,
        "data_class": data_class,
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "created_at": created_at,
        "expires_at": (created + timedelta(days=7)).isoformat(),
    }


def _insert_authority(connection: Connection, values: dict[str, object]) -> None:
    connection.execute(
        text(
            """
            INSERT INTO data_lifecycle_authorities (
                id, environment, namespace, team_id, data_class, owner_kind,
                owner_id, created_at, expires_at, pinned, state, deletion_token,
                metadata
            ) VALUES (
                CAST(:id AS uuid), 'staging', 'loom-staging', CAST(:team_id AS uuid),
                :data_class, :owner_kind, :owner_id,
                CAST(:created_at AS timestamptz), CAST(:expires_at AS timestamptz),
                false, 'active', NULL, '{}'::jsonb
            )
            """
        ),
        values,
    )


def _seed_prerepair_state(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, '0110-smoke-repair-fixture')"),
            {"id": TEAM_ID},
        )
        _insert_authority(
            connection,
            _authority_values(
                authority_id=RUN_AUTHORITY_ID,
                data_class="run",
                owner_kind="batch",
                owner_id=BATCH_ID,
                created_at="2026-08-25T14:05:19.221168+00:00",
            ),
        )
        for trial in TRIALS:
            _insert_authority(
                connection,
                _authority_values(
                    authority_id=trial.trial_authority_id,
                    data_class="trial",
                    owner_kind="trial",
                    owner_id=trial.trial_id,
                    created_at=trial.submitted_at,
                ),
            )
            _insert_authority(
                connection,
                _authority_values(
                    authority_id=trial.event_authority_id,
                    data_class="event",
                    owner_kind="trial",
                    owner_id=trial.trial_id,
                    created_at=trial.submitted_at,
                ),
            )
        connection.execute(
            text(
                """
                INSERT INTO batches (
                    id, team_id, name, task_filter, trial_config, state,
                    created_at, finished_at, created_by_token_prefix,
                    expected_trial_count, result_status, required_worker_pools,
                    lifecycle_authority_id
                ) VALUES (
                    CAST(:id AS uuid), CAST(:team_id AS uuid), :name,
                    '{}'::jsonb, '{}'::jsonb, 'finished',
                    CAST(:created_at AS timestamptz), CAST(:finished_at AS timestamptz),
                    'migration-0110', 2, 'all_failed', '["gb10"]'::jsonb,
                    CAST(:authority_id AS uuid)
                )
                """
            ),
            {
                "id": BATCH_ID,
                "team_id": TEAM_ID,
                "name": "rollout-538c01ce68b24be6-1",
                "created_at": "2026-08-25T14:05:19.221168+00:00",
                "finished_at": "2026-08-25T14:46:12.077020+00:00",
                "authority_id": RUN_AUTHORITY_ID,
            },
        )
        connection.execute(
            text(
                "INSERT INTO tasks (id, config, checksum) "
                "VALUES (:id, '{}'::jsonb, 'sha256:migration-0110')"
            ),
            {"id": TASK_ID},
        )
        for trial in TRIALS:
            connection.execute(
                text(
                    """
                    INSERT INTO trials (
                        id, team_id, task_id, config, requires_caps, state,
                        failure_reason, submit_priority, submitted_at, finished_at,
                        attempt_count, batch_id, sample_idx, autoscaler_pool_name,
                        autoscaler_pool_assigned_at,
                        lifecycle_authority_id
                    ) VALUES (
                        CAST(:id AS uuid), CAST(:team_id AS uuid), :task_id,
                        '{}'::jsonb, '{}'::jsonb, 'failed', 'retry_exhausted', 100,
                        CAST(:submitted_at AS timestamptz),
                        CAST(:finished_at AS timestamptz), 3, CAST(:batch_id AS uuid),
                        :sample_idx, :autoscaler_pool_name,
                        CAST(:autoscaler_pool_assigned_at AS timestamptz),
                        CAST(:authority_id AS uuid)
                    )
                    """
                ),
                {
                    "id": trial.trial_id,
                    "team_id": TEAM_ID,
                    "task_id": TASK_ID,
                    "submitted_at": trial.submitted_at,
                    "finished_at": trial.finished_at,
                    "batch_id": BATCH_ID,
                    "sample_idx": trial.sample_idx,
                    "autoscaler_pool_name": trial.autoscaler_pool_name,
                    "autoscaler_pool_assigned_at": trial.autoscaler_pool_assigned_at,
                    "authority_id": trial.trial_authority_id,
                },
            )


ObjectSnapshot = tuple[str, str, str, str | None, str, int, str, str, None, None]


def _object_snapshot(engine: Engine) -> tuple[ObjectSnapshot, ...]:
    with engine.connect() as connection:
        prefixes = [f"{TEAM_ID}/{trial.trial_id}/%" for trial in TRIALS]
        rows = connection.execute(
            text(
                """
                SELECT authority_id::text, bucket, object_key, version_id,
                       content_sha256, size_bytes, created_at, state,
                       deletion_token, verified_deleted_at
                  FROM data_lifecycle_objects
                 WHERE authority_id = ANY(CAST(:authority_ids AS uuid[]))
                    OR (bucket IN ('artifacts','trajectories')
                        AND (object_key LIKE :first_prefix
                             OR object_key LIKE :second_prefix))
                 ORDER BY bucket, object_key, COALESCE(version_id, '')
                """
            ),
            {
                "authority_ids": [trial.trial_authority_id for trial in TRIALS],
                "first_prefix": prefixes[0],
                "second_prefix": prefixes[1],
            },
        )
        return tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]) if row[3] is not None else None,
                str(row[4]),
                int(row[5]),
                row[6].isoformat(),
                str(row[7]),
                row[8],
                row[9],
            )
            for row in rows
        )


def _expected_object_snapshot() -> tuple[ObjectSnapshot, ...]:
    rows = (
        (
            item.authority_id,
            item.bucket,
            item.object_key,
            None,
            item.content_sha256,
            item.size_bytes,
            item.created_at,
            "active",
            None,
            None,
        )
        for item in OBJECTS
    )
    return tuple(sorted(rows, key=lambda row: (row[1], row[2], row[3] or "")))


def _insert_object(
    connection: Connection,
    item: LifecycleObject,
    *,
    authority_id: str | None = None,
) -> str:
    return str(
        connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_objects (
                    authority_id, environment, namespace, bucket, object_key,
                    version_id, content_sha256, size_bytes, created_at, state,
                    deletion_token, verified_deleted_at
                ) VALUES (
                    CAST(:authority_id AS uuid), 'staging', 'loom-staging',
                    :bucket, :object_key, NULL, :content_sha256, :size_bytes,
                    CAST(:created_at AS timestamptz), 'active', NULL, NULL
                ) RETURNING id
                """
            ),
            {
                "authority_id": authority_id or item.authority_id,
                "bucket": item.bucket,
                "object_key": item.object_key,
                "content_sha256": item.content_sha256,
                "size_bytes": item.size_bytes,
                "created_at": item.created_at,
            },
        ).scalar_one()
    )


def _seed_registered_objects(engine: Engine) -> list[str]:
    with engine.begin() as connection:
        return [_insert_object(connection, item) for item in OBJECTS]


def _revision(engine: Engine) -> str:
    with engine.connect() as connection:
        return str(connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one())


def test_0110_is_a_noop_when_none_of_the_target_identities_exist(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0109(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0110")
        assert _revision(engine) == "0110"
        assert _object_snapshot(engine) == ()
    finally:
        engine.dispose()


def test_0110_registers_the_exact_twenty_legacy_bucket_objects(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0109(isolated_migration_postgres_url)
    try:
        _seed_prerepair_state(engine)
        assert _object_snapshot(engine) == ()
        command.upgrade(config, "0110")
        assert _revision(engine) == "0110"
        assert _object_snapshot(engine) == _expected_object_snapshot()
    finally:
        engine.dispose()


def test_0110_accepts_the_exact_registered_state_idempotently(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0109(isolated_migration_postgres_url)
    try:
        _seed_prerepair_state(engine)
        command.upgrade(config, "0110")
        before = _object_snapshot(engine)
        command.stamp(config, "0109")
        command.upgrade(config, "0110")
        assert _revision(engine) == "0110"
        assert _object_snapshot(engine) == before == _expected_object_snapshot()
    finally:
        engine.dispose()


def test_0110_refuses_partial_object_registration_atomically(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0109(isolated_migration_postgres_url)
    try:
        _seed_prerepair_state(engine)
        with engine.begin() as connection:
            _insert_object(connection, OBJECTS[0])
        before = _object_snapshot(engine)
        with pytest.raises(Exception, match="0110 failed-smoke storage repair refused"):
            command.upgrade(config, "0110")
        assert _revision(engine) == "0109"
        assert _object_snapshot(engine) == before
    finally:
        engine.dispose()


def _batch_drift(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE batches SET result_status='partial_failed' WHERE id=:id"),
            {"id": BATCH_ID},
        )


def _trial_drift(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE trials SET failure_reason='operator_repaired' WHERE id=:id"),
            {"id": TRIALS[0].trial_id},
        )


def _run_authority_drift(engine: Engine) -> None:
    _authority_drift(engine, RUN_AUTHORITY_ID)


def _trial_authority_drift(engine: Engine) -> None:
    _authority_drift(engine, TRIALS[0].trial_authority_id)


def _event_authority_drift(engine: Engine) -> None:
    _authority_drift(engine, TRIALS[0].event_authority_id)


def _authority_drift(engine: Engine, authority_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE data_lifecycle_authorities SET state='quarantined' WHERE id=:id"),
            {"id": authority_id},
        )


def _typed_artifact_drift(engine: Engine) -> None:
    trial = TRIALS[0]
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO artifacts (
                    artifact_type, artifact_schema_version, name, team_id,
                    batch_id, trial_id, created_by, content_hash, storage,
                    visibility, share_status, redaction_state, safety_state,
                    retention, provenance, metadata, access_class
                ) VALUES (
                    'trajectory', '1.0', 'unexpected typed artifact',
                    CAST(:team_id AS uuid), CAST(:batch_id AS uuid),
                    CAST(:trial_id AS uuid), '{}'::jsonb,
                    'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    CAST(:storage AS jsonb), 'org', 'shared', 'not_required',
                    'safe', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                    'team_runtime'
                )
                """
            ),
            {
                "team_id": TEAM_ID,
                "batch_id": BATCH_ID,
                "trial_id": trial.trial_id,
                "storage": json.dumps(
                    {
                        "backend": "object_store",
                        "bucket": "trajectories",
                        "key": f"{TEAM_ID}/{trial.trial_id}/unexpected",
                    },
                    sort_keys=True,
                ),
            },
        )


def _insert_unrelated_authority(connection: Connection) -> None:
    _insert_authority(
        connection,
        _authority_values(
            authority_id=UNRELATED_AUTHORITY_ID,
            data_class="event",
            owner_kind="orphan",
            owner_id="migration-0110-prefix-evidence",
            created_at="2026-08-25T14:05:19.221168+00:00",
        ),
    )


def _wrong_authority_object_drift(engine: Engine) -> None:
    with engine.begin() as connection:
        _insert_unrelated_authority(connection)
        _insert_object(connection, OBJECTS[0], authority_id=UNRELATED_AUTHORITY_ID)


def _gc_authority_drift(engine: Engine) -> None:
    with engine.begin() as connection:
        run_id = connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_runs (
                    environment, namespace, mutation_epoch_before, state,
                    dry_run, requested_by, policy, inventory
                ) VALUES (
                    'staging', 'loom-staging', 0, 'planned', false,
                    'migration-0110-test', '{}'::jsonb, '{}'::jsonb
                ) RETURNING id
                """
            )
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO data_lifecycle_gc_authorities "
                "(gc_run_id, authority_id, deletion_token) "
                "VALUES (:run_id, :authority_id, gen_random_uuid())"
            ),
            {"run_id": run_id, "authority_id": RUN_AUTHORITY_ID},
        )


def _gc_item_drift(engine: Engine) -> None:
    object_ids = _seed_registered_objects(engine)
    item = OBJECTS[0]
    with engine.begin() as connection:
        run_id = connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_runs (
                    environment, namespace, mutation_epoch_before, state,
                    dry_run, requested_by, policy, inventory
                ) VALUES (
                    'staging', 'loom-staging', 0, 'planned', false,
                    'migration-0110-test', '{}'::jsonb, '{}'::jsonb
                ) RETURNING id
                """
            )
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_items (
                    gc_run_id, object_id, deletion_token, state, authority_id,
                    bucket, object_key, version_id, content_sha256, size_bytes
                ) VALUES (
                    :run_id, CAST(:object_id AS uuid), gen_random_uuid(), 'marked',
                    CAST(:authority_id AS uuid), :bucket, :object_key, NULL,
                    :content_sha256, :size_bytes
                )
                """
            ),
            {
                "run_id": run_id,
                "object_id": object_ids[0],
                "authority_id": item.authority_id,
                "bucket": item.bucket,
                "object_key": item.object_key,
                "content_sha256": item.content_sha256,
                "size_bytes": item.size_bytes,
            },
        )


def _gc_item_prefix_drift(engine: Engine) -> None:
    item = OBJECTS[0]
    with engine.begin() as connection:
        _insert_unrelated_authority(connection)
        run_id = connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_runs (
                    environment, namespace, mutation_epoch_before, state,
                    dry_run, requested_by, policy, inventory
                ) VALUES (
                    'staging', 'loom-staging', 0, 'planned', false,
                    'migration-0110-test', '{}'::jsonb, '{}'::jsonb
                ) RETURNING id
                """
            )
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_items (
                    gc_run_id, object_id, deletion_token, state, authority_id,
                    bucket, object_key, version_id, content_sha256, size_bytes
                ) VALUES (
                    :run_id, CAST(:object_id AS uuid), gen_random_uuid(), 'marked',
                    CAST(:authority_id AS uuid), :bucket, :object_key, NULL,
                    :content_sha256, :size_bytes
                )
                """
            ),
            {
                "run_id": run_id,
                "object_id": "11111111-1111-4111-8111-111111111111",
                "authority_id": UNRELATED_AUTHORITY_ID,
                "bucket": item.bucket,
                "object_key": item.object_key,
                "content_sha256": item.content_sha256,
                "size_bytes": item.size_bytes,
            },
        )


@pytest.mark.parametrize(
    "drift",
    [
        _batch_drift,
        _trial_drift,
        _run_authority_drift,
        _trial_authority_drift,
        _event_authority_drift,
        _typed_artifact_drift,
        _wrong_authority_object_drift,
        _gc_authority_drift,
        _gc_item_drift,
        _gc_item_prefix_drift,
    ],
    ids=[
        "batch",
        "trial",
        "run-authority",
        "trial-authority",
        "event-authority",
        "typed-artifact",
        "wrong-authority-object",
        "gc-authority",
        "gc-item",
        "gc-item-prefix",
    ],
)
def test_0110_refuses_drifted_or_gc_touched_state_without_changes(
    isolated_migration_postgres_url: str,
    drift: Callable[[Engine], None],
) -> None:
    config, engine = _at_0109(isolated_migration_postgres_url)
    try:
        _seed_prerepair_state(engine)
        drift(engine)
        before = _object_snapshot(engine)
        with pytest.raises(Exception, match="0110 failed-smoke storage repair refused"):
            command.upgrade(config, "0110")
        assert _revision(engine) == "0109"
        assert _object_snapshot(engine) == before
    finally:
        engine.dispose()


def test_0110_downgrade_refuses_registered_objects(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0109(isolated_migration_postgres_url)
    try:
        _seed_prerepair_state(engine)
        command.upgrade(config, "0110")
        before = _object_snapshot(engine)
        with pytest.raises(Exception, match="cannot downgrade 0110 after storage repair"):
            command.downgrade(config, "0109")
        assert _revision(engine) == "0110"
        assert _object_snapshot(engine) == before
    finally:
        engine.dispose()
