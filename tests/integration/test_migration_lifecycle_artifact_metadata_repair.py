"""Migration 0109 repairs three exact staging trajectory metadata snapshots."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text

TEAM_ID = "bbce1c49-8d6b-429c-a338-de37a6b533b7"
BATCH_ID = "6d646469-6f67-4f9e-81f8-735ca90c2c76"


@dataclass(frozen=True)
class RepairTarget:
    trial_id: str
    artifact_id: str
    authority_id: str
    object_id: str
    old_sha256: str
    old_size_bytes: int
    new_sha256: str
    new_size_bytes: int
    created_at: datetime

    @property
    def object_key(self) -> str:
        return f"{TEAM_ID}/{self.trial_id}/events.jsonl"


TARGETS = (
    RepairTarget(
        trial_id="0347026e-3322-4f88-ba71-3bf09ae68034",
        artifact_id="450edc6c-00b9-4544-ac58-dd714c1c9ddc",
        authority_id="ae6d8ea8-ede7-489d-81e6-d75d21d7e3ea",
        object_id="b8a7f492-78ee-4915-a7ee-174fdf060031",
        old_sha256="1c4bb4a2e7fa76687bc456082c473a2a98c7e6bda35e4de9846e3d210a1c4e1e",
        old_size_bytes=548555,
        new_sha256="857dc17e8ddd6bf56efe59bd30f2debd443509dfc2a120cc8b16ee869f0208c5",
        new_size_bytes=282275,
        created_at=datetime(2026, 8, 21, 3, 47, 48, 202812, tzinfo=UTC),
    ),
    RepairTarget(
        trial_id="193d6992-3522-41f0-bcb5-27411fdeaccd",
        artifact_id="62ea9240-b58f-48e3-9d64-7dbe14387c63",
        authority_id="b21627d0-9b05-4814-9087-e228b1e44af4",
        object_id="ff01e8ed-e215-4656-9703-147164f83a2e",
        old_sha256="32a9303e62cdc94dca24f66025b97d7f7f89e0287f293945fdc0b6cf7ee32510",
        old_size_bytes=213116,
        new_sha256="91b9566b6b60b1d794cf83366bd6c990968e08fdb90b7bd2619bc2ff1a87653e",
        new_size_bytes=189870,
        created_at=datetime(2026, 8, 21, 3, 50, 45, 397884, tzinfo=UTC),
    ),
    RepairTarget(
        trial_id="50394796-8537-400d-af62-112f2b2191cf",
        artifact_id="a355183e-af55-4066-aa2b-17f05f788348",
        authority_id="844ea300-78ba-4c70-bbb6-43e488cf279b",
        object_id="5f13c704-9744-4af9-962a-59edd3403b0c",
        old_sha256="bc5874903448cb5b3ddbba29165ca54422ca6b53d3100d78b60962e0069bacf1",
        old_size_bytes=630330,
        new_sha256="cb2460e17276bf62f95afdb33382fcadc6b2457d7df008959cd74b882457e5ea",
        new_size_bytes=490483,
        created_at=datetime(2026, 8, 21, 3, 24, 36, 650509, tzinfo=UTC),
    ),
)

ORPHAN_BATCH_ID = "a918a140-b1c7-4093-ba02-1640c8a9e71c"


@dataclass(frozen=True)
class OrphanTrial:
    trial_id: str
    authority_id: str
    submitted_at: str
    finished_at: str


ORPHAN_TRIALS = (
    OrphanTrial(
        trial_id="758f4c7c-2510-43e7-b9ae-9af0fcb31451",
        authority_id="f77e1999-a436-44c3-91c1-0c46e1b6dd7f",
        submitted_at="2026-08-24T20:09:33.132678+00:00",
        finished_at="2026-08-24T20:49:45.75801+00:00",
    ),
    OrphanTrial(
        trial_id="a28be4d2-fc3d-4301-8177-d01f964cac97",
        authority_id="004d05b9-799f-4933-b7cc-10f5f2121177",
        submitted_at="2026-08-24T20:09:33.182307+00:00",
        finished_at="2026-08-24T20:49:45.75801+00:00",
    ),
)


@dataclass(frozen=True)
class OrphanObject:
    trial_id: str
    bucket: str
    suffix: str
    content_sha256: str
    size_bytes: int
    created_at: str

    @property
    def object_key(self) -> str:
        return f"{TEAM_ID}/{self.trial_id}/{self.suffix}"


ORPHAN_OBJECTS = (
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "5a82c0d9c0f02dd7fa228a7b588460136c54af8ffdf05108a17746431f08653e",
        336,
        "2026-08-24T20:37:18.058000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-24T20:37:17.896000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "696564789c10a527879e6966597c9c9e65aa920a2a2a999b78173e45db3a0ffb",
        328,
        "2026-08-24T20:37:17.992000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-24T20:37:17.803000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "89a9b6c1385cd7ca612451234b022c580def15cd9d68821a57973dd7300f05b3",
        704,
        "2026-08-24T20:11:53.288000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "a28606071c5a680e54080f8fdd9231fa6b06f496ac5c010ff57998f6dbeab315",
        1282,
        "2026-08-24T20:11:53.194000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "dc4fb8842ed57bafdc0fe5174411148e3389b2cd3a90f70fd1edf5f5eaff4cc7",
        704,
        "2026-08-24T20:24:26.920000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "a6945344c9d73f345400013a36c029116346a3e8ddbdc88e17fb91fbab7150d2",
        1283,
        "2026-08-24T20:24:26.863000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "89a9b6c1385cd7ca612451234b022c580def15cd9d68821a57973dd7300f05b3",
        704,
        "2026-08-24T20:37:18.470000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "7114a3b7588b4b46e51727178b1c035854d15918883fae539fe42274ea69aa04",
        1282,
        "2026-08-24T20:37:18.398000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "404966d9ea82e8721bdf7b1601c891e86bf140bd3085ccd23bf759ce34cd4812",
        336,
        "2026-08-24T20:37:18.351000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-24T20:37:17.899000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "7486b1420ef6b466cc5b757fe2134964a458f9b0944dba015d56dd9262e2b598",
        326,
        "2026-08-24T20:37:18.247000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-24T20:37:17.804000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "45d72cf6e80268953341ac703c1649096e495fb608e527488d241c8b1fb8b5eb",
        704,
        "2026-08-24T20:11:53.157000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "52e3759d33e1871725e2a9888ecd559a972a696aae057976538ecef858d63afd",
        1283,
        "2026-08-24T20:11:53.052000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "9a255ad01e852169bfb641ea3edc44f923a11eee6291de169ac50043d6fb82c4",
        704,
        "2026-08-24T20:24:26.951000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "4ccc6ed170b513cf6b3c459e9e2552c3a3af4f60da6467b6c83155a57bfbd3cb",
        1282,
        "2026-08-24T20:24:26.871000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "45d72cf6e80268953341ac703c1649096e495fb608e527488d241c8b1fb8b5eb",
        704,
        "2026-08-24T20:37:18.790000+00:00",
    ),
    OrphanObject(
        ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "bb5e0f9da9ac1a32cccc417d2a6908a730a8da1061f0c13e8a1f26b6b6e4664a",
        1283,
        "2026-08-24T20:37:18.742000+00:00",
    ),
)


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def _at_0108(postgres_url: str) -> tuple[Config, Engine]:
    config = _config(postgres_url)
    command.downgrade(config, "0108")
    return config, create_engine(postgres_url)


def _seed_old_state(engine: Engine, targets: Iterable[RepairTarget] = TARGETS) -> None:
    selected = tuple(targets)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, '0109-repair-fixture')"),
            {"id": TEAM_ID},
        )
        connection.execute(
            text(
                """
                INSERT INTO batches (
                    id, team_id, name, task_filter, trial_config,
                    state, created_by_token_prefix
                ) VALUES (
                    :id, :team_id, '0109-repair-fixture', '{}'::jsonb, '{}'::jsonb,
                    'running', 'migration-0109'
                )
                """
            ),
            {"id": BATCH_ID, "team_id": TEAM_ID},
        )
        for target in selected:
            task_id = f"migration-0109-{target.trial_id}"
            connection.execute(
                text(
                    "INSERT INTO tasks (id, config, checksum) "
                    "VALUES (:id, '{}'::jsonb, 'sha256:migration-0109')"
                ),
                {"id": task_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO trials (
                        id, team_id, task_id, config, requires_caps, state,
                        submit_priority, batch_id, result, finished_at, attempt_count
                    ) VALUES (
                        :id, :team_id, :task_id, '{}'::jsonb, '{}'::jsonb,
                        'succeeded', 100, :batch_id, '{}'::jsonb,
                        CAST(:created_at AS timestamptz), 1
                    )
                    """
                ),
                {
                    "id": target.trial_id,
                    "team_id": TEAM_ID,
                    "task_id": task_id,
                    "batch_id": BATCH_ID,
                    "created_at": target.created_at.isoformat(),
                },
            )
            created_by = {
                "kind": "trial",
                "batch_id": BATCH_ID,
                "trial_id": target.trial_id,
            }
            storage = {
                "key": target.object_key,
                "bucket": "trajectories",
                "backend": "object_store",
                "media_type": "application/x-ndjson",
                "size_bytes": target.old_size_bytes,
            }
            provenance = {
                "batch_id": BATCH_ID,
                "relation": "produced_from",
                "trial_id": target.trial_id,
                "source_trial_ids": [target.trial_id],
            }
            connection.execute(
                text(
                    """
                    INSERT INTO artifacts (
                        id, artifact_type, artifact_schema_version, name,
                        team_id, batch_id, trial_id, created_by, content_hash,
                        storage, visibility, share_status, redaction_state,
                        safety_state, retention, provenance, metadata, created_at,
                        access_class
                    ) VALUES (
                        :id, 'trajectory', '1.0', 'Trajectory events',
                        :team_id, :batch_id, :trial_id, CAST(:created_by AS jsonb),
                        :content_hash, CAST(:storage AS jsonb), 'org', 'shared',
                        'not_required', 'safe', CAST(:retention AS jsonb),
                        CAST(:provenance AS jsonb), '{}'::jsonb,
                        CAST(:created_at AS timestamptz), 'team_runtime'
                    )
                    """
                ),
                {
                    "id": target.artifact_id,
                    "team_id": TEAM_ID,
                    "batch_id": BATCH_ID,
                    "trial_id": target.trial_id,
                    "created_by": json.dumps(created_by, sort_keys=True),
                    "content_hash": f"sha256:{target.old_sha256}",
                    "storage": json.dumps(storage, sort_keys=True),
                    "retention": json.dumps(
                        {"class": "release_evidence", "expires_at": None},
                        sort_keys=True,
                    ),
                    "provenance": json.dumps(provenance, sort_keys=True),
                    "created_at": target.created_at.isoformat(),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO data_lifecycle_authorities (
                        id, environment, namespace, team_id, data_class,
                        owner_kind, owner_id, created_at, expires_at, pinned,
                        state, deletion_token, metadata
                    ) VALUES (
                        :id, 'staging', 'loom-staging', :team_id, 'artifact',
                        'artifact', :owner_id, CAST(:created_at AS timestamptz),
                        CAST(:expires_at AS timestamptz), false, 'active', NULL,
                        '{}'::jsonb
                    )
                    """
                ),
                {
                    "id": target.authority_id,
                    "team_id": TEAM_ID,
                    "owner_id": target.artifact_id,
                    "created_at": target.created_at.isoformat(),
                    "expires_at": (target.created_at + timedelta(days=7)).isoformat(),
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO data_lifecycle_objects (
                        id, authority_id, environment, namespace, bucket,
                        object_key, version_id, content_sha256, size_bytes,
                        created_at, state, deletion_token, verified_deleted_at
                    ) VALUES (
                        :id, :authority_id, 'staging', 'loom-staging',
                        'trajectories', :object_key, NULL, :content_sha256,
                        :size_bytes, CAST(:created_at AS timestamptz),
                        'active', NULL, NULL
                    )
                    """
                ),
                {
                    "id": target.object_id,
                    "authority_id": target.authority_id,
                    "object_key": target.object_key,
                    "content_sha256": target.old_sha256,
                    "size_bytes": target.old_size_bytes,
                    "created_at": target.created_at.isoformat(),
                },
            )
            connection.execute(
                text(
                    "UPDATE artifacts SET lifecycle_authority_id=:authority_id "
                    "WHERE id=:artifact_id"
                ),
                {
                    "authority_id": target.authority_id,
                    "artifact_id": target.artifact_id,
                },
            )


def _seed_orphan_prerepair_state(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO teams (id, name) VALUES (:id, '0109-orphan-fixture') "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": TEAM_ID},
        )
        connection.execute(
            text(
                """
                INSERT INTO batches (
                    id, team_id, name, task_filter, trial_config,
                    state, created_by_token_prefix
                ) VALUES (
                    :id, :team_id, '0109-orphan-fixture', '{}'::jsonb, '{}'::jsonb,
                    'running', 'migration-0109'
                )
                """
            ),
            {"id": ORPHAN_BATCH_ID, "team_id": TEAM_ID},
        )
        for index, trial in enumerate(ORPHAN_TRIALS, start=1):
            task_id = f"migration-0109-orphan-{index}"
            connection.execute(
                text(
                    "INSERT INTO tasks (id, config, checksum) "
                    "VALUES (:id, '{}'::jsonb, 'sha256:migration-0109')"
                ),
                {"id": task_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO trials (
                        id, team_id, task_id, config, requires_caps, state,
                        submit_priority, batch_id, submitted_at, finished_at,
                        attempt_count, failure_reason
                    ) VALUES (
                        :id, :team_id, :task_id, '{}'::jsonb, '{}'::jsonb,
                        'failed', 100, :batch_id, CAST(:submitted_at AS timestamptz),
                        CAST(:finished_at AS timestamptz), 3, 'retry_exhausted'
                    )
                    """
                ),
                {
                    "id": trial.trial_id,
                    "team_id": TEAM_ID,
                    "task_id": task_id,
                    "batch_id": ORPHAN_BATCH_ID,
                    "submitted_at": trial.submitted_at,
                    "finished_at": trial.finished_at,
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO data_lifecycle_authorities (
                        id, environment, namespace, team_id, data_class,
                        owner_kind, owner_id, created_at, expires_at, pinned,
                        state, deletion_token, metadata
                    ) VALUES (
                        :id, 'staging', 'loom-staging', :team_id, 'trial',
                        'trial', :owner_id, CAST(:created_at AS timestamptz),
                        CAST(:created_at AS timestamptz) + interval '7 days',
                        false, 'active', NULL, '{}'::jsonb
                    )
                    """
                ),
                {
                    "id": trial.authority_id,
                    "team_id": TEAM_ID,
                    "owner_id": trial.trial_id,
                    "created_at": trial.submitted_at,
                },
            )
            connection.execute(
                text("UPDATE trials SET lifecycle_authority_id=:authority_id WHERE id=:trial_id"),
                {"authority_id": trial.authority_id, "trial_id": trial.trial_id},
            )


OrphanSnapshotRow = tuple[str, str, str, str | None, str, int, str, str, None, None]


def _orphan_snapshot(engine: Engine) -> tuple[OrphanSnapshotRow, ...]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT authority_id::text, bucket, object_key, version_id,
                       content_sha256, size_bytes, created_at, state,
                       deletion_token, verified_deleted_at
                  FROM data_lifecycle_objects
                 WHERE authority_id = ANY(CAST(:authority_ids AS uuid[]))
                 ORDER BY bucket, object_key, COALESCE(version_id, '')
                """
            ),
            {"authority_ids": [trial.authority_id for trial in ORPHAN_TRIALS]},
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


def _expected_orphan_snapshot() -> tuple[OrphanSnapshotRow, ...]:
    authority_by_trial = {trial.trial_id: trial.authority_id for trial in ORPHAN_TRIALS}
    rows = (
        (
            authority_by_trial[item.trial_id],
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
        for item in ORPHAN_OBJECTS
    )
    return tuple(sorted(rows, key=lambda row: (row[1], row[2], row[3] or "")))


ArtifactSnapshot = tuple[str, str, dict[str, Any]]
ObjectSnapshot = tuple[str, str, int]
Snapshot = tuple[tuple[ArtifactSnapshot, ...], tuple[ObjectSnapshot, ...]]


def _snapshot(engine: Engine) -> Snapshot:
    with engine.connect() as connection:
        artifacts = tuple(
            (str(row[0]), str(row[1]), dict(row[2]))
            for row in connection.execute(
                text(
                    """
                    SELECT id::text, content_hash, storage
                      FROM artifacts
                     WHERE id = ANY(CAST(:ids AS uuid[]))
                     ORDER BY id
                    """
                ),
                {"ids": [target.artifact_id for target in TARGETS]},
            )
        )
        objects = tuple(
            (str(row[0]), str(row[1]), int(row[2]))
            for row in connection.execute(
                text(
                    """
                    SELECT id::text, content_sha256, size_bytes
                      FROM data_lifecycle_objects
                     WHERE id = ANY(CAST(:ids AS uuid[]))
                     ORDER BY id
                    """
                ),
                {"ids": [target.object_id for target in TARGETS]},
            )
        )
    return artifacts, objects


def _revision(engine: Engine) -> str:
    with engine.connect() as connection:
        return str(connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one())


def _assert_repaired(engine: Engine) -> None:
    artifacts, objects = _snapshot(engine)
    artifact_by_id = {str(row[0]): row for row in artifacts}
    object_by_id = {str(row[0]): row for row in objects}
    for target in TARGETS:
        artifact = artifact_by_id[target.artifact_id]
        assert artifact[1] == f"sha256:{target.new_sha256}"
        assert artifact[2] == {
            "key": target.object_key,
            "bucket": "trajectories",
            "backend": "object_store",
            "media_type": "application/x-ndjson",
            "size_bytes": target.new_size_bytes,
        }
        lifecycle_object = object_by_id[target.object_id]
        assert lifecycle_object[1:] == (target.new_sha256, target.new_size_bytes)


def test_0109_is_a_noop_when_none_of_the_target_rows_exist(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0109")
        assert _revision(engine) == "0109"
        assert _snapshot(engine) == ((), ())
    finally:
        engine.dispose()


def test_0109_repairs_all_three_exact_old_snapshots_atomically(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_old_state(engine)
        command.upgrade(config, "0109")
        assert _revision(engine) == "0109"
        _assert_repaired(engine)
    finally:
        engine.dispose()


def test_0109_accepts_the_exact_repaired_state_idempotently(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_old_state(engine)
        command.upgrade(config, "0109")
        before = _snapshot(engine)
        command.stamp(config, "0108")
        command.upgrade(config, "0109")
        assert _revision(engine) == "0109"
        assert _snapshot(engine) == before
        _assert_repaired(engine)
    finally:
        engine.dispose()


def test_0109_registers_exact_objects_from_the_two_failed_smoke_trials(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_orphan_prerepair_state(engine)
        assert _orphan_snapshot(engine) == ()
        command.upgrade(config, "0109")
        assert _revision(engine) == "0109"
        assert _orphan_snapshot(engine) == _expected_orphan_snapshot()
    finally:
        engine.dispose()


def test_0109_repairs_metadata_and_failed_smoke_objects_in_one_transaction(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_old_state(engine)
        _seed_orphan_prerepair_state(engine)
        command.upgrade(config, "0109")
        assert _revision(engine) == "0109"
        _assert_repaired(engine)
        assert _orphan_snapshot(engine) == _expected_orphan_snapshot()
    finally:
        engine.dispose()


def test_0109_accepts_exact_failed_smoke_object_registration_idempotently(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_orphan_prerepair_state(engine)
        command.upgrade(config, "0109")
        before = _orphan_snapshot(engine)
        command.stamp(config, "0108")
        command.upgrade(config, "0109")
        assert _revision(engine) == "0109"
        assert _orphan_snapshot(engine) == before == _expected_orphan_snapshot()
    finally:
        engine.dispose()


def test_0109_refuses_partial_failed_smoke_object_registration(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_orphan_prerepair_state(engine)
        item = ORPHAN_OBJECTS[0]
        authority_id = next(
            trial.authority_id for trial in ORPHAN_TRIALS if trial.trial_id == item.trial_id
        )
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO data_lifecycle_objects (
                        authority_id, environment, namespace, bucket, object_key,
                        version_id, content_sha256, size_bytes, created_at, state
                    ) VALUES (
                        :authority_id, 'staging', 'loom-staging', :bucket, :object_key,
                        NULL, :sha, :size, CAST(:created_at AS timestamptz), 'active'
                    )
                    """
                ),
                {
                    "authority_id": authority_id,
                    "bucket": item.bucket,
                    "object_key": item.object_key,
                    "sha": item.content_sha256,
                    "size": item.size_bytes,
                    "created_at": item.created_at,
                },
            )
        before = _orphan_snapshot(engine)
        with pytest.raises(Exception, match="0109 lifecycle artifact metadata repair refused"):
            command.upgrade(config, "0109")
        assert _revision(engine) == "0108"
        assert _orphan_snapshot(engine) == before
    finally:
        engine.dispose()


def test_0109_refuses_drifted_failed_smoke_trial_identity(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_orphan_prerepair_state(engine)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE trials SET failure_reason='operator_repaired' WHERE id=:id"),
                {"id": ORPHAN_TRIALS[0].trial_id},
            )
        before = _orphan_snapshot(engine)
        with pytest.raises(Exception, match="0109 lifecycle artifact metadata repair refused"):
            command.upgrade(config, "0109")
        assert _revision(engine) == "0108"
        assert _orphan_snapshot(engine) == before
    finally:
        engine.dispose()


def test_0109_downgrade_refuses_registered_failed_smoke_objects(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_orphan_prerepair_state(engine)
        command.upgrade(config, "0109")
        before = _orphan_snapshot(engine)
        with pytest.raises(Exception, match="cannot downgrade 0109 after metadata repair"):
            command.downgrade(config, "0108")
        assert _revision(engine) == "0109"
        assert _orphan_snapshot(engine) == before
    finally:
        engine.dispose()


def _partial_state(engine: Engine) -> None:
    _seed_old_state(engine, TARGETS[:2])


def _mixed_state(engine: Engine) -> None:
    _seed_old_state(engine)
    target = TARGETS[0]
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE artifacts SET content_hash=:sha, "
                "storage=jsonb_set(storage, '{size_bytes}', to_jsonb(CAST(:size AS bigint))) "
                "WHERE id=:id"
            ),
            {
                "id": target.artifact_id,
                "sha": f"sha256:{target.new_sha256}",
                "size": target.new_size_bytes,
            },
        )
        connection.execute(
            text(
                "UPDATE data_lifecycle_objects "
                "SET content_sha256=:sha, size_bytes=:size WHERE id=:id"
            ),
            {
                "id": target.object_id,
                "sha": target.new_sha256,
                "size": target.new_size_bytes,
            },
        )


def _artifact_drift(engine: Engine) -> None:
    _seed_old_state(engine)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE artifacts SET storage=storage || CAST(:drift AS jsonb) WHERE id=:id"),
            {"id": TARGETS[0].artifact_id, "drift": '{"unexpected":true}'},
        )


def _authority_drift(engine: Engine) -> None:
    _seed_old_state(engine)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE data_lifecycle_authorities SET state='quarantined' WHERE id=:id"),
            {"id": TARGETS[0].authority_id},
        )


def _gc_item_drift(engine: Engine) -> None:
    _seed_old_state(engine)
    with engine.begin() as connection:
        run_id = connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_runs (
                    environment, namespace, mutation_epoch_before, state,
                    dry_run, requested_by, policy, inventory
                ) VALUES (
                    'staging', 'loom-staging', 0, 'planned', false,
                    'migration-0109-test', '{}'::jsonb, '{}'::jsonb
                ) RETURNING id
                """
            )
        ).scalar_one()
        target = TARGETS[0]
        connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_items (
                    gc_run_id, object_id, deletion_token, state, authority_id,
                    bucket, object_key, version_id, content_sha256, size_bytes
                ) VALUES (
                    :run_id, :object_id, gen_random_uuid(), 'marked', :authority_id,
                    'trajectories', :object_key, NULL, :sha, :size
                )
                """
            ),
            {
                "run_id": run_id,
                "object_id": target.object_id,
                "authority_id": target.authority_id,
                "object_key": target.object_key,
                "sha": target.old_sha256,
                "size": target.old_size_bytes,
            },
        )


def _gc_authority_drift(engine: Engine) -> None:
    _seed_old_state(engine)
    with engine.begin() as connection:
        run_id = connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_runs (
                    environment, namespace, mutation_epoch_before, state,
                    dry_run, requested_by, policy, inventory
                ) VALUES (
                    'staging', 'loom-staging', 0, 'planned', false,
                    'migration-0109-test', '{}'::jsonb, '{}'::jsonb
                ) RETURNING id
                """
            )
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_authorities (
                    gc_run_id, authority_id, deletion_token
                ) VALUES (:run_id, :authority_id, gen_random_uuid())
                """
            ),
            {
                "run_id": run_id,
                "authority_id": TARGETS[0].authority_id,
            },
        )


@pytest.mark.parametrize(
    "seed",
    [
        _partial_state,
        _mixed_state,
        _artifact_drift,
        _authority_drift,
        _gc_item_drift,
        _gc_authority_drift,
    ],
    ids=[
        "partial",
        "mixed",
        "artifact-drift",
        "authority-drift",
        "gc-item-drift",
        "gc-authority-drift",
    ],
)
def test_0109_refuses_partial_mixed_or_drifted_target_state_without_changes(
    isolated_migration_postgres_url: str,
    seed: Callable[[Engine], None],
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        seed(engine)
        before = _snapshot(engine)
        with pytest.raises(Exception, match="0109 lifecycle artifact metadata repair refused"):
            command.upgrade(config, "0109")
        assert _revision(engine) == "0108"
        assert _snapshot(engine) == before
    finally:
        engine.dispose()


def test_0109_downgrade_refuses_repaired_rows(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0108(isolated_migration_postgres_url)
    try:
        _seed_old_state(engine)
        command.upgrade(config, "0109")
        before = _snapshot(engine)
        with pytest.raises(Exception, match="cannot downgrade 0109 after metadata repair"):
            command.downgrade(config, "0108")
        assert _revision(engine) == "0109"
        assert _snapshot(engine) == before
        _assert_repaired(engine)
    finally:
        engine.dispose()
