"""Migration 0112 repairs exact version identities for four staging trials."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, Engine, create_engine, text

TEAM_ID = "bbce1c49-8d6b-429c-a338-de37a6b533b7"
TRIAL_IDS = (
    "23088f03-2b7c-43f1-86dc-08d8eba82a56",
    "5a21ccc7-efcf-431d-a429-859408244421",
    "c361a2bb-8e8d-4b8a-85e2-7aafb6a084d3",
    "e470adcf-df16-448a-90f7-e038f3796e8b",
)
EXTRA_AUTHORITY_ID = "11111111-1111-4111-8111-111111111112"
EXTRA_OBJECT_ID = "11111111-1111-4111-8111-111111111113"


@dataclass(frozen=True)
class Target:
    object_id: str
    authority_id: str
    bucket: str
    trial_id: str
    suffix: str
    version_id: str
    content_sha256: str
    size_bytes: int
    created_at: str

    @property
    def object_key(self) -> str:
        return f"{TEAM_ID}/{self.trial_id}/{self.suffix}"


TARGETS = (
    Target(
        "b54ba60e-97cf-4798-8207-5163041b2a46",
        "755b5da2-2e20-478d-8143-b17eb2de14bf",
        "loom-staging-artifacts",
        TRIAL_IDS[0],
        "main/.loom/verifier/junit.xml",
        "6ed6ecc3-530f-4bb5-bc49-959cda7363ce",
        "3a323eaa0d22a9470b7bb15ef837e808de0bd54357147b59f4f02d6950b652dc",
        336,
        "2026-08-25T22:14:42.793864+00:00",
    ),
    Target(
        "4cd52ff1-8ca7-418b-b1bc-0a83e15e4296",
        "f4dc62bd-95db-4715-ba53-a4eb75734cd0",
        "loom-staging-artifacts",
        TRIAL_IDS[0],
        "main/.loom/verifier/pytest.log",
        "5723a1e4-d822-44c8-9205-60a7d27e7fe5",
        "c5e47731832b27de4faab9c425d1035881f480e1d7912551d67982579aae2f61",
        113,
        "2026-08-25T22:14:42.776824+00:00",
    ),
    Target(
        "ac760fca-46f9-432b-96b3-c3ec53d423b1",
        "21d2f0d3-cfed-4231-b124-9c21b910eaf6",
        "loom-staging-artifacts",
        TRIAL_IDS[0],
        "main/.loom/verifier/pytest.log.meta.json",
        "9a5c2766-5e55-41ae-8f67-f6e11445ba90",
        "ab901d6e66c368a0ea8cdc3aa4bd1792d8fcb46db384c9f4d45acc6a4e7bc81f",
        326,
        "2026-08-25T22:14:42.786200+00:00",
    ),
    Target(
        "82abba35-5727-48bd-9d47-3b7416e14de9",
        "f8c949c4-14fd-46d1-ba08-aa697af28788",
        "loom-staging-artifacts",
        TRIAL_IDS[0],
        "main/result.txt",
        "b9625a70-9d09-4625-91c6-b3fc1fbe4221",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T22:14:42.739881+00:00",
    ),
    Target(
        "267845d8-aedd-4f07-bd0f-b9a5eec14b60",
        "7ae47184-76fc-4722-9e69-3d72d39c9ee5",
        "loom-staging-artifacts",
        TRIAL_IDS[1],
        "main/.loom/verifier/junit.xml",
        "b1519569-2621-4329-bfb3-baf0f073c892",
        "0e74afa414273efdf95a5c00a8d489509ced40766e6d60a303d1a7b5939f90d6",
        336,
        "2026-08-25T22:14:43.972384+00:00",
    ),
    Target(
        "1a32e2f9-4859-42c9-b9fe-0e5f6d56f27e",
        "8d57e5fe-e2a9-4a82-9520-6ff81361e173",
        "loom-staging-artifacts",
        TRIAL_IDS[1],
        "main/.loom/verifier/pytest.log",
        "5a79f994-9da1-482b-adcd-2173e5845843",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T22:14:43.957197+00:00",
    ),
    Target(
        "0186b9b0-0ad5-49cf-a9b4-e71a3cfc19a9",
        "4980470b-c992-4ae7-8ff7-0eb5570f03fb",
        "loom-staging-artifacts",
        TRIAL_IDS[1],
        "main/.loom/verifier/pytest.log.meta.json",
        "4c9ab40d-8d85-4726-8a28-ea877c48c9b0",
        "0802d7b5bbd1a9d81e82461b8b465caeddc11ec5c5831726183a4b46def22bb9",
        328,
        "2026-08-25T22:14:43.966668+00:00",
    ),
    Target(
        "4bb40751-7ace-4f5f-ab64-acd24451d2a8",
        "5af11067-a64b-4d81-8f5d-0be5e95f463d",
        "loom-staging-artifacts",
        TRIAL_IDS[1],
        "main/result.txt",
        "5920f831-11d1-41c7-b0fa-e2bd608486cc",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T22:14:43.950256+00:00",
    ),
    Target(
        "8cc774b2-49e7-4098-85bc-c9ca76d3bff6",
        "1c89f46c-853c-4619-bda1-644dc9d9b957",
        "loom-staging-artifacts",
        TRIAL_IDS[2],
        "main/.loom/verifier/junit.xml",
        "05e57a22-fecf-4a6c-8faa-13d65a8f90ee",
        "a844b7d510c1ffcaa69721853f856ec9d655eb706401b6d5e3717aad7b97aebf",
        336,
        "2026-08-25T21:59:04.961432+00:00",
    ),
    Target(
        "544e1488-2252-4bb4-83a6-6b7b37af024d",
        "06fb6816-cac1-4ab1-b705-6bde371a352a",
        "loom-staging-artifacts",
        TRIAL_IDS[2],
        "main/.loom/verifier/pytest.log",
        "14641a99-67a4-4dfa-8bc5-adc9aea74c89",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T21:59:04.951609+00:00",
    ),
    Target(
        "5af4d885-fa94-4361-b386-5625ef212870",
        "c89873f9-e115-45e4-bb99-cc1bb9f44ede",
        "loom-staging-artifacts",
        TRIAL_IDS[2],
        "main/.loom/verifier/pytest.log.meta.json",
        "30fd5b59-8d4f-45d9-a40c-447824f0a940",
        "12cdde01eb8b0e3a1ad5019f2784c0ea54888bb167fa70433ffd104851eb12e7",
        327,
        "2026-08-25T21:59:04.955793+00:00",
    ),
    Target(
        "9e1a14aa-cbd5-43f5-bd01-e4dc000d2d53",
        "3cdd9fbb-932a-4885-829b-102bd93c499c",
        "loom-staging-artifacts",
        TRIAL_IDS[2],
        "main/result.txt",
        "a67ff812-e9b9-4b05-80f4-c99647aa305d",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T21:59:04.944570+00:00",
    ),
    Target(
        "7ab0b320-1307-4be9-bef3-41a0815cf60c",
        "1784749c-2c5d-4328-bedb-062420396dbf",
        "loom-staging-artifacts",
        TRIAL_IDS[3],
        "main/.loom/verifier/junit.xml",
        "9986c656-a80c-4898-9c07-ae536bc03152",
        "a78ae1dfb5615235c6ec6a5ec69249c0d7621a7788ba154aa6516a84f9aeb38e",
        336,
        "2026-08-25T21:59:04.920827+00:00",
    ),
    Target(
        "6c56f31d-27be-4595-a1bf-abea29b79600",
        "299a6577-2cec-4fd3-a164-b83813067b98",
        "loom-staging-artifacts",
        TRIAL_IDS[3],
        "main/.loom/verifier/pytest.log",
        "d1b78f31-631e-472a-946e-8090537e47b4",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T21:59:04.905483+00:00",
    ),
    Target(
        "5435a483-fdd4-4151-8ac6-a0cf2caeda67",
        "84897225-cf80-4a09-9aff-c5fec2b950bb",
        "loom-staging-artifacts",
        TRIAL_IDS[3],
        "main/.loom/verifier/pytest.log.meta.json",
        "391fc49c-e4d5-4fa7-b393-d13999d7e126",
        "fd6b662c226ee06cd508c0ffbee488c49195c2700328995b585521ef7e1e43cc",
        328,
        "2026-08-25T21:59:04.912463+00:00",
    ),
    Target(
        "e8b6cbf5-3aab-4401-b5e0-f4aab471b209",
        "252a73c1-3cd6-4088-9f4d-1b5b11d01778",
        "loom-staging-artifacts",
        TRIAL_IDS[3],
        "main/result.txt",
        "d483f513-5e0f-4a37-8092-b7fae255bc2c",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T21:59:04.894371+00:00",
    ),
    Target(
        "41221629-7ba3-482b-9250-6404eaace3ef",
        "02732a08-7ed6-492a-89f7-2c47c412269a",
        "loom-staging-trajectories",
        TRIAL_IDS[0],
        "attempts/1/atif.json",
        "71d7dec3-4fe4-464c-b3d6-91478b72e26c",
        "a00c5a29d2f45eb93b61fdb083ccd7ffce43a532d73dd97108a71cbb0af72c07",
        704,
        "2026-08-25T22:14:42.718136+00:00",
    ),
    Target(
        "c83ab0ac-e692-40ae-b29a-3a1d5518be24",
        "369c6a15-58c2-4e91-8da5-2675790e0d62",
        "loom-staging-trajectories",
        TRIAL_IDS[0],
        "attempts/1/events.jsonl",
        "dfbf17e7-4bc6-4799-bff7-7c1855356f5e",
        "94941990a6e3cfa405953f8b2a86681cf4c283245f9659a31633217daf70a0d2",
        1282,
        "2026-08-25T22:14:42.690460+00:00",
    ),
    Target(
        "d8152310-a680-44c0-804c-7580a06a22a3",
        "21302d4d-c2a1-4e3d-820b-609f0603636e",
        "loom-staging-trajectories",
        TRIAL_IDS[1],
        "attempts/1/atif.json",
        "aef724d4-d95d-4198-af27-d1d20043be34",
        "f0d68c4c5ca2de6da3116a89425597238775ceca863138e918400e7b0e114406",
        704,
        "2026-08-25T22:14:43.946529+00:00",
    ),
    Target(
        "a2357a36-0148-49d2-ae6e-b6dd6dee56ee",
        "debfc7b6-fb3a-4c93-bb97-9340c5911385",
        "loom-staging-trajectories",
        TRIAL_IDS[1],
        "attempts/1/events.jsonl",
        "d29b58ec-b589-420b-a485-de92afdb30c5",
        "7b092706acf06ccc3cca7e476d8fded0228ece6cde3068f7d6546b240dee8faa",
        1283,
        "2026-08-25T22:14:43.941033+00:00",
    ),
    Target(
        "450b7999-f6c2-4034-b86a-d463e7ffc96e",
        "10eab821-8e13-47fa-b8a4-f52285370f2a",
        "loom-staging-trajectories",
        TRIAL_IDS[2],
        "attempts/1/atif.json",
        "f505030f-62fe-4553-b5c3-41bf94a58df1",
        "542b9f7f4d142f65fc4a0406570d853abd799ea905c50ec6537fb3f2230e1972",
        704,
        "2026-08-25T21:59:04.932334+00:00",
    ),
    Target(
        "368fa1ba-ac95-4320-9375-8aa6c2590b1a",
        "aea4a64c-8dd4-4ba5-bd45-bf6dd582c35a",
        "loom-staging-trajectories",
        TRIAL_IDS[2],
        "attempts/1/events.jsonl",
        "3d2f1476-1bc8-404f-9101-4a3dd31fc028",
        "3f2497f1855105cbffbc411e514876ed7ebdd7c884aef445e22cb173539a2199",
        1282,
        "2026-08-25T21:59:04.923913+00:00",
    ),
    Target(
        "346559ee-49d8-41e8-bb4e-b0bbeac645f8",
        "405934f8-9691-45ce-9d31-951b816da81a",
        "loom-staging-trajectories",
        TRIAL_IDS[3],
        "attempts/1/atif.json",
        "9369817f-057d-4606-bb47-66ef897e1299",
        "37559eb0acfcabcbb875500e5c1cf683aeac11447af708556ed7cb35c1bca579",
        704,
        "2026-08-25T21:59:04.886506+00:00",
    ),
    Target(
        "d1ac1cd0-67a9-43df-a4b9-f23c4333aa0f",
        "1cb79bb5-23e1-4b56-a94a-18896c26c04a",
        "loom-staging-trajectories",
        TRIAL_IDS[3],
        "attempts/1/events.jsonl",
        "e1209591-6d04-41f0-910a-a9d90a37e5e3",
        "516bd458889dd21d830a9da9cb69b4d7a359629706bf795480656a783b862dd6",
        1282,
        "2026-08-25T21:59:04.859546+00:00",
    ),
)


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def _at_0111(postgres_url: str) -> tuple[Config, Engine]:
    config = _config(postgres_url)
    command.downgrade(config, "0111")
    return config, create_engine(postgres_url)


def _insert_authority(connection: Connection, authority_id: str) -> None:
    connection.execute(
        text(
            """
            INSERT INTO data_lifecycle_authorities (
                id, environment, namespace, team_id, data_class, owner_kind,
                owner_id, created_at, expires_at, pinned, state, deletion_token,
                metadata
            ) VALUES (
                CAST(:id AS uuid), 'staging', 'loom-staging', CAST(:team_id AS uuid),
                'artifact', 'artifact', :id,
                '2026-08-25T21:00:00+00:00'::timestamptz,
                '2026-09-01T21:00:00+00:00'::timestamptz,
                false, 'active', NULL, '{}'::jsonb
            )
            """
        ),
        {"id": authority_id, "team_id": TEAM_ID},
    )


def _insert_target(
    connection: Connection,
    target: Target,
    *,
    version_id: str | None,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO data_lifecycle_objects (
                id, authority_id, environment, namespace, bucket, object_key,
                version_id, content_sha256, size_bytes, created_at, state,
                deletion_token, verified_deleted_at
            ) VALUES (
                CAST(:id AS uuid), CAST(:authority_id AS uuid), 'staging',
                'loom-staging', :bucket, :object_key, :version_id,
                :content_sha256, :size_bytes, CAST(:created_at AS timestamptz),
                'active', NULL, NULL
            )
            """
        ),
        {
            "id": target.object_id,
            "authority_id": target.authority_id,
            "bucket": target.bucket,
            "object_key": target.object_key,
            "version_id": version_id,
            "content_sha256": target.content_sha256,
            "size_bytes": target.size_bytes,
            "created_at": target.created_at,
        },
    )


def _seed_state(engine: Engine, *, repaired: bool = False) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, '0112-version-repair-fixture')"),
            {"id": TEAM_ID},
        )
        for target in TARGETS:
            _insert_authority(connection, target.authority_id)
            _insert_target(
                connection,
                target,
                version_id=target.version_id if repaired else None,
            )


def _snapshot(engine: Engine) -> tuple[tuple[object, ...], ...]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id::text, authority_id::text, environment, namespace,
                       bucket, object_key, version_id, content_sha256, size_bytes,
                       created_at, state, deletion_token, verified_deleted_at
                  FROM data_lifecycle_objects
                 WHERE id = ANY(CAST(:ids AS uuid[]))
                    OR authority_id = ANY(CAST(:authority_ids AS uuid[]))
                    OR (environment = 'staging' AND namespace = 'loom-staging'
                        AND bucket IN (
                            'loom-staging-artifacts', 'loom-staging-trajectories'
                        )
                        AND (object_key LIKE :prefix0 OR object_key LIKE :prefix1
                             OR object_key LIKE :prefix2 OR object_key LIKE :prefix3))
                 ORDER BY id
                """
            ),
            {
                "ids": [target.object_id for target in TARGETS],
                "authority_ids": [target.authority_id for target in TARGETS],
                **{
                    f"prefix{index}": f"{TEAM_ID}/{trial_id}/%"
                    for index, trial_id in enumerate(TRIAL_IDS)
                },
            },
        )
        return tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                str(row[3]),
                str(row[4]),
                str(row[5]),
                str(row[6]) if row[6] is not None else None,
                str(row[7]),
                int(row[8]),
                row[9].isoformat(),
                str(row[10]),
                row[11],
                row[12],
            )
            for row in rows
        )


def _expected_snapshot(*, repaired: bool) -> tuple[tuple[object, ...], ...]:
    return tuple(
        sorted(
            (
                target.object_id,
                target.authority_id,
                "staging",
                "loom-staging",
                target.bucket,
                target.object_key,
                target.version_id if repaired else None,
                target.content_sha256,
                target.size_bytes,
                target.created_at,
                "active",
                None,
                None,
            )
            for target in TARGETS
        )
    )


def _revision(engine: Engine) -> str:
    with engine.connect() as connection:
        return str(connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one())


def _insert_gc_run(connection: Connection) -> str:
    return str(
        connection.execute(
            text(
                """
                INSERT INTO data_lifecycle_gc_runs (
                    environment, namespace, mutation_epoch_before, state,
                    dry_run, requested_by, policy, inventory
                ) VALUES (
                    'staging', 'loom-staging', 0, 'planned', false,
                    'migration-0112-test', '{}'::jsonb, '{}'::jsonb
                ) RETURNING id
                """
            )
        ).scalar_one()
    )


def test_0112_is_a_noop_when_none_of_the_target_identities_exist(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0112")
        assert _revision(engine) == "0112"
        assert _snapshot(engine) == ()
    finally:
        engine.dispose()


def test_0112_repairs_all_twenty_four_exact_version_identities(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        assert _snapshot(engine) == _expected_snapshot(repaired=False)
        command.upgrade(config, "0112")
        assert _revision(engine) == "0112"
        assert _snapshot(engine) == _expected_snapshot(repaired=True)
    finally:
        engine.dispose()


def test_0112_accepts_the_exact_repaired_state_idempotently(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        command.upgrade(config, "0112")
        before = _snapshot(engine)
        command.stamp(config, "0111")
        command.upgrade(config, "0112")
        assert _revision(engine) == "0112"
        assert _snapshot(engine) == before == _expected_snapshot(repaired=True)
    finally:
        engine.dispose()


def test_0112_refuses_a_partial_repair_atomically(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE data_lifecycle_objects SET version_id=:version WHERE id=:id"),
                {"version": TARGETS[0].version_id, "id": TARGETS[0].object_id},
            )
        before = _snapshot(engine)
        with pytest.raises(Exception, match="0112 staging object-version repair refused"):
            command.upgrade(config, "0112")
        assert _revision(engine) == "0111"
        assert _snapshot(engine) == before
    finally:
        engine.dispose()


def test_0112_refuses_drifted_target_state_atomically(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE data_lifecycle_objects SET content_sha256=:sha WHERE id=:id"),
                {"sha": "f" * 64, "id": TARGETS[0].object_id},
            )
        before = _snapshot(engine)
        with pytest.raises(Exception, match="0112 staging object-version repair refused"):
            command.upgrade(config, "0112")
        assert _revision(engine) == "0111"
        assert _snapshot(engine) == before
    finally:
        engine.dispose()


def test_0112_refuses_extra_object_under_a_target_prefix_atomically(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        with engine.begin() as connection:
            _insert_authority(connection, EXTRA_AUTHORITY_ID)
            connection.execute(
                text(
                    """
                    INSERT INTO data_lifecycle_objects (
                        id, authority_id, environment, namespace, bucket,
                        object_key, version_id, content_sha256, size_bytes,
                        created_at, state, deletion_token, verified_deleted_at
                    ) VALUES (
                        CAST(:id AS uuid), CAST(:authority_id AS uuid), 'staging',
                        'loom-staging', 'loom-staging-artifacts', :object_key,
                        NULL, :sha, 1, '2026-08-25T22:14:44+00:00'::timestamptz,
                        'active', NULL, NULL
                    )
                    """
                ),
                {
                    "id": EXTRA_OBJECT_ID,
                    "authority_id": EXTRA_AUTHORITY_ID,
                    "object_key": f"{TEAM_ID}/{TRIAL_IDS[0]}/unexpected",
                    "sha": "1" * 64,
                },
            )
        before = _snapshot(engine)
        with pytest.raises(Exception, match="0112 staging object-version repair refused"):
            command.upgrade(config, "0112")
        assert _revision(engine) == "0111"
        assert _snapshot(engine) == before
    finally:
        engine.dispose()


@pytest.mark.parametrize("gc_kind", ["item", "authority"])
def test_0112_refuses_gc_touched_state_atomically(
    isolated_migration_postgres_url: str,
    gc_kind: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        with engine.begin() as connection:
            run_id = _insert_gc_run(connection)
            target = TARGETS[0]
            if gc_kind == "item":
                connection.execute(
                    text(
                        """
                        INSERT INTO data_lifecycle_gc_items (
                            gc_run_id, object_id, deletion_token, state,
                            authority_id, bucket, object_key, version_id,
                            content_sha256, size_bytes
                        ) VALUES (
                            CAST(:run_id AS uuid), CAST(:object_id AS uuid),
                            gen_random_uuid(), 'marked', CAST(:authority_id AS uuid),
                            :bucket, :object_key, NULL, :sha, :size_bytes
                        )
                        """
                    ),
                    {
                        "run_id": run_id,
                        "object_id": target.object_id,
                        "authority_id": target.authority_id,
                        "bucket": target.bucket,
                        "object_key": target.object_key,
                        "sha": target.content_sha256,
                        "size_bytes": target.size_bytes,
                    },
                )
            else:
                connection.execute(
                    text(
                        """
                        INSERT INTO data_lifecycle_gc_authorities (
                            gc_run_id, authority_id, deletion_token
                        ) VALUES (
                            CAST(:run_id AS uuid), CAST(:authority_id AS uuid),
                            gen_random_uuid()
                        )
                        """
                    ),
                    {"run_id": run_id, "authority_id": target.authority_id},
                )
        before = _snapshot(engine)
        with pytest.raises(Exception, match="0112 staging object-version repair refused"):
            command.upgrade(config, "0112")
        assert _revision(engine) == "0111"
        assert _snapshot(engine) == before
    finally:
        engine.dispose()


def test_0112_downgrade_refuses_repaired_rows(
    isolated_migration_postgres_url: str,
) -> None:
    config, engine = _at_0111(isolated_migration_postgres_url)
    try:
        _seed_state(engine)
        command.upgrade(config, "0112")
        before = _snapshot(engine)
        with pytest.raises(Exception, match="cannot downgrade 0112 after object-version repair"):
            command.downgrade(config, "0111")
        assert _revision(engine) == "0112"
        assert _snapshot(engine) == before
    finally:
        engine.dispose()
