"""Application identity/name claims coexist with frozen full-environment records."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, delete, insert, inspect, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.test_nebius_environment_migration import registration as legacy_registration


@pytest.fixture
def application_database(isolated_migration_postgres_url):
    engine = create_engine(isolated_migration_postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


def application(connection, slug="alice", **changes):
    from loom.db.nebius_application_schema import NebiusApplication
    from loom.db.schema import Team, User

    owner, team = uuid4(), uuid4()
    connection.execute(insert(Team).values(id=team, name=str(team)))
    connection.execute(insert(User).values(id=owner, username=str(owner), username_normalized=str(owner)))
    row = dict(
        application_id=uuid4(), incarnation=uuid4(), owner_user_id=owner, owner_team_id=team,
        data_environment_id=UUID("20000000-0000-4000-8000-000000000009"), cluster_id="cluster-1",
        slug=slug, application_namespace="loom-dev-" + slug, public_host=slug + ".dev.example.com",
        release_id=uuid4(), deployment_generation=1, access_generation=1, desired_state="active",
    ) | changes
    connection.execute(insert(NebiusApplication).values(**row))
    return row


def migrate(engine, direction, revision):
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", engine.url.render_as_string(hide_password=False).replace("%", "%%"))
    getattr(command, direction)(cfg, revision)


def test_application_reserves_global_host_slug_and_cluster_namespace(application_database):
    from loom.db.nebius_application_schema import NebiusApplication, NebiusDeploymentNameClaim

    for model in (NebiusApplication, NebiusDeploymentNameClaim):
        assert {column["name"] for column in inspect(application_database).get_columns(model.__tablename__)} == set(model.__table__.columns.keys())
    with application_database.begin() as connection:
        row = application(connection)
        claims = connection.execute(select(NebiusDeploymentNameClaim)).mappings().all()
        assert {(claim["kind"], claim["scope"], claim["name"]) for claim in claims} == {
            ("slug", "", "alice"), ("host", "", "alice.dev.example.com"),
            ("namespace", "cluster-1", "loom-dev-alice"),
        }
        assert all(claim["application_id"] == row["application_id"] and claim["environment_id"] is None for claim in claims)


@pytest.mark.parametrize("first", ["application", "legacy"])
@pytest.mark.parametrize("collision", ["slug", "host"])
def test_application_and_legacy_name_collisions_are_bidirectional(application_database, first, collision):
    from loom.db.nebius_application_schema import NebiusApplication, NebiusDeploymentNameClaim
    from loom.db.nebius_environment_schema import NebiusEnvironment

    create_first, create_second = (application, legacy_registration) if first == "application" else (legacy_registration, application)
    with application_database.begin() as connection:
        create_first(connection)
        changes = {"public_host": "other.dev.example.com"} if collision == "slug" else {"slug": "bob", "public_host": "alice.dev.example.com"}
        with pytest.raises(IntegrityError), connection.begin_nested():
            create_second(connection, **changes)
        assert len(connection.execute(select(NebiusApplication)).all()) + len(connection.execute(select(NebiusEnvironment)).all()) == 1
        assert len(connection.execute(select(NebiusDeploymentNameClaim)).all()) == (3 if first == "application" else 2)


@pytest.mark.parametrize("first", ["application", "legacy"])
def test_legacy_execution_namespace_and_application_namespace_cannot_collide(application_database, first):
    from loom.db.nebius_environment_schema import NebiusEnvironmentNamespace

    with application_database.begin() as connection:
        legacy = legacy_registration(connection, "legacy")
        def reserve_legacy():
            connection.execute(insert(NebiusEnvironmentNamespace).values(
                environment_id=legacy["environment_id"], cluster_id="cluster-1", role="execution", namespace_name="loom-dev-alice",
            ))
        if first == "application":
            application(connection)
            with pytest.raises(IntegrityError), connection.begin_nested():
                reserve_legacy()
        else:
            reserve_legacy()
            with pytest.raises(IntegrityError), connection.begin_nested():
                application(connection)


def test_concurrent_legacy_and_application_create_have_one_name_winner(application_database):
    from loom.db.nebius_application_schema import NebiusApplication
    from loom.db.nebius_environment_schema import NebiusEnvironment

    ready = Barrier(2)
    def claim(create):
        ready.wait(timeout=10)
        try:
            with application_database.begin() as connection:
                create(connection)
            return "reserved"
        except IntegrityError:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(claim, create) for create in (application, legacy_registration)]
        assert sorted(future.result(timeout=15) for future in futures) == ["conflict", "reserved"]
    with application_database.connect() as connection:
        assert len(connection.execute(select(NebiusApplication)).all()) + len(connection.execute(select(NebiusEnvironment)).all()) == 1


@pytest.mark.parametrize("changes", [
    {"application_id": UUID(int=0)}, {"incarnation": UUID(int=0)}, {"data_environment_id": UUID(int=0)},
    {"release_id": UUID(int=0)}, {"deployment_generation": 0}, {"access_generation": 0},
    {"slug": "shared", "application_namespace": "loom-dev-shared"}, {"application_namespace": "loom-dev-other"},
    {"desired_state": "unknown"}, {"purged_at": datetime(2026, 1, 1, tzinfo=UTC)},
    {"public_host": "alice..dev.example.com"}, {"public_host": "alice.-dev.example.com"},
    {"public_host": "a" * 64 + ".example.com"},
])
def test_application_database_rejects_invalid_identity_or_state(application_database, changes):
    with application_database.begin() as connection:
        with pytest.raises(IntegrityError), connection.begin_nested():
            application(connection, **changes)


def test_application_retained_destroy_keeps_names_until_verified_purge(application_database):
    from loom.db.nebius_application_schema import NebiusApplication

    with application_database.begin() as connection:
        old = application(connection, desired_state="destroyed")
        with pytest.raises(IntegrityError), connection.begin_nested():
            application(connection)
        connection.execute(update(NebiusApplication).where(NebiusApplication.application_id == old["application_id"]).values(purged_at=datetime.now(UTC)))
        new = application(connection)
        assert old["incarnation"] != new["incarnation"]
        assert len(connection.execute(select(NebiusApplication)).all()) == 2


def test_verified_legacy_namespace_release_allows_new_application_claim(application_database):
    from loom.db.nebius_environment_schema import NebiusEnvironmentNamespace

    with application_database.begin() as connection:
        row = legacy_registration(connection, desired_state="destroyed", purged_at=datetime.now(UTC))
        connection.execute(insert(NebiusEnvironmentNamespace).values(
            environment_id=row["environment_id"], cluster_id="cluster-1", role="application", namespace_name="loom-dev-alice",
        ))
        with pytest.raises(IntegrityError), connection.begin_nested():
            application(connection)
        connection.execute(delete(NebiusEnvironmentNamespace).where(NebiusEnvironmentNamespace.environment_id == row["environment_id"]))
        application(connection)
        assert connection.scalar(text("SELECT count(*) FROM nebius_deployment_name_claims WHERE environment_id IS NOT NULL")) == 0
        assert connection.scalar(text("SELECT count(*) FROM nebius_deployment_name_claims WHERE application_id IS NOT NULL")) == 3


def test_conflicting_name_update_rolls_back_original_claims(application_database):
    from loom.db.nebius_application_schema import NebiusApplication

    with application_database.begin() as connection:
        row = application(connection)
        legacy_registration(connection, "bob")
        before = connection.execute(text("SELECT * FROM nebius_deployment_name_claims ORDER BY kind,scope,name")).mappings().all()
        with pytest.raises(IntegrityError), connection.begin_nested():
            connection.execute(update(NebiusApplication).where(NebiusApplication.application_id == row["application_id"]).values(public_host="bob.dev.example.com"))
        assert connection.execute(text("SELECT * FROM nebius_deployment_name_claims ORDER BY kind,scope,name")).mappings().all() == before
        assert connection.scalar(select(NebiusApplication.public_host)) == "alice.dev.example.com"


def test_backfill_and_empty_downgrade_preserve_frozen_legacy_records(application_database):
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusEnvironmentNamespace

    migrate(application_database, "downgrade", "0159")
    with application_database.begin() as connection:
        row = legacy_registration(connection, desired_state="destroyed")
        connection.execute(insert(NebiusEnvironmentNamespace).values(
            environment_id=row["environment_id"], cluster_id="cluster-1", role="application", namespace_name="loom-dev-alice",
        ))
        before = connection.execute(select(NebiusEnvironment)).mappings().all()
        names_before = connection.execute(select(NebiusEnvironmentNamespace)).mappings().all()
    migrate(application_database, "upgrade", "0160")
    with application_database.begin() as connection:
        assert connection.execute(select(NebiusEnvironment)).mappings().all() == before
        assert connection.execute(select(NebiusEnvironmentNamespace)).mappings().all() == names_before
        assert connection.scalar(text("SELECT count(*) FROM nebius_deployment_name_claims")) == 3
        # v1 verified purge frees host/slug, not a retained namespace reservation.
        connection.execute(update(NebiusEnvironment).values(purged_at=datetime.now(UTC)))
        assert connection.scalar(text("SELECT count(*) FROM nebius_deployment_name_claims")) == 1
        with pytest.raises(IntegrityError), connection.begin_nested():
            application(connection)
        before = connection.execute(select(NebiusEnvironment)).mappings().all()
    migrate(application_database, "downgrade", "0159")
    with application_database.connect() as connection:
        assert connection.execute(select(NebiusEnvironment)).mappings().all() == before
        assert connection.execute(select(NebiusEnvironmentNamespace)).mappings().all() == names_before


def test_downgrade_refuses_to_erase_application_registration(application_database):
    with application_database.begin() as connection:
        application(connection)
    with pytest.raises(DBAPIError, match="cannot remove application registration history"):
        migrate(application_database, "downgrade", "0159")
    with application_database.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0160"
        assert connection.scalar(text("SELECT count(*) FROM nebius_applications")) == 1
