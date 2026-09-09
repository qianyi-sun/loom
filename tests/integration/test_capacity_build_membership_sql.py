"""Typed build membership uses identical SQL/Python identity and protected guards."""

from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.build_membership_contracts import personal_build_subject_id


def _config(connection):
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "capacity_migrations/alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_migrations"))
    config.attributes["connection"] = connection
    return config


@pytest.mark.parametrize("namespace,owner", ((1, 2), (1, 3), (2, 2), (2**128 - 1, 2**128 - 2)))
async def test_sql_build_subject_identity_matches_python(capacity_session, namespace, owner):
    namespace, owner = UUID(int=namespace), UUID(int=owner)
    actual = await capacity_session.scalar(text(
        "SELECT public.capacity_personal_build_subject_id(CAST(:namespace AS uuid), CAST(:owner AS uuid))"
    ), {"namespace": namespace, "owner": owner})
    assert actual == personal_build_subject_id(namespace, owner)


@pytest.mark.parametrize("namespace,owner", ((None, UUID(int=1)), (UUID(int=1), None), (UUID(int=0), UUID(int=1)), (UUID(int=1), UUID(int=0))))
async def test_sql_build_identity_rejects_absent_or_zero_owners(capacity_session, namespace, owner):
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            await capacity_session.scalar(text(
                "SELECT public.capacity_personal_build_subject_id(CAST(:namespace AS uuid), CAST(:owner AS uuid))"
            ), {"namespace": namespace, "owner": owner})
    assert error.value.orig.sqlstate == "23514"


async def test_sql_build_identity_helper_has_fixed_path_and_no_public_execute(capacity_session):
    row = (await capacity_session.execute(text(
        "SELECT prosecdef, proconfig, EXISTS (SELECT 1 FROM aclexplode(coalesce(proacl, acldefault('f', proowner))) "
        "WHERE grantee = 0 AND privilege_type = 'EXECUTE') FROM pg_proc "
        "WHERE oid = 'public.capacity_personal_build_subject_id(uuid,uuid)'::regprocedure"
    ))).one()
    assert not row[0]
    assert "search_path=pg_catalog" in row[1]
    assert not row[2]


def test_sql_build_identity_upgrade_preserves_existing_extension_location(empty_capacity_postgres_url):
    engine = create_engine(empty_capacity_postgres_url)
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text("CREATE SCHEMA build_test_existing_extensions"))
                connection.execute(text('CREATE EXTENSION "uuid-ossp" WITH SCHEMA build_test_existing_extensions'))
                config = _config(connection)
                command.upgrade(config, "head")
                extension_schema = connection.scalar(text(
                    "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='uuid-ossp'"
                ))
                assert extension_schema == "build_test_existing_extensions"
                assert connection.scalar(text(
                    "SELECT public.capacity_personal_build_subject_id(CAST(:namespace AS uuid), CAST(:owner AS uuid))"
                ), {"namespace": UUID(int=1), "owner": UUID(int=2)}) == personal_build_subject_id(UUID(int=1), UUID(int=2))
                command.downgrade(config, "capacity_0017")
                assert connection.scalar(text("SELECT to_regprocedure('public.capacity_personal_build_subject_id(uuid,uuid)')")) is None
                assert connection.scalar(text("SELECT count(*) FROM pg_extension WHERE extname='uuid-ossp'")) == 1
                command.upgrade(config, "head")
                assert connection.scalar(text("SELECT to_regprocedure('public.capacity_personal_build_subject_id(uuid,uuid)')")) is not None
            finally:
                transaction.rollback()
    finally:
        engine.dispose()


async def test_sql_build_identity_cannot_be_redirected_through_search_path(capacity_session):
    await capacity_session.execute(text("CREATE TEMP TABLE build_identity_path_probe (value integer)"))
    await capacity_session.execute(text(
        "CREATE FUNCTION pg_temp.uuid_generate_v5(uuid,text) RETURNS uuid LANGUAGE sql AS "
        "$$ SELECT '00000000-0000-0000-0000-000000000999'::uuid $$"
    ))
    await capacity_session.execute(text("SET LOCAL search_path=pg_temp,public"))
    actual = await capacity_session.scalar(text(
        "SELECT public.capacity_personal_build_subject_id(CAST(:namespace AS uuid),CAST(:owner AS uuid))"
    ), {"namespace": UUID(int=1), "owner": UUID(int=2)})
    assert actual == personal_build_subject_id(UUID(int=1), UUID(int=2))


def test_sql_build_identity_upgrade_preserves_conflicting_foreign_schema(empty_capacity_postgres_url):
    engine = create_engine(empty_capacity_postgres_url)
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                config = _config(connection)
                command.upgrade(config, "capacity_0017")
                connection.execute(text("CREATE SCHEMA capacity_build_extensions"))
                connection.execute(text("CREATE TABLE capacity_build_extensions.foreign_marker (value integer)"))
                connection.execute(text("INSERT INTO capacity_build_extensions.foreign_marker VALUES (7)"))
                with pytest.raises(RuntimeError, match="already exists"):
                    with connection.begin_nested():
                        command.upgrade(config, "head")
                assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "capacity_0017"
                assert connection.scalar(text("SELECT value FROM capacity_build_extensions.foreign_marker")) == 7
                assert connection.scalar(text("SELECT to_regprocedure('public.capacity_personal_build_subject_id(uuid,uuid)')")) is None
                assert connection.scalar(text("SELECT count(*) FROM pg_extension WHERE extname='uuid-ossp'")) == 0
            finally:
                transaction.rollback()
    finally:
        engine.dispose()


async def test_typed_sql_fixture_uses_installed_epoch_transitions(capacity_session):
    from tests.capacity_build_membership_fixtures import build_request, typed_sql_execution
    from loom_capacity_manager.typed_membership_commands import derive_build_member
    _store, preparation, fleet, execution = await typed_sql_execution(capacity_session)
    assert execution.execution_state == "active"
    assert derive_build_member(build_request(preparation, execution), preparation, fleet).purpose == "personal-build-worker"


async def test_sql_accepts_two_owner_build_events_in_one_shared_revision_sequence(capacity_session):
    from tests.capacity_build_membership_fixtures import build_request, staged_build_event, typed_sql_execution
    from loom_capacity_manager.typed_membership_events import validate_typed_membership_event_prefix
    management, preparation, fleet, execution = await typed_sql_execution(capacity_session)
    first = await staged_build_event(capacity_session, management, preparation, fleet, build_request(preparation, execution))
    capacity_session.add(first)
    await capacity_session.flush()
    second = await staged_build_event(capacity_session, management, preparation, fleet,
        build_request(preparation, execution, owner=88011, revision=1), previous_head=first.head_sha256)
    capacity_session.add(second)
    await capacity_session.flush()
    assert len(validate_typed_membership_event_prefix((first, second), preparation, fleet, execution_epoch=42)) == 2


@pytest.mark.parametrize("field,changed", (("actor", "foreign-manager"), ("owner_id", UUID(int=999)), ("writer_epoch", 999), ("head_sha256", "f" * 64), ("request_digest", "f" * 64), ("previous_sha256", "f" * 64)))
async def test_sql_rejects_changed_typed_event_authority(capacity_session, field, changed):
    from tests.capacity_build_membership_fixtures import build_request, staged_build_event, typed_sql_execution
    management, preparation, fleet, execution = await typed_sql_execution(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet, build_request(preparation, execution))
    setattr(row, field, changed)
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
