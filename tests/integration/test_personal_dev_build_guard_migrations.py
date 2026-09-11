"""The management build ledger has no application-runtime write authority."""

from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.fixture
def build_guard_database(isolated_migration_postgres_url):
    url = make_url(isolated_migration_postgres_url)
    suffix = uuid4().hex
    owner, migrator, agent = (f"build_{kind}_{suffix}" for kind in ("owner", "migrator", "agent"))
    engine = create_engine(url)
    quote = engine.dialect.identifier_preparer.quote
    password = "isolated-build-guard-test"
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"CREATE ROLE {quote(owner)} NOLOGIN NOINHERIT")
            for role in (migrator, agent):
                connection.exec_driver_sql(f"CREATE ROLE {quote(role)} LOGIN NOINHERIT PASSWORD '{password}'")
            connection.exec_driver_sql(f"GRANT {quote(owner)} TO {quote(migrator)}")
            connection.exec_driver_sql(f"GRANT CREATE ON DATABASE {quote(url.database)} TO {quote(owner)}")
            connection.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {quote(owner)}")
            connection.exec_driver_sql(f"GRANT REFERENCES ON public.personal_dev_build_platform_requests TO {quote(owner)}")
        root = Path(__file__).resolve().parents[2] / "capacity_build_guard_migrations"
        config = Config(str(root / "alembic.ini"))
        config.set_main_option("script_location", str(root))
        config.set_main_option("sqlalchemy.url", url.set(username=migrator, password=password).render_as_string(hide_password=False).replace("%", "%%"))
        config.attributes.update(build_guard_owner_role=owner, build_guard_agent_role=agent)
        yield config, engine, owner, agent, url.set(username=agent, password=password)
    finally:
        with engine.begin() as connection:
            for role in (agent, migrator, owner):
                connection.exec_driver_sql(f"DROP OWNED BY {quote(role)}")
            for role in (agent, migrator, owner):
                connection.exec_driver_sql(f"DROP ROLE {quote(role)}")
        engine.dispose()


def test_build_guard_is_private_owner_only_and_empty_rollback_is_reversible(build_guard_database):
    config, engine, owner, agent, agent_url = build_guard_database
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0023"
        assert connection.scalar(text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname='loom_capacity_build_guard'")) == owner
        assert connection.scalar(text("SELECT has_schema_privilege(:agent,'loom_capacity_build_guard','USAGE')"), {"agent": agent})
    runtime = create_engine(agent_url)
    try:
        with runtime.connect() as connection, pytest.raises(DBAPIError, match="permission denied"):
            connection.execute(text("INSERT INTO loom_capacity_build_guard.installations DEFAULT VALUES"))
    finally:
        runtime.dispose()
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 0


def test_build_guard_refuses_privileged_agent(build_guard_database):
    config, engine, _owner, agent, _url = build_guard_database
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER ROLE {engine.dialect.identifier_preparer.quote(agent)} CREATEDB")
    with pytest.raises(RuntimeError, match="least-privileged"):
        command.upgrade(config, "head")


@pytest.mark.parametrize("surface", ["TABLES", "FUNCTIONS"])
def test_build_guard_rejects_foreign_default_privileges(build_guard_database, surface):
    config, engine, owner, _agent, _url = build_guard_database
    foreign = f"build_foreign_{uuid4().hex}"
    quote = engine.dialect.identifier_preparer.quote
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"CREATE ROLE {quote(foreign)} NOLOGIN")
            connection.exec_driver_sql(f"ALTER DEFAULT PRIVILEGES FOR ROLE {quote(owner)} GRANT ALL ON {surface} TO {quote(foreign)}")
        with pytest.raises(RuntimeError, match="privilege"):
            command.upgrade(config, "head")
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"DROP OWNED BY {quote(foreign)}")
            connection.exec_driver_sql(f"DROP ROLE {quote(foreign)}")


@pytest.mark.parametrize("surface", ["schema", "table", "column", "function", "prepare-grant-option", "prepare-search-path"])
def test_build_guard_at_head_rejects_privilege_drift(build_guard_database, surface):
    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    target = {
        "schema": "CREATE ON SCHEMA loom_capacity_build_guard",
        "table": "INSERT ON loom_capacity_build_guard.installations",
        "column": "INSERT (id) ON loom_capacity_build_guard.installations",
        "function": "EXECUTE ON FUNCTION loom_capacity_build_guard.reject_evidence_mutation()",
    }.get(surface)
    with engine.begin() as connection:
        if surface == "prepare-search-path":
            connection.exec_driver_sql("ALTER FUNCTION loom_capacity_build_guard.prepare_plan(uuid,jsonb,bytea,text,jsonb) SET search_path=public")
        elif surface == "prepare-grant-option":
            connection.exec_driver_sql("GRANT EXECUTE ON FUNCTION loom_capacity_build_guard.prepare_plan(uuid,jsonb,bytea,text,jsonb) "
                f"TO {engine.dialect.identifier_preparer.quote(agent)} WITH GRANT OPTION")
        else:
            connection.exec_driver_sql(f"GRANT {target} TO {engine.dialect.identifier_preparer.quote(agent)}")
    with pytest.raises(RuntimeError, match="privilege"):
        command.upgrade(config, "head")


def test_build_guard_rejects_preexisting_schema_create_grant(build_guard_database):
    config, engine, owner, agent, _url = build_guard_database
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as connection:
        connection.exec_driver_sql(f"CREATE SCHEMA loom_capacity_build_guard AUTHORIZATION {quote(owner)}")
        connection.exec_driver_sql(f"GRANT CREATE ON SCHEMA loom_capacity_build_guard TO {quote(agent)}")
    with pytest.raises(RuntimeError, match="privilege"):
        command.upgrade(config, "head")


@pytest.mark.parametrize("boundary", ["function", "source-function", "contract-function", "execute", "usage",
    "publication-function", "publication-execute", "publication-search-path", "publication-grant-option"])
def test_build_guard_requires_revision_callable_surface(build_guard_database, boundary):
    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    quote = engine.dialect.identifier_preparer.quote
    function = "loom_capacity_build_guard.prepare_plan(uuid,jsonb,bytea,text,jsonb)"
    with engine.begin() as connection:
        if boundary.startswith("publication-"):
            publication = "loom_capacity_build_guard.authorize_publication(uuid,uuid)"
            if boundary == "publication-function":
                connection.exec_driver_sql(f"DROP FUNCTION {publication}")
            elif boundary == "publication-execute":
                connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {publication} FROM {quote(agent)}")
            elif boundary == "publication-search-path":
                connection.exec_driver_sql(f"ALTER FUNCTION {publication} SET search_path=public")
            else:
                connection.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {publication} TO {quote(agent)} WITH GRANT OPTION")
        elif boundary == "source-function":
            connection.exec_driver_sql("DROP FUNCTION loom_capacity_build_guard.assert_current_source(uuid,uuid,jsonb,bytea,text)")
        elif boundary == "contract-function":
            connection.exec_driver_sql("DROP FUNCTION loom_capacity_build_guard.assert_plan_contract(jsonb,bytea)")
        elif boundary == "function":
            connection.exec_driver_sql(f"DROP FUNCTION {function}")
        elif boundary == "execute":
            connection.exec_driver_sql(f"REVOKE EXECUTE ON FUNCTION {function} FROM {quote(agent)}")
        else:
            connection.exec_driver_sql(f"REVOKE USAGE ON SCHEMA loom_capacity_build_guard FROM {quote(agent)}")
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


def test_retained_installation_is_immutable_and_blocks_downgrade(build_guard_database):
    config, engine, owner, _agent, _url = build_guard_database
    command.upgrade(config, "head")
    with engine.begin() as connection:
        connection.exec_driver_sql(f"SET LOCAL ROLE {engine.dialect.identifier_preparer.quote(owner)}")
        connection.execute(text("""INSERT INTO loom_capacity_build_guard.installations
            (id, owner_user_id, subject_id, subject_incarnation, deployment_generation,
             reporter_incarnation, payload, wire_payload, payload_sha256)
            VALUES (:id, :owner, :subject, :incarnation, 1, :reporter, '{}'::jsonb, :wire, :digest)
        """), {"id": uuid4(), "owner": uuid4(), "subject": uuid4(), "incarnation": uuid4(),
            "reporter": uuid4(), "wire": b"{}", "digest": sha256(b"{}").hexdigest()})
    for statement in (
        "UPDATE loom_capacity_build_guard.installations SET deployment_generation=2",
        "DELETE FROM loom_capacity_build_guard.installations",
        "TRUNCATE loom_capacity_build_guard.installations CASCADE",
    ):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(config, "base")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) == "build_guard_0023"
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.installations")) == 1


async def test_only_one_exact_assignment_can_hold_a_platform_request(build_guard_database, sessions, tmp_path):
    from loom.personal_dev_build_platform_requests import stage_platform_requests
    from tests.integration.test_personal_dev_build_platform_requests import build_service
    from tests.integration.test_personal_dev_native_builder_store import _NOW, _seed_running_attempt

    config, engine, owner, _agent, agent_url = build_guard_database
    command.upgrade(config, "head")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        requests = await stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW)
    request = requests[0]
    installation_id, first, second = uuid4(), uuid4(), uuid4()
    wire = {"wire": b"{}", "digest": sha256(b"{}").hexdigest()}
    with engine.begin() as connection:
        connection.exec_driver_sql(f"SET LOCAL ROLE {engine.dialect.identifier_preparer.quote(owner)}")
        connection.execute(text("""INSERT INTO loom_capacity_build_guard.installations
            (id, owner_user_id, subject_id, subject_incarnation, deployment_generation,
             reporter_incarnation, payload, wire_payload, payload_sha256)
            VALUES (:id, :owner, :subject, :incarnation, 1, :reporter, '{}'::jsonb, :wire, :digest)
        """), {**wire, "id": installation_id, "owner": member.owner_id, "subject": request.subject_id,
            "incarnation": request.subject_incarnation, "reporter": member.configuration.demand_reporter_incarnation})
        for identity in (first, second):
            connection.execute(text("""INSERT INTO loom_capacity_build_guard.plans
                (id, installation_id, expires_at, payload, wire_payload, payload_sha256)
                VALUES (:id, :installation, now(), '{}'::jsonb, :wire, :digest)
            """), {**wire, "id": identity, "installation": installation_id})
            connection.execute(text("""INSERT INTO loom_capacity_build_guard.assignments
                (id, plan_id, request_id, submission_intent_id, shape_instance_id, shape_slot_index,
                 payload, wire_payload, payload_sha256)
                VALUES (:id, :id, :request, :id, 'native-shape', 0, '{}'::jsonb, :wire, :digest)
            """), {**wire, "id": identity, "request": request.id})
        insert = text("INSERT INTO loom_capacity_build_guard.request_holds VALUES (:request, :assignment)")
        connection.execute(insert, {"request": request.id, "assignment": first})
        with pytest.raises(DBAPIError, match="duplicate key"):
            with connection.begin_nested():
                connection.execute(insert, {"request": request.id, "assignment": second})
        with pytest.raises(DBAPIError, match="foreign key"):
            with connection.begin_nested():
                connection.execute(insert, {"request": uuid4(), "assignment": second})
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    agent_engine = create_engine(agent_url)
    try:
        with agent_engine.connect() as connection, pytest.raises(DBAPIError, match="permission denied"):
            connection.execute(text("DELETE FROM loom_capacity_build_guard.request_holds"))
    finally:
        agent_engine.dispose()
