"""Actual legacy provisioning to independent sealed profile, never live cutover."""

from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.application_ownership_transfer import transfer_application_ownership
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    application_schema_reference,
    require_application_schema_reference,
)
from loom.dev_instance import derive_identity
from loom.dev_instance_provision import render_create_database_sql, render_role_convergence_sql
from loom.dev_instance_runtime import PsycopgSharedFixtureSqlExecutor, instance_database_url
from loom.personal_dev_capacity_runtime import PsycopgPersonalDevCapacityDatabase, _new_credentials

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.fixture(scope="module")
def transfer_postgres(request: pytest.FixtureRequest):
    with PostgresContainer(
        application_schema_reference(postgres_major=getattr(request, "param", 16)).postgres_image,
        driver="psycopg",
        password=uuid4().hex,
    ).with_bind_ports(5432, ("127.0.0.1", None)) as postgres:
        yield postgres


@pytest.fixture(scope="module")
def transfer_postgres_url(transfer_postgres):
    return transfer_postgres.get_connection_url()


@pytest.fixture
async def transfer_database(
    transfer_postgres_url: str,
    request: pytest.FixtureRequest,
) -> AsyncIterator[tuple[str, str, dict[str, str]]]:
    identity = derive_identity(f"transfer-{uuid4().hex[:8]}")
    password = uuid4().hex
    if getattr(request, "param", None) == "protected-staging":
        from loom.staging_capacity_database_bootstrap import staging_capacity_identity
        identity = staging_capacity_identity()
    if getattr(request, "param", None) in {"staging-credential", "protected-staging"}:
        # Fixed staging DB/role names only inside this disposable PostgreSQL.
        identity = replace(identity, database="loom", db_role="loom")
        password = "ab" * 16
    baseline = getattr(request, "param", None) == "baseline"
    from scripts.application_schema_baseline import BaselineReferenceDatabase
    provisioner = (BaselineReferenceDatabase if baseline else PsycopgPersonalDevCapacityDatabase)(transfer_postgres_url)
    await PsycopgSharedFixtureSqlExecutor(transfer_postgres_url).apply_role_and_database(
        identity,
        role_sql=render_role_convergence_sql(identity, password),
        create_database_sql=render_create_database_sql(identity),
    )
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "migrations/alembic.ini"))
    config.set_main_option("script_location", str(root / "migrations"))
    config.set_main_option(
        "sqlalchemy.url", instance_database_url(transfer_postgres_url, identity, password)
    )
    target = "sealed_app_" + uuid4().hex
    url = (
        make_url(transfer_postgres_url)
        .set(database=identity.database)
        .render_as_string(hide_password=False)
    )
    url = url.replace("postgresql+psycopg://", "postgresql://", 1)
    try:
        command.upgrade(config, "0134" if baseline else "head")
        (
            owner,
            migrator,
            agent,
            executor,
            observer,
            runtime,
            migrator_url,
            _,
        ) = await provisioner._converge_roles(identity, _new_credentials())
        from scripts.build_application_schema_reference import _migrate_reference_guard
        await _migrate_reference_guard(
            guard_head="guard_0030" if baseline else "guard_0033",
            migrator_url=migrator_url,
            owner=owner,
            agent=agent,
            executor=executor,
            observer=observer,
            runtime=runtime,
        )
        await provisioner._seal_migrator(identity, owner=owner, migrator=migrator)
        bindings = {
            identity.db_role: "application-owner",
            owner: "guard-owner",
            migrator: "guard-migrator",
            agent: "guard-agent",
            executor: "guard-executor",
            observer: "guard-observer",
            runtime: "guard-runtime",
            str(make_url(transfer_postgres_url).username): "provisioner",
        }
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(
                psycopg.sql.SQL(
                    "CREATE ROLE {} NOLOGIN NOINHERIT; ALTER ROLE {} NOLOGIN PASSWORD NULL"
                ).format(psycopg.sql.Identifier(target), psycopg.sql.Identifier(identity.db_role))
            )
        yield url, target, bindings
    finally:
        # Explicitly delete this fixture's database before retiring its roles;
        # target owns only this disposable database and objects inside it.
        maintenance = transfer_postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
        with psycopg.connect(maintenance, autocommit=True) as admin:
            admin.execute(
                psycopg.sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    psycopg.sql.Identifier(identity.database)
                )
            )
            admin.execute(
                psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(target))
            )
        await provisioner.destroy(identity)


def _install_staging_readonly(admin):
    from loom_cli.rollout.readonly_database_bootstrap import (
        ReadonlyDatabaseCredential,
        render_readonly_role_sql,
    )

    credential = ReadonlyDatabaseCredential(role="loom_rollout_readonly", database="loom", password=uuid4().hex + uuid4().hex)
    payload = render_readonly_role_sql(credential)
    assert payload.startswith("\\set ON_ERROR_STOP on\n")
    # Execute the installer's unchanged SQL, minus its psql-only error directive.
    # This connection belongs exclusively to the disposable fixture database.
    admin.execute(payload.removeprefix("\\set ON_ERROR_STOP on\n"))
    database = admin.execute("SELECT current_database()").fetchone()[0]
    admin.execute(psycopg.sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC").format(psycopg.sql.Identifier(database)))
    return credential.password


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_acl_profile", ["application-only", "staging-readonly"])
@pytest.mark.parametrize("lose_guard_after_mutation", [False, True])
async def test_closed_handoff_and_replay_preserve_the_bound_coordination_guard(transfer_database, lose_guard_after_mutation, schema_acl_profile):
    from loom.application_database_admission import (
        capture_application_coordination_guard,
        capture_application_database_admission,
        close_application_database_admission,
        reopen_application_database_admission,
        require_application_database_drained,
    )
    from loom.application_login_sealing import seal_application_login
    from loom.staging_mutation_coordination import (
        STAGING_MUTATION_TRY_LOCK_SQL,
        rollout_guard_application_name,
        rollout_guard_bind_sql,
    )
    from loom_cli.rollout.operator.staging_mutation_guard import _HEALTH_SQL
    from tests.integration.test_application_database_admission import _handoff, _maintenance

    url, owner, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    request = dict(request_id="req-transfer-guard", candidate_sha="a" * 40,
                   candidate_tree="b" * 40, generation="c" * 32)
    with psycopg.connect(url, autocommit=True) as admin, _maintenance(admin) as maintenance:
        database = admin.execute("SELECT current_database()").fetchone()[0]
        password = "test-only-guard"
        staging = schema_acl_profile == "staging-readonly"
        legacy_profile = "staging-readonly-legacy-owner" if staging else "legacy-owner"
        if staging:
            password = _install_staging_readonly(admin)
        else:
            admin.execute("CREATE ROLE loom_rollout_readonly LOGIN NOINHERIT PASSWORD 'test-only-guard'")
        # Establish the fixture's existing guard without changing the independent
        # application ACL profile that the ownership transaction must still match.
        admin.execute(psycopg.sql.SQL("GRANT CONNECT ON DATABASE {} TO loom_rollout_readonly").format(psycopg.sql.Identifier(database)))
        try:
            with psycopg.connect(url, user="loom_rollout_readonly", password=password, autocommit=True) as guard:
                if not staging:
                    admin.execute(psycopg.sql.SQL("REVOKE CONNECT ON DATABASE {} FROM loom_rollout_readonly").format(psycopg.sql.Identifier(database)))
                else:
                    assert guard.execute("SELECT count(*) FROM public.staging_mutation_epochs").fetchone() == (0,)
                guard.execute(rollout_guard_bind_sql(rollout_guard_application_name(**request)))
                assert guard.execute(STAGING_MUTATION_TRY_LOCK_SQL).fetchone() == (True,)
                seal_application_login(admin, database=database, role=previous, provisioner_role=provisioner)
                handoff = _handoff(admin)
                target = capture_application_database_admission(
                    maintenance, database=database, owner_role=previous, successor_role=owner,
                    provisioner_role=provisioner, handoff_backend=handoff,
                )
                saved_guard = capture_application_coordination_guard(
                    maintenance, target=target, provisioner_role=provisioner,
                    backend_pid=guard.info.backend_pid, **request,
                )
                close_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
                try:
                    with admin.transaction(), pytest.raises(RuntimeError, match="reconciled sessions"):
                        transfer_application_ownership(admin, owner_role=owner, role_bindings=bindings)
                    if staging:
                        # The unchanged personal-development pin must not admit
                        # staging's real installer grants, even with the guard bound.
                        with admin.transaction(), pytest.raises(RuntimeError, match="trusted reference"):
                            transfer_application_ownership(
                                admin, owner_role=owner, role_bindings=bindings,
                                admission_target=target, coordination_guard=saved_guard,
                            )
                        for drift in (
                            "GRANT UPDATE ON public.trials TO loom_rollout_readonly",
                            "GRANT SELECT ON public.trials TO loom_rollout_readonly WITH GRANT OPTION",
                            "GRANT CREATE ON SCHEMA public TO loom_rollout_readonly",
                            "REVOKE SELECT ON public.trials FROM loom_rollout_readonly",
                        ):
                            with admin.transaction():
                                admin.execute(drift)
                                with pytest.raises(RuntimeError, match="trusted reference"):
                                    transfer_application_ownership(
                                        admin, owner_role=owner, role_bindings=bindings,
                                        admission_target=target, coordination_guard=saved_guard,
                                        schema_acl_profile=schema_acl_profile,
                                    )
                                assert admin.execute("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()").fetchone() == (previous,)
                                raise psycopg.Rollback()
                    if lose_guard_after_mutation:
                        changed = []
                        class LoseGuardAfterMutation:
                            @property
                            def info(self):
                                return admin.info

                            def transaction(self):
                                return admin.transaction()

                            def execute(self, query):
                                result = admin.execute(query)
                                rendered = query if isinstance(query, str) else query.as_string(admin)
                                if rendered.startswith("ALTER DATABASE "):
                                    assert admin.execute("SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()").fetchone() == (owner,)
                                    assert guard.execute("SELECT pg_advisory_unlock(5498691230183247727)").fetchone() == (True,)
                                    changed.append(True)
                                return result

                        with admin.transaction():
                            with pytest.raises(RuntimeError, match="coordination guard"):
                                transfer_application_ownership(
                                    LoseGuardAfterMutation(), owner_role=owner, role_bindings=bindings,
                                    admission_target=target, coordination_guard=saved_guard,
                                    schema_acl_profile=schema_acl_profile,
                                )
                            assert changed == [True]
                            # The caller transaction survives, but EVERY ownership,
                            # ACL and definer mutation in the helper rolled back.
                            require_application_schema_reference(
                                read_application_schema_inventory(admin, role_bindings=bindings),
                                profile=legacy_profile,
                            )
                        assert guard.execute(_HEALTH_SQL).fetchone() == (saved_guard.backend.pid, False)
                        return  # A lost guard is never reacquired to resume this test operation.
                    for _ in range(2):
                        require_application_database_drained(
                            maintenance, target=target, provisioner_role=provisioner,
                            handoff_backend=handoff, coordination_guard=saved_guard,
                        )
                        with admin.transaction():
                            transfer_application_ownership(
                                admin, owner_role=owner, role_bindings=bindings,
                                admission_target=target, coordination_guard=saved_guard,
                                schema_acl_profile=schema_acl_profile,
                            )
                            guard.execute("SET statement_timeout='1s'")
                            assert guard.execute(_HEALTH_SQL).fetchone() == (saved_guard.backend.pid, True)
                    guard.execute("SELECT pg_advisory_unlock(5498691230183247727)")
                    with admin.transaction(), pytest.raises(RuntimeError, match="coordination guard"):
                        transfer_application_ownership(
                            admin, owner_role=owner, role_bindings=bindings,
                            admission_target=target, coordination_guard=saved_guard,
                            schema_acl_profile=schema_acl_profile,
                        )
                finally:
                    reopen_application_database_admission(maintenance, target=target, provisioner_role=provisioner)
        finally:
            admin.execute("DROP OWNED BY loom_rollout_readonly")
            admin.execute("DROP ROLE loom_rollout_readonly")


@pytest.mark.asyncio
async def test_transfer_matches_independent_destination_preserves_data_and_replays(
    transfer_database,
):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    team = uuid4()
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute("INSERT INTO public.teams(id,name) VALUES (%s,'before-transfer')", (team,))
        admin.execute(
            "CREATE SCHEMA foreign_work; CREATE TABLE foreign_work.untouched (id integer); INSERT INTO foreign_work.untouched VALUES (42)"
        )
        for isolation in ("REPEATABLE READ", "SERIALIZABLE"):
            admin.execute("BEGIN ISOLATION LEVEL " + isolation)
            try:
                with pytest.raises(RuntimeError, match="READ COMMITTED"):
                    transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
            finally:
                admin.execute("ROLLBACK")
        with admin.transaction():
            admin.execute(
                psycopg.sql.SQL("SET LOCAL ROLE {}").format(psycopg.sql.Identifier(previous))
            )
            with pytest.raises(RuntimeError, match="protected administrator"):
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
            assert admin.execute("SELECT current_user").fetchone() == (previous,)
        for _ in range(2):
            with admin.transaction():
                admin.execute(
                    "SET LOCAL search_path=public,pg_catalog; SET LOCAL lock_timeout='50ms'"
                )
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
                assert admin.execute("SHOW search_path").fetchone() == ("public, pg_catalog",)
                assert admin.execute("SHOW lock_timeout").fetchone() == ("50ms",)
        with admin.transaction():
            sealed = {**bindings, previous: "application-runtime", target: "application-owner"}
            require_application_schema_reference(
                read_application_schema_inventory(admin, role_bindings=sealed),
                profile="sealed-owner",
            )
        assert admin.execute("SELECT id FROM foreign_work.untouched").fetchall() == [(42,)]
        admin.execute(psycopg.sql.SQL("SET ROLE {}").format(psycopg.sql.Identifier(previous)))
        assert admin.execute("SELECT name FROM public.teams WHERE id=%s", (team,)).fetchone() == (
            "before-transfer",
        )
        admin.execute("UPDATE public.teams SET name='after-transfer' WHERE id=%s", (team,))
        for statement in (
            "ALTER TABLE public.trials DISABLE TRIGGER ALL",
            "CREATE TABLE public.escape(id integer)",
            "CREATE TABLE loom_capacity_guard.escape(id integer)",
            "UPDATE public.alembic_version SET version_num='0001'",
            "SELECT public.loom_drop_trial_writer_triggers()",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                admin.execute(statement)
        admin.execute("RESET ROLE")


@pytest.mark.asyncio
@pytest.mark.parametrize("privilege", ["MAINTAIN", "pg_maintain"])
async def test_pg17_runtime_maintenance_authority_is_rejected(transfer_database, privilege):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    with psycopg.connect(url, autocommit=True) as admin:
        if admin.info.server_version // 10000 == 16:
            pytest.skip("MAINTAIN and pg_maintain were introduced in PostgreSQL 17")
        with admin.transaction():
            transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
        sealed = {**bindings, previous: "application-runtime", target: "application-owner"}
        with admin.transaction():
            require_application_schema_reference(
                read_application_schema_inventory(admin, role_bindings=sealed), profile="sealed-owner"
            )
        if privilege == "MAINTAIN":
            assert admin.execute(
                "SELECT has_table_privilege(%s, 'public.trials', 'MAINTAIN')", (previous,)
            ).fetchone() == (False,)
            admin.execute(
                psycopg.sql.SQL("GRANT MAINTAIN ON public.trials TO {}").format(
                    psycopg.sql.Identifier(previous)
                )
            )
            with admin.transaction(), pytest.raises(RuntimeError, match="trusted reference"):
                require_application_schema_reference(
                    read_application_schema_inventory(admin, role_bindings=sealed),
                    profile="sealed-owner",
                )
        else:
            admin.execute(
                psycopg.sql.SQL("GRANT pg_maintain TO {}").format(psycopg.sql.Identifier(previous))
            )
        with admin.transaction(), pytest.raises(
            RuntimeError, match=r"trusted reference|memberships are not sealed"
        ):
            transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
        assert admin.execute(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()"
        ).fetchone() == (target,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        "version",
        "empty_version",
        "extra_version",
        "view_version",
        "routine",
        "owner_login",
        "runtime_login",
        "target_authority",
    ],
)
async def test_transfer_refuses_drift_without_partial_ownership_changes(transfer_database, drift):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    with psycopg.connect(url, autocommit=True) as admin:
        if drift == "version":
            admin.execute("UPDATE public.alembic_version SET version_num='0001'")
        elif drift == "empty_version":
            admin.execute("DELETE FROM public.alembic_version")
        elif drift == "extra_version":
            admin.execute("INSERT INTO public.alembic_version VALUES ('untrusted')")
        elif drift == "view_version":
            admin.execute(
                "ALTER TABLE public.alembic_version RENAME TO old_version; CREATE FUNCTION public.unsafe_version() RETURNS text LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'untrusted version reader executed'; END$$; CREATE VIEW public.alembic_version AS SELECT public.unsafe_version() AS version_num"
            )
        elif drift == "routine":
            admin.execute(
                "CREATE OR REPLACE FUNCTION public.trials_inflight_delta() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$"
            )
        elif drift in {"owner_login", "runtime_login"}:
            role = target if drift == "owner_login" else previous
            admin.execute(
                psycopg.sql.SQL("ALTER ROLE {} LOGIN").format(psycopg.sql.Identifier(role))
            )
        else:
            admin.execute(
                psycopg.sql.SQL("GRANT SELECT ON public.teams TO {}").format(
                    psycopg.sql.Identifier(target)
                )
            )
        with admin.transaction():
            before = read_application_schema_inventory(admin, role_bindings=bindings)
        with pytest.raises(RuntimeError) as error:
            with admin.transaction():
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
        assert "untrusted version reader executed" not in str(error.value)
        with admin.transaction():
            assert read_application_schema_inventory(admin, role_bindings=bindings) == before
        assert admin.execute(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()"
        ).fetchone() == (previous,)


@pytest.mark.asyncio
async def test_transfer_refuses_busy_relation_and_releases_partial_locks(transfer_database):
    url, target, bindings = transfer_database
    with psycopg.connect(url, autocommit=True) as admin, psycopg.connect(url) as busy:
        busy.execute("SELECT id FROM public.trials")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            with admin.transaction():
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
        # Locks acquired before trials must have been released by refusal.
        busy.execute("LOCK TABLE public.teams IN ACCESS EXCLUSIVE MODE NOWAIT")
        busy.rollback()
        with admin.transaction():
            transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)


@pytest.mark.asyncio
async def test_transfer_refuses_foreign_owned_objects_in_the_same_database(transfer_database):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            "CREATE SCHEMA foreign_work; CREATE TABLE foreign_work.untouched (id integer)"
        )
        admin.execute(
            psycopg.sql.SQL("ALTER TABLE foreign_work.untouched OWNER TO {}").format(
                psycopg.sql.Identifier(previous)
            )
        )
        with admin.transaction():
            before = read_application_schema_inventory(admin, role_bindings=bindings)
            with pytest.raises(RuntimeError, match="foreign"):
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
            assert read_application_schema_inventory(admin, role_bindings=bindings) == before


@pytest.mark.asyncio
async def test_transfer_replay_refuses_lost_private_bridge_grants(transfer_database):
    url, target, bindings = transfer_database
    signature = (
        "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)"
    )
    with psycopg.connect(url, autocommit=True) as admin:
        with admin.transaction():
            transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
        admin.execute(
            psycopg.sql.SQL("REVOKE EXECUTE ON FUNCTION " + signature + " FROM {}").format(
                psycopg.sql.Identifier(target)
            )
        )
        with admin.transaction():
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="bridge"):
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
            assert admin.execute(
                "SELECT has_function_privilege(%s,%s,'EXECUTE')", (target, signature)
            ).fetchone() == (False,)


@pytest.mark.asyncio
async def test_transfer_late_refusal_rolls_back_inside_the_callers_transaction(transfer_database):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    signature = (
        "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)"
    )
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            psycopg.sql.SQL("REVOKE EXECUTE ON FUNCTION " + signature + " FROM {}").format(
                psycopg.sql.Identifier(previous)
            )
        )
        with admin.transaction():
            before = read_application_schema_inventory(admin, role_bindings=bindings)
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="bridge"):
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
            assert read_application_schema_inventory(admin, role_bindings=bindings) == before


@pytest.mark.asyncio
async def test_transfer_refuses_connected_old_owner_until_session_is_retired(transfer_database):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    password = uuid4().hex
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            psycopg.sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                psycopg.sql.Identifier(previous), psycopg.sql.Literal(password)
            )
        )
        old_url = (
            make_url(url)
            .set(username=previous, password=password)
            .render_as_string(hide_password=False)
        )
        with psycopg.connect(old_url) as old:
            old.execute("SELECT current_user")
            admin.execute(
                psycopg.sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(
                    psycopg.sql.Identifier(previous)
                )
            )
            with admin.transaction():
                with pytest.raises(RuntimeError, match="reconciled sessions"):
                    transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
            assert old.execute("SELECT current_user").fetchone() == (previous,)
        with admin.transaction():
            transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)


@pytest.mark.asyncio
async def test_transfer_refuses_foreign_global_default_grants_without_changing_them(
    transfer_database,
):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    foreign = "foreign_creator_" + uuid4().hex
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            psycopg.sql.SQL(
                "CREATE ROLE {} NOLOGIN; ALTER DEFAULT PRIVILEGES FOR ROLE {} GRANT SELECT ON TABLES TO {}"
            ).format(*(psycopg.sql.Identifier(role) for role in (foreign, foreign, previous)))
        )
        try:
            before = admin.execute(
                "SELECT defaclrole,defaclnamespace,defaclobjtype,defaclacl FROM pg_default_acl ORDER BY oid"
            ).fetchall()
            with admin.transaction():
                with pytest.raises(RuntimeError, match="foreign"):
                    transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
                assert (
                    admin.execute(
                        "SELECT defaclrole,defaclnamespace,defaclobjtype,defaclacl FROM pg_default_acl ORDER BY oid"
                    ).fetchall()
                    == before
                )
        finally:
            admin.execute(
                psycopg.sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} REVOKE SELECT ON TABLES FROM {}; DROP ROLE {}"
                ).format(*(psycopg.sql.Identifier(role) for role in (foreign, previous, foreign)))
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("privilege", ["private_create", "private_grant_option"])
async def test_transfer_refuses_residual_private_authority(transfer_database, privilege):
    url, target, bindings = transfer_database
    previous = next(role for role, alias in bindings.items() if alias == "application-owner")
    with psycopg.connect(url, autocommit=True) as admin:
        statement = (
            "GRANT CREATE ON SCHEMA loom_capacity_guard TO {}"
            if privilege == "private_create"
            else "GRANT EXECUTE ON FUNCTION loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer) TO {} WITH GRANT OPTION"
        )
        admin.execute(psycopg.sql.SQL(statement).format(psycopg.sql.Identifier(previous)))
        with admin.transaction():
            with pytest.raises(RuntimeError, match="private authority"):
                transfer_application_ownership(admin, owner_role=target, role_bindings=bindings)
