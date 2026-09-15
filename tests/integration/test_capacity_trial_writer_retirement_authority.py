"""Least-authority and replay boundaries for exact public trigger retirement."""

import importlib
from uuid import uuid4

import pytest
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from loom.trial_writer_trigger_authority import trial_writer_trigger_retirement_ddl
from tests.integration.test_capacity_agent_store import (
    _initialize_and_register,
    _seed_trial,
    _value,
)
from tests.integration.test_capacity_trial_writer_fence import _control_session, _legacy_engine
from tests.integration.test_capacity_trial_writer_retirement import _downgrade


@pytest.mark.parametrize("inherited_owner", [False, True])
def test_owner_login_sealing_does_not_revoke_open_session_ddl_authority(
    capacity_guard_database: dict[str, object], inherited_owner: bool
) -> None:
    """Characterize the real primitive required by application-owner cutover.

    NOLOGIN/password removal and membership revocation are not proof that an
    already connected application has lost ownership. Moving both the table and
    its definer helper to a fresh owner removes that authority from the same
    surviving session. This is not a production ownership-convergence API.
    """
    database = capacity_guard_database
    trial = _seed_trial(database)
    admin = create_engine(_value(database, "admin_url"))
    suffix = uuid4().hex
    login_role = f"writer_login_{suffix}"
    prior_owner = f"writer_owner_{suffix}" if inherited_owner else login_role
    new_owner = f"writer_sealed_{suffix}"
    roles = tuple(dict.fromkeys((login_role, prior_owner, new_owner)))
    quote = admin.dialect.identifier_preparer.quote
    password = uuid4().hex
    legacy = create_engine(
        make_url(_value(database, "admin_url")).set(username=login_role, password=password)
    )
    original_owner = None
    provisioned = False
    try:
        with admin.begin() as provisioner:
            original_owner = provisioner.execute(
                text(
                    "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.trials'::regclass"
                )
            ).scalar_one()
            for role in roles:
                provisioner.exec_driver_sql(
                    f"CREATE ROLE {quote(role)} NOLOGIN NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
                )
                provisioner.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {quote(role)}")
            provisioner.exec_driver_sql(
                f"ALTER ROLE {quote(login_role)} LOGIN PASSWORD '{password}'"
            )
            if inherited_owner:
                provisioner.exec_driver_sql(f"GRANT {quote(prior_owner)} TO {quote(login_role)}")
            provisioner.exec_driver_sql(f"ALTER TABLE public.trials OWNER TO {quote(prior_owner)}")
            provisioner.exec_driver_sql(
                f"ALTER FUNCTION public.loom_drop_trial_writer_triggers() OWNER TO {quote(prior_owner)}"
            )
        provisioned = True
        with legacy.connect() as surviving:
            if inherited_owner:
                surviving.exec_driver_sql(f"SET ROLE {quote(prior_owner)}")
            assert surviving.execute(text("SELECT current_user")).scalar_one() == prior_owner
            pid = surviving.execute(text("SELECT pg_backend_pid()")).scalar_one()
            surviving.commit()
            with admin.begin() as sealer:
                sealer.exec_driver_sql(f"ALTER ROLE {quote(login_role)} NOLOGIN PASSWORD NULL")
                if inherited_owner:
                    sealer.exec_driver_sql(f"REVOKE {quote(prior_owner)} FROM {quote(login_role)}")
            # A fresh connection is denied, but neither operation revokes the
            # current role of an existing backend. A password rotation alone
            # must never become evidence that legacy writers were disabled.
            fresh = create_engine(legacy.url)
            try:
                with pytest.raises(DBAPIError):
                    with fresh.connect():
                        pytest.fail("sealed owner login unexpectedly connected")
            finally:
                fresh.dispose()
            surviving.exec_driver_sql(
                "ALTER TABLE public.trials DISABLE TRIGGER capacity_guard_lock_trial_writer"
            )
            surviving.commit()
            surviving.exec_driver_sql(
                "ALTER TABLE public.trials ENABLE TRIGGER capacity_guard_lock_trial_writer"
            )
            surviving.commit()

            with admin.begin() as transfer:
                transfer.exec_driver_sql(f"ALTER TABLE public.trials OWNER TO {quote(new_owner)}")
                transfer.exec_driver_sql(
                    f"ALTER FUNCTION public.loom_drop_trial_writer_triggers() OWNER TO {quote(new_owner)}"
                )
                transfer.exec_driver_sql(
                    f"GRANT SELECT, UPDATE ON public.trials TO {quote(prior_owner)}"
                )
            for ddl in (
                "ALTER TABLE public.trials DISABLE TRIGGER capacity_guard_lock_trial_writer",
                "DROP TRIGGER capacity_guard_lock_trial_writer ON public.trials",
                "SELECT public.loom_drop_trial_writer_triggers()",
            ):
                with pytest.raises(DBAPIError) as denied:
                    surviving.exec_driver_sql(ddl)
                assert denied.value.orig.sqlstate == "42501"
                surviving.rollback()
            assert surviving.execute(text("SELECT pg_backend_pid()")).scalar_one() == pid
            surviving.execute(
                text("UPDATE public.trials SET submit_priority = 113 WHERE id = :id"), {"id": trial}
            )
            surviving.commit()
            assert (
                surviving.execute(
                    text("SELECT submit_priority FROM public.trials WHERE id = :id"), {"id": trial}
                ).scalar_one()
                == 113
            )
    finally:
        legacy.dispose()
        if provisioned:
            assert original_owner is not None
            with admin.begin() as cleanup:
                cleanup.exec_driver_sql(
                    f"ALTER TABLE public.trials OWNER TO {quote(original_owner)}"
                )
                cleanup.exec_driver_sql(
                    f"ALTER FUNCTION public.loom_drop_trial_writer_triggers() OWNER TO {quote(original_owner)}"
                )
                for role in roles:
                    cleanup.exec_driver_sql(f"REVOKE ALL ON public.trials FROM {quote(role)}")
                    cleanup.exec_driver_sql(f"REVOKE ALL ON SCHEMA public FROM {quote(role)}")
                    cleanup.exec_driver_sql(f"DROP ROLE {quote(role)}")
        admin.dispose()


@pytest.mark.asyncio
async def test_ordinary_runtime_cannot_call_trigger_retirement(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = _legacy_engine(capacity_guard_database)
    try:
        with pytest.raises(DBAPIError) as denied:
            with engine.begin() as runtime:
                runtime.execute(text("SELECT public.loom_drop_trial_writer_triggers()"))
        assert denied.value.orig.sqlstate == "42501"
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["disabled", "function", "arguments"])
async def test_retirement_refuses_changed_trigger_identity(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    database = capacity_guard_database
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as owner:
            if drift == "disabled":
                owner.exec_driver_sql(
                    "ALTER TABLE public.trials DISABLE TRIGGER capacity_guard_lock_trial_writer"
                )
            else:
                owner.exec_driver_sql(
                    "DROP TRIGGER capacity_guard_lock_trial_writer ON public.trials"
                )
                function = (
                    "account_trial_writer_mutation"
                    if drift == "function"
                    else "lock_trial_writer_statement"
                )
                arguments = "'unexpected'" if drift == "arguments" else ""
                owner.exec_driver_sql(
                    "CREATE TRIGGER capacity_guard_lock_trial_writer "
                    "BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.trials "
                    f"FOR EACH STATEMENT EXECUTE FUNCTION loom_capacity_guard.{function}({arguments})"
                )
        with pytest.raises(DBAPIError) as denied:
            _downgrade(database, monkeypatch)
        assert denied.value.orig.sqlstate == "55000"
        assert "identity changed" in str(denied.value.orig)
        with engine.connect() as observer:
            assert (
                observer.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0035"
            )
            assert (
                observer.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger WHERE tgrelid='public.trials'::regclass "
                        "AND tgname IN ('capacity_guard_lock_trial_writer', 'zz_capacity_guard_account_trial_writer')"
                    )
                ).scalar_one()
                == 2
            )
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_retirement_and_upgrade_reuse_exact_retained_helper(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = capacity_guard_database
    # Reuse the full downgrade environment but capture its config for upgrade.
    original_downgrade = command.downgrade

    def round_trip(config, revision):
        original_downgrade(config, revision)
        command.upgrade(config, "guard_0033")

    monkeypatch.setattr(command, "downgrade", round_trip)
    _downgrade(database, monkeypatch)
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as owner:
            owner.exec_driver_sql(
                trial_writer_trigger_retirement_ddl(
                    guard_owner=_value(database, "owner_role")
                ).as_string()
            )
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_retirement_refuses_in_flight_initialization_and_restores_triggers(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = capacity_guard_database
    _, registration = await _initialize_and_register(database)
    async with _control_session(database) as initializer:
        await initializer.execute(
            text("SELECT loom_capacity_guard.initialize_trial_writer_fence(:agent, :writer)"),
            {"agent": registration.agent_incarnation, "writer": uuid4()},
        )
        with pytest.raises(DBAPIError) as busy:
            _downgrade(database, monkeypatch)
        assert busy.value.orig.sqlstate == "55P03"
        assert (
            await initializer.execute(
                text(
                    "SELECT count(*) FROM pg_trigger WHERE tgrelid='public.trials'::regclass "
                    "AND tgname IN ('capacity_guard_lock_trial_writer', 'zz_capacity_guard_account_trial_writer') "
                    "AND tgenabled='O'"
                )
            )
        ).scalar_one() == 2


@pytest.mark.asyncio
async def test_initialization_cannot_cross_in_flight_retirement(
    capacity_guard_database: dict[str, object],
) -> None:
    database = capacity_guard_database
    _, registration = await _initialize_and_register(database)
    migration = importlib.import_module(
        "capacity_guard_migrations.versions.guard_0033_trial_writer_interception"
    )
    async with _control_session(database) as retirement:

        def downgrade(connection):
            with Operations.context(MigrationContext.configure(connection)):
                migration.downgrade()

        await retirement.run_sync(downgrade)
        with pytest.raises(DBAPIError) as busy:
            async with _control_session(database) as initializer:
                await initializer.execute(text("SET LOCAL lock_timeout = '250ms'"))
                await initializer.execute(
                    text(
                        "SELECT loom_capacity_guard.initialize_trial_writer_fence(:agent, :writer)"
                    ),
                    {"agent": registration.agent_incarnation, "writer": uuid4()},
                )
        assert busy.value.orig.sqlstate == "55P03"


@pytest.mark.parametrize("drift", ["body", "search_path", "owner", "execute_grant"])
def test_helper_provisioning_rejects_drift_and_rolls_back_the_transaction(
    capacity_guard_database: dict[str, object], drift: str
) -> None:
    database = capacity_guard_database
    trial = _seed_trial(database)
    engine = create_engine(_value(database, "admin_url"))
    quote = engine.dialect.identifier_preparer.quote
    try:
        with engine.begin() as owner:
            if drift == "body":
                owner.exec_driver_sql(
                    "CREATE OR REPLACE FUNCTION public.loom_drop_trial_writer_triggers() "
                    "RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog "
                    "AS 'BEGIN RETURN; END'"
                )
            elif drift == "search_path":
                owner.exec_driver_sql(
                    "ALTER FUNCTION public.loom_drop_trial_writer_triggers() SET search_path=public"
                )
            elif drift == "owner":
                role = quote(_value(database, "owner_role"))
                owner.exec_driver_sql(f"GRANT CREATE ON SCHEMA public TO {role}")
                owner.exec_driver_sql(
                    f"ALTER FUNCTION public.loom_drop_trial_writer_triggers() OWNER TO {role}"
                )
            else:
                role = quote(_value(database, "runtime_role"))
                owner.exec_driver_sql(
                    f"GRANT EXECUTE ON FUNCTION public.loom_drop_trial_writer_triggers() TO {role}"
                )
        with pytest.raises(DBAPIError) as denied:
            with engine.begin() as owner:
                owner.execute(
                    text("UPDATE public.trials SET submit_priority=200 WHERE id=:id"),
                    {"id": trial},
                )
                owner.exec_driver_sql(
                    trial_writer_trigger_retirement_ddl(
                        guard_owner=_value(database, "owner_role")
                    ).as_string()
                )
        assert denied.value.orig.sqlstate == ("42501" if drift == "execute_grant" else "55000")
        with engine.connect() as observer:
            assert (
                observer.execute(
                    text("SELECT submit_priority FROM public.trials WHERE id=:id"), {"id": trial}
                ).scalar_one()
                == 100
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("parent", "refusal_message"),
    [
        ("loom_capacity_guard.trial_mutation_permits", "inheritance is unsupported"),
        ("loom_capacity_guard.trial_writer_mutations", "inheritance crosses authority"),
        ("public.trials", "inheritance is unsupported"),
    ],
)
def test_downgrade_does_not_lock_a_foreign_inheritance_descendant(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    parent: str,
    refusal_message: str,
) -> None:
    database = capacity_guard_database
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as admin:
            admin.exec_driver_sql("CREATE SCHEMA foreign_scope")
            admin.exec_driver_sql(f"CREATE TABLE foreign_scope.ledger_child () INHERITS ({parent})")
        with engine.begin() as foreign_reader:
            foreign_reader.exec_driver_sql(
                "LOCK TABLE foreign_scope.ledger_child IN ACCESS SHARE MODE"
            )
            with pytest.raises(DBAPIError) as dependency:
                _downgrade(database, monkeypatch)
            # Refuse the inheritance edge before either LOCK or DROP can recurse
            # into the foreign child. RESTRICT alone does not prevent DROP locks.
            assert dependency.value.orig.sqlstate == "55000"
            assert refusal_message in str(dependency.value.orig)
            assert (
                foreign_reader.execute(
                    text("SELECT count(*) FROM foreign_scope.ledger_child")
                ).scalar_one()
                == 0
            )
    finally:
        engine.dispose()


def test_downgrade_rejects_unexpected_private_relation_ownership(
    capacity_guard_database: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    database = capacity_guard_database
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as admin:
            admin.exec_driver_sql("CREATE TABLE loom_capacity_guard.unexpected_owner (id integer)")
        with pytest.raises(DBAPIError) as refusal:
            _downgrade(database, monkeypatch)
        assert refusal.value.orig.sqlstate == "42501"
        assert "guard retirement relation ownership changed" in str(refusal.value.orig)
    finally:
        engine.dispose()
