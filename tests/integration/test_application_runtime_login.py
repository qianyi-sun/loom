"""Restore ordinary login only after independently verified ownership separation."""

from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.pq import TransactionStatus
from sqlalchemy.engine import make_url

from loom.application_database_admission import capture_application_database_admission
from loom.application_login_sealing import seal_application_login
from loom.application_ownership_transfer import transfer_application_ownership
from loom.application_runtime_login import restore_application_runtime_login
from tests.integration.test_application_database_admission import _handoff
from tests.integration.test_application_ownership_transfer import (
    _install_staging_readonly,
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


async def _prepare(database_fixture, *, schema_acl_profile="application-only"):
    url, owner, bindings = database_fixture
    runtime = next(role for role, alias in bindings.items() if alias == "application-owner")
    provisioner = next(role for role, alias in bindings.items() if alias == "provisioner")
    connection = psycopg.connect(url, autocommit=True)
    if schema_acl_profile == "staging-readonly":
        _install_staging_readonly(connection)
    seal_application_login(
        connection, database=make_url(url).database, role=runtime, provisioner_role=provisioner
    )
    with psycopg.connect(url, dbname="postgres", autocommit=True) as maintenance:
        target = capture_application_database_admission(
            maintenance,
            database=make_url(url).database,
            owner_role=runtime,
            successor_role=owner,
            provisioner_role=provisioner,
            handoff_backend=_handoff(connection),
        )
    with connection.transaction():
        transfer_application_ownership(connection, owner_role=owner, role_bindings=bindings,
                                       schema_acl_profile=schema_acl_profile)
    return connection, url, owner, runtime, bindings, target


@pytest.mark.asyncio
async def test_staging_readonly_grants_survive_login_and_extra_writes_are_rejected(transfer_database):  # noqa: F811
    admin, url, owner, runtime, bindings, target = await _prepare(
        transfer_database, schema_acl_profile="staging-readonly"
    )
    password = uuid4().hex
    with admin:
        try:
            for statement in (
                "GRANT UPDATE ON public.trials TO loom_rollout_readonly",
                "GRANT SELECT ON public.trials TO loom_rollout_readonly WITH GRANT OPTION",
                "GRANT CREATE ON SCHEMA public TO loom_rollout_readonly",
            ):
                try:
                    admin.execute(statement)
                    with pytest.raises(RuntimeError, match="trusted reference"):
                        restore_application_runtime_login(
                            admin, owner_role=owner, role_bindings=bindings, password=password,
                            target=target, schema_acl_profile="staging-readonly",
                        )
                    assert admin.execute(sql.SQL("SELECT rolcanlogin FROM pg_roles WHERE rolname={}").format(sql.Literal(runtime))).fetchone() == (False,)
                finally:
                    _install_staging_readonly(admin)
            with pytest.raises(RuntimeError, match="trusted reference"):
                restore_application_runtime_login(
                    admin, owner_role=owner, role_bindings=bindings, password=password, target=target
                )
            for _ in range(2):
                restore_application_runtime_login(
                    admin, owner_role=owner, role_bindings=bindings, password=password,
                    target=target, schema_acl_profile="staging-readonly",
                )
            with psycopg.connect(url, user=runtime, password=password, autocommit=True) as client:
                assert client.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    client.execute("ALTER TABLE public.trials DISABLE TRIGGER ALL")
            assert admin.execute("SELECT has_table_privilege('loom_rollout_readonly','public.trials','SELECT'),has_table_privilege('loom_rollout_readonly','public.trials','UPDATE')").fetchone() == (True, False)
        finally:
            admin.execute("DROP OWNED BY loom_rollout_readonly")
            admin.execute("DROP ROLE loom_rollout_readonly")


@pytest.mark.asyncio
@pytest.mark.parametrize("password_kind", ["ordinary", "verifier_shaped"])
async def test_restored_runtime_authenticates_but_cannot_regain_owner_authority(
    transfer_database,  # noqa: F811
    password_kind,
):
    admin, url, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex if password_kind == "ordinary" else "md5" + "a" * 32
    with admin:
        for _ in range(2):
            restore_application_runtime_login(
                admin, owner_role=owner, role_bindings=bindings, password=password, target=target
            )
        client_url = (
            make_url(url)
            .set(username=runtime, password=password)
            .render_as_string(hide_password=False)
        )
        wrong_url = (
            make_url(client_url).set(password=uuid4().hex).render_as_string(hide_password=False)
        )
        with pytest.raises(psycopg.OperationalError, match="password authentication failed"):
            psycopg.connect(wrong_url, connect_timeout=2).close()
        with psycopg.connect(client_url, autocommit=True) as client:
            assert client.execute("SELECT count(*) FROM public.trials").fetchone() == (0,)
            for statement in (
                "CREATE TABLE public.runtime_ddl(id integer)",
                "ALTER TABLE public.trials DISABLE TRIGGER ALL",
                "TRUNCATE public.trials",
                "UPDATE public.alembic_version SET version_num='wrong'",
                sql.SQL("SET ROLE {}").format(sql.Identifier(owner)),
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    client.execute(statement)
        with pytest.raises(RuntimeError, match="credential"):
            restore_application_runtime_login(
                admin, owner_role=owner, role_bindings=bindings, password=uuid4().hex, target=target
            )


@pytest.mark.asyncio
async def test_restore_does_not_replace_an_unknown_password_on_a_nologin_runtime(transfer_database):  # noqa: F811
    admin, _, owner, runtime, bindings, target = await _prepare(transfer_database)
    with admin:
        admin.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(
            sql.Identifier(runtime), sql.Literal("not-the-original-password"),
        ))
        before = admin.execute(
            "SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname=%s", (runtime,)
        ).fetchone()
        assert before[0] is False and before[1] is not None
        with pytest.raises(RuntimeError, match="credential state changed"):
            restore_application_runtime_login(
                admin, owner_role=owner, role_bindings=bindings, target=target,
                password="the-original-password",
            )
        assert admin.execute(
            "SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname=%s", (runtime,)
        ).fetchone() == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", ["owner_login", "runtime_membership", "runtime_ddl", "routine_body", "saved_identity"]
)
async def test_restore_refuses_drift_without_enabling_login(transfer_database, drift):  # noqa: F811
    admin, _, owner, runtime, bindings, target = await _prepare(transfer_database)
    with admin:
        if drift == "owner_login":
            admin.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(owner)))
        elif drift == "runtime_membership":
            admin.execute(
                sql.SQL("GRANT {} TO {}").format(sql.Identifier(owner), sql.Identifier(runtime))
            )
        elif drift == "runtime_ddl":
            admin.execute(
                sql.SQL("GRANT CREATE ON SCHEMA public TO {}").format(sql.Identifier(runtime))
            )
        elif drift == "saved_identity":
            target = replace(target, database_oid=target.database_oid + 1000)
        else:
            admin.execute(
                "CREATE FUNCTION public.foreign_restore_fn() RETURNS integer LANGUAGE sql AS 'SELECT 42'"
            )
        with pytest.raises(RuntimeError):
            restore_application_runtime_login(
                admin, owner_role=owner, role_bindings=bindings, password=uuid4().hex, target=target
            )
        assert admin.execute(
            "SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname=%s", (runtime,)
        ).fetchone() == (False, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("prepopulated_password", [False, True])
async def test_restore_recovers_lost_commit_acknowledgement_with_same_credential(
    transfer_database, prepopulated_password,  # noqa: F811
):
    admin, _, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex
    with admin:
        if prepopulated_password:
            admin.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                sql.Identifier(runtime), sql.Literal(password),
            ))

        class LostAcknowledgement:
            @property
            def info(self):
                return admin.info

            def execute(self, query):
                return admin.execute(query)

            @contextmanager
            def transaction(self):
                outer = admin.info.transaction_status == TransactionStatus.IDLE
                with admin.transaction():
                    yield
                if outer:
                    raise RuntimeError("lost acknowledgement")

        with pytest.raises(RuntimeError, match="lost acknowledgement"):
            restore_application_runtime_login(
                LostAcknowledgement(),
                owner_role=owner,
                role_bindings=bindings,
                password=password,
                target=target,
            )
        before = admin.execute(
            "SELECT rolpassword FROM pg_authid WHERE rolname=%s", (runtime,)
        ).fetchone()
        restore_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target
        )
        assert (
            admin.execute(
                "SELECT rolpassword FROM pg_authid WHERE rolname=%s", (runtime,)
            ).fetchone()
            == before
        )


@pytest.mark.asyncio
async def test_restoration_sends_no_plaintext_and_rolls_back_after_alter(transfer_database):  # noqa: F811
    admin, _, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = "restore-private-" + uuid4().hex
    with admin:

        class FailedAfterAlter:
            @property
            def info(self):
                return admin.info

            def transaction(self):
                return admin.transaction()

            def execute(self, query):
                statement = query if isinstance(query, str) else query.as_string()
                assert password not in statement
                result = admin.execute(query)
                if statement.startswith("ALTER ROLE"):
                    raise RuntimeError("injected after credential update")
                return result

        with admin.transaction(), pytest.raises(RuntimeError, match="idle"):
            restore_application_runtime_login(
                admin, owner_role=owner, role_bindings=bindings, password=password, target=target
            )
        with pytest.raises(RuntimeError, match="injected after credential"):
            restore_application_runtime_login(
                FailedAfterAlter(),
                owner_role=owner,
                role_bindings=bindings,
                password=password,
                target=target,
            )
        assert admin.execute(
            "SELECT rolcanlogin,rolpassword FROM pg_authid WHERE rolname=%s", (runtime,)
        ).fetchone() == (False, None)
