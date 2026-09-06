from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import ClassVar
from unittest.mock import AsyncMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import psycopg
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

import loom.personal_dev_capacity_runtime as capacity_runtime_module
import loom_cli.rollout.operator.protected_staging_capacity_database_component as capacity_database_component_module
from loom.dev_instance import derive_identity
from loom.personal_dev_capacity import (
    PersonalDevCapacityAvailability,
    PersonalDevCapacityManagerCheckpoint,
    PersonalDevCapacitySubjectStatus,
)
from loom.personal_dev_capacity_runtime import (
    PersonalDevCapacityInstallationError,
    PersonalDevCapacityStatusReader,
    PsycopgPersonalDevCapacityDatabase,
    _new_credentials,
    _role_names,
)
from loom.staging_capacity_database_bootstrap import staging_capacity_identity
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
)
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
    build_staging_reporter_configuration_for_candidate,
)


def _active_binding(subject_id: UUID, subject_incarnation: UUID) -> ExecutableIntentBindingV2:
    return ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=UUID(int=101),
            writer_epoch=3,
            configuration_epoch=5,
            execution_epoch=7,
            execution_manifest_sha256="1" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256="2" * 64,
            allocation_epoch=11,
        ),
        tranche_id=UUID(int=102),
        intent_id=UUID(int=103),
        shape_instance_id="oldlab-shape-0001",
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
        account_id="owner-alice",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="git-sha1",
            identity="a" * 40,
            publication_sha256="a" * 64,
        ),
        candidate_generation=7,
        deployment_generation=7,
        pool_id="oldlab",
        pool_generation=13,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=104),
        shape_id="oldlab-cpu-small",
        profile_id="oldlab-default",
        profile_generation=17,
        profile_digest="3" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(slots=1, cpu_millicores=1000, memory_bytes=1024),
        node_ids=("oldlab-node-01",),
    )


def _staging_seed(credentials) -> dict[str, object]:
    return {
        "agent_database_password": credentials.agent_password,
        "agent_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-agent:v1")),
        "authority_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-authority:v1")),
        "migrator_database_password": credentials.migrator_password,
        "observer_database_password": credentials.observer_password,
        "reporter_incarnation": str(credentials.reporter_incarnation),
        "reporter_token": credentials.reporter_token,
        "runtime_database_password": credentials.runtime_password,
        "schema_version": 1,
        "subject_id": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")),
        "subject_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1")),
    }


def test_capacity_guard_passfile_handles_short_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Break caught: accepting a partial kernel write as a complete private passfile."""

    real_write = os.write

    def short_write(fd: int, payload: bytes) -> int:
        return real_write(fd, payload[: max(1, len(payload) // 2)])

    monkeypatch.setattr(capacity_runtime_module.os, "write", short_write)
    password_free_url, fd = capacity_runtime_module._migration_url_with_passfile(
        "postgresql://migrator:secret-password@postgres.example.test:5432/loom"
    )
    try:
        assert "secret-password" not in password_free_url
        assert os.read(fd, 4096) == (b"postgres.example.test:5432:loom:migrator:secret-password\n")
    finally:
        os.close(fd)


@pytest.mark.asyncio
async def test_compensation_shutdown_observation_rejects_null_role_validity(
    postgres_url: str,
) -> None:
    """Break caught: SQL three-valued logic accepting an unset role validity."""

    identity = staging_capacity_identity()
    owner, migrator, agent, executor, observer, runtime = _role_names(identity)
    protected = (owner, migrator, agent, executor, observer, runtime)
    connection_url = (
        make_url(postgres_url)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    async with await psycopg.AsyncConnection.connect(
        connection_url,
        autocommit=True,
    ) as connection:
        try:
            for role in reversed(protected):
                await connection.execute(
                    psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
                )
            for role in protected:
                inherit = psycopg.sql.SQL("INHERIT" if role == migrator else "NOINHERIT")
                await connection.execute(
                    psycopg.sql.SQL(
                        "CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "{} NOREPLICATION NOBYPASSRLS PASSWORD NULL"
                    ).format(psycopg.sql.Identifier(role), inherit)
                )
                if role != agent:
                    await connection.execute(
                        psycopg.sql.SQL("ALTER ROLE {} VALID UNTIL 'infinity'").format(
                            psycopg.sql.Identifier(role)
                        )
                    )

            result = await connection.execute(
                capacity_database_component_module._COMPENSATION_SHUTDOWN_SQL
            )

            assert await result.fetchone() == (
                {"credentials_disabled": False, "sessions_terminated": True},
            )
        finally:
            for role in reversed(protected):
                await connection.execute(
                    psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
                )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "active_role",
    ["loom_cap_staging_owner", "loom_cap_staging_executor"],
)
async def test_full_compensation_covers_every_protected_role_session(
    postgres_url: str,
    active_role: str,
) -> None:
    """Break caught: full compensation accepting a live owner or executor session."""

    identity = staging_capacity_identity()
    owner, migrator, agent, executor, observer, runtime = _role_names(identity)
    protected = (owner, migrator, agent, executor, observer, runtime)
    assert active_role in protected
    password = f"protected-session-{uuid4().hex}"
    connection_url = (
        make_url(postgres_url)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    active_url = (
        make_url(postgres_url)
        .set(username=active_role, password=password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )

    class PayloadRunner:
        environment: ClassVar[dict[str, str]] = {}

        def __init__(self) -> None:
            self.payloads: list[bytes] = []

        def run_checked(
            self,
            _argv,
            *,
            env,
            input_payload,
            timeout_seconds,
        ) -> None:
            assert env == self.environment
            assert input_payload is not None
            assert timeout_seconds == 60.0
            self.payloads.append(input_payload)

    active_connection: psycopg.AsyncConnection | None = None
    async with await psycopg.AsyncConnection.connect(
        connection_url,
        autocommit=True,
    ) as connection:
        try:
            await connection.execute("DROP DATABASE IF EXISTS loom WITH (FORCE)")
            for role in reversed(protected):
                await connection.execute(
                    psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
                )
            await connection.execute("CREATE DATABASE loom")
            await connection.execute("REVOKE ALL PRIVILEGES ON DATABASE loom FROM PUBLIC")
            for role in protected:
                login = psycopg.sql.SQL("LOGIN" if role == active_role else "NOLOGIN")
                inherit = psycopg.sql.SQL("INHERIT" if role == migrator else "NOINHERIT")
                role_password = (
                    psycopg.sql.Literal(password)
                    if role == active_role
                    else psycopg.sql.SQL("NULL")
                )
                await connection.execute(
                    psycopg.sql.SQL(
                        "CREATE ROLE {} {} NOSUPERUSER NOCREATEDB NOCREATEROLE {} "
                        "NOREPLICATION NOBYPASSRLS PASSWORD {} VALID UNTIL 'infinity'"
                    ).format(
                        psycopg.sql.Identifier(role),
                        login,
                        inherit,
                        role_password,
                    )
                )

            active_connection = await psycopg.AsyncConnection.connect(
                active_url,
                autocommit=True,
            )
            backend_result = await active_connection.execute("SELECT pg_backend_pid()")
            backend_row = await backend_result.fetchone()
            assert backend_row is not None
            backend_pid = backend_row[0]
            await connection.execute(
                psycopg.sql.SQL(
                    "ALTER ROLE {} NOLOGIN PASSWORD NULL VALID UNTIL 'infinity'"
                ).format(psycopg.sql.Identifier(active_role))
            )

            before = await connection.execute(
                capacity_database_component_module._COMPENSATION_SHUTDOWN_SQL
            )
            assert await before.fetchone() == (
                {"credentials_disabled": True, "sessions_terminated": False},
            )

            runner = PayloadRunner()
            component = KubernetesProtectedStagingCapacityDatabaseComponent(
                runner=runner,  # type: ignore[arg-type]
                container_registry="registry.example.test/loom",
                seed_reader=lambda: {},
            )
            component._verify_transient_authority_sealed(
                preserve_runtime_credentials=False,
                durable_runtime_credentials=True,
            )
            assert len(runner.payloads) == 1
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="protected staging capacity transient authority is not sealed",
            ):
                await connection.execute(runner.payloads[0].decode("utf-8"))
            await connection.rollback()

            component._terminate_transient_sessions(preserve_runtime_credentials=False)
            assert len(runner.payloads) == 2
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="protected staging capacity transient sessions remain",
            ):
                await connection.execute(runner.payloads[1].decode("utf-8"))
            await connection.rollback()

            component._terminate_transient_sessions(preserve_runtime_credentials=False)
            assert len(runner.payloads) == 3
            await connection.execute(runner.payloads[2].decode("utf-8"))

            after = await connection.execute(
                capacity_database_component_module._COMPENSATION_SHUTDOWN_SQL
            )
            assert await after.fetchone() == (
                {"credentials_disabled": True, "sessions_terminated": True},
            )
            retained = await connection.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = %s",
                (backend_pid,),
            )
            assert await retained.fetchone() == (0,)
        finally:
            if active_connection is not None:
                with suppress(Exception):
                    await active_connection.close()
            await connection.rollback()
            await connection.execute("DROP DATABASE IF EXISTS loom WITH (FORCE)")
            for role in reversed(protected):
                await connection.execute(
                    psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
                )


@pytest.mark.asyncio
async def test_staging_peer_arm_composes_with_least_privileged_converge_and_seal(
    postgres_url: str,
) -> None:
    """Catch the protected staging peer arm producing an unusable migrator envelope."""

    credentials = _new_credentials()
    seed = _staging_seed(credentials)
    identity = staging_capacity_identity()
    owner, migrator, agent, executor, observer, runtime = _role_names(identity)
    protected = (owner, migrator, agent, executor, observer, runtime)
    parsed = make_url(postgres_url)
    application_password = f"loom-app-{uuid4().hex}"
    cluster_url = (
        parsed.set(database="template1")
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    loom_admin_url = (
        parsed.set(database="loom")
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    loom_application_url = (
        parsed.set(database="loom", username="loom", password=application_password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )

    class PayloadRunner:
        environment: ClassVar[dict[str, str]] = {}

        def __init__(self) -> None:
            self.payloads: list[bytes] = []

        def run_checked(
            self,
            _argv,
            *,
            env,
            input_payload,
            timeout_seconds,
        ) -> None:
            assert env == self.environment
            assert input_payload is not None
            assert timeout_seconds == 60.0
            self.payloads.append(input_payload)

    runner = PayloadRunner()
    component = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: seed,
    )
    component._arm_transient_migrator(seed)
    component._seal_transient_migrator()
    arm_payload, *seal_payloads = runner.payloads
    assert len(seal_payloads) == 6

    async with await psycopg.AsyncConnection.connect(
        cluster_url,
        autocommit=True,
    ) as connection:
        await connection.execute("DROP DATABASE IF EXISTS loom WITH (FORCE)")
        for role in (*reversed(protected), "loom", "postgres"):
            await connection.execute(
                psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
            )
        await connection.execute("CREATE ROLE postgres SUPERUSER NOLOGIN")
        await connection.execute(
            psycopg.sql.SQL(
                "CREATE ROLE loom LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {}"
            ).format(psycopg.sql.Literal(application_password))
        )
        await connection.execute("CREATE DATABASE loom OWNER loom")

    try:
        repo_root = Path(__file__).resolve().parents[2]
        cfg = AlembicConfig(str(repo_root / "migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(repo_root / "migrations"))
        cfg.set_main_option(
            "sqlalchemy.url",
            loom_application_url.replace("postgresql://", "postgresql+psycopg://", 1),
        )
        command.upgrade(cfg, "head")

        async with await psycopg.AsyncConnection.connect(
            loom_admin_url,
            autocommit=True,
        ) as connection:
            await connection.execute("SET SESSION AUTHORIZATION postgres")
            await connection.execute(arm_payload.decode("utf-8"))

        configuration = build_staging_reporter_configuration_for_candidate(
            candidate_sha="a" * 40,
            artifact_bundle_digest="b" * 64,
            mutation_epoch=41,
            seed=seed,
        )
        migrator_url = (
            parsed.set(
                database="loom",
                username=migrator,
                password=credentials.migrator_password,
            )
            .render_as_string(hide_password=False)
            .replace("postgresql+psycopg://", "postgresql://", 1)
        )
        installation = await PsycopgPersonalDevCapacityDatabase(
            migrator_url,
            transient_role_admin=True,
        ).converge_protected(
            identity=identity,
            credentials=credentials,
            configuration=configuration,
        )

        async with await psycopg.AsyncConnection.connect(
            loom_admin_url,
            autocommit=True,
        ) as connection:
            await connection.execute("SET SESSION AUTHORIZATION postgres")
            for seal_payload in seal_payloads:
                await connection.execute(seal_payload.decode("utf-8"))

        runtime_url = (
            parsed.set(
                database="loom",
                username=runtime,
                password=credentials.runtime_password,
            )
            .render_as_string(hide_password=False)
            .replace("postgresql+psycopg://", "postgresql://", 1)
        )
        async with await psycopg.AsyncConnection.connect(runtime_url) as runtime_connection:
            registration = await runtime_connection.execute(
                "SELECT loom_capacity_guard.current_protected_runtime_registration()"
            )
            row = await registration.fetchone()
            assert row is not None
            assert row[0]["candidate_digest"] == "b" * 64

        async with await psycopg.AsyncConnection.connect(loom_admin_url) as connection:
            revision = await connection.execute(
                "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
            )
            assert (await revision.fetchone())[0].startswith("guard_")
            roles = await connection.execute(
                "SELECT rolname, rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                "rolcreaterole, rolreplication, rolbypassrls, rolpassword IS NOT NULL, "
                "COALESCE(rolvaliduntil = 'infinity'::timestamptz, false) "
                "FROM pg_authid WHERE rolname = ANY(%s) ORDER BY rolname",
                ([*protected, "loom"],),
            )
            assert await roles.fetchall() == sorted(
                [
                    ("loom", True, False, False, False, False, False, False, True, False),
                    (owner, False, False, False, False, False, False, False, False, True),
                    (migrator, False, True, False, False, False, False, False, False, True),
                    (agent, True, False, False, False, False, False, False, True, True),
                    (executor, False, False, False, False, False, False, False, False, True),
                    (observer, True, False, False, False, False, False, False, True, True),
                    (runtime, True, False, False, False, False, False, False, True, True),
                ]
            )
            memberships = await connection.execute(
                "SELECT member.rolname, granted.rolname "
                "FROM pg_auth_members membership "
                "JOIN pg_roles member ON member.oid = membership.member "
                "JOIN pg_roles granted ON granted.oid = membership.roleid "
                "WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s)",
                (list(protected), list(protected)),
            )
            assert await memberships.fetchall() == []
            privileges = await connection.execute(
                "SELECT has_database_privilege(%s, 'loom', 'CONNECT'), "
                "has_database_privilege(%s, 'loom', 'CREATE'), "
                "has_database_privilege(%s, 'loom', 'TEMPORARY'), "
                "has_database_privilege(%s, 'loom', 'CREATE')",
                (migrator, migrator, migrator, owner),
            )
            assert await privileges.fetchone() == (False, False, False, False)
            sessions = await connection.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE usename = %s",
                (migrator,),
            )
            assert await sessions.fetchone() == (0,)
            details_result = await connection.execute(
                capacity_database_component_module._DETAIL_SQL
            )
            details_row = await details_result.fetchone()
            assert details_row is not None
            details = details_row[0]
            assert details["active_migrator_sessions"] == 0
            assert details["database_privileges"] == {
                "migrator_acl_count": 0,
                "migrator_connect": False,
                "migrator_create": False,
                "migrator_temporary": False,
                "owner_create": False,
            }
            await connection.execute("GRANT CONNECT ON DATABASE loom TO PUBLIC")
            public_details_result = await connection.execute(
                capacity_database_component_module._DETAIL_SQL
            )
            public_details_row = await public_details_result.fetchone()
            assert public_details_row is not None
            assert public_details_row[0]["database_privileges"] == {
                "migrator_acl_count": 0,
                "migrator_connect": True,
                "migrator_create": False,
                "migrator_temporary": False,
                "owner_create": False,
            }
            await connection.execute("REVOKE CONNECT ON DATABASE loom FROM PUBLIC")
            assert details["roles"][migrator]["credential_validity"] == "infinite"
            for role in (agent, observer, runtime):
                assert details["roles"][role]["credential_validity"] == "infinite"
        assert installation.runtime_database_url == runtime_url.replace(
            "postgresql://",
            "postgresql+psycopg://",
            1,
        )
    finally:
        async with await psycopg.AsyncConnection.connect(
            cluster_url,
            autocommit=True,
        ) as connection:
            await connection.execute("DROP DATABASE IF EXISTS loom WITH (FORCE)")
            for role in (*reversed(protected), "loom", "postgres"):
                await connection.execute(
                    psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
                )


@pytest.mark.asyncio
async def test_capacity_guard_migration_uses_password_free_url_and_private_passfile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = f"migrator-{uuid4().hex}"
    migrator_url = (
        "postgresql://loom_cap_staging_migrator:"
        f"{secret}@postgres.example.test:5432/loom?sslmode=require"
    )
    observed: dict[str, object] = {}

    class Process:
        returncode = 0

        async def wait(self) -> int:
            return 0

    async def create_subprocess_exec(*argv: object, **kwargs: object) -> Process:
        env = kwargs["env"]
        assert isinstance(env, dict)
        db_url = env["LOOM_CAPACITY_GUARD_DB_URL"]
        if secret in json.dumps(env, sort_keys=True):
            raise AssertionError("secret found in child environment")
        parsed = make_url(db_url)
        assert parsed.drivername == "postgresql+psycopg"
        assert parsed.password is None
        assert parsed.username == "loom_cap_staging_migrator"
        passfile = parsed.query["passfile"]
        assert isinstance(passfile, str)
        assert passfile.startswith("/proc/self/fd/")
        pass_fds = kwargs["pass_fds"]
        assert isinstance(pass_fds, tuple)
        assert int(os.path.basename(passfile)) in pass_fds
        with open(passfile, "rb") as handle:
            observed["passfile"] = handle.read().decode("utf-8")
        observed["argv"] = argv
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_subprocess_exec)

    await PsycopgPersonalDevCapacityDatabase("postgresql://unused")._migrate(
        migrator_url=migrator_url,
        owner="loom_cap_staging_owner",
        agent="loom_cap_staging_agent",
        executor="loom_cap_staging_executor",
        observer="loom_cap_staging_observer",
        runtime="loom_cap_staging_runtime",
    )

    assert observed["passfile"] == (
        f"postgres.example.test:5432:loom:loom_cap_staging_migrator:{secret}\n"
    )
    assert secret not in " ".join(str(item) for item in observed["argv"])


@pytest.mark.asyncio
async def test_capacity_role_convergence_provisions_isolated_runtime_role(
    postgres_url: str,
) -> None:
    name = f"runtime-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    credentials = _new_credentials()

    (
        owner,
        migrator,
        agent,
        executor,
        observer,
        runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, credentials)

    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
    ) as connection:
        role = await connection.execute(
            "SELECT rolcanlogin, rolinherit, rolpassword IS NULL, "
            "(SELECT count(*) FROM pg_auth_members membership "
            "WHERE membership.member = pg_roles.oid OR membership.roleid = pg_roles.oid) "
            "FROM pg_authid AS pg_roles WHERE rolname = %s",
            (runtime,),
        )
        assert await role.fetchone() == (True, False, False, 0)
        assert runtime not in {owner, migrator, agent, executor, observer}

    runtime_url = (
        make_url(postgres_url)
        .set(
            database=database_name,
            username=runtime,
            password=credentials.runtime_password,
        )
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    async with await psycopg.AsyncConnection.connect(runtime_url) as runtime_connection:
        current_user = await runtime_connection.execute("SELECT session_user")
        assert await current_user.fetchone() == (runtime,)


@pytest.mark.asyncio
async def test_capacity_role_convergence_removes_owner_password(
    postgres_url: str,
) -> None:
    name = f"ownpw-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    credentials = _new_credentials()

    owner, *_rest = await database._converge_roles(identity, credentials)
    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
        autocommit=True,
    ) as connection:
        await connection.execute(
            psycopg.sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                psycopg.sql.Identifier(owner),
                psycopg.sql.Literal("contaminated-owner-password"),
            )
        )

    await database._converge_roles(identity, credentials)

    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
    ) as connection:
        role = await connection.execute(
            "SELECT rolcanlogin, rolpassword IS NULL FROM pg_authid WHERE rolname = %s",
            (owner,),
        )
        assert await role.fetchone() == (False, True)


@pytest.mark.asyncio
async def test_executor_surface_convergence_preserves_exact_protected_runtime_functions(
    capacity_guard_database: dict[str, object],
) -> None:
    def value(key: str) -> str:
        result = capacity_guard_database[key]
        assert isinstance(result, str)
        return result

    runtime = value("runtime_role")
    database = PsycopgPersonalDevCapacityDatabase(value("admin_url"))
    await database._converge_executor_surface(
        migrator_url=value("migrator_url"),
        owner=value("owner_role"),
        executor=value("executor_role"),
        observer=value("observer_role"),
        runtime=runtime,
    )

    expected = {
        "assert_staging_worker_session",
        "cancel_protected_runtime_pending_trial",
        "claim_staging_assigned_trial",
        "current_protected_runtime_registration",
        "publish_protected_runtime_trial_readiness",
        "register_staging_public_worker",
        "retry_staging_claimed_trial",
        "submit_protected_runtime_trial_projection",
    }
    async with await psycopg.AsyncConnection.connect(
        value("admin_url").replace("postgresql+psycopg://", "postgresql://", 1)
    ) as connection:
        functions = await connection.execute(
            "SELECT routine.proname FROM pg_proc AS routine "
            "JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace "
            "WHERE namespace.nspname = 'loom_capacity_guard' "
            "AND has_function_privilege(%s, routine.oid, 'EXECUTE')",
            (runtime,),
        )
        assert {row[0] for row in await functions.fetchall()} == expected


@pytest.mark.asyncio
async def test_runtime_registration_rejects_privileged_runtime_role(
    capacity_guard_database: dict[str, object],
) -> None:
    runtime = str(capacity_guard_database["runtime_role"])
    runtime_url = str(capacity_guard_database["runtime_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    cluster_admin_url = str(capacity_guard_database["cluster_admin_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    alter = psycopg.sql.SQL("ALTER ROLE {} ").format(psycopg.sql.Identifier(runtime))

    async with await psycopg.AsyncConnection.connect(cluster_admin_url, autocommit=True) as admin:
        await admin.execute(alter + psycopg.sql.SQL("CREATEDB"))
    try:
        async with await psycopg.AsyncConnection.connect(runtime_url) as connection:
            with pytest.raises(
                InsufficientPrivilege,
                match="protected submission runtime role attributes drifted",
            ):
                await connection.execute(
                    "SELECT loom_capacity_guard.current_protected_runtime_registration()"
                )
    finally:
        async with await psycopg.AsyncConnection.connect(
            cluster_admin_url, autocommit=True
        ) as admin:
            await admin.execute(alter + psycopg.sql.SQL("NOCREATEDB"))


@pytest.mark.asyncio
async def test_runtime_registration_rejects_runtime_role_membership(
    capacity_guard_database: dict[str, object],
) -> None:
    runtime = str(capacity_guard_database["runtime_role"])
    runtime_url = str(capacity_guard_database["runtime_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    cluster_admin_url = str(capacity_guard_database["cluster_admin_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    outsider = f"loom_runtime_outsider_{uuid4().hex[:8]}"
    grant = psycopg.sql.SQL("GRANT {} TO {}").format(
        psycopg.sql.Identifier(outsider), psycopg.sql.Identifier(runtime)
    )
    revoke = psycopg.sql.SQL("REVOKE {} FROM {}").format(
        psycopg.sql.Identifier(outsider), psycopg.sql.Identifier(runtime)
    )

    async with await psycopg.AsyncConnection.connect(cluster_admin_url, autocommit=True) as admin:
        await admin.execute(
            psycopg.sql.SQL("CREATE ROLE {}").format(psycopg.sql.Identifier(outsider))
        )
        await admin.execute(grant)
    try:
        async with await psycopg.AsyncConnection.connect(runtime_url) as connection:
            with pytest.raises(
                InsufficientPrivilege,
                match="protected submission runtime role memberships drifted",
            ):
                await connection.execute(
                    "SELECT loom_capacity_guard.current_protected_runtime_registration()"
                )
    finally:
        async with await psycopg.AsyncConnection.connect(
            cluster_admin_url, autocommit=True
        ) as admin:
            await admin.execute(revoke)
            await admin.execute(
                psycopg.sql.SQL("DROP ROLE {}").format(psycopg.sql.Identifier(outsider))
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("relation_kind", ["table", "sequence"])
async def test_runtime_registration_rejects_direct_relation_privileges(
    capacity_guard_database: dict[str, object],
    relation_kind: str,
) -> None:
    runtime = str(capacity_guard_database["runtime_role"])
    runtime_url = str(capacity_guard_database["runtime_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    admin_url = str(capacity_guard_database["admin_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    if relation_kind == "table":
        grant = psycopg.sql.SQL("GRANT SELECT ON loom_capacity_guard.authority_state TO {}").format(
            psycopg.sql.Identifier(runtime)
        )
        cleanup = psycopg.sql.SQL(
            "REVOKE SELECT ON loom_capacity_guard.authority_state FROM {}"
        ).format(psycopg.sql.Identifier(runtime))
    else:
        grant = psycopg.sql.SQL(
            "CREATE SEQUENCE loom_capacity_guard.runtime_contamination; "
            "GRANT USAGE ON SEQUENCE loom_capacity_guard.runtime_contamination TO {}"
        ).format(psycopg.sql.Identifier(runtime))
        cleanup = psycopg.sql.SQL("DROP SEQUENCE loom_capacity_guard.runtime_contamination")

    async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
        await admin.execute(grant)
    try:
        async with await psycopg.AsyncConnection.connect(runtime_url) as connection:
            with pytest.raises(
                InsufficientPrivilege,
                match="protected submission runtime relation privileges drifted",
            ):
                await connection.execute(
                    "SELECT loom_capacity_guard.current_protected_runtime_registration()"
                )
    finally:
        async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
            await admin.execute(cleanup)


@pytest.mark.asyncio
@pytest.mark.parametrize("privilege_drift", ["extra", "missing"])
async def test_runtime_registration_rejects_function_privilege_drift(
    capacity_guard_database: dict[str, object],
    privilege_drift: str,
) -> None:
    runtime = str(capacity_guard_database["runtime_role"])
    runtime_url = str(capacity_guard_database["runtime_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    admin_url = str(capacity_guard_database["admin_url"]).replace(
        "postgresql+psycopg://", "postgresql://", 1
    )
    if privilege_drift == "extra":
        function = "observe_executable_intent(uuid,uuid,uuid)"
        mutation = "GRANT"
        cleanup = "REVOKE"
    else:
        function = "publish_protected_runtime_trial_readiness(uuid,uuid,uuid)"
        mutation = "REVOKE"
        cleanup = "GRANT"
    mutate = psycopg.sql.SQL(
        f"{mutation} EXECUTE ON FUNCTION loom_capacity_guard.{function} "
        + ("TO {}" if mutation == "GRANT" else "FROM {}")
    ).format(psycopg.sql.Identifier(runtime))
    restore = psycopg.sql.SQL(
        f"{cleanup} EXECUTE ON FUNCTION loom_capacity_guard.{function} "
        + ("TO {}" if cleanup == "GRANT" else "FROM {}")
    ).format(psycopg.sql.Identifier(runtime))

    async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
        await admin.execute(mutate)
    try:
        async with await psycopg.AsyncConnection.connect(runtime_url) as connection:
            with pytest.raises(
                InsufficientPrivilege,
                match="protected submission runtime function privileges drifted",
            ):
                await connection.execute(
                    "SELECT loom_capacity_guard.current_protected_runtime_registration()"
                )
    finally:
        async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
            await admin.execute(restore)


@pytest.mark.asyncio
async def test_capacity_role_convergence_seals_migrator_when_cancelled(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = derive_identity(f"cancel-{uuid4().hex[:8]}")
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    seal = AsyncMock()
    monkeypatch.setattr(database, "_seal_migrator", seal)

    async def cancelled_connect(*_args: object, **_kwargs: object) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", cancelled_connect)

    with pytest.raises(asyncio.CancelledError):
        await database._converge_roles(identity, _new_credentials())

    seal.assert_awaited_once()


@pytest.mark.asyncio
async def test_capacity_role_convergence_rejects_external_owner_membership(
    postgres_url: str,
) -> None:
    name = f"role-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    credentials = _new_credentials()
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)

    (
        owner,
        migrator,
        agent,
        executor,
        _observer,
        _runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, credentials)
    outsider = f"loom_cap_outsider_{uuid4().hex[:8]}"
    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
        autocommit=True,
    ) as connection:
        await connection.execute(f'GRANT SELECT ON TABLE public.trials TO "{agent}"')

    await database._converge_roles(identity, credentials)
    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
        autocommit=True,
    ) as connection:
        privilege = await connection.execute(
            "SELECT has_table_privilege(%s, 'public.trials', 'SELECT')",
            (agent,),
        )
        assert await privilege.fetchone() == (False,)
        executor_role = await connection.execute(
            "SELECT rolcanlogin, rolinherit, rolpassword IS NULL FROM pg_authid WHERE rolname = %s",
            (executor,),
        )
        assert await executor_role.fetchone() == (False, False, True)
        await connection.execute(f'CREATE ROLE "{outsider}" LOGIN')
        await connection.execute(f'GRANT "{owner}" TO "{outsider}"')

    with pytest.raises(
        PersonalDevCapacityInstallationError,
        match="unexpected memberships",
    ):
        await database._converge_roles(identity, credentials)
    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
    ) as connection:
        sealed = await connection.execute(
            "SELECT rolcanlogin, rolpassword IS NULL FROM pg_authid WHERE rolname = %s",
            (migrator,),
        )
        assert await sealed.fetchone() == (False, True)
        outsider_access = await connection.execute(
            "SELECT pg_has_role(%s, %s, 'MEMBER')",
            (outsider, owner),
        )
        assert await outsider_access.fetchone() == (False,)


@pytest.mark.asyncio
async def test_capacity_role_convergence_grants_only_required_reference_columns(
    postgres_url: str,
) -> None:
    name = f"references-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)

    (
        owner,
        _migrator,
        _agent,
        _executor,
        _observer,
        _runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, _new_credentials())

    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
    ) as connection:
        privileges = await connection.execute(
            "SELECT "
            "has_column_privilege(%s, 'public.trials', 'id', 'REFERENCES'), "
            "has_column_privilege(%s, 'public.trials', 'config', 'REFERENCES'), "
            "has_table_privilege(%s, 'public.trials', 'REFERENCES'), "
            "has_column_privilege(%s, 'public.data_lifecycle_authorities', "
            "'id', 'REFERENCES'), "
            "has_column_privilege(%s, 'public.trials', 'config', 'SELECT'), "
            "has_column_privilege(%s, 'public.data_lifecycle_authorities', "
            "'id', 'SELECT'), "
            "has_column_privilege(%s, 'public.data_lifecycle_authorities', "
            "'environment', 'SELECT')",
            (owner, owner, owner, owner, owner, owner, owner),
        )
        assert await privileges.fetchone() == (True, False, False, True, True, True, False)


@pytest.mark.asyncio
async def test_capacity_role_convergence_grants_bounded_protected_claim_surface(
    postgres_url: str,
) -> None:
    """A protected claim can mutate only its exact public scheduler surface."""

    name = f"claimsurf-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)

    (
        owner,
        _migrator,
        _agent,
        _executor,
        _observer,
        _runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, _new_credentials())

    expected_columns = {
        ("workers", "status", "SELECT"),
        ("workers", "drain_state", "SELECT"),
        ("workers", "status", "UPDATE"),
        ("trials", "execution_route_json", "SELECT"),
        ("trials", "worker_id", "UPDATE"),
        ("trials", "claimed_at", "UPDATE"),
        ("trials", "pre_start_heartbeat_at", "UPDATE"),
        ("trials", "failure_reason", "UPDATE"),
        ("trials", "failure_message", "UPDATE"),
        ("trials", "attempt_count", "UPDATE"),
        ("execution_attempts", "worker_id", "SELECT"),
        ("execution_attempts", "state", "SELECT"),
        ("worker_pool_autoscaler_policies", "actuator_config", "SELECT"),
        ("worker_pool_autoscaler_policies", "prod_pressure_state", "SELECT"),
        ("pipeline_acceptance_preflight_prerequisites", "worker_id", "SELECT"),
        ("pipeline_acceptance_preflight_prerequisites", "fence_state", "SELECT"),
        ("team_quotas", "max_attempts_ceiling", "SELECT"),
        ("task_image_materializations", "state", "SELECT"),
        ("task_image_materializations", "state", "UPDATE"),
        ("task_image_materializations", "registry_images", "SELECT"),
        ("batch_family_state", "state_uri", "SELECT"),
        ("batch_family_state", "state", "UPDATE"),
        ("batch_family_state", "updated_at", "UPDATE"),
        ("batches", "family_run_spec", "SELECT"),
        ("batches", "id", "UPDATE"),
        ("model_switch_plans", "id", "UPDATE"),
        ("team_quotas", "in_flight_count", "UPDATE"),
        ("execution_admission_policies", "active_count", "SELECT"),
        ("execution_admission_policies", "active_count", "UPDATE"),
        ("execution_admission_reservations", "id", "SELECT"),
        ("execution_admission_reservations", "trial_id", "SELECT"),
        ("execution_admission_reservations", "attempt", "SELECT"),
        ("execution_admission_reservations", "execution_role", "SELECT"),
        ("execution_admission_reservations", "trial_id", "INSERT"),
    }
    connect_url = postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(connect_url) as connection:
        missing: set[tuple[str, str, str]] = set()
        for table, column, privilege in expected_columns:
            result = await connection.execute(
                "SELECT has_column_privilege(%s, %s, %s, %s)",
                (owner, f"public.{table}", column, privilege),
            )
            if await result.fetchone() != (True,):
                missing.add((table, column, privilege))
        assert missing == set()
        for table in {table for table, _column, _privilege in expected_columns}:
            result = await connection.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT,INSERT,UPDATE,DELETE')",
                (owner, f"public.{table}"),
            )
            assert await result.fetchone() == (False,)


@pytest.mark.asyncio
async def test_capacity_role_convergence_removes_contaminated_executor_privileges(
    postgres_url: str,
) -> None:
    name = f"execcont-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    credentials = _new_credentials()
    (
        _owner,
        _migrator,
        _agent,
        executor,
        _observer,
        _runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, credentials)
    schema_name = f"executor_contamination_{uuid4().hex[:8]}"
    connect_url = postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(connect_url, autocommit=True) as connection:
        await connection.execute(f'CREATE ROLE "{identity.db_role}" LOGIN')
        await connection.execute(f'CREATE SCHEMA "{schema_name}"')
        await connection.execute(f'CREATE TABLE "{schema_name}".evidence (id bigint)')
        await connection.execute(f'CREATE SEQUENCE "{schema_name}".evidence_sequence')
        await connection.execute(
            f'CREATE FUNCTION "{schema_name}".evidence_function() RETURNS bigint '
            "LANGUAGE sql AS 'SELECT 1'"
        )
        await connection.execute(
            f'GRANT ALL PRIVILEGES ON DATABASE "{database_name}" TO "{executor}"'
        )
        await connection.execute(f'GRANT ALL PRIVILEGES ON SCHEMA "{schema_name}" TO "{executor}"')
        await connection.execute(
            f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema_name}" TO "{executor}"'
        )
        await connection.execute(
            f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "{schema_name}" TO "{executor}"'
        )
        await connection.execute(
            f'GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA "{schema_name}" TO "{executor}"'
        )
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema_name}" TO PUBLIC')
        await connection.execute(
            f'GRANT EXECUTE ON FUNCTION "{schema_name}".evidence_function() TO PUBLIC'
        )

    try:
        await database._converge_roles(identity, credentials)
        async with await psycopg.AsyncConnection.connect(connect_url) as connection:
            privileges = await connection.execute(
                "SELECT has_database_privilege(%s, %s, 'CREATE'), "
                "has_schema_privilege(%s, %s, 'USAGE'), "
                "has_table_privilege(%s, %s, 'SELECT'), "
                "has_sequence_privilege(%s, %s, 'USAGE'), "
                "has_function_privilege(%s, %s, 'EXECUTE')",
                (
                    executor,
                    database_name,
                    executor,
                    schema_name,
                    executor,
                    f"{schema_name}.evidence",
                    executor,
                    f"{schema_name}.evidence_sequence",
                    executor,
                    f"{schema_name}.evidence_function()",
                ),
            )
            assert await privileges.fetchone() == (False, False, False, False, True)

        async with await psycopg.AsyncConnection.connect(
            connect_url,
            autocommit=True,
        ) as connection:
            await connection.execute(f'SET ROLE "{identity.db_role}"')
            application_result = await connection.execute(
                f'SELECT "{schema_name}".evidence_function()'
            )
            assert await application_result.fetchone() == (1,)
            await connection.execute("RESET ROLE")
            await connection.execute(f'SET ROLE "{executor}"')
            with pytest.raises(InsufficientPrivilege):
                await connection.execute(f'SELECT "{schema_name}".evidence_function()')
            await connection.execute("RESET ROLE")
    finally:
        async with await psycopg.AsyncConnection.connect(
            connect_url,
            autocommit=True,
        ) as connection:
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            await connection.execute(f'DROP ROLE IF EXISTS "{identity.db_role}"')


@pytest.mark.asyncio
async def test_capacity_role_convergence_isolates_executor_from_public_functions(
    postgres_url: str,
) -> None:
    """Catch PUBLIC schema usage bypassing direct executor function revocation."""

    name = f"execpublic-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    function_name = f"executor_public_evidence_{uuid4().hex[:8]}"
    connect_url = postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(connect_url, autocommit=True) as connection:
        await connection.execute(f'CREATE ROLE "{identity.db_role}" LOGIN')
        await connection.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")
        await connection.execute(
            f'CREATE FUNCTION public."{function_name}"() RETURNS bigint '
            "LANGUAGE sql AS 'SELECT 1'"
        )

    executor = ""
    try:
        (
            _owner,
            _migrator,
            _agent,
            executor,
            _observer,
            _runtime,
            _migrator_url,
            _agent_url,
        ) = await database._converge_roles(identity, _new_credentials())
        async with await psycopg.AsyncConnection.connect(
            connect_url,
            autocommit=True,
        ) as connection:
            privileges = await connection.execute(
                "SELECT has_schema_privilege(%s, 'public', 'USAGE'), "
                "has_function_privilege(%s, %s, 'EXECUTE'), "
                "has_schema_privilege(%s, 'public', 'USAGE'), "
                "has_function_privilege(%s, %s, 'EXECUTE')",
                (
                    identity.db_role,
                    identity.db_role,
                    f'public."{function_name}"()',
                    executor,
                    executor,
                    f'public."{function_name}"()',
                ),
            )
            assert await privileges.fetchone() == (True, True, False, True)
            await connection.execute(f'SET ROLE "{identity.db_role}"')
            application_result = await connection.execute(f'SELECT public."{function_name}"()')
            assert await application_result.fetchone() == (1,)
            await connection.execute("RESET ROLE")
            await connection.execute(f'SET ROLE "{executor}"')
            with pytest.raises(InsufficientPrivilege):
                await connection.execute(f'SELECT public."{function_name}"()')
            await connection.execute("RESET ROLE")
            executor_role = await connection.execute(
                "SELECT rolcanlogin, rolinherit, rolpassword IS NULL, "
                "(SELECT count(*) FROM pg_auth_members membership "
                "JOIN pg_roles member ON member.oid = membership.member "
                "WHERE member.rolname = %s) FROM pg_authid WHERE rolname = %s",
                (executor, executor),
            )
            assert await executor_role.fetchone() == (False, False, True, 0)
    finally:
        async with await psycopg.AsyncConnection.connect(
            connect_url,
            autocommit=True,
        ) as connection:
            await connection.execute(f'DROP FUNCTION IF EXISTS public."{function_name}"()')
            await connection.execute(f'REVOKE USAGE ON SCHEMA public FROM "{identity.db_role}"')
            await connection.execute(f'DROP ROLE IF EXISTS "{identity.db_role}"')
            await connection.execute("GRANT USAGE ON SCHEMA public TO PUBLIC")


@pytest.mark.asyncio
async def test_capacity_migrator_authority_is_sealed_between_reconciliations(
    postgres_url: str,
) -> None:
    name = f"seal-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    (
        owner,
        migrator,
        _agent,
        _executor,
        _observer,
        _runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, _new_credentials())

    await database._seal_migrator(identity, owner=owner, migrator=migrator)

    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
    ) as connection:
        role = await connection.execute(
            "SELECT rolcanlogin, rolpassword IS NULL FROM pg_authid WHERE rolname = %s",
            (migrator,),
        )
        assert await role.fetchone() == (False, True)
        membership = await connection.execute(
            "SELECT pg_has_role(%s, %s, 'MEMBER')",
            (migrator, owner),
        )
        assert await membership.fetchone() == (False,)
        create_privilege = await connection.execute(
            "SELECT has_database_privilege(%s, %s, 'CREATE')",
            (owner, database_name),
        )
        assert await create_privilege.fetchone() == (False,)


@pytest.mark.asyncio
async def test_capacity_migrator_seal_is_idempotent_before_roles_exist(
    postgres_url: str,
) -> None:
    name = f"seal-absent-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    owner, migrator, *_rest = _role_names(identity)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)

    await database._seal_migrator(identity, owner=owner, migrator=migrator)

    async with await psycopg.AsyncConnection.connect(
        postgres_url.replace("postgresql+psycopg://", "postgresql://", 1),
    ) as connection:
        roles = await connection.execute(
            "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
            ([owner, migrator],),
        )
        assert await roles.fetchall() == []


@pytest.mark.asyncio
async def test_transient_migrator_admin_converges_roles_without_altering_itself(
    postgres_url: str,
) -> None:
    parsed = make_url(postgres_url)
    suffix = uuid4().hex[:8]
    database_name = f"transient_roles_{suffix}"
    application_role = f"transient_app_{suffix}"
    application_password = f"transient-app-{uuid4().hex}"
    identity = replace(
        derive_identity(f"transient-{uuid4().hex[:8]}"),
        database=database_name,
        db_role=application_role,
    )
    credentials = _new_credentials()
    owner, migrator, agent, executor, observer, runtime = _role_names(identity)
    protected = (owner, migrator, agent, executor, observer, runtime)
    cluster_url = (
        parsed.set(database="template1")
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    connect_url = (
        parsed.set(database=database_name)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    application_url = (
        parsed.set(database=database_name, username=application_role, password=application_password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    async with await psycopg.AsyncConnection.connect(
        cluster_url,
        autocommit=True,
    ) as connection:
        await connection.execute(
            psycopg.sql.SQL(
                "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {}"
            ).format(
                psycopg.sql.Identifier(application_role),
                psycopg.sql.Literal(application_password),
            )
        )
        await connection.execute(
            psycopg.sql.SQL("CREATE DATABASE {} OWNER {}").format(
                psycopg.sql.Identifier(database_name),
                psycopg.sql.Identifier(application_role),
            )
        )
    try:
        repo_root = Path(__file__).resolve().parents[2]
        cfg = AlembicConfig(str(repo_root / "migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(repo_root / "migrations"))
        cfg.set_main_option(
            "sqlalchemy.url",
            application_url.replace("postgresql://", "postgresql+psycopg://", 1),
        )
        command.upgrade(cfg, "head")
        async with await psycopg.AsyncConnection.connect(
            connect_url,
            autocommit=True,
        ) as connection:
            for role in (owner, executor):
                await connection.execute(
                    psycopg.sql.SQL(
                        "CREATE ROLE {} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD NULL"
                    ).format(psycopg.sql.Identifier(role))
                )
            await connection.execute(
                psycopg.sql.SQL(
                    "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                    "INHERIT NOREPLICATION NOBYPASSRLS PASSWORD {} VALID UNTIL '2099-01-01'"
                ).format(
                    psycopg.sql.Identifier(migrator),
                    psycopg.sql.Literal(credentials.migrator_password),
                )
            )
            for role, password in (
                (agent, credentials.agent_password),
                (observer, credentials.observer_password),
                (runtime, credentials.runtime_password),
            ):
                await connection.execute(
                    psycopg.sql.SQL(
                        "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {} "
                        "VALID UNTIL '2099-01-01'"
                    ).format(psycopg.sql.Identifier(role), psycopg.sql.Literal(password))
                )
            await connection.execute(
                psycopg.sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
                    psycopg.sql.Identifier(application_role),
                    psycopg.sql.Identifier(migrator),
                )
            )
            await connection.execute(
                psycopg.sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE").format(
                    psycopg.sql.Identifier(owner),
                    psycopg.sql.Identifier(migrator),
                )
            )

        migrator_url = parsed.set(
            database=database_name,
            username=migrator,
            password=credentials.migrator_password,
        ).render_as_string(hide_password=False)
        database = PsycopgPersonalDevCapacityDatabase(
            migrator_url,
            transient_role_admin=True,
        )

        observed = await database._converge_roles(identity, credentials)

        assert observed[:6] == protected
        async with await psycopg.AsyncConnection.connect(connect_url) as connection:
            role = await connection.execute(
                "SELECT rolcanlogin, rolcreaterole, rolpassword IS NULL "
                "FROM pg_authid WHERE rolname = %s",
                (migrator,),
            )
            assert await role.fetchone() == (True, False, False)
            memberships = await connection.execute(
                "SELECT member.rolname, granted.rolname, membership.admin_option, "
                "membership.inherit_option, membership.set_option "
                "FROM pg_auth_members membership "
                "JOIN pg_roles member ON member.oid = membership.member "
                "JOIN pg_roles granted ON granted.oid = membership.roleid "
                "WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s) "
                "ORDER BY member.rolname, granted.rolname",
                (list(protected), list(protected)),
            )
            assert await memberships.fetchall() == sorted(
                [
                    (migrator, application_role, False, True, True),
                    (migrator, owner, False, True, True),
                ]
            )
    finally:
        async with await psycopg.AsyncConnection.connect(
            cluster_url,
            autocommit=True,
        ) as connection:
            await connection.execute(
                psycopg.sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    psycopg.sql.Identifier(database_name)
                )
            )
            for role in (*reversed(protected), application_role):
                await connection.execute(
                    psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
                )


@pytest.mark.asyncio
async def test_peer_sql_arms_and_seals_exact_staging_migrator_authority(
    postgres_url: str,
) -> None:
    credentials = _new_credentials()

    class PayloadRunner:
        def __init__(self) -> None:
            self.environment: dict[str, str] = {}
            self.payloads: list[bytes] = []

        def run_checked(
            self,
            _argv,
            *,
            env,
            input_payload,
            timeout_seconds,
        ) -> None:
            assert env == self.environment
            assert input_payload is not None
            assert timeout_seconds == 60.0
            self.payloads.append(input_payload)

    runner = PayloadRunner()
    component = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: {},
    )
    component._arm_transient_migrator(_staging_seed(credentials))
    component._seal_transient_migrator()
    arm_payload, *seal_payloads = runner.payloads
    assert len(seal_payloads) == 6
    parsed = make_url(postgres_url)
    identity = staging_capacity_identity()
    owner, migrator, agent, executor, observer, runtime = _role_names(identity)
    protected = (owner, migrator, agent, executor, observer, runtime)
    superuser_url = (
        parsed.set(database="template1")
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )

    async with await psycopg.AsyncConnection.connect(
        superuser_url,
        autocommit=True,
    ) as connection:
        await connection.execute("DROP DATABASE IF EXISTS loom")
        for role in (*reversed(protected), "loom", "postgres"):
            await connection.execute(
                psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(psycopg.sql.Identifier(role))
            )
        await connection.execute("CREATE ROLE postgres SUPERUSER NOLOGIN")
        await connection.execute(
            "CREATE ROLE loom NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOINHERIT NOREPLICATION NOBYPASSRLS"
        )
        await connection.execute("CREATE DATABASE loom OWNER loom")

    loom_superuser_url = (
        parsed.set(database="loom")
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1)
    )
    async with await psycopg.AsyncConnection.connect(
        loom_superuser_url,
        autocommit=True,
    ) as connection:
        await connection.execute("SET SESSION AUTHORIZATION postgres")
        await connection.execute(arm_payload.decode("utf-8"))

    async with await psycopg.AsyncConnection.connect(loom_superuser_url) as connection:
        armed_state = await connection.execute(
            "SELECT rolname, rolcanlogin, rolinherit, rolcreaterole, "
            "rolpassword IS NOT NULL, rolvaliduntil IS NOT NULL "
            "FROM pg_authid WHERE rolname = ANY(%s) ORDER BY rolname",
            (list(protected),),
        )
        assert await armed_state.fetchall() == sorted(
            [
                (owner, False, False, False, False, True),
                (migrator, True, True, False, True, True),
                (agent, True, False, False, True, True),
                (executor, False, False, False, False, True),
                (observer, True, False, False, True, True),
                (runtime, True, False, False, True, True),
            ]
        )
        armed_memberships = await connection.execute(
            "SELECT member.rolname, granted.rolname, membership.admin_option, "
            "membership.inherit_option, membership.set_option "
            "FROM pg_auth_members membership "
            "JOIN pg_roles member ON member.oid = membership.member "
            "JOIN pg_roles granted ON granted.oid = membership.roleid "
            "WHERE member.rolname = ANY(%s) OR granted.rolname = ANY(%s) "
            "ORDER BY member.rolname, granted.rolname",
            (list(protected), list(protected)),
        )
        assert await armed_memberships.fetchall() == sorted(
            [
                (migrator, "loom", False, True, True),
                (migrator, owner, False, True, True),
            ]
        )

    disable_payload, terminate_payload, cleanup_payload, *finish_payloads = seal_payloads
    async with await psycopg.AsyncConnection.connect(
        loom_superuser_url,
        autocommit=True,
    ) as connection:
        await connection.execute("SET SESSION AUTHORIZATION postgres")
        await connection.execute(disable_payload.decode("utf-8"))
        await connection.execute(terminate_payload.decode("utf-8"))
        await connection.execute("CREATE ROLE loom_cap_staging_foreign NOLOGIN")
        await connection.execute("CREATE ROLE loom_cap_staging_dependent NOLOGIN")
        await connection.execute(
            "GRANT loom_cap_staging_foreign TO loom_cap_staging_migrator WITH ADMIN OPTION"
        )
        await connection.execute("SET ROLE loom_cap_staging_migrator")
        await connection.execute("GRANT loom_cap_staging_foreign TO loom_cap_staging_dependent")
        await connection.execute("RESET ROLE")
        with pytest.raises(psycopg.errors.DependentObjectsStillExist):
            await connection.execute(cleanup_payload.decode("utf-8"))
        await connection.execute("ROLLBACK")
        disabled = await connection.execute(
            "SELECT rolcanlogin, rolpassword IS NULL FROM pg_authid "
            "WHERE rolname = 'loom_cap_staging_migrator'"
        )
        assert await disabled.fetchone() == (False, True)
        await connection.execute("DROP ROLE loom_cap_staging_dependent")
        await connection.execute(cleanup_payload.decode("utf-8"))
        for seal_payload in finish_payloads:
            await connection.execute(seal_payload.decode("utf-8"))
        await connection.execute("DROP ROLE loom_cap_staging_foreign")

    async with await psycopg.AsyncConnection.connect(loom_superuser_url) as connection:
        migrator_state = await connection.execute(
            "SELECT rolname, rolcanlogin, rolinherit, rolcreaterole, "
            "rolpassword IS NOT NULL, "
            "COALESCE(rolvaliduntil = 'infinity'::timestamptz, false) "
            "FROM pg_authid WHERE rolname = ANY(%s) ORDER BY rolname",
            (list(protected),),
        )
        assert await migrator_state.fetchall() == sorted(
            [
                (owner, False, False, False, False, True),
                (migrator, False, True, False, False, True),
                (agent, True, False, False, True, True),
                (executor, False, False, False, False, True),
                (observer, True, False, False, True, True),
                (runtime, True, False, False, True, True),
            ]
        )
        memberships = await connection.execute(
            "SELECT count(*) FROM pg_auth_members membership "
            "JOIN pg_roles member ON member.oid = membership.member "
            "JOIN pg_roles granted ON granted.oid = membership.roleid "
            "WHERE member.rolname = %s OR granted.rolname = %s",
            (migrator, migrator),
        )
        assert await memberships.fetchone() == (0,)
        application_state = await connection.execute(
            "SELECT rolsuper, rolcreaterole FROM pg_roles WHERE rolname = 'loom'"
        )
        assert await application_state.fetchone() == (False, False)

    async with await psycopg.AsyncConnection.connect(
        superuser_url,
        autocommit=True,
    ) as connection:
        await connection.execute("DROP DATABASE loom")
        for role in (*reversed(protected), "loom", "postgres"):
            await connection.execute(
                psycopg.sql.SQL("DROP ROLE {}").format(psycopg.sql.Identifier(role))
            )


@pytest.mark.asyncio
async def test_destroy_seal_disables_primary_and_capacity_database_logins(
    postgres_url: str,
) -> None:
    name = f"retain-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    connect_url = postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    async with await psycopg.AsyncConnection.connect(
        connect_url,
        autocommit=True,
    ) as connection:
        await connection.execute(
            f"CREATE ROLE \"{identity.db_role}\" LOGIN PASSWORD 'primary-password'"
        )
    (
        owner,
        migrator,
        agent,
        executor,
        observer,
        runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, _new_credentials())

    await database.seal(identity)

    async with await psycopg.AsyncConnection.connect(connect_url) as connection:
        roles = await connection.execute(
            "SELECT rolname, rolcanlogin, rolpassword IS NULL FROM pg_authid "
            "WHERE rolname = ANY(%s) ORDER BY rolname",
            ([identity.db_role, owner, migrator, agent, executor, observer, runtime],),
        )
        assert await roles.fetchall() == sorted(
            (role, False, True)
            for role in (identity.db_role, owner, migrator, agent, executor, observer, runtime)
        )


@pytest.mark.asyncio
async def test_destroy_seal_terminates_live_sessions_and_blocks_reconnects(
    postgres_url: str,
) -> None:
    name = f"sealrace-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    connect_url = postgres_url.replace("postgresql+psycopg://", "postgresql://", 1)
    application_password = f"app-password-{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(connect_url, autocommit=True) as connection:
        await connection.execute(
            f"CREATE ROLE \"{identity.db_role}\" LOGIN PASSWORD '{application_password}'"
        )
    credentials = _new_credentials()
    (
        owner,
        migrator,
        agent,
        executor,
        observer,
        runtime,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(identity, credentials)

    role_urls = {
        migrator: make_url(postgres_url)
        .set(database=database_name, username=migrator, password=credentials.migrator_password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1),
        agent: make_url(postgres_url)
        .set(database=database_name, username=agent, password=credentials.agent_password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1),
        observer: make_url(postgres_url)
        .set(database=database_name, username=observer, password=credentials.observer_password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1),
        identity.db_role: make_url(postgres_url)
        .set(database=database_name, username=identity.db_role, password=application_password)
        .render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://", 1),
    }
    live_connections: dict[str, psycopg.AsyncConnection] = {}
    backend_pids: dict[str, int] = {}
    protected_roles = (*_role_names(identity), identity.db_role)
    role_lock: psycopg.AsyncConnection | None = None
    try:
        for role, url in role_urls.items():
            connection = await psycopg.AsyncConnection.connect(url, autocommit=True)
            live_connections[role] = connection
            row = await connection.execute("SELECT pg_backend_pid()")
            pid = await row.fetchone()
            assert pid is not None
            backend_pids[role] = pid[0]

        race_connected = asyncio.Event()

        async def race_reconnect(role: str) -> str:
            try:
                connection = await psycopg.AsyncConnection.connect(role_urls[role], autocommit=True)
            except psycopg.Error:
                return "blocked"
            try:
                race_connected.set()
                await seal_task
                with pytest.raises(psycopg.Error):
                    await connection.execute("SELECT 1")
                return "terminated"
            finally:
                with suppress(Exception):
                    await connection.close()

        # Hold the first role update so a reversed terminate-before-NOLOGIN
        # implementation has a deterministic observable failure window.
        role_lock = await psycopg.AsyncConnection.connect(connect_url)
        await role_lock.execute(
            "SELECT oid FROM pg_authid WHERE rolname = %s FOR UPDATE",
            (owner,),
        )
        seal_task = asyncio.create_task(database.seal(identity))
        async with asyncio.timeout(5):
            while True:
                async with await psycopg.AsyncConnection.connect(connect_url) as probe:
                    waiting = await probe.execute(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE query LIKE %s AND wait_event_type = 'Lock')",
                        (f'ALTER ROLE "%{owner}%" NOLOGIN PASSWORD NULL',),
                    )
                    if bool((await waiting.fetchone())[0]):
                        break
                await asyncio.sleep(0.01)
        for connection in live_connections.values():
            await connection.execute("SELECT 1")

        race_task = asyncio.create_task(race_reconnect(agent))
        async with asyncio.timeout(5):
            await race_connected.wait()
        await role_lock.commit()
        race_result = await race_task
        await seal_task

        for connection in live_connections.values():
            with pytest.raises(psycopg.Error):
                await connection.execute("SELECT 1")

        async with await psycopg.AsyncConnection.connect(connect_url) as admin:
            roles = await admin.execute(
                "SELECT rolname, rolcanlogin, rolpassword IS NULL FROM pg_authid "
                "WHERE rolname = ANY(%s) ORDER BY rolname",
                ([identity.db_role, owner, migrator, agent, executor, observer, runtime],),
            )
            assert await roles.fetchall() == sorted(
                (role, False, True)
                for role in (
                    identity.db_role,
                    owner,
                    migrator,
                    agent,
                    executor,
                    observer,
                    runtime,
                )
            )
            active = await admin.execute(
                "SELECT pid FROM pg_stat_activity WHERE pid = ANY(%s)",
                (list(backend_pids.values()),),
            )
            assert await active.fetchall() == []

        assert race_result in {"blocked", "terminated"}
        for _role, url in role_urls.items():
            with pytest.raises(psycopg.Error):
                async with await psycopg.AsyncConnection.connect(url, autocommit=True):
                    pass
    finally:
        if role_lock is not None:
            with suppress(Exception):
                await role_lock.rollback()
            with suppress(Exception):
                await role_lock.close()
        for connection in live_connections.values():
            with suppress(Exception):
                await connection.close()
        async with await psycopg.AsyncConnection.connect(
            connect_url, autocommit=True
        ) as connection:
            existing_roles = await connection.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                (list(protected_roles),),
            )
            for (role_name,) in await existing_roles.fetchall():
                await connection.execute(f'DROP OWNED BY "{role_name}"')
                await connection.execute(f'DROP ROLE IF EXISTS "{role_name}"')


@pytest.mark.asyncio
async def test_personal_capacity_status_reader_accepts_jsonb_uuid_dict_observation(
    postgres_url: str,
) -> None:
    database_name = f"loom_capacity_status_{uuid4().hex[:8]}"
    admin_url = (
        make_url(postgres_url).set(database="postgres").render_as_string(hide_password=False)
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    quoted_database = admin_engine.dialect.identifier_preparer.quote(database_name)
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"CREATE DATABASE {quoted_database} TEMPLATE template0")
        repo_root = Path(__file__).resolve().parents[2]
        cfg = AlembicConfig(str(repo_root / "migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(repo_root / "migrations"))
        cfg.set_main_option(
            "sqlalchemy.url",
            make_url(postgres_url)
            .set(database=database_name)
            .render_as_string(hide_password=False),
        )
        command.upgrade(cfg, "head")

        identity = replace(derive_identity(f"status-{uuid4().hex[:8]}"), database=database_name)
        database = PsycopgPersonalDevCapacityDatabase(postgres_url)
        credentials = _new_credentials()
        (
            owner,
            _migrator,
            agent,
            executor,
            observer,
            runtime,
            migrator_url,
            _agent_url,
        ) = await database._converge_roles(identity, credentials)
        await database._migrate(
            migrator_url=migrator_url,
            owner=owner,
            agent=agent,
            executor=executor,
            observer=observer,
            runtime=runtime,
        )
        await database._converge_executor_surface(
            migrator_url=migrator_url,
            owner=owner,
            executor=executor,
            observer=observer,
            runtime=runtime,
        )

        subject_id = uuid4()
        subject_incarnation = uuid4()
        binding = _active_binding(subject_id, subject_incarnation)
        binding_json = json.dumps(binding.model_dump(mode="json"), sort_keys=True)
        worker_id = uuid4()
        worker_incarnation = uuid4()
        engine = create_engine(
            make_url(postgres_url)
            .set(database=database_name)
            .render_as_string(hide_password=False),
            isolation_level="SERIALIZABLE",
        )
        quoted_owner = engine.dialect.identifier_preparer.quote(owner)
        try:
            with engine.begin() as connection:
                connection.exec_driver_sql(f"SET LOCAL ROLE {quoted_owner}")
                connection.execute(
                    text(
                        "INSERT INTO loom_capacity_guard.authority_state "
                        "(singleton_id, schema_version, environment_id, subject_id, "
                        "subject_incarnation, authority_mode, authority_incarnation, "
                        "reporter_incarnation, reporter_high_water, allocation_epoch, "
                        "deployment_generation, configuration_generation, candidate_digest) "
                        "VALUES (1, 1, 'dev-status', :subject_id, :subject_incarnation, "
                        "'disabled', :authority_incarnation, :reporter_incarnation, 0, 0, 7, 5, "
                        ":digest)"
                    ),
                    {
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "authority_incarnation": binding.execution.authority_incarnation,
                        "reporter_incarnation": uuid4(),
                        "digest": "b" * 64,
                    },
                )
                agent_incarnation = uuid4()
                connection.execute(
                    text(
                        "INSERT INTO loom_capacity_guard.agent_registrations "
                        "(agent_incarnation, singleton_id, schema_version, environment_id, "
                        "subject_id, subject_incarnation, authority_incarnation, "
                        "reporter_incarnation, authority_mode, allocation_epoch, "
                        "candidate_digest, candidate_identity_algorithm, "
                        "candidate_identity, candidate_publication_sha256, "
                        "deployment_generation, configuration_generation, registration_state) "
                        "VALUES (:agent_incarnation, 1, 1, 'dev-status', :subject_id, "
                        ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
                        "'disabled', 0, :digest, 'source-sha256', :digest, :digest, "
                        "7, 5, 'registered')"
                    ),
                    {
                        "agent_incarnation": agent_incarnation,
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "authority_incarnation": binding.execution.authority_incarnation,
                        "reporter_incarnation": uuid4(),
                        "digest": "c" * 64,
                    },
                )
                connection.execute(
                    text(
                        "INSERT INTO loom_capacity_guard.executable_claim_state "
                        "(intent_id, subject_id, subject_incarnation, binding, claim_high_water, "
                        "terminal_high_water, draining) VALUES (:intent_id, :subject_id, "
                        ":subject_incarnation, CAST(:binding AS jsonb), 0, 0, false)"
                    ),
                    {
                        "intent_id": binding.intent_id,
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "binding": binding_json,
                    },
                )
                connection.execute(
                    text(
                        "INSERT INTO loom_capacity_guard.executable_admission_events "
                        "(operation_id, event_kind, agent_incarnation, subject_id, "
                        "subject_incarnation, intent_id, bootstrap_registration_epoch, "
                        "protected_registration_epoch, physical_job_id, worker_id, "
                        "worker_incarnation, worker_credential_sha256, bootstrap_revoked, "
                        "predecessor_credential_revoked, worker_credential_revoked, binding, "
                        "request_payload, request_digest, receipt) "
                        "VALUES (:operation_id, 'worker-registered', :agent_incarnation, "
                        ":subject_id, :subject_incarnation, :intent_id, 19, 23, 'oldlab-12345', "
                        ":worker_id, :worker_incarnation, :worker_credential_sha256, true, "
                        "false, false, CAST(:binding AS jsonb), "
                        "CAST(:request_payload AS jsonb), :request_digest, "
                        "CAST(:receipt AS jsonb))"
                    ),
                    {
                        "operation_id": uuid4(),
                        "agent_incarnation": agent_incarnation,
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "intent_id": binding.intent_id,
                        "worker_id": worker_id,
                        "worker_incarnation": worker_incarnation,
                        "worker_credential_sha256": "d" * 64,
                        "binding": binding_json,
                        "request_payload": '{"schema_version":2}',
                        "request_digest": "e" * 64,
                        "receipt": '{"schema_version":2}',
                    },
                )
                connection.execute(
                    text(
                        "INSERT INTO loom_capacity_guard.executable_admission_events "
                        "(operation_id, event_kind, agent_incarnation, subject_id, "
                        "subject_incarnation, intent_id, bootstrap_registration_epoch, "
                        "bootstrap_sha256, binding, request_payload, request_digest, receipt) "
                        "VALUES (:operation_id, 'prepared', :agent_incarnation, :subject_id, "
                        ":subject_incarnation, :intent_id, 19, :bootstrap_sha256, "
                        "CAST(:binding AS jsonb), CAST(:request_payload AS jsonb), "
                        ":request_digest, CAST(:receipt AS jsonb))"
                    ),
                    {
                        "operation_id": uuid4(),
                        "agent_incarnation": agent_incarnation,
                        "subject_id": subject_id,
                        "subject_incarnation": subject_incarnation,
                        "intent_id": binding.intent_id,
                        "bootstrap_sha256": "f" * 64,
                        "binding": binding_json,
                        "request_payload": '{"schema_version":2}',
                        "request_digest": "0" * 64,
                        "receipt": '{"schema_version":2}',
                    },
                )
        finally:
            engine.dispose()

        class _Kubectl:
            async def read_secret_optional(self, namespace: str, name: str):
                assert namespace == identity.namespace
                assert name == "loom-capacity-agent-credentials"
                return {
                    "observer-password": credentials.observer_password.encode("ascii"),
                    "subject-incarnation": str(subject_incarnation).encode("ascii"),
                }

        class _Projector:
            async def subject_status(self, **kwargs: object) -> PersonalDevCapacitySubjectStatus:
                assert kwargs == {
                    "subject_id": subject_id,
                    "subject_incarnation": subject_incarnation,
                    "deployment_generation": 7,
                }
                return PersonalDevCapacitySubjectStatus(
                    subject_id=subject_id,
                    subject_incarnation=subject_incarnation,
                    deployment_generation=7,
                    checkpoint=PersonalDevCapacityManagerCheckpoint(
                        configuration_epoch=5,
                        execution_state="active",
                        execution_epoch=7,
                        executable_new_capacity_ceiling=1,
                    ),
                    capacity_prepared=True,
                    capacity_status="waiting",
                    active_bindings=(binding,),
                )

        reader = PersonalDevCapacityStatusReader(
            kubectl=_Kubectl(),  # type: ignore[arg-type]
            database_admin_url=postgres_url,
            projector=_Projector(),  # type: ignore[arg-type]
        )

        assert await reader.read(
            namespace=identity.namespace,
            database=database_name,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            deployment_generation=7,
        ) == PersonalDevCapacityAvailability("available", True, True)
        await database.destroy(identity)
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted_database}")
        admin_engine.dispose()
