from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from psycopg.errors import InsufficientPrivilege
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

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
)
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
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
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(
        identity,
        credentials,
    )
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

    owner, _migrator, _agent, _executor, _migrator_url, _agent_url = (
        await database._converge_roles(identity, _new_credentials())
    )

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
        assert await privileges.fetchone() == (True, False, False, True, False, True, False)


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
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(
        identity,
        _new_credentials(),
    )

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
        _observer,
        _migrator_url,
        _agent_url,
    ) = await database._converge_roles(
        identity,
        _new_credentials(),
    )

    await database.seal(identity)

    async with await psycopg.AsyncConnection.connect(connect_url) as connection:
        roles = await connection.execute(
            "SELECT rolname, rolcanlogin, rolpassword IS NULL FROM pg_authid "
            "WHERE rolname = ANY(%s) ORDER BY rolname",
            ([identity.db_role, owner, migrator, agent, executor],),
        )
        assert await roles.fetchall() == sorted(
            (role, False, True) for role in (identity.db_role, owner, migrator, agent, executor)
        )


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
            migrator_url,
            _agent_url,
        ) = await database._converge_roles(identity, credentials)
        await database._migrate(
            migrator_url=migrator_url,
            owner=owner,
            agent=agent,
            executor=executor,
            observer=observer,
        )
        await database._converge_executor_surface(
            migrator_url=migrator_url,
            owner=owner,
            executor=executor,
            observer=observer,
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
                        "candidate_digest, deployment_generation, configuration_generation, "
                        "registration_state) "
                        "VALUES (:agent_incarnation, 1, 1, 'dev-status', :subject_id, "
                        ":subject_incarnation, :authority_incarnation, :reporter_incarnation, "
                        "'disabled', 0, :digest, 7, 5, 'registered')"
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


@pytest.mark.asyncio
async def test_destroy_seal_disables_all_logins_before_terminating_sessions(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = replace(
        derive_identity(f"ordering-{uuid4().hex[:8]}"),
        database=make_url(postgres_url).database,
    )
    assert identity.database is not None
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    protected = (
        f"loom_cap_{identity.name.replace('-', '_')}_owner",
        f"loom_cap_{identity.name.replace('-', '_')}_migrator",
        f"loom_cap_{identity.name.replace('-', '_')}_agent",
        f"loom_cap_{identity.name.replace('-', '_')}_executor",
        f"loom_cap_{identity.name.replace('-', '_')}_observer",
        identity.db_role,
    )
    statements: list[str] = []

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        async def fetchall(self):
            return self._rows

        async def fetchone(self):
            return self._rows[0] if self._rows else None

    class _Connection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, query, params=None):
            statement = str(query)
            statements.append(statement)
            if "SELECT rolname FROM pg_roles" in statement:
                return _Result([(role,) for role in protected])
            if "SELECT EXISTS (SELECT 1 FROM pg_database" in statement:
                return _Result([(False,)])
            return _Result([])

    async def fake_connect(*_args: object, **_kwargs: object) -> _Connection:
        return _Connection()

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", fake_connect)

    await database.seal(identity)

    terminate_index = next(
        index for index, statement in enumerate(statements) if "pg_terminate_backend" in statement
    )
    alter_indices = [
        index for index, statement in enumerate(statements) if "ALTER ROLE " in statement
    ]
    assert len(alter_indices) == len(protected)
    assert max(alter_indices) < terminate_index
