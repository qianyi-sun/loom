from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

import psycopg
import pytest
from psycopg.errors import InsufficientPrivilege
from sqlalchemy.engine import make_url

from loom.dev_instance import derive_identity
from loom.personal_dev_capacity_runtime import (
    PersonalDevCapacityInstallationError,
    PsycopgPersonalDevCapacityDatabase,
    _new_credentials,
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

    owner, migrator, agent, executor, _migrator_url, _agent_url = await database._converge_roles(
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
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema_name}" TO "{identity.db_role}"')
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
async def test_capacity_migrator_authority_is_sealed_between_reconciliations(
    postgres_url: str,
) -> None:
    name = f"seal-{uuid4().hex[:8]}"
    database_name = make_url(postgres_url).database
    assert database_name is not None
    identity = replace(derive_identity(name), database=database_name)
    database = PsycopgPersonalDevCapacityDatabase(postgres_url)
    owner, migrator, _agent, _executor, _migrator_url, _agent_url = await database._converge_roles(
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
    owner, migrator, agent, executor, _migrator_url, _agent_url = await database._converge_roles(
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
