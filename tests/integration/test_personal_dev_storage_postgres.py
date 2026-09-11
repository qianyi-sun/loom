"""Full-length incarnation roles stay disjoint on real PostgreSQL."""

from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.dev_instance_provision import provisioning_plan_for_identity
from loom.dev_instance_runtime import PsycopgSharedFixtureSqlExecutor
from loom.personal_dev_capacity_identity import capacity_role_names
from loom.personal_dev_capacity_runtime import PsycopgPersonalDevCapacityDatabase, _new_credentials
from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1


async def test_incarnation_database_and_protected_role_isolation():
    with PostgresContainer("postgres:16") as postgres:
        admin_url = postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        fixture_sql = PsycopgSharedFixtureSqlExecutor(admin_url)
        capacity = PsycopgPersonalDevCapacityDatabase(admin_url)
        first = PersonalDevStorageBindingV1(
            layout="incarnation-v1",
            environment_name="a" * 20,
            subject_id=uuid4(),
            subject_incarnation=uuid4(),
            owner_user_id=uuid4(),
            owner_team_id=uuid4(),
        )
        bindings = (
            first,
            first.model_copy(update={"subject_incarnation": uuid4()}),
            first.model_copy(
                update={
                    "environment_name": "b" * 20,
                    "subject_id": uuid4(),
                    "subject_incarnation": uuid4(),
                    "owner_user_id": uuid4(),
                    "owner_team_id": uuid4(),
                }
            ),
        )
        identities = []
        for binding in bindings:
            identity = binding.identity
            plan = provisioning_plan_for_identity(identity, "b" * 32)
            await fixture_sql.apply_role_and_database(
                identity, role_sql=plan["role_sql"], create_database_sql=plan["create_database_sql"]
            )
            # Capacity grants target application tables; use the real schema,
            # as the lifecycle does before installing protected capacity roles.
            repo_root = Path(__file__).resolve().parents[2]
            cfg = Config(str(repo_root / "migrations" / "alembic.ini"))
            cfg.set_main_option("script_location", str(repo_root / "migrations"))
            cfg.set_main_option(
                "sqlalchemy.url",
                make_url(admin_url)
                .set(drivername="postgresql+psycopg", database=identity.database)
                .render_as_string(hide_password=False),
            )
            command.upgrade(cfg, "head")
            credentials = _new_credentials()
            roles = await capacity._converge_roles(identity, credentials)
            assert roles[:6] == capacity_role_names(identity)
            assert all(len(role) == 58 for role in roles[:6])
            identities.append((identity, credentials))

        for identity, credentials in identities:
            for role, password in (
                (identity.db_role, "b" * 32),
                (capacity_role_names(identity)[-1], credentials.runtime_password),
            ):
                url = make_url(admin_url).set(
                    database=identity.database, username=role, password=password
                )
                async with await psycopg.AsyncConnection.connect(
                    url.render_as_string(hide_password=False)
                ) as connection:
                    result = await connection.execute("SELECT current_user, current_database()")
                    assert await result.fetchone() == (role, identity.database)
                for foreign, _ in identities:
                    if foreign != identity:
                        with pytest.raises(psycopg.OperationalError):
                            async with await psycopg.AsyncConnection.connect(
                                url.set(database=foreign.database).render_as_string(
                                    hide_password=False
                                )
                            ):
                                pass
        # A retry of the original incarnation leaves new/other credentials valid.
        await capacity._converge_roles(*identities[0])
        for identity, credentials in identities[1:]:
            url = make_url(admin_url).set(
                database=identity.database,
                username=capacity_role_names(identity)[-1],
                password=credentials.runtime_password,
            )
            async with await psycopg.AsyncConnection.connect(
                url.render_as_string(hide_password=False)
            ) as connection:
                assert (await (await connection.execute("SELECT current_database()")).fetchone())[
                    0
                ] == identity.database

        old_identity, old_credentials = identities[0]
        old_admin_url = make_url(admin_url).set(database=old_identity.database)
        async with await psycopg.AsyncConnection.connect(
            old_admin_url.render_as_string(hide_password=False)
        ) as connection:
            await connection.execute("CREATE TABLE public.retained_probe (value text)")
            await connection.execute("INSERT INTO public.retained_probe VALUES ('retained')")
        await capacity.seal(old_identity)
        await capacity.seal(old_identity)
        for role, password in (
            (old_identity.db_role, "b" * 32),
            (capacity_role_names(old_identity)[-1], old_credentials.runtime_password),
        ):
            with pytest.raises(psycopg.OperationalError):
                async with await psycopg.AsyncConnection.connect(
                    old_admin_url.set(username=role, password=password).render_as_string(
                        hide_password=False
                    )
                ):
                    pass
        # Keep-data sealing revokes login, not bytes. Final cleanup is retryable
        # and cannot drop the fresh incarnation's database or protected roles.
        async with await psycopg.AsyncConnection.connect(
            old_admin_url.render_as_string(hide_password=False)
        ) as connection:
            result = await connection.execute("SELECT value FROM public.retained_probe")
            assert await result.fetchone() == ("retained",)
        await capacity.destroy(old_identity)
        await capacity.destroy(old_identity)
        for identity, credentials in identities[1:]:
            url = make_url(admin_url).set(
                database=identity.database,
                username=capacity_role_names(identity)[-1],
                password=credentials.runtime_password,
            )
            async with await psycopg.AsyncConnection.connect(
                url.render_as_string(hide_password=False)
            ) as connection:
                result = await connection.execute("SELECT current_database()")
                assert await result.fetchone() == (identity.database,)
