"""Rebuild reference metadata using only a fresh, pinned PostgreSQL container.

No database URL, output file, or alternate reference is accepted on the command
line. Output is metadata only. The enclosing trusted source/image release binds
the reviewed pin; running this generator is not a deployment or transfer receipt.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from loom.application_runtime_grants import application_runtime_grants_ddl
from loom.application_schema_inventory import (
    ApplicationSchemaInventory,
    read_application_schema_inventory,
)
from loom.application_schema_reference import (
    ApplicationSchemaProfile,
    ApplicationSchemaReference,
    ApplicationSchemaRevision,
    application_reference_postgres_image,
    application_schema_revisions,
)
from loom.dev_instance import DevInstanceIdentity, derive_identity
from loom.dev_instance_provision import render_create_database_sql, render_role_convergence_sql
from loom.dev_instance_runtime import PsycopgSharedFixtureSqlExecutor, instance_database_url
from loom.personal_dev_capacity_runtime import (
    ApplicationOwnerBinding,
    PsycopgPersonalDevCapacityDatabase,
    _new_credentials,
)
from loom_cli.rollout.readonly_database_bootstrap import (
    ReadonlyDatabaseCredential,
    render_readonly_role_sql,
)

_ROOT = Path(__file__).resolve().parents[1]


def _head(directory: str) -> str:
    config = Config(str(_ROOT / directory / "alembic.ini"))
    config.set_main_option("script_location", str(_ROOT / directory))
    heads = ScriptDirectory.from_config(config).get_heads()
    if len(heads) != 1:
        raise RuntimeError("reference generation requires one migration head")
    return heads[0]


async def _observe_fresh_database(
    admin_url: str,
    identity: DevInstanceIdentity,
    *,
    profile: ApplicationSchemaProfile = "legacy-owner",
    revision: ApplicationSchemaRevision = "0146/guard_0033",
) -> ApplicationSchemaInventory:
    """Internal helper: admin_url belongs exclusively to our disposable container."""
    from scripts.application_schema_baseline import BaselineReferenceDatabase

    application_head, guard_head = application_schema_revisions(revision)
    factory = BaselineReferenceDatabase if revision == "0134/guard_0030" else PsycopgPersonalDevCapacityDatabase
    sealed = profile in {"sealed-owner", "staging-readonly-sealed-owner", "cnpg-staging-sealed-owner"}
    staging_readonly = profile in {"staging-readonly-legacy-owner", "staging-readonly-sealed-owner", "cnpg-staging-legacy-owner", "cnpg-staging-sealed-owner"}
    password = uuid4().hex
    bootstrap = PsycopgSharedFixtureSqlExecutor(admin_url)
    database = factory(admin_url)
    await bootstrap.apply_role_and_database(
        identity,
        role_sql=render_role_convergence_sql(identity, password),
        create_database_sql=(
            psycopg.sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'").format(
                psycopg.sql.Identifier(identity.database), psycopg.sql.Identifier(identity.db_role),
            ).as_string() if profile.startswith("cnpg-staging-") else render_create_database_sql(identity)
        ),
    )
    application_owner = identity.db_role
    application_migrator = None
    application_url = instance_database_url(admin_url, identity, password)
    try:
        if sealed:
            application_owner, application_migrator, application_url = await _prepare_sealed_owner(
                admin_url, identity
            )
            database = factory(
                admin_url,
                application_owner_binding=ApplicationOwnerBinding(
                    database=identity.database,
                    runtime_role=identity.db_role,
                    owner_role=application_owner,
                ),
            )
        environment = os.environ.copy()
        environment.pop("LOOM_DB_OWNER_ROLE", None)
        if sealed:
            environment["LOOM_DB_OWNER_ROLE"] = application_owner
        environment["LOOM_DB_URL"] = application_url
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(_ROOT / "migrations/alembic.ini"),
            "upgrade",
            application_head,
            cwd=_ROOT,
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            if await asyncio.wait_for(process.wait(), timeout=120) != 0:
                raise RuntimeError("isolated application reference migration failed")
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        if application_migrator is not None:
            await _retire_application_migrator(
                admin_url, application_owner, application_migrator, database=identity.database
            )
            application_migrator = None
        (
            owner,
            migrator,
            agent,
            executor,
            observer,
            runtime,
            migrator_url,
            _,
        ) = await database._converge_roles(identity, _new_credentials())
        await _migrate_reference_guard(
            migrator_url=migrator_url, owner=owner, agent=agent, executor=executor,
            observer=observer, runtime=runtime, guard_head=guard_head,
        )
        await database._seal_migrator(identity, owner=owner, migrator=migrator)
        bindings = {
            identity.db_role: "application-owner" if not sealed else "application-runtime",
            owner: "guard-owner",
            migrator: "guard-migrator",
            agent: "guard-agent",
            executor: "guard-executor",
            observer: "guard-observer",
            runtime: "guard-runtime",
            str(make_url(admin_url).username): "provisioner",
        }
        if sealed:
            bindings[application_owner] = "application-owner"
        target_url = make_url(admin_url).set(database=identity.database)
        with psycopg.connect(
            target_url.render_as_string(hide_password=False).replace(
                "postgresql+psycopg://", "postgresql://", 1
            ),
            autocommit=True,
        ) as connection:
            if sealed:
                with connection.transaction():
                    connection.execute(
                        application_runtime_grants_ddl(
                            owner_role=application_owner,
                            runtime_role=identity.db_role,
                        )
                    )
            if staging_readonly:
                # Same fixed grants as the staging installer, on this fresh DB.
                # Keep the exact readonly role name unnormalized in the inventory.
                payload = render_readonly_role_sql(
                    ReadonlyDatabaseCredential(
                        role="loom_rollout_readonly",
                        database="loom",
                        password=uuid4().hex + uuid4().hex,
                    )
                )
                if not payload.startswith("\\set ON_ERROR_STOP on\n"):
                    raise RuntimeError("readonly bootstrap psql directive changed")
                connection.execute(payload.removeprefix("\\set ON_ERROR_STOP on\n"))
                # The protected staging capacity bootstrap closes PUBLIC database
                # privileges when it seals its transient migrator. Personal-dev
                # provisioning does not do this; both are explicit fixed recipes.
                connection.execute(psycopg.sql.SQL(
                    "REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC"
                ).format(psycopg.sql.Identifier(identity.database)))
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                return read_application_schema_inventory(connection, role_bindings=bindings)
    finally:
        if application_migrator is not None:
            await _retire_application_migrator(
                admin_url, application_owner, application_migrator, database=identity.database
            )
        await database.destroy(identity)


async def _migrate_reference_guard(
    *, migrator_url: str, owner: str, agent: str, executor: str,
    observer: str, runtime: str, guard_head: str,
) -> None:
    """The caller has just created this disposable database and its roles."""
    environment = os.environ.copy()
    environment.update({
        "LOOM_CAPACITY_GUARD_DB_URL": migrator_url,
        "LOOM_CAPACITY_GUARD_OWNER_ROLE": owner,
        "LOOM_CAPACITY_GUARD_AGENT_ROLE": agent,
        "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE": executor,
        "LOOM_CAPACITY_GUARD_OBSERVER_ROLE": observer,
        "LOOM_CAPACITY_GUARD_RUNTIME_ROLE": runtime,
    })
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "alembic", "-c",
        str(_ROOT / "capacity_guard_migrations/alembic.ini"), "upgrade", guard_head,
        cwd=_ROOT, env=environment, stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        if await asyncio.wait_for(process.wait(), timeout=180) != 0:
            raise RuntimeError("isolated guard reference migration failed")
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def _prepare_sealed_owner(
    admin_url: str, identity: DevInstanceIdentity
) -> tuple[str, str, str]:
    """Establish ownership in an EMPTY reference DB, before creating any app objects."""
    owner = f"reference_owner_{uuid4().hex}"
    migrator = f"reference_migrator_{uuid4().hex}"
    password = uuid4().hex
    target = make_url(admin_url).set(database=identity.database)
    async with await psycopg.AsyncConnection.connect(
        target.render_as_string(hide_password=False).replace(
            "postgresql+psycopg://", "postgresql://", 1
        )
    ) as connection:
        cursor = await connection.execute(
            "SELECT (current_timestamp + interval '15 minutes')::text"
        )
        expiry = await cursor.fetchone()
        if expiry is None:
            raise RuntimeError("reference migrator expiry is unavailable")
        await connection.execute(
            psycopg.sql.SQL(
                "CREATE ROLE {} NOLOGIN NOINHERIT; CREATE ROLE {} LOGIN NOINHERIT PASSWORD {} VALID UNTIL {}; "
                "GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE; "
                "ALTER DATABASE {} OWNER TO {}; ALTER SCHEMA public OWNER TO {}; "
                "GRANT CONNECT ON DATABASE {} TO {}; "
                "ALTER ROLE {} NOLOGIN PASSWORD NULL"
            ).format(
                psycopg.sql.Identifier(owner),
                psycopg.sql.Identifier(migrator),
                psycopg.sql.Literal(password),
                psycopg.sql.Literal(expiry[0]),
                psycopg.sql.Identifier(owner),
                psycopg.sql.Identifier(migrator),
                psycopg.sql.Identifier(identity.database),
                psycopg.sql.Identifier(owner),
                psycopg.sql.Identifier(owner),
                psycopg.sql.Identifier(identity.database),
                psycopg.sql.Identifier(migrator),
                psycopg.sql.Identifier(identity.db_role),
            )
        )
    return (
        owner,
        migrator,
        target.set(username=migrator, password=password).render_as_string(hide_password=False),
    )


async def _retire_application_migrator(
    admin_url: str, owner: str, migrator: str, *, database: str
) -> None:
    async with await psycopg.AsyncConnection.connect(
        admin_url.replace("postgresql+psycopg://", "postgresql://", 1)
    ) as connection:
        await connection.execute(
            psycopg.sql.SQL(
                "REVOKE {} FROM {}; ALTER ROLE {} NOLOGIN PASSWORD NULL; "
                "REVOKE ALL ON DATABASE {} FROM {}; DROP ROLE {}"
            ).format(
                *(
                    psycopg.sql.Identifier(value)
                    for value in (owner, migrator, migrator, database, migrator, migrator)
                )
            )
        )


async def build_application_schema_reference(
    *, profile: ApplicationSchemaProfile = "legacy-owner", postgres_major: int = 16,
    revision: ApplicationSchemaRevision = "0146/guard_0033",
) -> ApplicationSchemaReference:
    """Require two independent fresh installations to agree before emitting metadata."""
    if profile not in {
        "legacy-owner",
        "sealed-owner",
        "staging-readonly-legacy-owner",
        "staging-readonly-sealed-owner",
        "cnpg-staging-legacy-owner",
        "cnpg-staging-sealed-owner",
    }:
        raise ValueError("application schema reference profile is invalid")
    application_head, guard_head = application_schema_revisions(revision)
    if revision == "0146/guard_0033" and (application_head, guard_head) != (_head("migrations"), _head("capacity_guard_migrations")):
        raise RuntimeError("current application schema reference revisions require review")
    image = application_reference_postgres_image(postgres_major=postgres_major)
    with PostgresContainer(
        image,
        driver="psycopg",
        password=uuid4().hex,
    ).with_bind_ports(5432, ("127.0.0.1", None)) as postgres:
        first = await _observe_fresh_database(
            postgres.get_connection_url(),
            derive_identity(f"reference-{uuid4().hex[:8]}"),
            profile=profile, revision=revision,
        )
        second = await _observe_fresh_database(
            postgres.get_connection_url(),
            derive_identity(f"reference-{uuid4().hex[:8]}"),
            profile=profile, revision=revision,
        )
    if first != second:
        raise RuntimeError("independent application schema references disagree")
    if first.postgres_major != postgres_major:
        raise RuntimeError("reference PostgreSQL major differs from selected image")
    return ApplicationSchemaReference(
        format_version=1,
        profile=profile,
        application_head=application_head,
        guard_head=guard_head,
        postgres_image=image,
        postgres_major=first.postgres_major,
        object_count=len(first.objects),
        inventory_sha256=first.sha256,
    )


def main() -> int:
    if len(sys.argv) != 1:
        raise SystemExit("reference builder accepts no arguments or database address")
    try:
        result = asyncio.run(_build_profiles())
    except Exception:
        raise SystemExit("isolated application reference generation failed") from None
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


async def _build_profiles() -> dict[str, object]:
    result: dict[str, object] = {}
    profiles: tuple[ApplicationSchemaProfile, ...] = (
        "legacy-owner",
        "sealed-owner",
        "staging-readonly-legacy-owner",
        "staging-readonly-sealed-owner",
        "cnpg-staging-legacy-owner",
        "cnpg-staging-sealed-owner",
    )
    revisions: tuple[ApplicationSchemaRevision, ...] = ("0146/guard_0033", "0142/guard_0033", "0134/guard_0030")
    for major in (16, 17):
        result[str(major)] = {
            revision: {
                profile: asdict(await build_application_schema_reference(
                    profile=profile, postgres_major=major, revision=revision,
                )) for profile in profiles
            } for revision in revisions
        }
    return result


if __name__ == "__main__":
    # Script-path invocation must resolve the companion trusted reference recipe.
    sys.path.insert(0, str(_ROOT))
    raise SystemExit(main())
