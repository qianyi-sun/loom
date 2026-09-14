"""A completed handoff survives later owner migrations without restoring runtime DDL."""

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_handoff_completion import complete_application_handoff_database
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.mark.asyncio
async def test_completed_authority_survives_new_owner_objects_and_refuses_pending_handoff(transfer_database):  # noqa: F811
    from loom.application_completed_authority import observe_completed_application_authority

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        target = args["target"]
        options = dict(target=target, runtime_password=args["password"])
        with pytest.raises(RuntimeError, match="completed application"):
            observe_completed_application_authority(peer, **options)
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        before = observe_completed_application_authority(peer, **options)
        # A later trusted migration may add objects and advance schema versions.
        with peer.transaction():
            peer.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(target.successor_role)))
            peer.execute("CREATE TABLE public.after_handoff(id bigint PRIMARY KEY)")
            peer.execute("UPDATE public.alembic_version SET version_num='next'")
        assert observe_completed_application_authority(peer, **options) == before
        with psycopg.connect(transfer_database[0], user=target.owner_role,
                             password=args["password"], autocommit=True) as runtime:
            runtime.execute("INSERT INTO public.after_handoff VALUES (1)")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                runtime.execute("ALTER TABLE public.after_handoff ADD COLUMN bypass text")


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["database-owner", "table-owner", "superuser", "inherit", "schema-create", "database-create", "trigger", "definer", "credential", "closed"])
async def test_completed_authority_refuses_runtime_privilege_or_identity_regression(transfer_database, drift):  # noqa: F811
    from loom.application_completed_authority import observe_completed_application_authority

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        statements = {
            "database-owner": sql.SQL("ALTER DATABASE {} OWNER TO {}").format(sql.Identifier(target.database), sql.Identifier(target.owner_role)),
            "table-owner": sql.SQL("ALTER TABLE public.trials OWNER TO {}").format(sql.Identifier(target.owner_role)),
            "superuser": sql.SQL("ALTER ROLE {} SUPERUSER").format(sql.Identifier(target.owner_role)),
            "inherit": sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(target.owner_role)),
            "schema-create": sql.SQL("GRANT CREATE ON SCHEMA public TO {}").format(sql.Identifier(target.owner_role)),
            "database-create": sql.SQL("GRANT CREATE ON DATABASE {} TO {}").format(sql.Identifier(target.database), sql.Identifier(target.owner_role)),
            "trigger": sql.SQL("GRANT TRIGGER ON public.trials TO {}").format(sql.Identifier(target.owner_role)),
            "definer": sql.SQL("CREATE FUNCTION public.unsafe_completed_definer() RETURNS void LANGUAGE sql SECURITY DEFINER AS 'SELECT'; ALTER FUNCTION public.unsafe_completed_definer() OWNER TO {}").format(sql.Identifier(target.successor_role)),
            "credential": sql.SQL("ALTER ROLE {} PASSWORD 'unrelated'").format(sql.Identifier(target.owner_role)),
            "closed": sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)),
        }
        (maintenance if drift == "closed" else peer).execute(statements[drift])
        with pytest.raises(RuntimeError, match="completed application"):
            observe_completed_application_authority(peer, target=target, runtime_password=args["password"])
        # Changes belong only to this disposable fixture; leave its role sealed
        # enough for ordinary fixture teardown.
        if drift == "superuser":
            peer.execute(sql.SQL("ALTER ROLE {} NOSUPERUSER").format(sql.Identifier(target.owner_role)))


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "oid", "admin", "foreign-membership"])
async def test_only_the_recorded_successor_migrator_may_hold_owner_membership(transfer_database, drift):  # noqa: F811
    from loom.application_completed_authority import (
        ApplicationOwnerSuccessor,
        observe_completed_application_authority,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        migrator = "app_successor_" + uuid4().hex
        peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT; GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE").format(sql.Identifier(migrator), sql.Identifier(target.successor_role), sql.Identifier(migrator)))
        oid = peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone()[0]
        try:
            with pytest.raises(RuntimeError, match="completed application"):
                observe_completed_application_authority(peer, target=target, runtime_password=args["password"])
            successor = ApplicationOwnerSuccessor(migrator, oid + (1 if drift == "oid" else 0))
            if drift == "admin":
                peer.execute(sql.SQL("GRANT {} TO {} WITH ADMIN TRUE").format(sql.Identifier(target.successor_role), sql.Identifier(migrator)))
            if drift == "foreign-membership":
                peer.execute(sql.SQL("GRANT pg_read_all_data TO {}").format(sql.Identifier(migrator)))
            if drift:
                with pytest.raises(RuntimeError, match="completed application"):
                    observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=successor)
            else:
                observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=successor)
        finally:
            peer.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(migrator)))
