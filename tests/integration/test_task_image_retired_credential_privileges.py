from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db.schema import TaskImageMaterializationAttempt, TaskImageRegistryCredentialGeneration
from loom_task_image_authority.materializations import (
    claim_session_materialization,
    fail_session_materialization,
)
from tests.integration.test_task_image_authority_materializations import _queued_materialization
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW, _issue_first
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retired_credential_ingress import (
    _insert,
    _prepared_insert,
    _retire,
)
from tests.integration.test_task_image_retirement_snapshot import _setup


@pytest.mark.parametrize("retired", [False, True])
async def test_restricted_insert_role_cannot_hide_retirement_with_rls_or_search_path(
    registry_authority_session, registry_issuer, retired,
):
    factory = registry_authority_session
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    if retired:
        await _retire(factory, attempt_id)
    role = "credential_insert_" + uuid4().hex
    async with factory() as session:
        # All role/policy/decoy DDL is transaction-local in this disposable DB.
        await session.execute(text(f"CREATE ROLE {role} NOLOGIN NOSUPERUSER NOBYPASSRLS"))
        await session.execute(text(f"GRANT USAGE ON SCHEMA public TO {role}"))
        await session.execute(text(f"GRANT INSERT ON public.task_image_registry_credentials TO {role}"))
        await session.execute(text(f"GRANT SELECT ON public.task_image_attempt_retention TO {role}"))
        await session.execute(text("ALTER TABLE public.task_image_attempt_retention ENABLE ROW LEVEL SECURITY"))
        await session.execute(text(f"CREATE POLICY hidden_retirement ON public.task_image_attempt_retention FOR SELECT TO {role} USING (false)"))
        await session.execute(text("CREATE SCHEMA decoy; CREATE TABLE decoy.task_image_attempt_retention (attempt_id uuid, retired_at timestamptz); CREATE TABLE decoy.task_image_materialization_attempts (id uuid)"))
        await session.execute(text(f"GRANT USAGE ON SCHEMA decoy TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA decoy TO {role}"))
        await session.execute(text(f"SET LOCAL ROLE {role}"))
        await session.execute(text("SET LOCAL search_path = decoy, public"))
        assert await session.scalar(text("SELECT count(*) FROM public.task_image_attempt_retention")) == 0
        if retired:
            with pytest.raises(IntegrityError) as error:
                await _insert(session, values)
            assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
        else:
            await _insert(session, values)
        await session.rollback()


async def test_definer_without_rls_bypass_fails_closed_instead_of_missing_marker(
    registry_authority_session, registry_issuer,
):
    factory = registry_authority_session
    attempt_id, values = await _prepared_insert(factory, registry_issuer)
    await _retire(factory, attempt_id)
    role = "credential_definer_" + uuid4().hex
    async with factory() as session:
        await session.execute(text(f"CREATE ROLE {role} NOLOGIN NOSUPERUSER NOBYPASSRLS"))
        await session.execute(text(f"GRANT USAGE ON SCHEMA public TO {role}"))
        await session.execute(text(f"GRANT SELECT, UPDATE ON public.task_image_materialization_attempts TO {role}"))
        await session.execute(text(f"GRANT SELECT ON public.task_image_attempt_retention TO {role}"))
        await session.execute(text("ALTER TABLE public.task_image_attempt_retention ENABLE ROW LEVEL SECURITY"))
        await session.execute(text(f"ALTER FUNCTION public.task_image_registry_reject_retired_attempt() OWNER TO {role}"))
        with pytest.raises(DBAPIError) as error:
            await _insert(session, values)
        assert error.value.orig.sqlstate == "42501"
        assert "row-level security" in str(error.value.orig)
        await session.rollback()


async def _pair(factory, issuer):
    _, first, options = await _setup(factory, issuer)

    async def issuance(options):
        async with factory() as session:
            await _issue_first(session, **options, request_id=uuid4(), credential_id_factory=uuid4)
            row = (await session.scalars(select(TaskImageRegistryCredentialGeneration))).one()
            values = {name: getattr(row, name) for name in row.__table__.columns.keys()}
            await session.rollback()
            return values

    first_values = await issuance(options)
    async with factory() as session:
        # Release the grant's single live claim through the production failure
        # path; its retry backoff leaves only the second task eligible below.
        await fail_session_materialization(
            session, authorization=options["authorization"],
            materialization_id=first.materialization_id, attempt_id=first.id,
            lease_epoch=first.lease_epoch, operation_id=uuid4(),
            now=NOW + timedelta(seconds=11),
        )
        await _queued_materialization(session, task_id="second-credential-attempt")
        claimed = await claim_session_materialization(
            session, authorization=options["authorization"], claim_id=uuid4(),
            now=NOW + timedelta(seconds=12), lease_seconds=300,
        )
        assert claimed is not None
        row, _ = claimed
        second = await session.scalar(select(TaskImageMaterializationAttempt).where(
            TaskImageMaterializationAttempt.materialization_id == row.id,
        ))
        await session.commit()
    second_values = await issuance({**options, "row": row, "attempt": second})
    await _retire(factory, first.id)
    return first_values, second_values


async def test_retired_row_rolls_back_entire_multirow_credential_insert(
    registry_authority_session, registry_issuer,
):
    factory = registry_authority_session
    retired, live = await _pair(factory, registry_issuer)
    async with factory() as session:
        with pytest.raises(IntegrityError) as error:
            await session.execute(insert(TaskImageRegistryCredentialGeneration).values([live, retired]))
        assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
        await session.rollback()
        assert await session.scalar(select(TaskImageRegistryCredentialGeneration.credential_id)) is None


async def test_after_insert_checks_final_identity_after_before_trigger_rewrite(
    registry_authority_session, registry_issuer,
):
    factory = registry_authority_session
    retired, live = await _pair(factory, registry_issuer)
    async with factory() as session:
        # Model a future BEFORE transformation changing the final attempt. Its
        # public payload would also be invalid at application validation, but the
        # database retirement guard must reject final identity independently.
        await session.execute(text(f"""
            CREATE FUNCTION test_rewrite_credential_attempt() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
              NEW.materialization_attempt_id := '{retired['materialization_attempt_id']}'::uuid;
              NEW.materialization_id := '{retired['materialization_id']}'::uuid;
              NEW.attempt_number := {retired['attempt_number']};
              NEW.lease_epoch := {retired['lease_epoch']};
              RETURN NEW;
            END $$;
            CREATE TRIGGER zz_rewrite_attempt BEFORE INSERT ON public.task_image_registry_credentials
              FOR EACH ROW EXECUTE FUNCTION test_rewrite_credential_attempt();
        """))
        with pytest.raises(IntegrityError) as error:
            await _insert(session, live)
        assert error.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
        await session.rollback()
