"""Preserve typed application terminal evidence in the protected importer.

Revision ID: guard_0031
Revises: guard_0030

Manager authentication owns historical allocation and signature verification.
This importer still owns exact local registration, attempt, claim and worker
binding. It cannot import build purpose or turn cleanup into admission.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0031"
down_revision: str | Sequence[str] | None = "guard_0030"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SIGNATURE = (
    "loom_capacity_guard.import_executable_terminal_inventory_evidence(uuid,uuid,jsonb,bytea,text)"
)
_MARKER = "          SELECT registration.* INTO v_registration"
_TYPED_CHECK = """
          -- guard_0031: exact application-only typed provenance, never admission.
          IF p_payload->>'schema_version' = '3' AND (
            jsonb_typeof(v_metadata->'subject_authority') = 'object'
            AND v_metadata ?& ARRAY['schema_version','binding','subject_authority',
              'launch_profile_sha256','controller_authority_sha256','trusted_launcher_sha256',
              'slurm_cluster','submitter_identity','association','submitted_at']
            AND v_metadata - ARRAY['schema_version','binding','subject_authority',
              'launch_profile_sha256','controller_authority_sha256','trusted_launcher_sha256',
              'slurm_cluster','submitter_identity','association','submitted_at'] = '{}'::jsonb
            AND v_proof ?& ARRAY['schema_version','metadata','signing_key_id','signature_base64']
            AND v_proof - ARRAY['schema_version','metadata','signing_key_id','signature_base64'] = '{}'::jsonb
            AND v_metadata->>'launch_profile_sha256' ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(v_metadata->'launch_profile_sha256') = 'string'
            AND v_metadata->>'launch_profile_sha256' <> repeat('0',64)
            AND v_metadata->>'controller_authority_sha256' <> repeat('0',64)
            AND v_metadata->>'trusted_launcher_sha256' <> repeat('0',64)
            AND v_metadata->'subject_authority' ?& ARRAY['schema_version','source','purpose',
              'configuration','acknowledgement_sha256','membership']
            AND (v_metadata->'subject_authority') - ARRAY['schema_version','source','purpose',
              'configuration','acknowledgement_sha256','membership'] = '{}'::jsonb
            AND v_metadata #>> '{subject_authority,schema_version}' = '3'
            AND jsonb_typeof(v_metadata #> '{subject_authority,schema_version}') = 'number'
            AND v_metadata #>> '{subject_authority,purpose}' = 'application-worker'
            AND v_metadata #>> '{subject_authority,acknowledgement_sha256}' ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(v_metadata #> '{subject_authority,acknowledgement_sha256}') = 'string'
            AND v_metadata #>> '{subject_authority,acknowledgement_sha256}' <> repeat('0',64)
            AND jsonb_typeof(v_metadata #> '{subject_authority,configuration}') = 'object'
            AND (v_metadata #> '{subject_authority,configuration}') ?& ARRAY['schema_version',
              'scope','generation','digest','subject_id','subject_incarnation']
            AND (v_metadata #> '{subject_authority,configuration}') - ARRAY['schema_version',
              'scope','generation','digest','subject_id','subject_incarnation'] = '{}'::jsonb
            AND v_metadata #>> '{subject_authority,configuration,schema_version}' = '1'
            AND jsonb_typeof(v_metadata #> '{subject_authority,configuration,schema_version}') = 'number'
            AND v_metadata #>> '{subject_authority,configuration,scope}' = 'subject'
            AND v_metadata #> '{subject_authority,configuration,subject_id}' = v_binding->'subject_id'
            AND v_metadata #> '{subject_authority,configuration,subject_incarnation}' = v_binding->'subject_incarnation'
            AND v_metadata #>> '{subject_authority,configuration,subject_id}' <> '00000000-0000-0000-0000-000000000000'
            AND v_metadata #>> '{subject_authority,configuration,subject_incarnation}' <> '00000000-0000-0000-0000-000000000000'
            AND v_metadata #>> '{subject_authority,configuration,generation}' ~ '^[1-9][0-9]*$'
            AND jsonb_typeof(v_metadata #> '{subject_authority,configuration,generation}') = 'number'
            AND (v_metadata #>> '{subject_authority,configuration,generation}')::numeric <= 9223372036854775807
            AND v_metadata #>> '{subject_authority,configuration,digest}' ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(v_metadata #> '{subject_authority,configuration,digest}') = 'string'
            AND v_metadata #>> '{subject_authority,configuration,digest}' <> repeat('0',64)
            AND (
              (v_metadata #>> '{subject_authority,source}' = 'immutable-base'
                AND v_metadata #> '{subject_authority,membership}' = 'null'::jsonb)
              OR (v_metadata #>> '{subject_authority,source}' = 'personal-membership'
                AND jsonb_typeof(v_metadata #> '{subject_authority,membership}') = 'object'
                AND (v_metadata #> '{subject_authority,membership}') ?& ARRAY['schema_version',
                  'namespace_id','owner_id','revision','head_sha256','execution_manifest_sha256']
                AND (v_metadata #> '{subject_authority,membership}') - ARRAY['schema_version',
                  'namespace_id','owner_id','revision','head_sha256','execution_manifest_sha256'] = '{}'::jsonb
                AND v_metadata #>> '{subject_authority,membership,schema_version}' = '3'
                AND jsonb_typeof(v_metadata #> '{subject_authority,membership,schema_version}') = 'number'
                AND v_metadata #>> '{subject_authority,membership,namespace_id}' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                AND v_metadata #>> '{subject_authority,membership,namespace_id}' <> '00000000-0000-0000-0000-000000000000'
                AND v_metadata #>> '{subject_authority,membership,owner_id}' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                AND v_metadata #>> '{subject_authority,membership,owner_id}' <> '00000000-0000-0000-0000-000000000000'
                AND v_binding->>'account_id' = 'dev-owner-' || replace(v_metadata #>> '{subject_authority,membership,owner_id}', '-', '')
                AND v_binding->>'tier_id' = 'development'
                AND v_metadata #>> '{subject_authority,membership,revision}' ~ '^[1-9][0-9]*$'
                AND jsonb_typeof(v_metadata #> '{subject_authority,membership,revision}') = 'number'
                AND (v_metadata #>> '{subject_authority,membership,revision}')::numeric <= 9223372036854775807
                AND v_metadata #>> '{subject_authority,membership,head_sha256}' ~ '^[0-9a-f]{64}$'
                AND jsonb_typeof(v_metadata #> '{subject_authority,membership,head_sha256}') = 'string'
                AND v_metadata #>> '{subject_authority,membership,head_sha256}' <> repeat('0',64)
                AND v_metadata #> '{subject_authority,membership,execution_manifest_sha256}' = v_execution->'execution_manifest_sha256'
                AND v_metadata #>> '{subject_authority,membership,execution_manifest_sha256}' <> repeat('0',64)
              )
            )
          ) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'typed terminal inventory requires exact application provenance'
              USING ERRCODE = '22023';
          END IF;

"""


def _rewrite(*, upgrading: bool) -> None:
    definition = (
        op.get_bind()
        .execute(
            sa.text("SELECT pg_catalog.pg_get_functiondef(CAST(:signature AS regprocedure))"),
            {"signature": _SIGNATURE},
        )
        .scalar_one()
    )
    replacements = [
        (
            "p_payload->'schema_version' IS DISTINCT FROM '2'::jsonb",
            "(p_payload->'schema_version' IN ('2'::jsonb,'3'::jsonb) "
            "AND p_payload->>'schema_version' IN ('2','3')) IS DISTINCT FROM true",
        ),
        *[
            (
                f"{name}->'schema_version' IS DISTINCT FROM '2'::jsonb",
                f"({name}->'schema_version' = p_payload->'schema_version' "
                f"AND {name}->>'schema_version' = p_payload->>'schema_version') IS DISTINCT FROM true",
            )
            for name in ("v_record", "v_proof", "v_metadata")
        ],
        (_MARKER, _TYPED_CHECK + _MARKER),
    ]
    for old, new in replacements if upgrading else reversed(replacements):
        source, target = (old, new) if upgrading else (new, old)
        if definition.count(source) != 1:
            raise RuntimeError("typed terminal import migration found unexpected prior definition")
        definition = definition.replace(source, target, 1)
    # CREATE OR REPLACE preserves the existing owner, exact agent-only grants,
    # signature, SECURITY DEFINER and pinned search_path. No new callable surface.
    op.execute(definition)


def upgrade() -> None:
    _set_schema_constraint(typed=True)
    _rewrite(upgrading=True)


def _set_schema_constraint(*, typed: bool) -> None:
    op.execute(
        "ALTER TABLE loom_capacity_guard.executable_terminal_inventory_evidence "
        "DROP CONSTRAINT IF EXISTS guard_terminal_inventory_schema_check"
    )
    allowed = "('2'::jsonb,'3'::jsonb)" if typed else "('2'::jsonb)"
    op.create_check_constraint(
        "guard_terminal_inventory_schema_check",
        "executable_terminal_inventory_evidence",
        f"(evidence_payload->'schema_version' IN {allowed}) IS TRUE",
        schema="loom_capacity_guard",
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE loom_capacity_guard.executable_terminal_inventory_evidence "
        "IN ACCESS EXCLUSIVE MODE"
    )
    retained = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM loom_capacity_guard.executable_terminal_inventory_evidence "
                "WHERE evidence_payload->>'schema_version' = '3')"
            )
        )
        .scalar_one()
    )
    if retained:
        raise RuntimeError(
            "cannot downgrade guard_0031 while typed terminal inventory evidence exists"
        )
    # A function invocation can outlive CREATE OR REPLACE. Leave the V2 table
    # constraint at the old head so an already-running typed import cannot
    # commit after this transaction releases the lock and restores the V2 body.
    _set_schema_constraint(typed=False)
    _rewrite(upgrading=False)
