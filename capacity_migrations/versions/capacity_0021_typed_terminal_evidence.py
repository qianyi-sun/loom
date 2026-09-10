"""Retain purpose-authenticated typed terminal evidence through predecessor release.

Revision ID: capacity_0021
Revises: capacity_0020
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0021"
down_revision: str | Sequence[str] | None = "capacity_0020"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "capacity_executable_terminal_inventory_evidence"
_FENCE = "capacity_terminal_legacy_evidence_check"
_FUNCTIONS = (
    ("capacity_executable_terminal_inventory_insert_guard", ""),
    ("capacity_personal_predecessor_release_digest", "uuid,uuid"),
)


def _once(value: str, old: str, new: str) -> str:
    if value.count(old) != 1:
        raise RuntimeError(f"capacity_0021 SQL definition drift: {old[:100]}")
    return value.replace(old, new, 1)


def _definition(name: str, signature: str) -> str:
    return str(op.get_bind().scalar(sa.text("SELECT pg_get_functiondef(CAST(:name AS regprocedure))"),
        {"name": f"public.{name}({signature})"}))


def _rename(value: str, old: str, new: str) -> str:
    return _once(value, f"FUNCTION public.{old}(", f"FUNCTION public.{new}(")


def upgrade() -> None:
    op.execute("LOCK TABLE public.capacity_authority_state IN EXCLUSIVE MODE")
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    op.execute(f"ALTER TABLE public.{_TABLE} DROP CONSTRAINT IF EXISTS {_FENCE}")
    op.execute("""
      CREATE FUNCTION public.capacity_typed_terminal_subject_matches(binding jsonb, proof jsonb)
      RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
      DECLARE pinned jsonb; subject jsonb; member jsonb; membership jsonb; expected jsonb;
        epoch_record record; event_record record;
      BEGIN
        pinned := public.capacity_membership_binding_target(binding);
        SELECT * INTO STRICT epoch_record FROM public.capacity_execution_epochs
          WHERE execution_epoch=(binding #>> '{execution,execution_epoch}')::bigint;
        IF epoch_record.manifest_payload -> 'schema_version' IS DISTINCT FROM '4'::jsonb THEN RETURN false; END IF;
        subject := pinned -> 'configuration'; member := nullif(pinned -> 'member','null'::jsonb);
        IF member IS NOT NULL THEN
          SELECT * INTO STRICT event_record FROM public.capacity_personal_membership_events
            WHERE execution_epoch=epoch_record.execution_epoch AND revision=(member ->> 'revision')::bigint;
          membership := jsonb_build_object('schema_version',3,
            'namespace_id',event_record.namespace_id,'owner_id',event_record.owner_id,
            'revision',event_record.revision,'head_sha256',event_record.head_sha256,
            'execution_manifest_sha256',event_record.execution_manifest_sha256);
        END IF;
        expected := jsonb_build_object('schema_version',3,
          'source',CASE WHEN member IS NULL THEN 'immutable-base' ELSE 'personal-membership' END,
          'purpose',pinned -> 'purpose',
          'configuration',jsonb_build_object('schema_version',1,'scope','subject',
            'subject_id',subject -> 'subject_id','subject_incarnation',subject -> 'subject_incarnation',
            'generation',subject -> 'configuration_generation','digest',public.capacity_membership_json_digest(subject)),
          'acknowledgement_sha256',public.capacity_membership_json_digest(pinned -> 'acknowledgement'),
          'membership',membership);
        RETURN public.capacity_personal_build_json_exact(proof -> 'schema_version','3'::jsonb)
          AND public.capacity_personal_build_json_exact(proof #> '{metadata,schema_version}','3'::jsonb)
          AND public.capacity_personal_build_json_exact(proof #> '{metadata,binding}',binding)
          AND public.capacity_personal_build_json_exact(proof #> '{metadata,subject_authority}',expected);
      END $$;
      REVOKE ALL ON FUNCTION public.capacity_typed_terminal_subject_matches(jsonb,jsonb) FROM PUBLIC;
    """)
    for index, (original, signature) in enumerate(_FUNCTIONS):
        definition = _definition(original, signature)
        backup = f"capacity_0021_prior_{index}"
        op.execute(_rename(definition, original, backup))
        op.execute(f"REVOKE ALL ON FUNCTION public.{backup}({signature}) FROM PUBLIC")
        if index == 0:
            definition = _once(definition, "payload_record jsonb;", "payload_record jsonb; manifest_version jsonb; expected_version jsonb;")
            definition = _once(definition,
                "          payload_record := NEW.evidence_payload -> 'record';",
                """          SELECT manifest_payload -> 'schema_version' INTO manifest_version
            FROM public.capacity_execution_epochs WHERE execution_epoch=NEW.execution_epoch
              AND execution_manifest_sha256=NEW.execution_manifest_sha256;
          expected_version := CASE WHEN manifest_version IN ('2'::jsonb,'3'::jsonb) THEN '2'::jsonb
            WHEN manifest_version='4'::jsonb THEN '3'::jsonb ELSE NULL END;
          payload_record := NEW.evidence_payload -> 'record';
          IF expected_version IS NULL
             OR executor_record.inventory_payload -> 'schema_version' IS DISTINCT FROM expected_version
             OR payload_record -> 'schema_version' IS DISTINCT FROM expected_version
             OR (manifest_version='4'::jsonb AND (
               NOT public.capacity_personal_build_json_exact(NEW.evidence_payload -> 'schema_version','3'::jsonb)
               OR public.capacity_typed_terminal_subject_matches(
                 intent_record.binding_payload,payload_record -> 'ownership_proof') IS DISTINCT FROM true
               OR NOT public.capacity_personal_build_json_exact(payload_record,jsonb_build_object(
                 'schema_version',3,'physical_identity',NEW.physical_identity,'physical_kind',NEW.physical_kind,
                 'authority_scope','dedicated-loom-association','state','terminal',
                 'resources',intent_record.binding_payload -> 'resources','node_ids',intent_record.binding_payload -> 'node_ids',
                 'controller_evidence_sha256',NEW.controller_evidence_sha256,'ownership_proof',payload_record -> 'ownership_proof',
                 'terminal_evidence_sha256',NEW.terminal_evidence_sha256)))) THEN
            RAISE EXCEPTION 'terminal inventory typed authority changed' USING ERRCODE='23514';
          END IF;""")
            definition = _once(definition,
                "NEW.evidence_payload -> 'schema_version' IS DISTINCT FROM '2'::jsonb",
                "NEW.evidence_payload -> 'schema_version' IS DISTINCT FROM expected_version")
        else:
            definition = _once(definition,
                "payload jsonb; binding jsonb; item jsonb; kind text; terminal_digest text;",
                "payload jsonb; binding jsonb; item jsonb; kind text; terminal_digest text; terminal_version integer;")
            definition = _once(definition,
                "            payload := terminal.evidence_payload;",
                """            SELECT CASE WHEN manifest_payload -> 'schema_version'='4'::jsonb THEN 3
                WHEN manifest_payload -> 'schema_version' IN ('2'::jsonb,'3'::jsonb) THEN 2 ELSE NULL END
              INTO terminal_version FROM public.capacity_execution_epochs
              WHERE execution_epoch=intent.execution_epoch AND execution_manifest_sha256=intent.execution_manifest_sha256;
            payload := terminal.evidence_payload;
            IF terminal_version IS NULL OR (terminal_version=3 AND public.capacity_typed_terminal_subject_matches(
                binding,payload #> '{record,ownership_proof}') IS DISTINCT FROM true) THEN
              RAISE EXCEPTION 'predecessor typed terminal authority changed' USING ERRCODE='23514';
            END IF;""")
            definition = _once(definition,
                "                 'schema_version',2,'executable',true,'binding',binding,",
                "                 'schema_version',terminal_version,'executable',true,'binding',binding,")
            definition = _once(definition,
                "                 'schema_version',2,'physical_identity',terminal.physical_identity,'physical_kind',terminal.physical_kind,",
                "                 'schema_version',terminal_version,'physical_identity',terminal.physical_identity,'physical_kind',terminal.physical_kind,")
        op.execute(definition)


def downgrade() -> None:
    op.execute("LOCK TABLE public.capacity_authority_state IN EXCLUSIVE MODE")
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE} WHERE evidence_payload -> 'schema_version'='3'::jsonb)")):
        raise RuntimeError("cannot downgrade capacity_0021 with retained typed terminal evidence")
    op.create_check_constraint(_FENCE, _TABLE, "evidence_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb")
    for index, (original, signature) in enumerate(_FUNCTIONS):
        backup = f"capacity_0021_prior_{index}"
        op.execute(_rename(_definition(backup, signature), backup, original))
        op.execute(f"DROP FUNCTION public.{backup}({signature})")
    op.execute("DROP FUNCTION public.capacity_typed_terminal_subject_matches(jsonb,jsonb)")
