"""Bind explicit prepared abort to exact durable request evidence.

Revision ID: capacity_0013
Revises: capacity_0012
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "capacity_0013"
down_revision: str | Sequence[str] | None = "capacity_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_WRITER_REPLACEMENT_ONLY_GUARD = r"""CREATE OR REPLACE FUNCTION capacity_prepared_retirement_evidence_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  expected_name text;
  expected_uuid_bytes bytea;
  expected_idempotency_key uuid;
  expected_payload jsonb;
  expected_canonical_payload text;
  expected_digest text;
BEGIN
  expected_name := format(
    'retire-prepared:%%s:%%s:%%s:%%s:%%s',
    OLD.authority_incarnation,
    OLD.current_writer_epoch,
    NEW.current_writer_epoch,
    NEW.execution_epoch,
    NEW.execution_manifest_sha256
  );
  expected_uuid_bytes := substring(
    sha256(
      uuid_send('9e40e05d-f1c0-4aa8-9ee2-21cc4b46f489'::uuid)
      || convert_to(expected_name, 'UTF8')
    )
    FROM 1 FOR 16
  );
  expected_uuid_bytes := set_byte(
    expected_uuid_bytes,
    6,
    (get_byte(expected_uuid_bytes, 6) & 15) | 128
  );
  expected_uuid_bytes := set_byte(
    expected_uuid_bytes,
    8,
    (get_byte(expected_uuid_bytes, 8) & 63) | 128
  );
  expected_idempotency_key := encode(expected_uuid_bytes, 'hex')::uuid;
  expected_payload := jsonb_build_object(
    'schema_version', 2,
    'transition', 'retire-prepared',
    'reason', 'writer-replacement',
    'authority_incarnation', OLD.authority_incarnation::text,
    'previous_writer_epoch', OLD.current_writer_epoch,
    'successor_writer_epoch', NEW.current_writer_epoch,
    'execution_epoch', NEW.execution_epoch,
    'execution_manifest_sha256', NEW.execution_manifest_sha256,
    'executable', true
  );
  expected_canonical_payload := format(
    '{"authority_incarnation":"%%s","executable":true,'
    '"execution_epoch":%%s,"execution_manifest_sha256":"%%s",'
    '"previous_writer_epoch":%%s,"reason":"writer-replacement",'
    '"schema_version":2,"successor_writer_epoch":%%s,'
    '"transition":"retire-prepared"}',
    OLD.authority_incarnation,
    NEW.execution_epoch,
    NEW.execution_manifest_sha256,
    OLD.current_writer_epoch,
    NEW.current_writer_epoch
  );
  expected_digest := encode(
    sha256(convert_to(expected_canonical_payload, 'UTF8')),
    'hex'
  );

  IF NEW.retirement_actor IS DISTINCT FROM
       'capacity-manager:' || OLD.authority_incarnation::text
     OR NEW.retirement_idempotency_key IS DISTINCT FROM expected_idempotency_key
     OR NEW.retirement_request_payload IS DISTINCT FROM expected_payload
     OR NEW.retirement_request_digest IS DISTINCT FROM expected_digest THEN
    RAISE EXCEPTION 'prepared execution retirement evidence is not exact'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$"""


_ABORT_OR_WRITER_REPLACEMENT_GUARD = r"""CREATE OR REPLACE FUNCTION capacity_prepared_retirement_evidence_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  writer_name text;
  writer_uuid_bytes bytea;
  writer_idempotency_key uuid;
  writer_payload jsonb;
  writer_canonical_payload text;
  writer_digest text;
  abort_payload jsonb;
  abort_canonical_payload text;
  abort_digest text;
BEGIN
  writer_name := format(
    'retire-prepared:%%s:%%s:%%s:%%s:%%s',
    OLD.authority_incarnation,
    OLD.current_writer_epoch,
    NEW.current_writer_epoch,
    NEW.execution_epoch,
    NEW.execution_manifest_sha256
  );
  writer_uuid_bytes := substring(
    sha256(
      uuid_send('9e40e05d-f1c0-4aa8-9ee2-21cc4b46f489'::uuid)
      || convert_to(writer_name, 'UTF8')
    )
    FROM 1 FOR 16
  );
  writer_uuid_bytes := set_byte(
    writer_uuid_bytes,
    6,
    (get_byte(writer_uuid_bytes, 6) & 15) | 128
  );
  writer_uuid_bytes := set_byte(
    writer_uuid_bytes,
    8,
    (get_byte(writer_uuid_bytes, 8) & 63) | 128
  );
  writer_idempotency_key := encode(writer_uuid_bytes, 'hex')::uuid;
  writer_payload := jsonb_build_object(
    'schema_version', 2,
    'transition', 'retire-prepared',
    'reason', 'writer-replacement',
    'authority_incarnation', OLD.authority_incarnation::text,
    'previous_writer_epoch', OLD.current_writer_epoch,
    'successor_writer_epoch', NEW.current_writer_epoch,
    'execution_epoch', NEW.execution_epoch,
    'execution_manifest_sha256', NEW.execution_manifest_sha256,
    'executable', true
  );
  writer_canonical_payload := format(
    '{"authority_incarnation":"%%s","executable":true,'
    '"execution_epoch":%%s,"execution_manifest_sha256":"%%s",'
    '"previous_writer_epoch":%%s,"reason":"writer-replacement",'
    '"schema_version":2,"successor_writer_epoch":%%s,'
    '"transition":"retire-prepared"}',
    OLD.authority_incarnation,
    NEW.execution_epoch,
    NEW.execution_manifest_sha256,
    OLD.current_writer_epoch,
    NEW.current_writer_epoch
  );
  writer_digest := encode(
    sha256(convert_to(writer_canonical_payload, 'UTF8')),
    'hex'
  );

  IF NEW.retirement_actor IS NOT DISTINCT FROM
       'capacity-manager:' || OLD.authority_incarnation::text
     AND NEW.retirement_idempotency_key IS NOT DISTINCT FROM writer_idempotency_key
     AND NEW.retirement_request_payload IS NOT DISTINCT FROM writer_payload
     AND NEW.retirement_request_digest IS NOT DISTINCT FROM writer_digest THEN
    RETURN NEW;
  END IF;

  abort_payload := jsonb_build_object(
    'schema_version', 2,
    'authority_incarnation', OLD.authority_incarnation::text,
    'expected_writer_epoch', OLD.current_writer_epoch,
    'execution_epoch', NEW.execution_epoch,
    'execution_manifest_sha256', NEW.execution_manifest_sha256,
    'executable', true
  );
  abort_canonical_payload := format(
    '{"authority_incarnation":"%%s","executable":true,'
    '"execution_epoch":%%s,"execution_manifest_sha256":"%%s",'
    '"expected_writer_epoch":%%s,"schema_version":2}',
    OLD.authority_incarnation,
    NEW.execution_epoch,
    NEW.execution_manifest_sha256,
    OLD.current_writer_epoch
  );
  abort_digest := encode(
    sha256(convert_to(abort_canonical_payload, 'UTF8')),
    'hex'
  );

  IF NEW.retirement_request_payload IS DISTINCT FROM abort_payload
     OR NEW.retirement_request_digest IS DISTINCT FROM abort_digest THEN
    RAISE EXCEPTION 'prepared execution retirement evidence is not exact'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$"""


def upgrade() -> None:
    op.get_bind().exec_driver_sql(_ABORT_OR_WRITER_REPLACEMENT_GUARD)


def downgrade() -> None:
    op.get_bind().exec_driver_sql(_WRITER_REPLACEMENT_ONLY_GUARD)
