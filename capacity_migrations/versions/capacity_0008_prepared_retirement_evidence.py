"""bind prepared writer retirement to exact derived evidence

Revision ID: capacity_0008
Revises: capacity_0007
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "capacity_0008"
down_revision: str | Sequence[str] | None = "capacity_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.get_bind().exec_driver_sql(
        r"""
        CREATE FUNCTION capacity_prepared_retirement_evidence_guard()
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
             OR NEW.retirement_idempotency_key IS DISTINCT FROM
               expected_idempotency_key
             OR NEW.retirement_request_payload IS DISTINCT FROM expected_payload
             OR NEW.retirement_request_digest IS DISTINCT FROM expected_digest THEN
            RAISE EXCEPTION 'prepared execution retirement evidence is not exact'
              USING ERRCODE = '23514', DETAIL = jsonb_build_object(
                'expected_name', expected_name,
                'expected_canonical_payload', expected_canonical_payload,
                'expected_actor',
                  'capacity-manager:' || OLD.authority_incarnation::text,
                'observed_actor', NEW.retirement_actor,
                'expected_idempotency_key', expected_idempotency_key,
                'observed_idempotency_key', NEW.retirement_idempotency_key,
                'expected_digest', expected_digest,
                'observed_digest', NEW.retirement_request_digest,
                'expected_payload', expected_payload,
                'observed_payload', NEW.retirement_request_payload
              )::text;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_prepared_retirement_evidence_guard
        BEFORE UPDATE ON capacity_execution_epochs
        FOR EACH ROW
        WHEN (OLD.state = 'prepared' AND NEW.state = 'retired')
        EXECUTE FUNCTION capacity_prepared_retirement_evidence_guard()
        """
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER capacity_prepared_retirement_evidence_guard ON capacity_execution_epochs"
    )
    op.execute("DROP FUNCTION capacity_prepared_retirement_evidence_guard()")
