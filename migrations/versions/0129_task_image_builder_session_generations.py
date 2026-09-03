"""Persist immutable task-image build-session generations.

Revision ID: 0129
Revises: 0128
Create Date: 2026-09-03
"""

from __future__ import annotations

from alembic import op

revision: str = "0129"
down_revision: str | None = "0128"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        r"""
        LOCK TABLE task_image_build_projections,
          task_image_build_containment_attestations IN ACCESS EXCLUSIVE MODE;

        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
              FROM task_image_build_projections AS projection
             WHERE num_nonnulls(
                     projection.session_id,
                     projection.session_token_hash,
                     projection.session_secret_ref,
                     projection.session_json,
                     projection.session_sha256,
                     projection.session_issued_at,
                     projection.session_expires_at
                   ) NOT IN (0, 7)
                OR (projection.state = 'exchanged' AND projection.session_id IS NULL)
                OR (
                  projection.session_id IS NOT NULL
                  AND (
                    projection.attestation_generation IS NULL
                    OR projection.attestation_sha256 IS NULL
                    OR jsonb_typeof(projection.session_json) IS DISTINCT FROM 'object'
                    OR projection.session_json->>'schema_version' IS DISTINCT FROM '1'
                    OR projection.session_json->>'grant_id'
                       IS DISTINCT FROM projection.grant_id::text
                    OR projection.session_json->>'session_id'
                       IS DISTINCT FROM projection.session_id::text
                    OR projection.session_json->>'session_token_sha256'
                       IS DISTINCT FROM encode(projection.session_token_hash, 'hex')
                    OR projection.session_json->>'attestation_generation'
                       IS DISTINCT FROM projection.attestation_generation::text
                    OR projection.session_json->>'attestation_sha256'
                       IS DISTINCT FROM projection.attestation_sha256
                    OR CASE
                         WHEN pg_input_is_valid(
                           projection.session_json->>'issued_at', 'timestamptz'
                         )
                         THEN (projection.session_json->>'issued_at')::timestamptz
                              IS DISTINCT FROM projection.session_issued_at
                         ELSE TRUE
                       END
                    OR CASE
                         WHEN pg_input_is_valid(
                           projection.session_json->>'expires_at', 'timestamptz'
                         )
                         THEN (projection.session_json->>'expires_at')::timestamptz
                              IS DISTINCT FROM projection.session_expires_at
                         ELSE TRUE
                       END
                    OR NOT EXISTS (
                         SELECT 1
                           FROM task_image_build_containment_attestations AS attestation
                          WHERE attestation.grant_id = projection.grant_id
                            AND attestation.generation = projection.attestation_generation
                            AND attestation.attestation_sha256 = projection.attestation_sha256
                            AND projection.session_issued_at >= attestation.issued_at
                            AND projection.session_expires_at <= attestation.expires_at
                       )
                  )
                )
          ) THEN
            RAISE EXCEPTION 'contradictory legacy task-image session';
          END IF;
        END
        $$;

        ALTER TABLE task_image_build_projections
          ADD COLUMN session_generation BIGINT;

        ALTER TABLE task_image_build_projection_events
          DROP CONSTRAINT task_image_build_projection_events_type_check,
          ADD CONSTRAINT task_image_build_projection_events_type_check CHECK (
            event_type IN (
              'challenged','challenge_replayed','projected','projection_replayed',
              'exchanged','exchange_replayed','attested','attestation_replayed',
              'renewed','renewal_replayed','revoked','expired'
            )
          );

        CREATE TABLE task_image_build_session_generations (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          grant_id UUID NOT NULL,
          generation BIGINT NOT NULL,
          session_id UUID NOT NULL,
          session_token_hash BYTEA NOT NULL,
          session_secret_ref TEXT NOT NULL,
          session_json JSONB NOT NULL,
          session_sha256 VARCHAR(64) NOT NULL,
          attestation_generation BIGINT NOT NULL,
          attestation_sha256 VARCHAR(64) NOT NULL,
          renewal_id UUID,
          renewal_sha256 VARCHAR(64),
          predecessor_session_id UUID,
          issued_at TIMESTAMPTZ NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT task_image_build_session_generations_projection_fkey
            FOREIGN KEY (grant_id) REFERENCES task_image_build_projections(grant_id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_build_session_generations_attestation_fkey
            FOREIGN KEY (grant_id, attestation_generation)
            REFERENCES task_image_build_containment_attestations(grant_id, generation)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_build_session_generations_predecessor_fkey
            FOREIGN KEY (predecessor_session_id)
            REFERENCES task_image_build_session_generations(session_id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_build_session_generations_generation_uidx
            UNIQUE (grant_id, generation),
          CONSTRAINT task_image_build_session_generations_session_uidx
            UNIQUE (session_id),
          CONSTRAINT task_image_build_session_generations_current_uidx
            UNIQUE (grant_id, generation, session_id),
          CONSTRAINT task_image_build_session_generations_renewal_uidx
            UNIQUE (renewal_id),
          CONSTRAINT task_image_build_session_generations_identity_check CHECK (
            id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND generation > 0
            AND octet_length(session_token_hash) = 32
            AND session_secret_ref ~ '^loom://task-image-session/[A-Za-z0-9._/-]+$'
            AND octet_length(session_secret_ref) BETWEEN
              octet_length('loom://task-image-session/') + 1 AND
              octet_length('loom://task-image-session/') + 512
          ),
          CONSTRAINT task_image_build_session_generations_digest_check CHECK (
            jsonb_typeof(session_json) = 'object'
            AND session_sha256 ~ '^[0-9a-f]{64}$'
            AND session_sha256 <> repeat('0', 64)
            AND attestation_sha256 ~ '^[0-9a-f]{64}$'
            AND attestation_sha256 <> repeat('0', 64)
          ),
          CONSTRAINT task_image_build_session_generations_chain_check CHECK (
            (generation = 1 AND renewal_id IS NULL
              AND renewal_sha256 IS NULL AND predecessor_session_id IS NULL)
            OR
            (generation > 1
              AND renewal_id IS NOT NULL
              AND renewal_id <> '00000000-0000-0000-0000-000000000000'::uuid
              AND renewal_sha256 ~ '^[0-9a-f]{64}$'
              AND renewal_sha256 <> repeat('0', 64)
              AND predecessor_session_id IS NOT NULL
              AND predecessor_session_id <> session_id)
          ),
          CONSTRAINT task_image_build_session_generations_time_check CHECK (
            issued_at < expires_at
          )
        );
        CREATE INDEX task_image_build_session_generations_expiry_idx
          ON task_image_build_session_generations (expires_at, grant_id, generation);

        INSERT INTO task_image_build_session_generations (
          grant_id, generation, session_id, session_token_hash,
          session_secret_ref, session_json, session_sha256,
          attestation_generation, attestation_sha256,
          issued_at, expires_at, recorded_at
        )
        SELECT grant_id, 1, session_id, session_token_hash,
          session_secret_ref, session_json, session_sha256,
          attestation_generation, attestation_sha256,
          session_issued_at, session_expires_at, session_issued_at
          FROM task_image_build_projections
         WHERE session_id IS NOT NULL;

        UPDATE task_image_build_projections
           SET session_generation = 1
         WHERE session_id IS NOT NULL;

        ALTER TABLE task_image_build_projections
          ADD CONSTRAINT task_image_build_projections_session_generation_check
            CHECK (
              (session_id IS NULL AND session_generation IS NULL)
              OR (session_id IS NOT NULL AND session_generation > 0)
            ),
          ADD CONSTRAINT task_image_build_projections_current_session_fkey
            FOREIGN KEY (grant_id, session_generation, session_id)
            REFERENCES task_image_build_session_generations(
              grant_id, generation, session_id
            ) ON DELETE RESTRICT;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE task_image_build_projections
          DROP CONSTRAINT task_image_build_projections_current_session_fkey,
          DROP CONSTRAINT task_image_build_projections_session_generation_check,
          DROP COLUMN session_generation;
        ALTER TABLE task_image_build_projection_events
          DROP CONSTRAINT task_image_build_projection_events_type_check,
          ADD CONSTRAINT task_image_build_projection_events_type_check CHECK (
            event_type IN (
              'challenged','challenge_replayed','projected','projection_replayed',
              'exchanged','exchange_replayed','attested','attestation_replayed',
              'revoked','expired'
            )
          );
        DROP TABLE task_image_build_session_generations;
        """
    )
