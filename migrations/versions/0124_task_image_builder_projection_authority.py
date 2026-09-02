"""Persist task-image builder projection and containment authority.

Revision ID: 0124
Revises: 0123
"""

from alembic import op

revision = "0124"
down_revision = "0123"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        LOCK TABLE task_image_build_grants IN ACCESS EXCLUSIVE MODE;
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM task_image_build_grants LIMIT 1) THEN
            RAISE EXCEPTION 'unexpected pre-authority task-image build grants';
          END IF;
        END
        $$;

        ALTER TABLE task_image_build_grants
          ADD COLUMN authority_spec JSONB NOT NULL,
          ADD COLUMN authority_sha256 VARCHAR(64) NOT NULL,
          ADD COLUMN grant_expires_at TIMESTAMPTZ NOT NULL,
          ADD CONSTRAINT task_image_build_grants_authority_check CHECK (
            jsonb_typeof(authority_spec) = 'object'
            AND authority_sha256 ~ '^[0-9a-f]{64}$'
            AND authority_sha256 <> repeat('0', 64)
            AND isfinite(grant_expires_at)
          );

        CREATE TABLE task_image_build_projections (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          grant_id UUID NOT NULL,
          state TEXT NOT NULL,
          principal_id VARCHAR(128) NOT NULL,
          principal_sha256 VARCHAR(64) NOT NULL,
          request_id UUID NOT NULL,
          request_json JSONB NOT NULL,
          request_sha256 VARCHAR(64) NOT NULL,
          node_name VARCHAR(253) NOT NULL,
          node_boot_id UUID NOT NULL,
          slurm_cluster_id TEXT NOT NULL,
          slurm_job_id VARCHAR(32) NOT NULL,
          supervisor_pid BIGINT NOT NULL,
          supervisor_uid BIGINT NOT NULL,
          supervisor_gid BIGINT NOT NULL,
          supervisor_executable_sha256 VARCHAR(64) NOT NULL,
          cgroup_path TEXT NOT NULL,
          cgroup_inode BIGINT NOT NULL,
          challenge_nonce UUID NOT NULL,
          challenge_json JSONB NOT NULL,
          challenge_sha256 VARCHAR(64) NOT NULL,
          challenge_issued_at TIMESTAMPTZ NOT NULL,
          challenge_expires_at TIMESTAMPTZ NOT NULL,
          proof_id UUID,
          proof_json JSONB,
          proof_sha256 VARCHAR(64),
          bootstrap_token_hash BYTEA,
          bootstrap_secret_ref TEXT,
          bootstrap_issued_at TIMESTAMPTZ,
          bootstrap_expires_at TIMESTAMPTZ,
          exchange_id UUID,
          exchange_json JSONB,
          exchange_sha256 VARCHAR(64),
          session_id UUID,
          session_token_hash BYTEA,
          session_secret_ref TEXT,
          session_json JSONB,
          session_sha256 VARCHAR(64),
          session_issued_at TIMESTAMPTZ,
          session_expires_at TIMESTAMPTZ,
          attestation_generation BIGINT,
          attestation_sha256 VARCHAR(64),
          attestation_expires_at TIMESTAMPTZ,
          event_sequence INTEGER NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          revoked_at TIMESTAMPTZ,
          revoke_reason VARCHAR(128),
          expired_at TIMESTAMPTZ,
          CONSTRAINT task_image_build_projections_grant_fkey
            FOREIGN KEY (grant_id) REFERENCES task_image_build_grants(id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_build_projections_grant_uidx UNIQUE (grant_id),
          CONSTRAINT task_image_build_projections_request_uidx
            UNIQUE (grant_id, request_id),
          CONSTRAINT task_image_build_projections_proof_uidx
            UNIQUE (grant_id, proof_id),
          CONSTRAINT task_image_build_projections_exchange_uidx
            UNIQUE (grant_id, exchange_id),
          CONSTRAINT task_image_build_projections_session_uidx UNIQUE (session_id),
          CONSTRAINT task_image_build_projections_state_check CHECK (
            state IN ('challenged','projected','exchanged','revoked','expired')
          ),
          CONSTRAINT task_image_build_projections_identity_check CHECK (
            id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND node_boot_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND challenge_nonce <> '00000000-0000-0000-0000-000000000000'::uuid
            AND principal_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'
            AND principal_sha256 ~ '^[0-9a-f]{64}$'
            AND principal_sha256 <> repeat('0', 64)
            AND request_sha256 ~ '^[0-9a-f]{64}$'
            AND request_sha256 <> repeat('0', 64)
            AND challenge_sha256 ~ '^[0-9a-f]{64}$'
            AND challenge_sha256 <> repeat('0', 64)
            AND node_name ~ '^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$'
            AND slurm_cluster_id IN ('oldlab','gb10')
            AND slurm_job_id ~ '^[1-9][0-9]{0,31}$'
            AND supervisor_pid > 0 AND supervisor_uid > 0 AND supervisor_gid > 0
            AND supervisor_executable_sha256 ~ '^[0-9a-f]{64}$'
            AND supervisor_executable_sha256 <> repeat('0', 64)
            AND cgroup_path LIKE '/sys/fs/cgroup/%'
            AND cgroup_inode > 0
            AND jsonb_typeof(request_json) = 'object'
            AND jsonb_typeof(challenge_json) = 'object'
            AND event_sequence >= 0
          ),
          CONSTRAINT task_image_build_projections_state_fields_check CHECK (
            (
              (proof_id IS NULL AND proof_json IS NULL AND proof_sha256 IS NULL
                AND bootstrap_token_hash IS NULL AND bootstrap_secret_ref IS NULL
                AND bootstrap_issued_at IS NULL AND bootstrap_expires_at IS NULL
                AND attestation_generation IS NULL AND attestation_sha256 IS NULL
                AND attestation_expires_at IS NULL)
              OR
              (proof_id IS NOT NULL AND proof_json IS NOT NULL
                AND jsonb_typeof(proof_json) = 'object'
                AND proof_sha256 ~ '^[0-9a-f]{64}$'
                AND proof_sha256 <> repeat('0', 64)
                AND octet_length(bootstrap_token_hash) = 32
                AND bootstrap_secret_ref ~ '^loom://task-image-bootstrap/[A-Za-z0-9._/-]+$'
                AND octet_length(bootstrap_secret_ref) BETWEEN
                  octet_length('loom://task-image-bootstrap/') + 1 AND
                  octet_length('loom://task-image-bootstrap/') + 512
                AND bootstrap_issued_at IS NOT NULL AND bootstrap_expires_at IS NOT NULL
                AND attestation_generation > 0
                AND attestation_sha256 ~ '^[0-9a-f]{64}$'
                AND attestation_sha256 <> repeat('0', 64)
                AND attestation_expires_at IS NOT NULL)
            )
            AND
            (
              (exchange_id IS NULL AND exchange_json IS NULL AND exchange_sha256 IS NULL
                AND session_id IS NULL AND session_token_hash IS NULL
                AND session_secret_ref IS NULL AND session_json IS NULL
                AND session_sha256 IS NULL AND session_issued_at IS NULL
                AND session_expires_at IS NULL)
              OR
              (exchange_id IS NOT NULL AND exchange_json IS NOT NULL
                AND jsonb_typeof(exchange_json) = 'object'
                AND exchange_sha256 ~ '^[0-9a-f]{64}$'
                AND exchange_sha256 <> repeat('0', 64)
                AND session_id IS NOT NULL AND octet_length(session_token_hash) = 32
                AND session_secret_ref ~ '^loom://task-image-session/[A-Za-z0-9._/-]+$'
                AND octet_length(session_secret_ref) BETWEEN
                  octet_length('loom://task-image-session/') + 1 AND
                  octet_length('loom://task-image-session/') + 512
                AND jsonb_typeof(session_json) = 'object'
                AND session_sha256 ~ '^[0-9a-f]{64}$'
                AND session_sha256 <> repeat('0', 64)
                AND session_issued_at IS NOT NULL AND session_expires_at IS NOT NULL)
            )
            AND (exchange_id IS NULL OR proof_id IS NOT NULL)
            AND (
              (state = 'challenged' AND proof_id IS NULL AND exchange_id IS NULL)
              OR (state = 'projected' AND proof_id IS NOT NULL AND exchange_id IS NULL)
              OR (state = 'exchanged' AND proof_id IS NOT NULL AND exchange_id IS NOT NULL)
              OR state IN ('revoked','expired')
            )
          ),
          CONSTRAINT task_image_build_projections_terminal_check CHECK (
            (state NOT IN ('revoked','expired')
              AND revoked_at IS NULL AND revoke_reason IS NULL AND expired_at IS NULL)
            OR
            (state = 'revoked' AND revoked_at IS NOT NULL
              AND revoke_reason ~ '^[a-z][a-z0-9_]{0,127}$' AND expired_at IS NULL)
            OR
            (state = 'expired' AND expired_at IS NOT NULL
              AND revoked_at IS NULL AND revoke_reason IS NULL)
          ),
          CONSTRAINT task_image_build_projections_time_check CHECK (
            challenge_issued_at < challenge_expires_at
            AND created_at <= updated_at
            AND (
              proof_id IS NULL
              OR (bootstrap_issued_at < bootstrap_expires_at
                AND bootstrap_issued_at < attestation_expires_at)
            )
            AND (
              exchange_id IS NULL OR session_issued_at < session_expires_at
            )
          )
        );
        CREATE INDEX task_image_build_projections_active_session_idx
          ON task_image_build_projections (session_expires_at, grant_id)
          WHERE state = 'exchanged';
        CREATE INDEX task_image_build_projections_attestation_expiry_idx
          ON task_image_build_projections (attestation_expires_at, grant_id)
          WHERE state IN ('projected','exchanged');

        CREATE TABLE task_image_build_projection_events (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          grant_id UUID NOT NULL,
          event_sequence INTEGER NOT NULL,
          event_type VARCHAR(32) NOT NULL,
          event_key VARCHAR(128) NOT NULL,
          payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT task_image_build_projection_events_projection_fkey
            FOREIGN KEY (grant_id) REFERENCES task_image_build_projections(grant_id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_build_projection_events_sequence_uidx
            UNIQUE (grant_id, event_sequence),
          CONSTRAINT task_image_build_projection_events_type_key_uidx
            UNIQUE (grant_id, event_type, event_key),
          CONSTRAINT task_image_build_projection_events_sequence_check
            CHECK (event_sequence > 0),
          CONSTRAINT task_image_build_projection_events_type_check CHECK (
            event_type IN (
              'challenged','challenge_replayed','projected','projection_replayed',
              'exchanged','exchange_replayed','attested','attestation_replayed',
              'revoked','expired'
            )
          ),
          CONSTRAINT task_image_build_projection_events_key_check CHECK (
            event_key ~ '^[a-z0-9][a-z0-9_.:-]{0,127}$'
            AND jsonb_typeof(payload_json) = 'object'
          )
        );
        CREATE INDEX task_image_build_projection_events_created_idx
          ON task_image_build_projection_events (grant_id, created_at, event_sequence);

        CREATE TABLE task_image_build_containment_attestations (
          id UUID PRIMARY KEY,
          grant_id UUID NOT NULL,
          generation BIGINT NOT NULL,
          attestation_json JSONB NOT NULL,
          attestation_sha256 VARCHAR(64) NOT NULL,
          issued_at TIMESTAMPTZ NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT task_image_build_containment_attestations_projection_fkey
            FOREIGN KEY (grant_id) REFERENCES task_image_build_projections(grant_id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_build_containment_attestations_generation_uidx
            UNIQUE (grant_id, generation),
          CONSTRAINT task_image_build_containment_attestations_generation_check CHECK (
            id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND generation > 0
          ),
          CONSTRAINT task_image_build_containment_attestations_digest_check CHECK (
            jsonb_typeof(attestation_json) = 'object'
            AND attestation_sha256 ~ '^[0-9a-f]{64}$'
            AND attestation_sha256 <> repeat('0', 64)
          ),
          CONSTRAINT task_image_build_containment_attestations_time_check CHECK (
            issued_at < expires_at
          )
        );
        CREATE INDEX task_image_build_containment_attestations_expiry_idx
          ON task_image_build_containment_attestations (expires_at, grant_id, generation);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE task_image_build_containment_attestations;
        DROP TABLE task_image_build_projection_events;
        DROP TABLE task_image_build_projections;
        ALTER TABLE task_image_build_grants
          DROP CONSTRAINT task_image_build_grants_authority_check,
          DROP COLUMN grant_expires_at,
          DROP COLUMN authority_sha256,
          DROP COLUMN authority_spec;
        """
    )
