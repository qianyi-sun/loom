"""Add fenced registry credentials and inert publication candidates.

Revision ID: 0131
Revises: 0130
Create Date: 2026-09-04
"""

from __future__ import annotations

from alembic import op

revision: str = "0131"
down_revision: str | None = "0130"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE TABLE task_image_registry_credentials (
          credential_id UUID PRIMARY KEY,
          request_id UUID NOT NULL,
          materialization_attempt_id UUID NOT NULL,
          materialization_id UUID NOT NULL,
          attempt_number INTEGER NOT NULL,
          lease_epoch BIGINT NOT NULL,
          builder_id VARCHAR(128) NOT NULL,
          grant_id UUID NOT NULL,
          session_id UUID NOT NULL,
          session_generation BIGINT NOT NULL,
          attestation_generation BIGINT NOT NULL,
          attestation_sha256 VARCHAR(64) NOT NULL,
          component VARCHAR(136) NOT NULL,
          generation INTEGER NOT NULL,
          predecessor_credential_id UUID,
          lease_heartbeat_operation_id UUID,
          repository TEXT NOT NULL,
          registry_origin TEXT NOT NULL,
          registry_service VARCHAR(128) NOT NULL,
          registry_issuer VARCHAR(128) NOT NULL,
          registry_key_id VARCHAR(128) NOT NULL,
          request_sha256 VARCHAR(64) NOT NULL,
          response_public_json JSONB NOT NULL,
          response_sha256 VARCHAR(64) NOT NULL,
          token_hash BYTEA NOT NULL,
          secret_response_ref TEXT NOT NULL,
          issued_at TIMESTAMPTZ NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          recorded_at TIMESTAMPTZ NOT NULL,
          CONSTRAINT task_image_registry_credentials_attempt_fkey
            FOREIGN KEY (
              materialization_attempt_id, materialization_id,
              attempt_number, lease_epoch, builder_id, grant_id
            ) REFERENCES task_image_materialization_attempts (
              id, materialization_id, attempt_number,
              lease_epoch, builder_id, grant_id
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_registry_credentials_session_fkey
            FOREIGN KEY (grant_id, session_generation, session_id)
            REFERENCES task_image_build_session_generations (
              grant_id, generation, session_id
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_registry_credentials_attestation_fkey
            FOREIGN KEY (grant_id, attestation_generation)
            REFERENCES task_image_build_containment_attestations (
              grant_id, generation
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_registry_credentials_predecessor_fkey
            FOREIGN KEY (predecessor_credential_id)
            REFERENCES task_image_registry_credentials (credential_id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_registry_credentials_heartbeat_fkey
            FOREIGN KEY (lease_heartbeat_operation_id)
            REFERENCES task_image_materialization_operation_events (operation_id)
            ON DELETE RESTRICT,
          CONSTRAINT task_image_registry_credentials_request_uidx
            UNIQUE (request_id),
          CONSTRAINT task_image_registry_credentials_component_generation_uidx
            UNIQUE (materialization_attempt_id, component, generation),
          CONSTRAINT task_image_registry_credentials_candidate_binding_uidx
            UNIQUE (
              credential_id, materialization_attempt_id, component, repository
            ),
          CONSTRAINT task_image_registry_credentials_binding_check CHECK (
            credential_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND request_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND materialization_attempt_id <>
              '00000000-0000-0000-0000-000000000000'::uuid
            AND materialization_id <>
              '00000000-0000-0000-0000-000000000000'::uuid
            AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND attempt_number > 0 AND lease_epoch > 0
            AND session_generation > 0 AND attestation_generation > 0
            AND generation BETWEEN 1 AND 512
            AND component ~ '^(task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$'
          ),
          CONSTRAINT task_image_registry_credentials_chain_check CHECK (
            (generation = 1
              AND predecessor_credential_id IS NULL
              AND lease_heartbeat_operation_id IS NULL)
            OR
            (generation > 1
              AND predecessor_credential_id IS NOT NULL
              AND predecessor_credential_id <> credential_id
              AND lease_heartbeat_operation_id IS NOT NULL)
          ),
          CONSTRAINT task_image_registry_credentials_digest_check CHECK (
            attestation_sha256 ~ '^[0-9a-f]{64}$'
            AND attestation_sha256 <> repeat('0', 64)
            AND request_sha256 ~ '^[0-9a-f]{64}$'
            AND request_sha256 <> repeat('0', 64)
            AND jsonb_typeof(response_public_json) = 'object'
            AND response_sha256 ~ '^[0-9a-f]{64}$'
            AND response_sha256 <> repeat('0', 64)
            AND octet_length(token_hash) = 32
            AND registry_key_id ~ '^[A-Za-z0-9_-]{43}$'
          ),
          CONSTRAINT task_image_registry_credentials_registry_check CHECK (
            octet_length(repository) BETWEEN 1 AND 255
            AND repository ~ (
              '^loom-task-image-attempts/(x86_64|arm64)/'
              '[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-'
              '[89ab][0-9a-f]{3}-[0-9a-f]{12}/'
              '(task|sidecar-sha256-[0-9a-f]{64})$'
            )
            AND octet_length(registry_origin) BETWEEN 9 AND 2048
            AND registry_origin ~ '^https://[^/@?#]+(:[1-9][0-9]{0,4})?$'
            AND registry_service ~ '^[a-z0-9][a-z0-9_.:-]{0,127}$'
            AND registry_issuer ~ '^[a-z0-9][a-z0-9_.:-]{0,127}$'
          ),
          CONSTRAINT task_image_registry_credentials_secret_check CHECK (
            secret_response_ref ~
              '^loom://task-image-registry-credential/'
              '[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-'
              '[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          ),
          CONSTRAINT task_image_registry_credentials_time_check CHECK (
            issued_at < expires_at
            AND expires_at <= issued_at + INTERVAL '45 seconds'
            AND issued_at <= recorded_at
          )
        );
        CREATE INDEX task_image_registry_credentials_expiry_idx
          ON task_image_registry_credentials (expires_at, credential_id);
        CREATE INDEX task_image_registry_credentials_renewal_idx
          ON task_image_registry_credentials (
            materialization_attempt_id, component, generation DESC
          );

        CREATE TABLE task_image_publication_candidates (
          candidate_id UUID PRIMARY KEY,
          operation_id UUID NOT NULL,
          credential_id UUID NOT NULL,
          materialization_attempt_id UUID NOT NULL,
          materialization_id UUID NOT NULL,
          attempt_number INTEGER NOT NULL,
          lease_epoch BIGINT NOT NULL,
          builder_id VARCHAR(128) NOT NULL,
          grant_id UUID NOT NULL,
          session_id UUID NOT NULL,
          session_generation BIGINT NOT NULL,
          component VARCHAR(136) NOT NULL,
          repository TEXT NOT NULL,
          manifest_digest VARCHAR(71) NOT NULL,
          manifest_size BIGINT NOT NULL,
          oci_file_sha256 VARCHAR(64) NOT NULL,
          oci_file_size BIGINT NOT NULL,
          platform VARCHAR(16) NOT NULL,
          response_json JSONB NOT NULL,
          response_sha256 VARCHAR(64) NOT NULL,
          recorded_at TIMESTAMPTZ NOT NULL,
          CONSTRAINT task_image_publication_candidates_attempt_fkey
            FOREIGN KEY (
              materialization_attempt_id, materialization_id,
              attempt_number, lease_epoch, builder_id, grant_id
            ) REFERENCES task_image_materialization_attempts (
              id, materialization_id, attempt_number,
              lease_epoch, builder_id, grant_id
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_candidates_session_fkey
            FOREIGN KEY (grant_id, session_generation, session_id)
            REFERENCES task_image_build_session_generations (
              grant_id, generation, session_id
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_candidates_credential_fkey
            FOREIGN KEY (
              credential_id, materialization_attempt_id, component, repository
            ) REFERENCES task_image_registry_credentials (
              credential_id, materialization_attempt_id, component, repository
            ) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_candidates_operation_uidx
            UNIQUE (operation_id),
          CONSTRAINT task_image_publication_candidates_attempt_component_uidx
            UNIQUE (materialization_attempt_id, component),
          CONSTRAINT task_image_publication_candidates_binding_check CHECK (
            candidate_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND operation_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND credential_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND materialization_attempt_id <>
              '00000000-0000-0000-0000-000000000000'::uuid
            AND materialization_id <>
              '00000000-0000-0000-0000-000000000000'::uuid
            AND grant_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND session_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND attempt_number > 0 AND lease_epoch > 0
            AND session_generation > 0
            AND component ~ '^(task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$'
            AND repository ~ (
              '^loom-task-image-attempts/(x86_64|arm64)/'
              '[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-'
              '[89ab][0-9a-f]{3}-[0-9a-f]{12}/'
              '(task|sidecar-sha256-[0-9a-f]{64})$'
            )
            AND platform IN ('linux/amd64', 'linux/arm64')
          ),
          CONSTRAINT task_image_publication_candidates_digest_check CHECK (
            manifest_digest ~ '^sha256:[0-9a-f]{64}$'
            AND manifest_digest <> 'sha256:' || repeat('0', 64)
            AND manifest_size > 0
            AND oci_file_sha256 ~ '^[0-9a-f]{64}$'
            AND oci_file_sha256 <> repeat('0', 64)
            AND oci_file_size > 0
          ),
          CONSTRAINT task_image_publication_candidates_response_check CHECK (
            jsonb_typeof(response_json) = 'object'
            AND response_sha256 ~ '^[0-9a-f]{64}$'
            AND response_sha256 <> repeat('0', 64)
          )
        );
        CREATE INDEX task_image_publication_candidates_observed_idx
          ON task_image_publication_candidates (recorded_at, candidate_id);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE task_image_publication_candidates;
        DROP TABLE task_image_registry_credentials;
        """
    )
