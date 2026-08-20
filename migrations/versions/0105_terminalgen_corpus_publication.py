"""Persist immutable TerminalGen corpus publication and alias authority.

Revision ID: 0105
Revises: 0104
"""

from alembic import op

revision = "0105"
down_revision = "0104"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE terminalgen_corpus_versions (
          id UUID PRIMARY KEY,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
          corpus_id TEXT NOT NULL,
          corpus_version INTEGER NOT NULL,
          version_sha256 TEXT NOT NULL,
          recipe_digest TEXT NOT NULL,
          plan_identity_sha256 TEXT NOT NULL,
          final_audit_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
          authoring_corpus_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
          runtime_corpus_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
          authoring_tree_sha256 TEXT NOT NULL,
          runtime_tree_sha256 TEXT NOT NULL,
          task_count INTEGER NOT NULL,
          taskset_smoke_task_count INTEGER NOT NULL,
          taskset_smoke_object_key TEXT NOT NULL,
          taskset_smoke_sha256 TEXT NOT NULL,
          taskset_smoke_size_bytes BIGINT NOT NULL,
          taskset_manifest_object_key TEXT NOT NULL,
          taskset_manifest_json JSONB NOT NULL,
          taskset_manifest_sha256 TEXT NOT NULL,
          published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT terminalgen_corpus_versions_counts_check
            CHECK (corpus_version > 0 AND task_count > 0 AND task_count <= 9000),
          CONSTRAINT terminalgen_corpus_versions_digest_check CHECK (
            version_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND recipe_digest ~ '^sha256:[0-9a-f]{64}$'
            AND plan_identity_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND authoring_tree_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND runtime_tree_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND taskset_smoke_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND taskset_manifest_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT terminalgen_corpus_versions_smoke_check CHECK (
            taskset_smoke_task_count > 0 AND taskset_smoke_task_count <= 500
            AND taskset_smoke_task_count <= task_count AND taskset_smoke_size_bytes > 0
          ),
          CONSTRAINT terminalgen_corpus_versions_identity_uidx
            UNIQUE (team_id, corpus_id, corpus_version),
          CONSTRAINT terminalgen_corpus_versions_digest_uidx
            UNIQUE (team_id, version_sha256),
          CONSTRAINT terminalgen_corpus_versions_run_uidx UNIQUE (pipeline_run_id)
        );
        CREATE INDEX terminalgen_corpus_versions_team_created_idx
          ON terminalgen_corpus_versions (team_id, published_at, id);

        CREATE TABLE terminalgen_corpus_tasks (
          corpus_version_id UUID NOT NULL
            REFERENCES terminalgen_corpus_versions(id) ON DELETE CASCADE,
          task_ordinal INTEGER NOT NULL,
          slot_id TEXT NOT NULL,
          task_id TEXT NOT NULL,
          task_name TEXT NOT NULL,
          source_task_tree_sha256 TEXT NOT NULL,
          projected_task_tree_sha256 TEXT NOT NULL,
          source_task_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
          validation_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
          authoring_bundle_path TEXT NOT NULL,
          authoring_bundle_sha256 TEXT NOT NULL,
          authoring_bundle_size_bytes BIGINT NOT NULL,
          runtime_bundle_path TEXT NOT NULL,
          runtime_bundle_sha256 TEXT NOT NULL,
          runtime_bundle_size_bytes BIGINT NOT NULL,
          verifier_bridge_sha256 TEXT NOT NULL,
          PRIMARY KEY (corpus_version_id, task_ordinal),
          CONSTRAINT terminalgen_corpus_tasks_ordinal_check CHECK (task_ordinal >= 0),
          CONSTRAINT terminalgen_corpus_tasks_digest_check CHECK (
            source_task_tree_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND projected_task_tree_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND authoring_bundle_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND runtime_bundle_sha256 ~ '^sha256:[0-9a-f]{64}$'
            AND verifier_bridge_sha256 ~ '^sha256:[0-9a-f]{64}$'
          ),
          CONSTRAINT terminalgen_corpus_tasks_size_check CHECK (
            authoring_bundle_size_bytes > 0 AND runtime_bundle_size_bytes > 0
          ),
          CONSTRAINT terminalgen_corpus_tasks_slot_uidx
            UNIQUE (corpus_version_id, slot_id),
          CONSTRAINT terminalgen_corpus_tasks_task_uidx
            UNIQUE (corpus_version_id, task_id),
          CONSTRAINT terminalgen_corpus_tasks_ordinal_uidx
            UNIQUE (corpus_version_id, task_ordinal)
        );
        CREATE INDEX terminalgen_corpus_tasks_task_id_idx
          ON terminalgen_corpus_tasks (task_id);

        CREATE TABLE terminalgen_corpus_aliases (
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
          alias TEXT NOT NULL,
          corpus_version_id UUID NOT NULL
            REFERENCES terminalgen_corpus_versions(id) ON DELETE RESTRICT,
          previous_corpus_version_id UUID
            REFERENCES terminalgen_corpus_versions(id) ON DELETE RESTRICT,
          generation BIGINT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (team_id, alias),
          CONSTRAINT terminalgen_corpus_aliases_generation_check CHECK (generation > 0),
          CONSTRAINT terminalgen_corpus_aliases_identity_uidx UNIQUE (team_id, alias)
        );

        CREATE TABLE terminalgen_corpus_publications (
          id UUID PRIMARY KEY,
          team_id UUID NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
          pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
          request_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
          request_sha256 TEXT NOT NULL,
          state TEXT NOT NULL,
          corpus_version_id UUID REFERENCES terminalgen_corpus_versions(id) ON DELETE RESTRICT,
          reason_code TEXT,
          receipt_json JSONB,
          receipt_bytes BYTEA,
          receipt_sha256 TEXT,
          finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT terminalgen_corpus_publications_state_check
            CHECK (state IN ('published','failed')),
          CONSTRAINT terminalgen_corpus_publications_request_digest_check
            CHECK (request_sha256 ~ '^sha256:[0-9a-f]{64}$'),
          CONSTRAINT terminalgen_corpus_publications_terminal_group_check CHECK (
            (state='published' AND corpus_version_id IS NOT NULL AND reason_code IS NULL
             AND receipt_json IS NOT NULL AND receipt_bytes IS NOT NULL
             AND receipt_sha256 ~ '^sha256:[0-9a-f]{64}$')
            OR
            (state='failed' AND corpus_version_id IS NULL AND reason_code IS NOT NULL
             AND receipt_json IS NULL AND receipt_bytes IS NULL AND receipt_sha256 IS NULL)
          ),
          CONSTRAINT terminalgen_corpus_publications_run_uidx UNIQUE (pipeline_run_id),
          CONSTRAINT terminalgen_corpus_publications_request_uidx UNIQUE (request_artifact_id)
        );
        CREATE INDEX terminalgen_corpus_publications_team_state_idx
          ON terminalgen_corpus_publications (team_id, state, finished_at);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM terminalgen_corpus_publications)
             OR EXISTS (SELECT 1 FROM terminalgen_corpus_versions) THEN
            RAISE EXCEPTION 'cannot downgrade 0105 with TerminalGen corpus publication data';
          END IF;
        END $$;
        DROP TABLE terminalgen_corpus_publications;
        DROP TABLE terminalgen_corpus_aliases;
        DROP TABLE terminalgen_corpus_tasks;
        DROP TABLE terminalgen_corpus_versions;
        """
    )
