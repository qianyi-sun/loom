"""Add Pipeline GPU capability and dual-Slurm selection evidence.

Revision ID: 0089
Revises: 0088
Create Date: 2026-08-11
"""

from __future__ import annotations

from alembic import op

revision = "0089"
down_revision = "0088"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE workers ADD COLUMN capability_snapshot_json JSONB")
    op.execute("ALTER TABLE workers ADD COLUMN slurm_gpu_allocation_evidence_json JSONB")
    op.execute("ALTER TABLE workers ADD COLUMN slurm_gpu_allocation_evidence_digest TEXT")
    op.execute(
        "ALTER TABLE workers ADD CONSTRAINT workers_capability_snapshot_json_check "
        "CHECK (capability_snapshot_json IS NULL OR "
        "jsonb_typeof(capability_snapshot_json) = 'object')"
    )
    op.execute(
        "ALTER TABLE workers ADD CONSTRAINT workers_slurm_gpu_evidence_group_check "
        "CHECK ((slurm_gpu_allocation_evidence_json IS NULL) = "
        "(slurm_gpu_allocation_evidence_digest IS NULL))"
    )
    op.execute(
        "ALTER TABLE workers ADD CONSTRAINT workers_slurm_gpu_evidence_json_check "
        "CHECK (slurm_gpu_allocation_evidence_json IS NULL OR "
        "jsonb_typeof(slurm_gpu_allocation_evidence_json) = 'object')"
    )
    op.execute(
        "ALTER TABLE workers ADD CONSTRAINT workers_slurm_gpu_evidence_digest_check "
        "CHECK (slurm_gpu_allocation_evidence_digest IS NULL OR "
        "slurm_gpu_allocation_evidence_digest ~ '^sha256:[0-9a-f]{64}$')"
    )

    op.execute("ALTER TABLE slurm_worker_jobs ADD COLUMN slurm_cluster_id TEXT")
    op.execute(
        "UPDATE slurm_worker_jobs SET slurm_cluster_id = "
        "CASE WHEN pool_name = 'gb10' THEN 'gb10' ELSE 'oldlab' END"
    )
    op.execute("ALTER TABLE slurm_worker_jobs ALTER COLUMN slurm_cluster_id SET NOT NULL")
    op.execute(
        "ALTER TABLE slurm_worker_jobs ALTER COLUMN slurm_cluster_id SET DEFAULT 'oldlab'"
    )
    op.execute(
        "ALTER TABLE slurm_worker_jobs ADD CONSTRAINT slurm_worker_jobs_cluster_check "
        "CHECK (slurm_cluster_id IN ('oldlab','gb10'))"
    )
    op.execute("DROP INDEX slurm_worker_jobs_job_id_uidx")
    op.execute(
        "CREATE UNIQUE INDEX slurm_worker_jobs_job_id_uidx "
        "ON slurm_worker_jobs(slurm_cluster_id,job_id) WHERE job_id IS NOT NULL"
    )

    op.execute(
        """
        CREATE TABLE pipeline_run_gpu_backend_selections (
            id UUID PRIMARY KEY,
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            scope TEXT NOT NULL
                CHECK (scope IN ('all_gpu_nodes','oldlab_preflight','gb10_preflight')),
            variant_id TEXT NOT NULL
                CHECK (variant_id IN ('gb10-shared-1gpu','oldlab-rtx5080-2gpu')),
            policy_id TEXT NOT NULL
                CHECK (policy_id IN ('behavior-gpu-gb10','behavior-gpu-oldlab')),
            selection_source TEXT NOT NULL
                CHECK (selection_source IN
                    ('recipe_hash','acceptance_authority','profile_calibration_authority')),
            selected_at TIMESTAMPTZ NOT NULL,
            selection_json JSONB NOT NULL,
            selection_bytes BYTEA NOT NULL,
            gpu_backend_selection_sha256 TEXT NOT NULL,
            CONSTRAINT pipeline_gpu_selection_run_scope_uidx
                UNIQUE(pipeline_run_id,scope)
        )
        """
    )
    op.execute(
        "ALTER TABLE pipeline_run_gpu_backend_selections ADD CONSTRAINT "
        "pipeline_gpu_selection_digest_check CHECK "
        "(gpu_backend_selection_sha256 ~ '^sha256:[0-9a-f]{64}$')"
    )
    op.execute(
        "ALTER TABLE pipeline_run_gpu_backend_selections ADD CONSTRAINT "
        "pipeline_gpu_selection_document_check CHECK "
        "(jsonb_typeof(selection_json) = 'object' AND octet_length(selection_bytes) > 1 "
        "AND get_byte(selection_bytes, octet_length(selection_bytes) - 1) = 10)"
    )
    op.execute(
        "ALTER TABLE pipeline_run_gpu_backend_selections ADD CONSTRAINT "
        "pipeline_gpu_selection_variant_policy_check CHECK ((variant_id = "
        "'gb10-shared-1gpu' AND policy_id = 'behavior-gpu-gb10') OR (variant_id = "
        "'oldlab-rtx5080-2gpu' AND policy_id = 'behavior-gpu-oldlab'))"
    )
    op.execute(
        "ALTER TABLE pipeline_run_gpu_backend_selections ADD CONSTRAINT "
        "pipeline_gpu_selection_scope_authority_check CHECK ((selection_source = "
        "'recipe_hash' AND scope = 'all_gpu_nodes') OR (selection_source <> "
        "'recipe_hash' AND (scope = 'all_gpu_nodes' OR ((variant_id = "
        "'gb10-shared-1gpu' AND scope = "
        "'gb10_preflight') OR (variant_id = 'oldlab-rtx5080-2gpu' AND scope = "
        "'oldlab_preflight')))))"
    )
    op.execute(
        """
        CREATE TABLE pipeline_scoped_policy_activations (
            id UUID PRIMARY KEY,
            environment TEXT NOT NULL CHECK (length(trim(environment)) > 0),
            policy_id TEXT NOT NULL CHECK (policy_id IN
                ('behavior-cpu-data','behavior-gpu-oldlab','behavior-gpu-gb10')),
            policy_config_sha256 TEXT NOT NULL
                CHECK (policy_config_sha256 ~ '^sha256:[0-9a-f]{64}$'),
            authority_kind TEXT NOT NULL
                CHECK (authority_kind IN ('acceptance','profile_calibration')),
            authority_id UUID NOT NULL,
            activation_epoch BIGINT NOT NULL CHECK (activation_epoch > 0),
            state TEXT NOT NULL CHECK (state IN ('active','draining','disabled')),
            desired_slots INTEGER NOT NULL,
            activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pipeline_policy_activation_state_slots_check CHECK
                ((state='active' AND desired_slots > 0) OR
                 (state IN ('draining','disabled') AND desired_slots=0)),
            CONSTRAINT pipeline_policy_activation_slot_ceiling_check CHECK
                ((policy_id='behavior-cpu-data' AND desired_slots <= 2) OR
                 (policy_id IN ('behavior-gpu-oldlab','behavior-gpu-gb10')
                  AND desired_slots <= 1)),
            CONSTRAINT pipeline_policy_activation_authority_policy_uidx UNIQUE
                (authority_kind,authority_id,policy_id),
            CONSTRAINT pipeline_policy_activation_environment_epoch_uidx UNIQUE
                (environment,policy_id,activation_epoch)
        )
        """
    )
    op.execute(
        "CREATE UNIQUE INDEX pipeline_policy_activation_active_policy_uidx "
        "ON pipeline_scoped_policy_activations(environment,policy_id) "
        "WHERE state='active'"
    )


def downgrade() -> None:
    op.execute("DROP TABLE pipeline_scoped_policy_activations")
    op.execute("DROP TABLE pipeline_run_gpu_backend_selections")
    op.execute("DROP INDEX slurm_worker_jobs_job_id_uidx")
    op.execute(
        "CREATE UNIQUE INDEX slurm_worker_jobs_job_id_uidx "
        "ON slurm_worker_jobs(job_id) WHERE job_id IS NOT NULL"
    )
    op.execute("ALTER TABLE slurm_worker_jobs DROP CONSTRAINT slurm_worker_jobs_cluster_check")
    op.execute("ALTER TABLE slurm_worker_jobs DROP COLUMN slurm_cluster_id")
    op.execute("ALTER TABLE workers DROP CONSTRAINT workers_slurm_gpu_evidence_group_check")
    op.execute("ALTER TABLE workers DROP CONSTRAINT workers_slurm_gpu_evidence_json_check")
    op.execute("ALTER TABLE workers DROP CONSTRAINT workers_slurm_gpu_evidence_digest_check")
    op.execute("ALTER TABLE workers DROP CONSTRAINT workers_capability_snapshot_json_check")
    op.execute("ALTER TABLE workers DROP COLUMN slurm_gpu_allocation_evidence_digest")
    op.execute("ALTER TABLE workers DROP COLUMN slurm_gpu_allocation_evidence_json")
    op.execute("ALTER TABLE workers DROP COLUMN capability_snapshot_json")
