"""Persist immutable Pipeline RunGraph contracts.

Revision ID: 0078
Revises: 0077
Create Date: 2026-08-10
"""

from __future__ import annotations

from alembic import op

revision = "0078"
down_revision = "0077"
branch_labels = None
depends_on = None


def upgrade() -> None:
    statements = (
        """
        CREATE TABLE pipeline_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            team_id UUID NOT NULL REFERENCES teams(id),
            created_by_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
            submission_policy TEXT NOT NULL,
            acceptance_authorization_id UUID,
            acceptance_candidate_sha256 TEXT,
            official_submission_kind TEXT,
            official_submission_authority_id UUID,
            official_submission_authority_snapshot_digest TEXT,
            official_submission_identity_digest TEXT,
            recipe_name TEXT NOT NULL,
            recipe_version INTEGER NOT NULL,
            recipe_digest TEXT NOT NULL,
            graph_spec_json JSONB NOT NULL,
            graph_spec_digest TEXT NOT NULL,
            parameters_json JSONB NOT NULL,
            parameters_digest TEXT NOT NULL,
            resolved_inputs_json JSONB NOT NULL,
            budget_json JSONB NOT NULL,
            request_digest TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'submitted',
            result TEXT,
            result_reason TEXT,
            retry_of_pipeline_run_id UUID REFERENCES pipeline_runs(id) ON DELETE RESTRICT,
            retry_from_stage_run_id UUID,
            cancellation_requested_at TIMESTAMPTZ,
            budget_exhausted_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            next_event_seq BIGINT NOT NULL DEFAULT 1,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_runs_submission_policy_check CHECK (
                submission_policy IN ('ordinary', 'acceptance_authorization_only')
            ),
            CONSTRAINT pipeline_runs_state_check CHECK (
                state IN ('submitted', 'running', 'cancelling', 'finished')
            ),
            CONSTRAINT pipeline_runs_result_check CHECK (
                result IS NULL OR result IN
                ('succeeded','partial_failed','failed','cancelled','budget_exhausted')
            ),
            CONSTRAINT pipeline_runs_terminal_result_check CHECK (
                (state = 'finished') = (result IS NOT NULL AND finished_at IS NOT NULL)
            ),
            CONSTRAINT pipeline_runs_result_reason_state_check CHECK (
                state = 'finished' OR result_reason IS NULL
            ),
            CONSTRAINT pipeline_runs_official_origin_group_check CHECK (
                (official_submission_kind IS NULL
                 AND official_submission_authority_id IS NULL
                 AND official_submission_authority_snapshot_digest IS NULL
                 AND official_submission_identity_digest IS NULL)
                OR
                (official_submission_kind IS NOT NULL
                 AND official_submission_authority_id IS NOT NULL
                 AND official_submission_authority_snapshot_digest IS NOT NULL
                 AND official_submission_identity_digest IS NOT NULL)
            ),
            CONSTRAINT pipeline_runs_submission_origin_check CHECK (
                (submission_policy = 'acceptance_authorization_only'
                 AND acceptance_authorization_id IS NOT NULL
                 AND acceptance_candidate_sha256 IS NOT NULL
                 AND recipe_name = 'behavior-recovery-acceptance-preflight'
                 AND recipe_version = 1
                 AND retry_of_pipeline_run_id IS NULL
                 AND retry_from_stage_run_id IS NULL
                 AND official_submission_kind IS NULL)
                OR
                (submission_policy = 'ordinary'
                 AND acceptance_authorization_id IS NULL
                 AND acceptance_candidate_sha256 IS NULL)
            ),
            CONSTRAINT pipeline_runs_retry_group_check CHECK (
                (retry_of_pipeline_run_id IS NULL) = (retry_from_stage_run_id IS NULL)
            ),
            CONSTRAINT pipeline_runs_official_not_retry_check CHECK (
                official_submission_kind IS NULL
                OR (submission_policy = 'ordinary'
                    AND retry_of_pipeline_run_id IS NULL
                    AND retry_from_stage_run_id IS NULL)
            ),
            CONSTRAINT pipeline_runs_next_event_seq_positive CHECK (next_event_seq > 0),
            CONSTRAINT pipeline_runs_version_nonnegative CHECK (version >= 0),
            CONSTRAINT pipeline_runs_team_idempotency_uidx UNIQUE (team_id, idempotency_key)
        )
        """,
        """
        CREATE UNIQUE INDEX pipeline_runs_official_identity_uidx
        ON pipeline_runs(team_id, official_submission_kind, official_submission_identity_digest)
        WHERE official_submission_identity_digest IS NOT NULL
        """,
        """
        CREATE INDEX pipeline_runs_team_created_idx
        ON pipeline_runs(team_id, created_at DESC, id)
        """,
        """
        CREATE INDEX pipeline_runs_state_created_idx
        ON pipeline_runs(state, created_at, id)
        """,
        """
        CREATE TABLE pipeline_stage_runs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            node_key TEXT NOT NULL,
            shard_key TEXT NOT NULL,
            node_kind TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'blocked',
            domain_outcome TEXT,
            reason_code TEXT,
            resolved_execution_spec_json JSONB,
            resolved_execution_spec_bytes BYTEA,
            execution_spec_digest TEXT,
            resource_profile_json JSONB,
            resource_profile_digest TEXT,
            resolved_input_bindings_json JSONB,
            resolved_input_bindings_digest TEXT,
            fanout_parameters_json JSONB,
            fanout_item_digest TEXT,
            fanout_expansion_id UUID,
            gate_subject_stage_run_id UUID,
            request_renderer_json JSONB,
            request_renderer_digest TEXT,
            failure_policy TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TIMESTAMPTZ,
            latest_checkpoint_artifact_id UUID,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ready_at TIMESTAMPTZ,
            claimed_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_stage_runs_kind_check CHECK (node_kind IN ('container','gate')),
            CONSTRAINT pipeline_stage_runs_state_check CHECK (
                state IN ('blocked','ready','queued','claimed','running','retry_wait',
                          'succeeded','failed','cancelled','skipped')
            ),
            CONSTRAINT pipeline_stage_runs_execution_spec_group_check CHECK (
                (resolved_execution_spec_json IS NULL
                 AND resolved_execution_spec_bytes IS NULL
                 AND execution_spec_digest IS NULL)
                OR
                (resolved_execution_spec_json IS NOT NULL
                 AND resolved_execution_spec_bytes IS NOT NULL
                 AND execution_spec_digest IS NOT NULL)
            ),
            CONSTRAINT pipeline_stage_runs_bindings_group_check CHECK (
                (resolved_input_bindings_json IS NULL
                 AND resolved_input_bindings_digest IS NULL)
                OR
                (resolved_input_bindings_json IS NOT NULL
                 AND resolved_input_bindings_digest IS NOT NULL)
            ),
            CONSTRAINT pipeline_stage_runs_frozen_groups_together_check CHECK (
                (resolved_execution_spec_json IS NULL) =
                (resolved_input_bindings_json IS NULL)
            ),
            CONSTRAINT pipeline_stage_runs_resource_group_check CHECK (
                (resource_profile_json IS NULL) = (resource_profile_digest IS NULL)
            ),
            CONSTRAINT pipeline_stage_runs_renderer_group_check CHECK (
                (request_renderer_json IS NULL) = (request_renderer_digest IS NULL)
            ),
            CONSTRAINT pipeline_stage_runs_fanout_group_check CHECK (
                (fanout_expansion_id IS NULL
                 AND fanout_parameters_json IS NULL
                 AND fanout_item_digest IS NULL)
                OR
                (fanout_expansion_id IS NOT NULL
                 AND fanout_parameters_json IS NOT NULL
                 AND fanout_item_digest IS NOT NULL)
            ),
            CONSTRAINT pipeline_stage_runs_kind_fields_check CHECK (
                (node_kind = 'gate'
                 AND gate_subject_stage_run_id IS NOT NULL
                 AND resolved_execution_spec_json IS NULL
                 AND resource_profile_json IS NULL
                 AND resolved_input_bindings_json IS NULL
                 AND fanout_expansion_id IS NULL
                 AND request_renderer_json IS NULL
                 AND failure_policy IS NULL
                 AND latest_checkpoint_artifact_id IS NULL
                 AND next_attempt_at IS NULL
                 AND claimed_at IS NULL
                 AND started_at IS NULL
                 AND attempt_count = 0)
                OR
                (node_kind = 'container'
                 AND gate_subject_stage_run_id IS NULL
                 AND resource_profile_json IS NOT NULL
                 AND failure_policy IN ('fail_run','continue'))
            ),
            CONSTRAINT pipeline_stage_runs_ready_frozen_check CHECK (
                node_kind != 'container'
                OR NOT (state IN ('ready','queued','claimed','running','retry_wait','succeeded')
                        OR attempt_count > 0)
                OR (resolved_execution_spec_json IS NOT NULL
                    AND resolved_input_bindings_json IS NOT NULL)
            ),
            CONSTRAINT pipeline_stage_runs_gate_state_check CHECK (
                node_kind != 'gate' OR state IN ('blocked','succeeded','skipped')
            ),
            CONSTRAINT pipeline_stage_runs_shard_expansion_check CHECK (
                node_kind != 'container'
                OR ((shard_key = 'singleton') = (fanout_expansion_id IS NULL))
            ),
            CONSTRAINT pipeline_stage_runs_attempt_count_range CHECK (attempt_count BETWEEN 0 AND 3),
            CONSTRAINT pipeline_stage_runs_terminal_fields_check CHECK (
                state IN ('succeeded','failed','cancelled','skipped')
                OR (domain_outcome IS NULL AND reason_code IS NULL)
            ),
            CONSTRAINT pipeline_stage_runs_outcome_state_check CHECK (
                domain_outcome IS NULL OR state = 'succeeded'
            ),
            CONSTRAINT pipeline_stage_runs_version_nonnegative CHECK (version >= 0),
            CONSTRAINT pipeline_stage_runs_terminal_timestamp_check CHECK (
                (state IN ('succeeded','failed','cancelled','skipped')) = (finished_at IS NOT NULL)
            ),
            CONSTRAINT pipeline_stage_runs_identity_uidx
                UNIQUE (pipeline_run_id, node_key, shard_key),
            CONSTRAINT pipeline_stage_runs_run_id_uidx UNIQUE (pipeline_run_id, id),
            CONSTRAINT pipeline_stage_runs_run_id_shard_uidx
                UNIQUE (pipeline_run_id, id, shard_key),
            CONSTRAINT pipeline_stage_runs_gate_subject_same_shard_fk
                FOREIGN KEY (pipeline_run_id, gate_subject_stage_run_id, shard_key)
                REFERENCES pipeline_stage_runs(pipeline_run_id, id, shard_key)
                DEFERRABLE INITIALLY DEFERRED
        )
        """,
        """
        CREATE INDEX pipeline_stage_runs_state_retry_idx
        ON pipeline_stage_runs(state, next_attempt_at, created_at, id)
        """,
        """
        CREATE INDEX pipeline_stage_runs_run_node_state_idx
        ON pipeline_stage_runs(pipeline_run_id, node_key, state)
        """,
        """
        CREATE INDEX pipeline_stage_runs_expansion_shard_idx
        ON pipeline_stage_runs(fanout_expansion_id, shard_key)
        """,
        """
        ALTER TABLE pipeline_runs
        ADD CONSTRAINT pipeline_runs_retry_from_stage_fk
        FOREIGN KEY (retry_from_stage_run_id) REFERENCES pipeline_stage_runs(id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
        """,
        """
        CREATE TABLE execution_attempts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            stage_run_id UUID NOT NULL REFERENCES pipeline_stage_runs(id) ON DELETE CASCADE,
            attempt_number INTEGER NOT NULL,
            state TEXT NOT NULL,
            worker_id UUID REFERENCES workers(id) ON DELETE RESTRICT,
            claim_id UUID,
            lease_epoch BIGINT NOT NULL DEFAULT 0,
            lease_token_digest TEXT,
            lease_expires_at TIMESTAMPTZ,
            stage_request_json JSONB,
            stage_request_bytes BYTEA,
            stage_request_digest TEXT,
            exit_code INTEGER,
            retry_class TEXT,
            reason_code TEXT,
            result_manifest_json JSONB,
            result_manifest_digest TEXT,
            resumed_checkpoint_artifact_id UUID REFERENCES artifacts(id) ON DELETE RESTRICT,
            cancellation_requested_at TIMESTAMPTZ,
            cancellation_observed_at TIMESTAMPTZ,
            cancellation_outcome TEXT,
            queued_at TIMESTAMPTZ,
            claimed_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT execution_attempts_state_check CHECK (
                state IN ('fault_pending','queued','claimed','running',
                          'succeeded','failed','cancelled','lost')
            ),
            CONSTRAINT execution_attempts_retry_class_check CHECK (
                retry_class IS NULL OR retry_class IN
                ('none','contract_error','provider_transient','infrastructure_transient',
                 'internal_defect','cancelled')
            ),
            CONSTRAINT execution_attempts_number_check CHECK (attempt_number BETWEEN 1 AND 3),
            CONSTRAINT execution_attempts_lease_epoch_nonnegative CHECK (lease_epoch >= 0),
            CONSTRAINT execution_attempts_version_nonnegative CHECK (version >= 0),
            CONSTRAINT execution_attempts_stage_request_group_check CHECK (
                (stage_request_json IS NULL AND stage_request_bytes IS NULL
                 AND stage_request_digest IS NULL)
                OR
                (stage_request_json IS NOT NULL AND stage_request_bytes IS NOT NULL
                 AND stage_request_digest IS NOT NULL)
            ),
            CONSTRAINT execution_attempts_result_group_check CHECK (
                (result_manifest_json IS NULL) = (result_manifest_digest IS NULL)
            ),
            CONSTRAINT execution_attempts_fault_pending_check CHECK (
                state != 'fault_pending'
                OR (worker_id IS NULL AND claim_id IS NULL AND lease_token_digest IS NULL
                    AND lease_expires_at IS NULL AND queued_at IS NULL
                    AND claimed_at IS NULL AND started_at IS NULL)
            ),
            CONSTRAINT execution_attempts_queued_timestamp_check CHECK (
                state NOT IN ('queued','claimed','running','succeeded','failed','lost')
                OR queued_at IS NOT NULL
            ),
            CONSTRAINT execution_attempts_claim_group_check CHECK (
                (worker_id IS NULL AND claim_id IS NULL AND lease_token_digest IS NULL
                 AND lease_expires_at IS NULL)
                OR
                (worker_id IS NOT NULL AND claim_id IS NOT NULL
                 AND lease_token_digest IS NOT NULL AND lease_expires_at IS NOT NULL)
            ),
            CONSTRAINT execution_attempts_claimed_state_check CHECK (
                state NOT IN ('claimed','running','succeeded','failed','lost')
                OR claim_id IS NOT NULL
            ),
            CONSTRAINT execution_attempts_unclaimed_state_check CHECK (
                state NOT IN ('fault_pending','queued') OR claim_id IS NULL
            ),
            CONSTRAINT execution_attempts_cancellation_group_check CHECK (
                (cancellation_observed_at IS NULL) = (cancellation_outcome IS NULL)
            ),
            CONSTRAINT execution_attempts_succeeded_result_check CHECK (
                state != 'succeeded' OR (exit_code = 0 AND result_manifest_json IS NOT NULL)
            ),
            CONSTRAINT execution_attempts_terminal_fields_check CHECK (
                state IN ('succeeded','failed','cancelled','lost')
                OR (exit_code IS NULL AND retry_class IS NULL AND reason_code IS NULL
                    AND result_manifest_json IS NULL)
            ),
            CONSTRAINT execution_attempts_terminal_timestamp_check CHECK (
                (state IN ('succeeded','failed','cancelled','lost')) = (finished_at IS NOT NULL)
            ),
            CONSTRAINT execution_attempts_stage_number_uidx UNIQUE (stage_run_id, attempt_number)
        )
        """,
        """
        CREATE UNIQUE INDEX execution_attempts_claim_uidx
        ON execution_attempts(claim_id) WHERE claim_id IS NOT NULL
        """,
        """
        CREATE INDEX execution_attempts_state_lease_queue_idx
        ON execution_attempts(state, lease_expires_at, queued_at, id)
        """,
        """
        CREATE INDEX execution_attempts_worker_state_idx
        ON execution_attempts(worker_id, state)
        """,
        """
        ALTER TABLE artifacts ADD COLUMN pipeline_run_id UUID,
            ADD COLUMN pipeline_stage_run_id UUID,
            ADD COLUMN execution_attempt_id UUID,
            ADD COLUMN producer_kind TEXT,
            ADD COLUMN control_producer_kind TEXT,
            ADD COLUMN control_producer_id UUID
        """,
        """
        ALTER TABLE artifacts
            ADD CONSTRAINT artifacts_pipeline_run_fk
                FOREIGN KEY (pipeline_run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            ADD CONSTRAINT artifacts_pipeline_stage_run_fk
                FOREIGN KEY (pipeline_stage_run_id) REFERENCES pipeline_stage_runs(id)
                ON DELETE CASCADE,
            ADD CONSTRAINT artifacts_execution_attempt_fk
                FOREIGN KEY (execution_attempt_id) REFERENCES execution_attempts(id)
                ON DELETE CASCADE,
            ADD CONSTRAINT artifacts_pipeline_producer_kind_check CHECK (
                producer_kind IS NULL OR producer_kind IN ('container','platform','checkpoint')
            ),
            ADD CONSTRAINT artifacts_pipeline_identity_group_check CHECK (
                (pipeline_run_id IS NULL AND pipeline_stage_run_id IS NULL
                 AND execution_attempt_id IS NULL AND producer_kind IS NULL)
                OR
                (pipeline_run_id IS NOT NULL AND pipeline_stage_run_id IS NOT NULL
                 AND execution_attempt_id IS NOT NULL AND producer_kind IS NOT NULL)
            ),
            ADD CONSTRAINT artifacts_control_producer_group_check CHECK (
                (producer_kind IS NOT NULL AND control_producer_kind IS NULL
                 AND control_producer_id IS NULL)
                OR
                (producer_kind IS NULL AND
                 ((control_producer_kind IS NULL) = (control_producer_id IS NULL)))
            )
        """,
        """
        CREATE UNIQUE INDEX artifacts_pipeline_stage_output_uidx
        ON artifacts(pipeline_stage_run_id, name)
        WHERE producer_kind IN ('container','platform')
        """,
        """
        CREATE TABLE pipeline_fanout_expansions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            node_key TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            source_stage_run_id UUID,
            source_artifact_id UUID NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
            source_manifest_digest TEXT NOT NULL,
            fanout_spec_digest TEXT NOT NULL,
            item_count INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT pipeline_fanout_source_check CHECK (
                source_kind IN ('run_input','stage_output')
            ),
            CONSTRAINT pipeline_fanout_source_stage_check CHECK (
                (source_kind = 'run_input' AND source_stage_run_id IS NULL)
                OR
                (source_kind = 'stage_output' AND source_stage_run_id IS NOT NULL)
            ),
            CONSTRAINT pipeline_fanout_item_count_range CHECK (item_count BETWEEN 0 AND 5000),
            CONSTRAINT pipeline_fanout_expansions_identity_uidx
                UNIQUE (pipeline_run_id, node_key, source_artifact_id),
            CONSTRAINT pipeline_fanout_expansions_run_id_uidx UNIQUE (pipeline_run_id, id),
            CONSTRAINT pipeline_fanout_expansions_source_run_fk
                FOREIGN KEY (pipeline_run_id, source_stage_run_id)
                REFERENCES pipeline_stage_runs(pipeline_run_id, id)
                DEFERRABLE INITIALLY DEFERRED
        )
        """,
        """
        ALTER TABLE pipeline_stage_runs
            ADD CONSTRAINT pipeline_stage_runs_fanout_expansion_fk
                FOREIGN KEY (pipeline_run_id, fanout_expansion_id)
                REFERENCES pipeline_fanout_expansions(pipeline_run_id, id)
                ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            ADD CONSTRAINT pipeline_stage_runs_latest_checkpoint_fk
                FOREIGN KEY (latest_checkpoint_artifact_id) REFERENCES artifacts(id)
                ON DELETE RESTRICT
        """,
        """
        CREATE TABLE pipeline_stage_dependencies (
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            upstream_stage_run_id UUID NOT NULL,
            downstream_stage_run_id UUID NOT NULL,
            dependency_kind TEXT NOT NULL,
            selected BOOLEAN,
            satisfied_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (upstream_stage_run_id, downstream_stage_run_id, dependency_kind),
            CONSTRAINT pipeline_stage_dependencies_kind_check CHECK (
                dependency_kind IN ('required','terminal_barrier','gate_matched','gate_unmatched',
                                    'gate_approved','gate_rejected_or_expired')
            ),
            CONSTRAINT pipeline_stage_dependencies_distinct_check CHECK (
                upstream_stage_run_id <> downstream_stage_run_id
            ),
            CONSTRAINT pipeline_stage_dependencies_upstream_run_fk
                FOREIGN KEY (pipeline_run_id, upstream_stage_run_id)
                REFERENCES pipeline_stage_runs(pipeline_run_id, id) ON DELETE CASCADE,
            CONSTRAINT pipeline_stage_dependencies_downstream_run_fk
                FOREIGN KEY (pipeline_run_id, downstream_stage_run_id)
                REFERENCES pipeline_stage_runs(pipeline_run_id, id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE pipeline_terminal_snapshots (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            consumer_stage_run_id UUID NOT NULL UNIQUE,
            renderer_digest TEXT NOT NULL,
            run_graph_digest TEXT NOT NULL,
            terminal_stage_keys_json JSONB NOT NULL,
            stages_json JSONB NOT NULL,
            snapshot_json JSONB NOT NULL,
            snapshot_bytes BYTEA NOT NULL,
            snapshot_digest TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT pipeline_terminal_snapshots_size_check CHECK (
                octet_length(snapshot_bytes) <= 16777216
            ),
            CONSTRAINT pipeline_terminal_snapshots_consumer_run_fk
                FOREIGN KEY (pipeline_run_id, consumer_stage_run_id)
                REFERENCES pipeline_stage_runs(pipeline_run_id, id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE pipeline_events (
            pipeline_run_id UUID NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            seq BIGINT NOT NULL,
            stage_run_id UUID REFERENCES pipeline_stage_runs(id) ON DELETE CASCADE,
            execution_attempt_id UUID REFERENCES execution_attempts(id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            actor_kind TEXT NOT NULL,
            actor_id TEXT,
            payload_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (pipeline_run_id, seq),
            CONSTRAINT pipeline_events_seq_positive CHECK (seq > 0)
        )
        """,
        """
        CREATE TABLE pipeline_acceptance_preflight_prerequisites (
            pipeline_run_id UUID PRIMARY KEY REFERENCES pipeline_runs(id) ON DELETE CASCADE,
            authorization_id UUID NOT NULL,
            candidate_sha256 TEXT NOT NULL,
            preflight_input_set_id TEXT NOT NULL DEFAULT 'S02',
            sealed_input_descriptor_set_sha256 TEXT NOT NULL,
            worker_id UUID REFERENCES workers(id) ON DELETE RESTRICT,
            worker_capability_snapshot_digest TEXT,
            policy_id TEXT,
            policy_config_sha256 TEXT,
            policy_activation_epoch BIGINT,
            state TEXT NOT NULL DEFAULT 'pending',
            eviction_result_json JSONB,
            eviction_result_bytes BYTEA,
            eviction_result_sha256 TEXT,
            exclusive_fence_id UUID,
            fence_state TEXT NOT NULL DEFAULT 'pending',
            consumed_attempt_id UUID UNIQUE REFERENCES execution_attempts(id) ON DELETE RESTRICT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            satisfied_at TIMESTAMPTZ,
            consumed_at TIMESTAMPTZ,
            fence_acquired_at TIMESTAMPTZ,
            fence_released_at TIMESTAMPTZ,
            fence_release_reason TEXT,
            version BIGINT NOT NULL DEFAULT 0,
            CONSTRAINT pipeline_preflight_input_set_check CHECK (preflight_input_set_id = 'S02'),
            CONSTRAINT pipeline_preflight_state_check CHECK (
                state IN ('pending','satisfied','consumed')
            ),
            CONSTRAINT pipeline_preflight_fence_state_check CHECK (
                fence_state IN ('pending','active','released')
            ),
            CONSTRAINT pipeline_preflight_activation_epoch_positive CHECK (
                policy_activation_epoch IS NULL OR policy_activation_epoch > 0
            ),
            CONSTRAINT pipeline_preflight_result_group_check CHECK (
                (eviction_result_json IS NULL AND eviction_result_bytes IS NULL
                 AND eviction_result_sha256 IS NULL)
                OR
                (eviction_result_json IS NOT NULL AND eviction_result_bytes IS NOT NULL
                 AND eviction_result_sha256 IS NOT NULL)
            ),
            CONSTRAINT pipeline_preflight_state_fields_check CHECK (
                (state = 'pending' AND eviction_result_json IS NULL
                 AND consumed_attempt_id IS NULL AND satisfied_at IS NULL AND consumed_at IS NULL)
                OR
                (state = 'satisfied' AND eviction_result_json IS NOT NULL
                 AND consumed_attempt_id IS NULL AND satisfied_at IS NOT NULL
                 AND consumed_at IS NULL)
                OR
                (state = 'consumed' AND eviction_result_json IS NOT NULL
                 AND consumed_attempt_id IS NOT NULL AND satisfied_at IS NOT NULL
                 AND consumed_at IS NOT NULL)
            ),
            CONSTRAINT pipeline_preflight_fence_fields_check CHECK (
                (fence_state = 'pending' AND worker_id IS NULL
                 AND worker_capability_snapshot_digest IS NULL AND policy_id IS NULL
                 AND policy_config_sha256 IS NULL AND policy_activation_epoch IS NULL
                 AND exclusive_fence_id IS NULL
                 AND fence_acquired_at IS NULL AND fence_released_at IS NULL
                 AND fence_release_reason IS NULL)
                OR
                (fence_state = 'active' AND worker_id IS NOT NULL
                 AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL
                 AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL
                 AND exclusive_fence_id IS NOT NULL AND fence_acquired_at IS NOT NULL
                 AND fence_released_at IS NULL AND fence_release_reason IS NULL)
                OR
                (fence_state = 'released' AND worker_id IS NOT NULL
                 AND worker_capability_snapshot_digest IS NOT NULL AND policy_id IS NOT NULL
                 AND policy_config_sha256 IS NOT NULL AND policy_activation_epoch IS NOT NULL
                 AND exclusive_fence_id IS NOT NULL
                 AND fence_acquired_at IS NOT NULL AND fence_released_at IS NOT NULL
                 AND fence_release_reason IS NOT NULL)
            ),
            CONSTRAINT pipeline_preflight_version_nonnegative CHECK (version >= 0)
        )
        """,
        """
        CREATE UNIQUE INDEX pipeline_preflight_active_worker_uidx
        ON pipeline_acceptance_preflight_prerequisites(worker_id)
        WHERE fence_state = 'active'
        """,
        """
        CREATE FUNCTION validate_pipeline_retry_linkage() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.retry_of_pipeline_run_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM pipeline_stage_runs s
                WHERE s.id = NEW.retry_from_stage_run_id
                  AND s.pipeline_run_id = NEW.retry_of_pipeline_run_id
            ) THEN
                RAISE EXCEPTION 'retry_from_stage_run_id must belong to retry_of_pipeline_run_id'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_runs_retry_linkage_trigger
        AFTER INSERT OR UPDATE OF retry_of_pipeline_run_id, retry_from_stage_run_id
        ON pipeline_runs DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_retry_linkage()
        """,
        """
        CREATE FUNCTION validate_pipeline_artifact_linkage() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.producer_kind IS NOT NULL AND NOT EXISTS (
                SELECT 1
                FROM pipeline_stage_runs s
                JOIN pipeline_runs r ON r.id = s.pipeline_run_id
                JOIN execution_attempts a
                  ON a.id = NEW.execution_attempt_id AND a.stage_run_id = s.id
                WHERE s.id = NEW.pipeline_stage_run_id
                  AND s.pipeline_run_id = NEW.pipeline_run_id
                  AND r.team_id = NEW.team_id
            ) THEN
                RAISE EXCEPTION 'Pipeline Artifact run/stage/attempt/team linkage mismatch'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.producer_kind = 'checkpoint'
               AND NEW.name !~ '^checkpoint-[0-9]{12}$' THEN
                RAISE EXCEPTION 'checkpoint Artifact name is invalid'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER artifacts_pipeline_linkage_trigger
        AFTER INSERT OR UPDATE OF pipeline_run_id, pipeline_stage_run_id,
            execution_attempt_id, producer_kind, team_id, name
        ON artifacts DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_artifact_linkage()
        """,
        """
        CREATE FUNCTION validate_pipeline_event_linkage() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.stage_run_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM pipeline_stage_runs s
                WHERE s.id = NEW.stage_run_id AND s.pipeline_run_id = NEW.pipeline_run_id
            ) THEN
                RAISE EXCEPTION 'Pipeline Event StageRun belongs to another run'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.execution_attempt_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM execution_attempts a
                JOIN pipeline_stage_runs s ON s.id = a.stage_run_id
                WHERE a.id = NEW.execution_attempt_id
                  AND s.pipeline_run_id = NEW.pipeline_run_id
                  AND (NEW.stage_run_id IS NULL OR s.id = NEW.stage_run_id)
            ) THEN
                RAISE EXCEPTION 'Pipeline Event Attempt belongs to another run or stage'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_events_linkage_trigger
        AFTER INSERT OR UPDATE OF pipeline_run_id, stage_run_id, execution_attempt_id
        ON pipeline_events DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_event_linkage()
        """,
        """
        CREATE FUNCTION validate_pipeline_preflight_linkage() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pipeline_runs r
                WHERE r.id = NEW.pipeline_run_id
                  AND r.submission_policy = 'acceptance_authorization_only'
                  AND r.acceptance_authorization_id = NEW.authorization_id
                  AND r.acceptance_candidate_sha256 = NEW.candidate_sha256
                  AND r.recipe_name = 'behavior-recovery-acceptance-preflight'
                  AND r.recipe_version = 1
            ) THEN
                RAISE EXCEPTION 'preflight prerequisite does not match its acceptance run'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.consumed_attempt_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM execution_attempts attempt
                JOIN pipeline_stage_runs stage ON stage.id = attempt.stage_run_id
                WHERE attempt.id = NEW.consumed_attempt_id
                  AND stage.pipeline_run_id = NEW.pipeline_run_id
                  AND stage.node_kind = 'container'
            ) THEN
                RAISE EXCEPTION 'consumed preflight Attempt must belong to this PipelineRun'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_preflight_linkage_trigger
        AFTER INSERT OR UPDATE OF pipeline_run_id, authorization_id, candidate_sha256,
            consumed_attempt_id
        ON pipeline_acceptance_preflight_prerequisites DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_preflight_linkage()
        """,
        """
        CREATE FUNCTION validate_pipeline_stage_linkage() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.node_kind = 'gate' AND NOT EXISTS (
                SELECT 1 FROM pipeline_stage_runs subject
                WHERE subject.id = NEW.gate_subject_stage_run_id
                  AND subject.pipeline_run_id = NEW.pipeline_run_id
                  AND subject.shard_key = NEW.shard_key
                  AND subject.node_kind = 'container'
            ) THEN
                RAISE EXCEPTION 'gate subject must be a same-run, same-shard container StageRun'
                    USING ERRCODE = '23514';
            END IF;
            IF NEW.latest_checkpoint_artifact_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM artifacts artifact
                WHERE artifact.id = NEW.latest_checkpoint_artifact_id
                  AND artifact.pipeline_run_id = NEW.pipeline_run_id
                  AND artifact.pipeline_stage_run_id = NEW.id
                  AND artifact.producer_kind = 'checkpoint'
            ) THEN
                RAISE EXCEPTION 'latest checkpoint must be committed by this StageRun'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_stage_linkage_trigger
        AFTER INSERT OR UPDATE OF node_kind, gate_subject_stage_run_id, pipeline_run_id,
            shard_key, latest_checkpoint_artifact_id
        ON pipeline_stage_runs DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_stage_linkage()
        """,
        """
        CREATE FUNCTION validate_pipeline_attempt_stage() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pipeline_stage_runs stage
                WHERE stage.id = NEW.stage_run_id AND stage.node_kind = 'container'
            ) THEN
                RAISE EXCEPTION 'ExecutionAttempt requires a container StageRun'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_attempt_stage_trigger
        AFTER INSERT OR UPDATE OF stage_run_id
        ON execution_attempts DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_attempt_stage()
        """,
        """
        CREATE FUNCTION validate_pipeline_fanout_source() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM artifacts artifact
                JOIN pipeline_runs run ON run.id = NEW.pipeline_run_id
                WHERE artifact.id = NEW.source_artifact_id
                  AND artifact.team_id = run.team_id
                  AND (
                    NEW.source_kind = 'run_input'
                    OR (
                      artifact.pipeline_run_id = NEW.pipeline_run_id
                      AND artifact.pipeline_stage_run_id = NEW.source_stage_run_id
                      AND artifact.producer_kind = 'platform'
                    )
                  )
            ) THEN
                RAISE EXCEPTION 'fanout source Artifact does not match its run/stage provenance'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_fanout_source_trigger
        AFTER INSERT OR UPDATE OF pipeline_run_id, source_kind, source_stage_run_id,
            source_artifact_id
        ON pipeline_fanout_expansions DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_fanout_source()
        """,
        """
        CREATE FUNCTION validate_pipeline_preflight_cardinality() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            target_run_id UUID;
            target_policy TEXT;
            prerequisite_count BIGINT;
        BEGIN
            IF TG_TABLE_NAME = 'pipeline_runs' THEN
                target_run_id := NEW.id;
            ELSIF TG_OP = 'DELETE' THEN
                target_run_id := OLD.pipeline_run_id;
            ELSE
                target_run_id := NEW.pipeline_run_id;
            END IF;
            SELECT submission_policy INTO target_policy FROM pipeline_runs WHERE id = target_run_id;
            IF target_policy IS NULL THEN
                RETURN NULL;
            END IF;
            SELECT count(*) INTO prerequisite_count
            FROM pipeline_acceptance_preflight_prerequisites
            WHERE pipeline_run_id = target_run_id;
            IF (target_policy = 'acceptance_authorization_only' AND prerequisite_count != 1)
               OR (target_policy = 'ordinary' AND prerequisite_count != 0) THEN
                RAISE EXCEPTION 'PipelineRun prerequisite cardinality does not match submission policy'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
        END;
        $$
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_runs_preflight_cardinality_trigger
        AFTER INSERT OR UPDATE OF submission_policy
        ON pipeline_runs DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_preflight_cardinality()
        """,
        """
        CREATE CONSTRAINT TRIGGER pipeline_preflight_cardinality_trigger
        AFTER INSERT OR UPDATE OR DELETE
        ON pipeline_acceptance_preflight_prerequisites DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION validate_pipeline_preflight_cardinality()
        """,
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    statements = (
        "DROP TRIGGER IF EXISTS pipeline_preflight_cardinality_trigger "
        "ON pipeline_acceptance_preflight_prerequisites",
        "DROP TRIGGER IF EXISTS pipeline_runs_preflight_cardinality_trigger ON pipeline_runs",
        "DROP FUNCTION IF EXISTS validate_pipeline_preflight_cardinality()",
        "DROP TRIGGER IF EXISTS pipeline_fanout_source_trigger ON pipeline_fanout_expansions",
        "DROP FUNCTION IF EXISTS validate_pipeline_fanout_source()",
        "DROP TRIGGER IF EXISTS pipeline_attempt_stage_trigger ON execution_attempts",
        "DROP FUNCTION IF EXISTS validate_pipeline_attempt_stage()",
        "DROP TRIGGER IF EXISTS pipeline_stage_linkage_trigger ON pipeline_stage_runs",
        "DROP FUNCTION IF EXISTS validate_pipeline_stage_linkage()",
        "DROP TRIGGER IF EXISTS pipeline_preflight_linkage_trigger "
        "ON pipeline_acceptance_preflight_prerequisites",
        "DROP FUNCTION IF EXISTS validate_pipeline_preflight_linkage()",
        "DROP TRIGGER IF EXISTS pipeline_events_linkage_trigger ON pipeline_events",
        "DROP FUNCTION IF EXISTS validate_pipeline_event_linkage()",
        "DROP TRIGGER IF EXISTS artifacts_pipeline_linkage_trigger ON artifacts",
        "DROP FUNCTION IF EXISTS validate_pipeline_artifact_linkage()",
        "DROP TRIGGER IF EXISTS pipeline_runs_retry_linkage_trigger ON pipeline_runs",
        "DROP FUNCTION IF EXISTS validate_pipeline_retry_linkage()",
        "DROP TABLE pipeline_acceptance_preflight_prerequisites",
        "DROP TABLE pipeline_events",
        "DROP TABLE pipeline_terminal_snapshots",
        "DROP TABLE pipeline_stage_dependencies",
        "ALTER TABLE pipeline_stage_runs DROP CONSTRAINT pipeline_stage_runs_fanout_expansion_fk",
        "ALTER TABLE pipeline_stage_runs DROP CONSTRAINT pipeline_stage_runs_latest_checkpoint_fk",
        "DROP TABLE pipeline_fanout_expansions",
        "DROP INDEX artifacts_pipeline_stage_output_uidx",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_control_producer_group_check",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_identity_group_check",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_producer_kind_check",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_execution_attempt_fk",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_stage_run_fk",
        "ALTER TABLE artifacts DROP CONSTRAINT artifacts_pipeline_run_fk",
        "ALTER TABLE artifacts DROP COLUMN control_producer_id",
        "ALTER TABLE artifacts DROP COLUMN control_producer_kind",
        "ALTER TABLE artifacts DROP COLUMN producer_kind",
        "ALTER TABLE artifacts DROP COLUMN execution_attempt_id",
        "ALTER TABLE artifacts DROP COLUMN pipeline_stage_run_id",
        "ALTER TABLE artifacts DROP COLUMN pipeline_run_id",
        "DROP TABLE execution_attempts",
        "ALTER TABLE pipeline_runs DROP CONSTRAINT pipeline_runs_retry_from_stage_fk",
        "DROP TABLE pipeline_stage_runs",
        "DROP TABLE pipeline_runs",
    )
    for statement in statements:
        op.execute(statement)
