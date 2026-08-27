"""Persist versioned hybrid execution routing decisions.

Revision ID: 0116
Revises: 0115
"""

import hashlib
import json
from datetime import UTC, datetime

import rfc8785
from alembic import op
from sqlalchemy import text

revision = "0116"
down_revision = "0115"
branch_labels = None
depends_on = None


def _canonical_digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(rfc8785.dumps(value) + b"\n").hexdigest()


def _json_timestamp(value: object) -> object:
    if not isinstance(value, (str, datetime)):
        return value
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_routing_digests() -> None:
    connection = op.get_bind()
    trials = connection.execute(
        text(
            "SELECT id, requires_caps, execution_route_json FROM trials "
            "WHERE execution_route_json IS NOT NULL"
        )
    ).mappings()
    for row in trials:
        decision = dict(row["execution_route_json"])
        decision["decided_at"] = _json_timestamp(decision["decided_at"])
        decision["candidates"] = [dict(item) for item in decision["candidates"]]
        for candidate in decision["candidates"]:
            candidate["capacity_observed_at"] = _json_timestamp(
                candidate.get("capacity_observed_at")
            )
        if decision["selected_adapter_kind"] == "legacy_worker_claim":
            decision["requirements_sha256"] = _canonical_digest(row["requires_caps"])
        connection.execute(
            text(
                "UPDATE trials SET execution_route_json=CAST(:decision AS jsonb), "
                "execution_route_sha256=:digest WHERE id=:id"
            ),
            {
                "id": row["id"],
                "decision": json.dumps(decision, separators=(",", ":"), sort_keys=True),
                "digest": _canonical_digest(decision),
            },
        )

    leases = connection.execute(
        text(
            "SELECT lease.id, lease.target_id, lease.execution_class_id, "
            "lease.workload_requirements_sha256, lease.created_at, "
            "target.logical_pool_id, target.environment, target.region, "
            "target.data_residency FROM execution_leases lease "
            "JOIN execution_targets target ON target.id=lease.target_id"
        )
    ).mappings()
    for row in leases:
        decision = {
            "schema_version": "loom.execution-routing-decision.v1",
            "generation": 1,
            "requirements_sha256": row["workload_requirements_sha256"],
            "selected_pool_id": row["logical_pool_id"],
            "selected_adapter_kind": "kubernetes_job",
            "selected_target_id": row["target_id"],
            "selected_execution_class_id": row["execution_class_id"],
            "reason": "preexisting_assignment",
            "decided_at": _json_timestamp(row["created_at"]),
            "candidates": [
                {
                    "logical_pool_id": row["logical_pool_id"],
                    "adapter_kind": "kubernetes_job",
                    "target_id": row["target_id"],
                    "execution_class_id": row["execution_class_id"],
                    "environment": row["environment"],
                    "region": row["region"],
                    "data_residency": row["data_residency"],
                    "operator_weight": 0,
                    "budget_eligible": True,
                    "estimated_cost_microusd_per_slot_hour": None,
                    "enabled": True,
                    "healthy": True,
                    "draining": False,
                    "configured_slots": 0,
                    "active_slots": 0,
                    "occupied_slots": 0,
                    "pending_slots": 0,
                    "assigned_queued_slots": 0,
                    "available_slots": 0,
                    "capacity_evidence_kind": "preexisting_assignment",
                    "capacity_observed_at": None,
                    "blockers": [],
                }
            ],
        }
        connection.execute(
            text("UPDATE execution_leases SET routing_decision_sha256=:digest WHERE id=:id"),
            {"id": row["id"], "digest": _canonical_digest(decision)},
        )


def upgrade() -> None:
    op.execute(
        r"""
        ALTER TABLE trials
          ADD COLUMN execution_route_generation BIGINT NOT NULL DEFAULT 0,
          ADD COLUMN execution_route_pool_name TEXT,
          ADD COLUMN execution_route_json JSONB,
          ADD COLUMN execution_route_sha256 TEXT;

        WITH decisions AS (
          SELECT id,
                 jsonb_build_object(
                   'schema_version', 'loom.execution-routing-decision.v1',
                   'generation', 1,
                   'requirements_sha256',
                     'sha256:' || encode(sha256(convert_to(requires_caps::text, 'UTF8')), 'hex'),
                   'selected_pool_id', autoscaler_pool_name,
                   'selected_adapter_kind', 'legacy_worker_claim',
                   'selected_target_id', NULL,
                   'selected_execution_class_id', NULL,
                   'reason', 'preexisting_assignment',
                   'decided_at', autoscaler_pool_assigned_at,
                   'candidates', jsonb_build_array(jsonb_build_object(
                     'logical_pool_id', autoscaler_pool_name,
                     'adapter_kind', 'legacy_worker_claim',
                     'target_id', NULL,
                     'execution_class_id', NULL,
                     'environment', NULL,
                     'region', NULL,
                     'data_residency', NULL,
                     'operator_weight', 0,
                     'budget_eligible', true,
                     'estimated_cost_microusd_per_slot_hour', NULL,
                     'enabled', true,
                     'healthy', true,
                     'draining', false,
                     'configured_slots', 0,
                     'active_slots', 0,
                     'occupied_slots', 0,
                     'pending_slots', 0,
                     'assigned_queued_slots', 0,
                     'available_slots', 0,
                     'capacity_evidence_kind', 'preexisting_assignment',
                     'capacity_observed_at', NULL,
                     'blockers', jsonb_build_array()
                   ))
                 ) AS decision
            FROM trials
           WHERE autoscaler_pool_name IS NOT NULL
        )
        UPDATE trials AS trial
           SET execution_route_generation = 1,
               execution_route_pool_name = trial.autoscaler_pool_name,
               execution_route_json = decisions.decision,
               execution_route_sha256 = 'sha256:' || encode(
                 sha256(convert_to(decisions.decision::text, 'UTF8')), 'hex'
               )
          FROM decisions
         WHERE decisions.id = trial.id;

        WITH service_routes AS (
          SELECT DISTINCT ON (lease.trial_id)
                 lease.trial_id,
                 target.logical_pool_id,
                 lease.target_id,
                 lease.execution_class_id,
                 lease.workload_requirements_sha256,
                 lease.created_at,
                 jsonb_build_object(
                   'schema_version', 'loom.execution-routing-decision.v1',
                   'generation', 1,
                   'requirements_sha256', lease.workload_requirements_sha256,
                   'selected_pool_id', target.logical_pool_id,
                   'selected_adapter_kind', 'kubernetes_job',
                   'selected_target_id', lease.target_id,
                   'selected_execution_class_id', lease.execution_class_id,
                   'reason', 'preexisting_assignment',
                   'decided_at', lease.created_at,
                   'candidates', jsonb_build_array(jsonb_build_object(
                     'logical_pool_id', target.logical_pool_id,
                     'adapter_kind', 'kubernetes_job',
                     'target_id', lease.target_id,
                     'execution_class_id', lease.execution_class_id,
                     'environment', target.environment,
                     'region', target.region,
                     'data_residency', target.data_residency,
                     'operator_weight', 0,
                     'budget_eligible', true,
                     'estimated_cost_microusd_per_slot_hour', NULL,
                     'enabled', true,
                     'healthy', true,
                     'draining', false,
                     'configured_slots', 0,
                     'active_slots', 0,
                     'occupied_slots', 0,
                     'pending_slots', 0,
                     'assigned_queued_slots', 0,
                     'available_slots', 0,
                     'capacity_evidence_kind', 'preexisting_assignment',
                     'capacity_observed_at', NULL,
                     'blockers', jsonb_build_array()
                   ))
                 ) AS decision
            FROM execution_leases lease
            JOIN execution_targets target ON target.id = lease.target_id
           WHERE lease.execution_role = 'attempt'
             AND lease.revoked_at IS NULL
           ORDER BY lease.trial_id, lease.attempt DESC
        )
        UPDATE trials AS trial
           SET autoscaler_pool_name = NULL,
               autoscaler_pool_assigned_at = NULL,
               execution_route_generation = 1,
               execution_route_pool_name = service_routes.logical_pool_id,
               execution_route_json = service_routes.decision,
               execution_route_sha256 = 'sha256:' || encode(
                 sha256(convert_to(service_routes.decision::text, 'UTF8')), 'hex'
               )
         FROM service_routes
         WHERE service_routes.trial_id = trial.id;

        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
              FROM execution_leases lease
              JOIN trials trial ON trial.id = lease.trial_id
             WHERE lease.execution_role = 'attempt'
               AND lease.revoked_at IS NULL
               AND trial.worker_id IS NOT NULL
          ) THEN
            RAISE EXCEPTION 'existing execution lease conflicts with worker claim';
          END IF;
        END $$;

        ALTER TABLE execution_leases
          ADD COLUMN routing_generation BIGINT,
          ADD COLUMN selected_pool_id TEXT,
          ADD COLUMN routing_reason TEXT,
          ADD COLUMN routing_decision_sha256 TEXT;
        WITH lease_routes AS (
          SELECT lease.id,
                 target.logical_pool_id,
                 jsonb_build_object(
                   'schema_version', 'loom.execution-routing-decision.v1',
                   'generation', 1,
                   'requirements_sha256', lease.workload_requirements_sha256,
                   'selected_pool_id', target.logical_pool_id,
                   'selected_adapter_kind', 'kubernetes_job',
                   'selected_target_id', lease.target_id,
                   'selected_execution_class_id', lease.execution_class_id,
                   'reason', 'preexisting_assignment',
                   'decided_at', lease.created_at,
                   'candidates', jsonb_build_array(jsonb_build_object(
                     'logical_pool_id', target.logical_pool_id,
                     'adapter_kind', 'kubernetes_job',
                     'target_id', lease.target_id,
                     'execution_class_id', lease.execution_class_id,
                     'environment', target.environment,
                     'region', target.region,
                     'data_residency', target.data_residency,
                     'operator_weight', 0,
                     'budget_eligible', true,
                     'estimated_cost_microusd_per_slot_hour', NULL,
                     'enabled', true,
                     'healthy', true,
                     'draining', false,
                     'configured_slots', 0,
                     'active_slots', 0,
                     'occupied_slots', 0,
                     'pending_slots', 0,
                     'assigned_queued_slots', 0,
                     'available_slots', 0,
                     'capacity_evidence_kind', 'preexisting_assignment',
                     'capacity_observed_at', NULL,
                     'blockers', jsonb_build_array()
                   ))
                 ) AS decision
            FROM execution_leases lease
            JOIN execution_targets target ON target.id = lease.target_id
        )
        UPDATE execution_leases lease
           SET routing_generation = 1,
               selected_pool_id = lease_routes.logical_pool_id,
               routing_reason = 'preexisting_assignment',
               routing_decision_sha256 = 'sha256:' || encode(
                 sha256(convert_to(lease_routes.decision::text, 'UTF8')), 'hex'
               )
          FROM lease_routes
         WHERE lease_routes.id = lease.id;
        WITH snapshots AS (
          SELECT history.id,
                 history.snapshot_json || jsonb_build_object(
                   'routing_generation', lease.routing_generation,
                   'selected_pool_id', lease.selected_pool_id,
                   'routing_reason', lease.routing_reason,
                   'routing_decision_sha256', lease.routing_decision_sha256
                 ) AS snapshot
            FROM execution_lease_history history
            JOIN execution_leases lease ON lease.id = history.lease_id
        )
        UPDATE execution_lease_history history
           SET snapshot_json = snapshots.snapshot,
               snapshot_sha256 = 'sha256:' || encode(
                 sha256(convert_to(snapshots.snapshot::text, 'UTF8')), 'hex'
               )
          FROM snapshots
         WHERE snapshots.id = history.id;
        ALTER TABLE execution_leases
          ALTER COLUMN routing_generation SET NOT NULL,
          ALTER COLUMN selected_pool_id SET NOT NULL,
          ALTER COLUMN routing_reason SET NOT NULL,
          ALTER COLUMN routing_decision_sha256 SET NOT NULL,
          ADD CONSTRAINT execution_leases_routing_identity_check CHECK (
            routing_generation > 0 AND length(selected_pool_id) BETWEEN 1 AND 80
            AND routing_reason IN (
              'fresh_executable_capacity','configured_scale_headroom','operator_pin',
              'preexisting_assignment','admin_target_binding'
            )
            AND routing_decision_sha256 ~ '^sha256:[0-9a-f]{64}$'
          );

        ALTER TABLE trials
          ADD CONSTRAINT trials_execution_route_generation_check CHECK (
            execution_route_generation >= 0
          ),
          ADD CONSTRAINT trials_execution_route_group_check CHECK (
            (execution_route_pool_name IS NULL AND execution_route_json IS NULL
             AND execution_route_sha256 IS NULL) OR
            (length(trim(execution_route_pool_name)) BETWEEN 1 AND 80
             AND execution_route_generation > 0
             AND execution_route_json->>'schema_version' =
               'loom.execution-routing-decision.v1'
             AND execution_route_json->>'selected_pool_id' = execution_route_pool_name
             AND execution_route_sha256 ~ '^sha256:[0-9a-f]{64}$')
          ),
          ADD CONSTRAINT trials_autoscaler_route_pool_check CHECK (
            autoscaler_pool_name IS NULL OR
            execution_route_pool_name = autoscaler_pool_name
          );
        CREATE INDEX trials_queued_execution_route_idx
          ON trials (execution_route_pool_name, submitted_at)
          WHERE state = 'queued';
        """
    )
    _normalize_routing_digests()
    op.execute(
        r"""
        WITH snapshots AS (
          SELECT history.id,
                 history.snapshot_json || jsonb_build_object(
                   'routing_generation', lease.routing_generation,
                   'selected_pool_id', lease.selected_pool_id,
                   'routing_reason', lease.routing_reason,
                   'routing_decision_sha256', lease.routing_decision_sha256
                 ) AS snapshot
            FROM execution_lease_history history
            JOIN execution_leases lease ON lease.id = history.lease_id
        )
        UPDATE execution_lease_history history
           SET snapshot_json = snapshots.snapshot,
               snapshot_sha256 = 'sha256:' || encode(
                 sha256(convert_to(snapshots.snapshot::text, 'UTF8')), 'hex'
               )
          FROM snapshots
         WHERE snapshots.id = history.id;
        """
    )
    op.execute(
        r"""
        CREATE OR REPLACE FUNCTION validate_execution_lease_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.request_id, NEW.trial_id, NEW.team_id, NEW.attempt,
                 NEW.execution_role, NEW.parent_lease_id,
                 NEW.resource_generation,
                 NEW.execution_class_id, NEW.workload_requirements_json,
                 NEW.workload_requirements_sha256, NEW.runtime_contract_json,
                 NEW.runtime_contract_sha256, NEW.routing_generation,
                 NEW.selected_pool_id, NEW.routing_reason,
                 NEW.routing_decision_sha256, NEW.provider_scope_key,
                 NEW.namespace_name, NEW.job_name, NEW.execution_unit_key,
                 NEW.created_at)
             IS DISTINCT FROM
             ROW(OLD.request_id, OLD.trial_id, OLD.team_id, OLD.attempt,
                 OLD.execution_role, OLD.parent_lease_id,
                 OLD.resource_generation,
                 OLD.execution_class_id, OLD.workload_requirements_json,
                 OLD.workload_requirements_sha256, OLD.runtime_contract_json,
                 OLD.runtime_contract_sha256, OLD.routing_generation,
                 OLD.selected_pool_id, OLD.routing_reason,
                 OLD.routing_decision_sha256, OLD.provider_scope_key,
                 OLD.namespace_name, OLD.job_name, OLD.execution_unit_key,
                 OLD.created_at) THEN
            RAISE EXCEPTION 'execution lease immutable identity changed';
          END IF;
          IF NEW.generation < OLD.generation OR NEW.generation > OLD.generation + 1 THEN
            RAISE EXCEPTION 'execution lease generation must advance monotonically by one';
          END IF;
          IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS NULL THEN
            RAISE EXCEPTION 'revoked execution generation cannot regain authority';
          END IF;
          IF OLD.deleted_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'deleted execution lease is immutable';
          END IF;
          RETURN NEW;
        END $$;

        CREATE FUNCTION validate_trial_execution_route_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.execution_route_generation > NEW.execution_route_generation
             OR NEW.execution_route_generation > OLD.execution_route_generation + 1 THEN
            RAISE EXCEPTION 'trial execution route generation must advance by at most one';
          END IF;
          IF ROW(NEW.execution_route_pool_name, NEW.execution_route_json,
                 NEW.execution_route_sha256)
             IS DISTINCT FROM
             ROW(OLD.execution_route_pool_name, OLD.execution_route_json,
                 OLD.execution_route_sha256) THEN
            IF OLD.state <> 'queued' THEN
              RAISE EXCEPTION 'trial execution route can change only while queued';
            END IF;
            IF NEW.execution_route_json IS NOT NULL
               AND NEW.execution_route_generation <> OLD.execution_route_generation + 1 THEN
              RAISE EXCEPTION 'new trial execution route requires the next generation';
            END IF;
          ELSIF NEW.execution_route_generation <> OLD.execution_route_generation THEN
            RAISE EXCEPTION 'trial execution route generation changed without a new decision';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER trials_execution_route_mutation_trigger
          BEFORE UPDATE OF state, execution_route_generation,
            execution_route_pool_name, execution_route_json, execution_route_sha256
          ON trials
          FOR EACH ROW EXECUTE FUNCTION validate_trial_execution_route_mutation();

        CREATE OR REPLACE FUNCTION append_execution_lease_history() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE next_ordinal BIGINT;
        DECLARE snapshot JSONB;
        DECLARE digest TEXT;
        BEGIN
          SELECT COALESCE(MAX(transition_ordinal), 0) + 1 INTO next_ordinal
            FROM execution_lease_history WHERE lease_id = NEW.id;
          snapshot := jsonb_build_object(
            'generation', NEW.generation,
            'desired_state', NEW.desired_state,
            'observed_state', NEW.observed_state,
            'cleanup_state', NEW.cleanup_state,
            'cleanup_requested_at', NEW.cleanup_requested_at,
            'cleanup_deadline_at', NEW.cleanup_deadline_at,
            'last_event_ordinal', NEW.last_event_ordinal,
            'target_id', NEW.target_id,
            'routing_generation', NEW.routing_generation,
            'selected_pool_id', NEW.selected_pool_id,
            'routing_reason', NEW.routing_reason,
            'routing_decision_sha256', NEW.routing_decision_sha256,
            'execution_role', NEW.execution_role,
            'parent_lease_id', NEW.parent_lease_id,
            'runtime_contract_sha256', NEW.runtime_contract_sha256,
            'candidate_sha', NEW.runtime_contract_json->>'candidate_sha',
            'task_revision_sha256', NEW.runtime_contract_json->>'task_revision_sha256',
            'task_image_ref', NEW.runtime_contract_json->>'task_image_ref',
            'runtime_image_ref', NEW.runtime_contract_json->>'runtime_image_ref',
            'command_identity_sha256', NEW.runtime_contract_json->>'command_identity_sha256',
            'job_uid', NEW.job_uid,
            'pod_uid', NEW.pod_uid,
            'pod_ip', NEW.pod_ip,
            'kubernetes_resource_version', NEW.kubernetes_resource_version,
            'output_commit_state', NEW.output_commit_state,
            'output_upload_session_id', NEW.output_upload_session_id,
            'output_generation', NEW.output_generation,
            'output_manifest_sha256', NEW.output_manifest_sha256,
            'output_marker_sha256', NEW.output_marker_sha256,
            'output_committed_at', NEW.output_committed_at,
            'output_unavailable_reason', NEW.output_unavailable_reason,
            'revoked_at', NEW.revoked_at,
            'finalized_at', NEW.finalized_at,
            'deleted_at', NEW.deleted_at,
            'error_class', NEW.error_class,
            'error_code', NEW.error_code
          );
          digest := 'sha256:' || encode(sha256(convert_to(snapshot::text, 'UTF8')), 'hex');
          INSERT INTO execution_lease_history (
            lease_id, transition_ordinal, generation, desired_state,
            observed_state, cleanup_state, snapshot_json, snapshot_sha256
          ) VALUES (
            NEW.id, next_ordinal, NEW.generation, NEW.desired_state,
            NEW.observed_state, NEW.cleanup_state, snapshot, digest
          );
          RETURN NULL;
        END $$;
        """
    )


def downgrade() -> None:
    op.execute(
        r"""
        DROP TRIGGER trials_execution_route_mutation_trigger ON trials;
        DROP FUNCTION validate_trial_execution_route_mutation();

        CREATE OR REPLACE FUNCTION validate_execution_lease_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF ROW(NEW.request_id, NEW.trial_id, NEW.team_id, NEW.attempt,
                 NEW.execution_role, NEW.parent_lease_id,
                 NEW.resource_generation,
                 NEW.execution_class_id, NEW.workload_requirements_json,
                 NEW.workload_requirements_sha256, NEW.runtime_contract_json,
                 NEW.runtime_contract_sha256, NEW.provider_scope_key,
                 NEW.namespace_name, NEW.job_name, NEW.execution_unit_key,
                 NEW.created_at)
             IS DISTINCT FROM
             ROW(OLD.request_id, OLD.trial_id, OLD.team_id, OLD.attempt,
                 OLD.execution_role, OLD.parent_lease_id,
                 OLD.resource_generation,
                 OLD.execution_class_id, OLD.workload_requirements_json,
                 OLD.workload_requirements_sha256, OLD.runtime_contract_json,
                 OLD.runtime_contract_sha256, OLD.provider_scope_key,
                 OLD.namespace_name, OLD.job_name, OLD.execution_unit_key,
                 OLD.created_at) THEN
            RAISE EXCEPTION 'execution lease immutable identity changed';
          END IF;
          IF NEW.generation < OLD.generation OR NEW.generation > OLD.generation + 1 THEN
            RAISE EXCEPTION 'execution lease generation must advance monotonically by one';
          END IF;
          IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS NULL THEN
            RAISE EXCEPTION 'revoked execution generation cannot regain authority';
          END IF;
          IF OLD.deleted_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'deleted execution lease is immutable';
          END IF;
          RETURN NEW;
        END $$;

        CREATE OR REPLACE FUNCTION append_execution_lease_history() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE next_ordinal BIGINT;
        DECLARE snapshot JSONB;
        DECLARE digest TEXT;
        BEGIN
          SELECT COALESCE(MAX(transition_ordinal), 0) + 1 INTO next_ordinal
            FROM execution_lease_history WHERE lease_id = NEW.id;
          snapshot := jsonb_build_object(
            'generation', NEW.generation,
            'desired_state', NEW.desired_state,
            'observed_state', NEW.observed_state,
            'cleanup_state', NEW.cleanup_state,
            'cleanup_requested_at', NEW.cleanup_requested_at,
            'cleanup_deadline_at', NEW.cleanup_deadline_at,
            'last_event_ordinal', NEW.last_event_ordinal,
            'target_id', NEW.target_id,
            'execution_role', NEW.execution_role,
            'parent_lease_id', NEW.parent_lease_id,
            'runtime_contract_sha256', NEW.runtime_contract_sha256,
            'candidate_sha', NEW.runtime_contract_json->>'candidate_sha',
            'task_revision_sha256', NEW.runtime_contract_json->>'task_revision_sha256',
            'task_image_ref', NEW.runtime_contract_json->>'task_image_ref',
            'runtime_image_ref', NEW.runtime_contract_json->>'runtime_image_ref',
            'command_identity_sha256', NEW.runtime_contract_json->>'command_identity_sha256',
            'job_uid', NEW.job_uid,
            'pod_uid', NEW.pod_uid,
            'pod_ip', NEW.pod_ip,
            'kubernetes_resource_version', NEW.kubernetes_resource_version,
            'output_commit_state', NEW.output_commit_state,
            'output_upload_session_id', NEW.output_upload_session_id,
            'output_generation', NEW.output_generation,
            'output_manifest_sha256', NEW.output_manifest_sha256,
            'output_marker_sha256', NEW.output_marker_sha256,
            'output_committed_at', NEW.output_committed_at,
            'output_unavailable_reason', NEW.output_unavailable_reason,
            'revoked_at', NEW.revoked_at,
            'finalized_at', NEW.finalized_at,
            'deleted_at', NEW.deleted_at,
            'error_class', NEW.error_class,
            'error_code', NEW.error_code
          );
          digest := 'sha256:' || encode(sha256(convert_to(snapshot::text, 'UTF8')), 'hex');
          INSERT INTO execution_lease_history (
            lease_id, transition_ordinal, generation, desired_state,
            observed_state, cleanup_state, snapshot_json, snapshot_sha256
          ) VALUES (
            NEW.id, next_ordinal, NEW.generation, NEW.desired_state,
            NEW.observed_state, NEW.cleanup_state, snapshot, digest
          );
          RETURN NULL;
        END $$;
        """
    )
    op.execute(
        r"""
        WITH snapshots AS (
          SELECT id,
                 snapshot_json - 'routing_generation' - 'selected_pool_id'
                   - 'routing_reason' - 'routing_decision_sha256' AS snapshot
            FROM execution_lease_history
        )
        UPDATE execution_lease_history history
           SET snapshot_json = snapshots.snapshot,
               snapshot_sha256 = 'sha256:' || encode(
                 sha256(convert_to(snapshots.snapshot::text, 'UTF8')), 'hex'
               )
          FROM snapshots
         WHERE snapshots.id = history.id;
        DROP INDEX trials_queued_execution_route_idx;
        ALTER TABLE execution_leases
          DROP CONSTRAINT execution_leases_routing_identity_check,
          DROP COLUMN routing_decision_sha256,
          DROP COLUMN routing_reason,
          DROP COLUMN selected_pool_id,
          DROP COLUMN routing_generation;
        ALTER TABLE trials
          DROP CONSTRAINT trials_autoscaler_route_pool_check,
          DROP CONSTRAINT trials_execution_route_group_check,
          DROP CONSTRAINT trials_execution_route_generation_check,
          DROP COLUMN execution_route_sha256,
          DROP COLUMN execution_route_json,
          DROP COLUMN execution_route_pool_name,
          DROP COLUMN execution_route_generation;
        """
    )
