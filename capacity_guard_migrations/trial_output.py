"""Atomic protected output publication, installed by unreleased guard_0034."""

from __future__ import annotations

from collections.abc import Callable

from alembic import op

_FUNCTION = "loom_capacity_guard.publish_staging_trial_output(uuid,text,jsonb)"
_ISSUER = "loom_capacity_guard.authorize_frozen_trial_update(uuid,uuid,bigint,uuid,uuid,uuid,text,jsonb)"
_VALIDATOR = "loom_capacity_guard.current_protected_runtime_registration()"
_OLD_ALLOWED = "'loom_capacity_guard.report_staging_trial_state(uuid,text,jsonb)'::regprocedure::oid\n          ];"
_NEW_ALLOWED = "'loom_capacity_guard.report_staging_trial_state(uuid,text,jsonb)'::regprocedure::oid,\n            'loom_capacity_guard.publish_staging_trial_output(uuid,text,jsonb)'::regprocedure::oid\n          ];"
_OLD = """          ELSE
            RAISE EXCEPTION 'frozen retry operation is unavailable' USING ERRCODE = '55000';"""
_NEW = """          ELSIF p_operation = 'output' THEN
            IF v_old->>'state' NOT IN ('claimed','running','materializing')
               OR v_old->>'worker_id' IS DISTINCT FROM p_worker::text
               OR jsonb_typeof(p_changes) IS DISTINCT FROM 'object'
               OR NOT (p_changes ?& ARRAY['trajectory_index','result'])
               OR p_changes - ARRAY['trajectory_index','result'] <> '{}'::jsonb
               OR jsonb_typeof(p_changes->'trajectory_index') IS DISTINCT FROM 'object' THEN
              RAISE EXCEPTION 'frozen output row transition changed' USING ERRCODE = '55000';
            END IF;
          ELSE
            RAISE EXCEPTION 'frozen retry operation is unavailable' USING ERRCODE = '55000';"""


def install_output_reporting(rewrite: Callable[..., None]) -> None:
    rewrite(_ISSUER, [(_OLD, _NEW)], upgrading=True)
    op.execute("""
        CREATE FUNCTION loom_capacity_guard.publish_staging_trial_output(
          p_worker_id uuid, p_worker_credential text, p_report jsonb
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_session jsonb;
          v_current record;
          v_lease record;
          v_changes jsonb;
          v_permit uuid;
          v_expected jsonb;
          v_parent record;
          v_descriptor jsonb;
          v_artifact record;
          v_artifact_id uuid;
          v_created_at timestamptz;
          v_authority_id uuid;
          v_before jsonb;
          v_after jsonb;
          v_storage jsonb;
          v_object record;
          v_edge jsonb;
          v_synced uuid[] := ARRAY[]::uuid[];
        BEGIN
          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'protected output requires SERIALIZABLE' USING ERRCODE = '25000';
          END IF;
          IF jsonb_typeof(p_report) IS DISTINCT FROM 'object'
             OR p_report - ARRAY['trial_id','index','result','execution_lease_id',
                                 'execution_generation','expected','artifacts','lineage','scope'] <> '{}'::jsonb
             OR NOT (p_report ?& ARRAY['trial_id','index','result','execution_lease_id',
                                      'execution_generation','expected','artifacts','lineage','scope'])
             OR jsonb_typeof(p_report->'index') IS DISTINCT FROM 'object'
             OR jsonb_typeof(p_report->'artifacts') IS DISTINCT FROM 'array'
             OR jsonb_typeof(p_report->'lineage') IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'protected output request is malformed' USING ERRCODE = '22023';
          END IF;
          -- Serialize before shared authentication locks can require an upgrade.
          PERFORM 1 FROM loom_capacity_guard.agent_runtime_authority
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          v_session := loom_capacity_guard.assert_staging_worker_session(p_worker_id, p_worker_credential);
          SELECT trial.id AS trial_id, trial.state, trial.result, trial.config, trial.family_key, trial.batch_id, trial.team_id,
                 trial.attempt_count, trial.failure_message, trial.started_at, trial.finished_at,
                 runtime.protected_attempt_id, attempt.execution_generation,
                 claim.operation_id AS claim_operation_id
            INTO v_current
            FROM loom_capacity_guard.protected_runtime_trial_submissions AS runtime
            JOIN loom_capacity_guard.protected_runtime_trial_readiness AS readiness
              ON readiness.trial_id = runtime.trial_id AND readiness.protected_attempt_id = runtime.protected_attempt_id
            JOIN loom_capacity_guard.atomic_trial_submissions AS submission ON submission.trial_id = runtime.trial_id
            JOIN loom_capacity_guard.trial_attempts AS attempt ON attempt.protected_attempt_id = runtime.protected_attempt_id
            JOIN loom_capacity_guard.attempt_lifecycle_heads AS head ON head.protected_attempt_id = attempt.protected_attempt_id
            JOIN loom_capacity_guard.attempt_lifecycle_events AS assignment
              ON assignment.transition_id = head.transition_id AND assignment.protected_attempt_id = head.protected_attempt_id
            JOIN loom_capacity_guard.executable_claim_leases AS claim
              ON claim.protected_attempt_id = attempt.protected_attempt_id
             AND claim.execution_generation = attempt.execution_generation AND claim.requirements_digest = attempt.requirements_digest
            JOIN loom_capacity_guard.executable_claim_state AS claim_state ON claim_state.intent_id = claim.intent_id
            JOIN public.trials AS trial ON trial.id = runtime.trial_id
           WHERE trial.id = (p_report->>'trial_id')::uuid
             AND runtime.public_attempt_count + 1 = trial.attempt_count
             AND runtime.not_before IS NOT DISTINCT FROM trial.next_attempt_at
             AND trial.state IN ('claimed','running','materializing') AND trial.worker_id = p_worker_id
             AND attempt.claim_state = 'queued' AND head.lifecycle_state = 'assigned' AND NOT head.executable
             AND assignment.operation = 'assign' AND assignment.previous_state = 'pending-unassigned'
             AND assignment.lifecycle_state = 'assigned' AND assignment.transition_sequence = head.transition_sequence
             AND assignment.execution_generation = attempt.execution_generation
             AND assignment.requirements_digest = attempt.requirements_digest AND NOT assignment.executable
             AND assignment.submission_intent_id = (v_session->>'intent_id')::uuid
             AND assignment.submission_intent_id = claim.intent_id
             AND claim.worker_id = p_worker_id AND claim.worker_incarnation = (v_session->>'worker_incarnation')::uuid
             AND claim.lease_state = 'live' AND claim.executable
             AND claim_state.subject_id = claim.subject_id AND claim_state.subject_incarnation = claim.subject_incarnation
             AND claim_state.claim_high_water >= claim.claim_high_water
             AND claim_state.terminal_high_water < claim_state.claim_high_water
             AND NOT EXISTS (SELECT 1 FROM loom_capacity_guard.executable_claim_terminal_events AS terminal
                             WHERE terminal.admitted_operation_id = claim.operation_id
                                OR terminal.protected_attempt_id = claim.protected_attempt_id)
           FOR UPDATE OF trial, head, claim_state NOWAIT
           FOR KEY SHARE OF runtime, readiness, submission, attempt, assignment, claim NOWAIT;
          IF NOT FOUND THEN RETURN NULL; END IF;

          SELECT lease.id, lease.generation, lease.revoked_at, lease.deleted_at INTO v_lease
            FROM public.execution_leases AS lease
           WHERE lease.trial_id = v_current.trial_id
             AND ((p_report->>'execution_lease_id' IS NULL AND lease.execution_role = 'attempt')
                  OR lease.id = (p_report->>'execution_lease_id')::uuid)
           ORDER BY lease.attempt DESC LIMIT 1 FOR UPDATE NOWAIT;
          IF FOUND AND (p_report->>'execution_lease_id' IS DISTINCT FROM v_lease.id::text
                        OR (p_report->>'execution_generation')::bigint IS DISTINCT FROM v_lease.generation
                        OR v_lease.revoked_at IS NOT NULL OR v_lease.deleted_at IS NOT NULL) THEN
            RAISE EXCEPTION 'protected progress execution generation changed' USING ERRCODE = '55000';
          END IF;
          IF NOT FOUND AND p_report->>'execution_lease_id' IS NOT NULL THEN
            RAISE EXCEPTION 'protected progress execution lease is unavailable' USING ERRCODE = '55000';
          END IF;
          SELECT jsonb_build_object(
                   'trial', jsonb_build_object('id', trial.id, 'team_id', trial.team_id,
                     'batch_id', trial.batch_id, 'visibility', trial.visibility,
                     'share_status', trial.share_status, 'source_provenance', trial.source_provenance),
                   'batch', CASE WHEN batch.id IS NULL THEN 'null'::jsonb ELSE
                     jsonb_build_object('id', batch.id, 'visibility', batch.visibility,
                                        'source_provenance', batch.source_provenance) END)
            INTO v_expected FROM public.trials trial
            LEFT JOIN public.batches batch ON batch.id = trial.batch_id
           WHERE trial.id = v_current.trial_id;
          IF p_report->'expected' IS DISTINCT FROM v_expected THEN
            RAISE EXCEPTION 'protected output projection inputs changed' USING ERRCODE = '55000';
          END IF;
          -- Protected submissions already own their exact lifecycle authority.
          -- Never substitute a different owner or revive a deleting authority.
          SELECT authority.* INTO v_parent FROM public.trials trial
            JOIN public.data_lifecycle_authorities authority ON authority.id = trial.lifecycle_authority_id
            JOIN loom_capacity_guard.atomic_trial_submissions submission ON submission.trial_id = trial.id
           WHERE trial.id = v_current.trial_id AND authority.team_id = trial.team_id
             AND authority.owner_kind = 'trial' AND authority.data_class = 'trial'
             AND authority.owner_id = trial.id::text AND authority.state = 'active'
             AND authority.environment = submission.lifecycle_environment
             AND authority.namespace = submission.lifecycle_namespace
             AND authority.created_at = trial.submitted_at
             AND ((authority.environment = 'staging' AND NOT authority.pinned
                   AND authority.expires_at = authority.created_at + interval '7 days')
                  OR (authority.environment <> 'staging' AND authority.pinned AND authority.expires_at IS NULL))
           FOR SHARE OF authority NOWAIT;
          IF NOT FOUND OR p_report->'scope' IS DISTINCT FROM jsonb_build_object(
              'environment', v_parent.environment, 'namespace', v_parent.namespace) THEN
            RAISE EXCEPTION 'protected output lifecycle parent conflicts' USING ERRCODE = '55000';
          END IF;
          v_changes := jsonb_build_object('trajectory_index', p_report->'index',
            'result', CASE WHEN p_report->'result' = 'null'::jsonb THEN v_current.result ELSE p_report->'result' END);
          v_permit := loom_capacity_guard.authorize_frozen_trial_update(
            v_current.trial_id, v_current.protected_attempt_id, v_current.execution_generation,
            p_worker_id, (v_session->>'worker_incarnation')::uuid, v_current.claim_operation_id,
            'output', v_changes);
          UPDATE public.trials SET trajectory_index = v_changes->'trajectory_index',
                 result = NULLIF(v_changes->'result', 'null'::jsonb)
           WHERE id = v_current.trial_id AND worker_id = p_worker_id AND state = v_current.state;
          IF NOT FOUND THEN RAISE EXCEPTION 'protected output update lost its row' USING ERRCODE = '55000'; END IF;
          PERFORM loom_capacity_guard.assert_frozen_trial_mutation_consumed(v_permit);

          FOR v_descriptor IN SELECT value FROM jsonb_array_elements(p_report->'artifacts') LOOP
            IF jsonb_typeof(v_descriptor) IS DISTINCT FROM 'object'
               OR NOT (v_descriptor ?& ARRAY['artifact_type','artifact_schema_version','name','team_id',
                   'batch_id','trial_id','created_by','content_hash','storage','visibility','share_status',
                   'redaction_state','safety_state','blocked_reason','retention','provenance','metadata'])
               OR v_descriptor - ARRAY['artifact_type','artifact_schema_version','name','team_id',
                   'batch_id','trial_id','created_by','content_hash','storage','visibility','share_status',
                   'redaction_state','safety_state','blocked_reason','retention','provenance','metadata'] <> '{}'::jsonb
               OR v_descriptor->>'trial_id' IS DISTINCT FROM v_current.trial_id::text
               OR v_descriptor->>'batch_id' IS DISTINCT FROM v_current.batch_id::text
               OR v_descriptor->>'team_id' IS DISTINCT FROM v_current.team_id::text
               OR v_descriptor->>'content_hash' !~ '^sha256:[0-9a-f]{64}$' THEN
              RAISE EXCEPTION 'protected output artifact ownership is malformed' USING ERRCODE = '22023';
            END IF;
            v_storage := v_descriptor->'storage';
            IF jsonb_typeof(v_storage) IS DISTINCT FROM 'object'
               OR NOT (v_storage ?& ARRAY['bucket','key','version_id','size_bytes'])
               OR jsonb_typeof(v_storage->'bucket') IS DISTINCT FROM 'string'
               OR jsonb_typeof(v_storage->'key') IS DISTINCT FROM 'string'
               OR v_storage->>'bucket' = '' OR v_storage->>'bucket' <> btrim(v_storage->>'bucket')
               OR v_storage->>'key' = '' OR v_storage->>'key' <> btrim(v_storage->>'key')
               OR jsonb_typeof(v_storage->'version_id') NOT IN ('null','string')
               OR (v_storage->>'version_id' IS NOT NULL AND
                   (v_storage->>'version_id' = '' OR v_storage->>'version_id' <> btrim(v_storage->>'version_id')))
               OR jsonb_typeof(v_storage->'size_bytes') IS DISTINCT FROM 'number'
               OR v_storage->>'size_bytes' !~ '^[0-9]+$' THEN
              RAISE EXCEPTION 'protected output object identity is malformed' USING ERRCODE = '22023';
            END IF;
            SELECT artifact.* INTO v_artifact FROM public.artifacts artifact
             WHERE artifact.trial_id = v_current.trial_id AND artifact.artifact_type = v_descriptor->>'artifact_type'
               AND artifact.storage->>'key' = v_storage->>'key'
             ORDER BY artifact.created_at DESC, artifact.id DESC LIMIT 1 FOR UPDATE NOWAIT;
            IF FOUND THEN
              IF v_artifact.team_id IS DISTINCT FROM v_current.team_id OR v_artifact.batch_id IS DISTINCT FROM v_current.batch_id THEN
                RAISE EXCEPTION 'protected output existing artifact owner conflicts' USING ERRCODE = '55000';
              END IF;
              v_artifact_id := v_artifact.id;
              v_created_at := v_artifact.created_at;
              v_before := to_jsonb(v_artifact);
            ELSE
              v_artifact_id := gen_random_uuid();
              v_created_at := now();
              v_before := NULL;
            END IF;
            INSERT INTO public.data_lifecycle_authorities
              (environment, namespace, team_id, data_class, owner_kind, owner_id, created_at, expires_at, pinned, state)
            VALUES (v_parent.environment, v_parent.namespace, v_current.team_id, 'artifact', 'artifact', v_artifact_id::text,
              v_created_at, CASE WHEN v_parent.environment = 'staging' THEN v_created_at + interval '7 days' ELSE NULL END,
              v_parent.environment <> 'staging', 'active') ON CONFLICT DO NOTHING;
            SELECT authority.id INTO v_authority_id FROM public.data_lifecycle_authorities authority
             WHERE authority.environment = v_parent.environment AND authority.namespace = v_parent.namespace
               AND authority.team_id = v_current.team_id AND authority.data_class = 'artifact'
               AND authority.owner_kind = 'artifact' AND authority.owner_id = v_artifact_id::text
               AND authority.created_at = v_created_at AND authority.state = 'active'
               AND authority.pinned = (v_parent.environment <> 'staging')
               AND authority.expires_at IS NOT DISTINCT FROM
                   CASE WHEN v_parent.environment = 'staging' THEN v_created_at + interval '7 days' ELSE NULL END
             FOR SHARE NOWAIT;
            IF NOT FOUND OR (v_before->>'lifecycle_authority_id' IS NOT NULL
                AND v_before->>'lifecycle_authority_id' <> v_authority_id::text) THEN
              RAISE EXCEPTION 'protected output artifact lifecycle conflicts' USING ERRCODE = '55000';
            END IF;
            v_descriptor := v_descriptor || jsonb_build_object('lifecycle_authority_id', v_authority_id);
            IF v_before IS NULL THEN
              INSERT INTO public.artifacts (id, created_at, lifecycle_authority_id,
                artifact_type, artifact_schema_version, name, team_id, batch_id, trial_id, created_by,
                content_hash, storage, visibility, share_status, redaction_state, safety_state,
                blocked_reason, retention, provenance, metadata)
              SELECT v_artifact_id, v_created_at, v_authority_id, row.artifact_type, row.artifact_schema_version,
                row.name, row.team_id, row.batch_id, row.trial_id, row.created_by, row.content_hash,
                row.storage, row.visibility, row.share_status, row.redaction_state, row.safety_state,
                row.blocked_reason, row.retention, row.provenance, row.metadata
                FROM jsonb_populate_record(NULL::public.artifacts, v_descriptor) row;
            ELSE
              UPDATE public.artifacts artifact SET lifecycle_authority_id = v_authority_id,
                artifact_type = row.artifact_type, artifact_schema_version = row.artifact_schema_version,
                name = row.name, team_id = row.team_id, batch_id = row.batch_id, trial_id = row.trial_id,
                created_by = row.created_by, content_hash = row.content_hash, storage = row.storage,
                visibility = row.visibility, share_status = row.share_status, redaction_state = row.redaction_state,
                safety_state = row.safety_state, blocked_reason = row.blocked_reason, retention = row.retention,
                provenance = row.provenance, metadata = row.metadata
                FROM jsonb_populate_record(NULL::public.artifacts, v_descriptor) row
               WHERE artifact.id = v_artifact_id;
            END IF;
            IF NOT FOUND THEN RAISE EXCEPTION 'protected output artifact write was suppressed' USING ERRCODE = '55000'; END IF;
            SELECT to_jsonb(artifact) INTO v_after FROM public.artifacts artifact WHERE artifact.id = v_artifact_id;
            IF v_before IS NULL THEN
              SELECT to_jsonb(expected) INTO v_expected
                FROM jsonb_populate_record(NULL::public.artifacts, v_descriptor || jsonb_build_object(
                    'id', v_artifact_id, 'created_at', v_created_at, 'access_class', 'team_runtime')) expected;
            ELSE
              v_expected := v_before || v_descriptor;
            END IF;
            IF v_after IS DISTINCT FROM v_expected THEN
              RAISE EXCEPTION 'protected output artifact write changed columns' USING ERRCODE = '55000';
            END IF;
            INSERT INTO public.data_lifecycle_objects
              (authority_id, environment, namespace, bucket, object_key, version_id, content_sha256, size_bytes, created_at, state)
            VALUES (v_authority_id, v_parent.environment, v_parent.namespace, v_storage->>'bucket', v_storage->>'key',
                    v_storage->>'version_id', substr(v_descriptor->>'content_hash', 8), (v_storage->>'size_bytes')::bigint,
                    v_created_at, 'active') ON CONFLICT DO NOTHING;
            SELECT object.* INTO v_object FROM public.data_lifecycle_objects object
             WHERE object.environment = v_parent.environment AND object.namespace = v_parent.namespace
               AND object.bucket = v_storage->>'bucket' AND object.object_key = v_storage->>'key'
               AND object.version_id IS NOT DISTINCT FROM v_storage->>'version_id'
             FOR SHARE NOWAIT;
            IF NOT FOUND OR v_object.authority_id IS DISTINCT FROM v_authority_id
               OR v_object.content_sha256 IS DISTINCT FROM substr(v_descriptor->>'content_hash', 8)
               OR v_object.size_bytes IS DISTINCT FROM (v_storage->>'size_bytes')::bigint
               OR v_object.created_at IS DISTINCT FROM v_created_at OR v_object.state IS DISTINCT FROM 'active' THEN
              RAISE EXCEPTION 'protected output object registration conflicts' USING ERRCODE = '55000';
            END IF;
            v_synced := array_append(v_synced, v_artifact_id);
          END LOOP;
          DELETE FROM public.artifact_lineage_edges WHERE child_artifact_id = ANY(v_synced);
          IF EXISTS (SELECT 1 FROM public.artifact_lineage_edges WHERE child_artifact_id = ANY(v_synced)) THEN
            RAISE EXCEPTION 'protected output lineage retirement was suppressed' USING ERRCODE = '55000';
          END IF;
          FOREACH v_artifact_id IN ARRAY v_synced LOOP
            FOR v_edge IN SELECT value FROM jsonb_array_elements(p_report->'lineage') LOOP
              IF jsonb_typeof(v_edge) IS DISTINCT FROM 'object'
                 OR NOT (v_edge ?& ARRAY['parent_id','relation','metadata'])
                 OR v_edge - ARRAY['parent_id','relation','metadata'] <> '{}'::jsonb
                 OR jsonb_typeof(v_edge->'relation') IS DISTINCT FROM 'string'
                 OR v_edge->>'relation' = '' OR jsonb_typeof(v_edge->'metadata') IS DISTINCT FROM 'object' THEN
                RAISE EXCEPTION 'protected output lineage is malformed' USING ERRCODE = '22023';
              END IF;
              PERFORM 1 FROM public.artifacts WHERE id = (v_edge->>'parent_id')::uuid FOR SHARE NOWAIT;
              IF FOUND THEN
                INSERT INTO public.artifact_lineage_edges (child_artifact_id, parent_artifact_id, relation, metadata)
                VALUES (v_artifact_id, (v_edge->>'parent_id')::uuid, v_edge->>'relation', v_edge->'metadata');
                IF NOT FOUND OR NOT EXISTS (SELECT 1 FROM public.artifact_lineage_edges
                    WHERE child_artifact_id = v_artifact_id AND parent_artifact_id = (v_edge->>'parent_id')::uuid
                      AND relation = v_edge->>'relation' AND metadata = v_edge->'metadata') THEN
                  RAISE EXCEPTION 'protected output lineage write was suppressed' USING ERRCODE = '55000';
                END IF;
              END IF;
            END LOOP;
          END LOOP;
          RETURN jsonb_build_object('trial_id', v_current.trial_id);
        END
        $function$;
    """)
    configuration = op.get_context().config
    if configuration is None:
        raise RuntimeError("protected output migration is missing configuration")
    role = configuration.attributes.get("capacity_guard_runtime_role")
    if not isinstance(role, str) or not role:
        raise RuntimeError("protected output migration is missing runtime role")
    quoted = op.get_bind().dialect.identifier_preparer.quote(role)
    op.execute(f"REVOKE ALL ON FUNCTION {_FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {_FUNCTION} TO {quoted}")
    rewrite(_VALIDATOR, [(_OLD_ALLOWED, _NEW_ALLOWED)], upgrading=True)


def uninstall_output_reporting(rewrite: Callable[..., None]) -> None:
    rewrite(_VALIDATOR, [(_OLD_ALLOWED, _NEW_ALLOWED)], upgrading=False)
    op.execute(f"DROP FUNCTION {_FUNCTION}")
    rewrite(_ISSUER, [(_OLD, _NEW)], upgrading=False)
