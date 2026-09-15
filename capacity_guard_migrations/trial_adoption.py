"""Explicit, identity-preserving admission of untouched legacy queued trials."""

from __future__ import annotations

from collections.abc import Callable

import sqlalchemy as sa
from alembic import op

S = "loom_capacity_guard"
_INERT_ARGS = "uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text"
_RUNTIME_ARGS = _INERT_ARGS + ",jsonb,bytea,text"
_BINDING_ARGS = ",timestamp with time zone,uuid,uuid"
_EXTRA = ", p_expected_submitted_at timestamp with time zone, p_expected_lifecycle_authority_id uuid, p_operation_id uuid"
_OUTER = f"{S}.adopt_protected_runtime_trial_projection({_RUNTIME_ARGS}{_BINDING_ARGS})"
_INNER = f"{S}.adopt_inert_trial_projection({_INERT_ARGS}{_BINDING_ARGS},jsonb)"
_REGISTER = f"{S}.register_adopted_trial_submission(uuid,jsonb,bytea,text,bytea,text)"
_PERMIT = f"{S}.authorize_frozen_trial_adoption(uuid,uuid,bigint,uuid)"
_VALIDATOR = f"{S}.current_protected_runtime_registration()"
_OLD_ALLOWED = "'loom_capacity_guard.publish_staging_trial_output(uuid,text,jsonb)'::regprocedure::oid\n          ];"
_NEW_ALLOWED = "'loom_capacity_guard.publish_staging_trial_output(uuid,text,jsonb)'::regprocedure::oid,\n            '" + _OUTER + "'::regprocedure::oid\n          ];"


def _copy(signature: str, name: str, extra: str = "") -> str:
    row = op.get_bind().execute(sa.text(
        "SELECT pg_get_functiondef(p.oid) AS definition, p.prosecdef, p.proconfig, "
        "p.proowner=current_user::regrole::oid AS owned FROM pg_proc p "
        "WHERE p.oid=CAST(:signature AS regprocedure)"
    ), {"signature": signature}).mappings().one()
    if not row["owned"] or not row["prosecdef"] or row["proconfig"] != ["search_path=pg_catalog"]:
        raise RuntimeError("legacy adoption source function authority changed")
    definition = str(row["definition"])
    old_name = signature.split("(")[0]
    prefix = f"CREATE OR REPLACE FUNCTION {old_name}("
    if not definition.startswith(prefix):
        raise RuntimeError("legacy adoption source function header changed")
    end = definition.index("\n RETURNS")
    if definition[end - 1] != ")":
        raise RuntimeError("legacy adoption source function arguments changed")
    definition = definition[:end - 1] + extra + definition[end - 1:]
    return definition.replace(prefix, f"CREATE OR REPLACE FUNCTION {S}.{name}(", 1)


def _replace(definition: str, before: str, after: str) -> str:
    if definition.count(before) != 1:
        raise RuntimeError("legacy adoption source function clause changed")
    return definition.replace(before, after, 1)


def install_trial_adoption(rewrite: Callable[..., None]) -> None:
    op.execute(f"""
        CREATE TABLE {S}.trial_adoptions (
          operation_id uuid PRIMARY KEY CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'),
          trial_id uuid NOT NULL UNIQUE,
          protected_attempt_id uuid NOT NULL UNIQUE,
          lifecycle_authority_id uuid NOT NULL,
          submitted_at timestamptz NOT NULL,
          canonical_payload bytea NOT NULL,
          protected_canonical_payload bytea NOT NULL,
          public_requires_caps jsonb NOT NULL
        );
        CREATE TRIGGER trial_adoptions_retained_row BEFORE UPDATE OR DELETE ON {S}.trial_adoptions
          FOR EACH ROW EXECUTE FUNCTION {S}.reject_append_only_mutation();
        CREATE TRIGGER trial_adoptions_retained_truncate BEFORE TRUNCATE ON {S}.trial_adoptions
          FOR EACH STATEMENT EXECUTE FUNCTION {S}.reject_append_only_mutation();
        REVOKE ALL ON TABLE {S}.trial_adoptions FROM PUBLIC;
    """)
    _install_permission()
    register = _copy(f"{S}.register_inert_trial_submission(uuid,jsonb,bytea,text,bytea,text)",
                     "register_adopted_trial_submission")
    normalized = """trial.autoscaler_pool_name,
                 (trial.requires_caps - ARRAY['backend','terminus2_model_switch']) ||
                 CASE WHEN trial.requires_caps ? 'worker_pool' THEN jsonb_build_object(
                   'worker_pool', CASE WHEN trial.requires_caps->>'worker_pool' IN
                     ('gb10','behavior-gpu-gb10') THEN 'gb10' ELSE 'oldlab' END)
                 ELSE '{}'::jsonb END"""
    register = _replace(register, "trial.autoscaler_pool_name, trial.requires_caps", normalized)
    op.execute(register)
    op.execute(f"REVOKE ALL ON FUNCTION {_REGISTER} FROM PUBLIC")

    inner = _copy(f"{S}.submit_inert_trial_projection({_INERT_ARGS})", "adopt_inert_trial_projection",
                  _EXTRA + ", p_public_requires_caps jsonb")
    inner = _replace(inner, "          v_updated_rows bigint;",
                     "          v_updated_rows bigint;\n          v_adoption_permit uuid;")
    start = inner.index("          INSERT INTO public.trials\n")
    end = inner.index(f"          PERFORM {S}.register_inert_trial_submission(", start)
    fields = {
        "team_id": "uuid", "task_id": "text", "config": "jsonb", "submit_priority": "integer",
        "batch_id": "uuid", "idempotency_key": "text", "sample_idx": "integer",
        "combination_idx": "integer", "provider_connection_id": "uuid", "provider_model_id": "text",
        "submitted_by_user_id": "uuid", "usage_attributed_user_id": "uuid",
        "usage_attributed_actor": "text", "family_key": "text",
    }
    bindings = "\n".join(
        f"             AND trial.{field} IS NOT DISTINCT FROM "
        + (f"p_payload->'{field}'" if kind == "jsonb" else f"(p_payload->>'{field}')::{kind}")
        for field, kind in fields.items()
    )
    existing = f"""          SELECT trial.id, trial.submitted_at, trial.lifecycle_authority_id
            INTO v_trial_id, v_submitted_at, v_lifecycle_authority_id
            FROM public.trials AS trial
           WHERE trial.id = v_requested_trial_id AND trial.state = 'queued'
             AND trial.submitted_at = p_expected_submitted_at
             AND trial.lifecycle_authority_id = p_expected_lifecycle_authority_id
             AND trial.requires_caps = p_public_requires_caps
             AND trial.worker_id IS NULL AND trial.attempt_count = 0
             AND trial.claimed_at IS NULL AND trial.started_at IS NULL AND trial.finished_at IS NULL
             AND trial.pre_start_heartbeat_at IS NULL AND trial.failure_reason IS NULL
             AND trial.failure_message IS NULL AND trial.result IS NULL
             AND trial.cancellation_requested_at IS NULL AND trial.cancellation_observed_at IS NULL
             AND trial.next_attempt_at IS NULL AND trial.autoscaler_pool_name IS NULL
{bindings}
           FOR UPDATE NOWAIT;
          IF NOT FOUND OR EXISTS (SELECT 1 FROM {S}.trial_attempts WHERE trial_id=v_trial_id)
             OR EXISTS (SELECT 1 FROM {S}.trial_requirements WHERE trial_id=v_trial_id)
             OR EXISTS (SELECT 1 FROM {S}.protected_runtime_trial_submissions WHERE trial_id=v_trial_id) THEN
            RAISE EXCEPTION 'legacy trial no longer matches its untouched original projection' USING ERRCODE='55000';
          END IF;
          PERFORM 1 FROM public.data_lifecycle_authorities AS lifecycle
           WHERE lifecycle.id=v_lifecycle_authority_id
             AND lifecycle.environment=v_scope.lifecycle_environment
             AND lifecycle.namespace=v_scope.lifecycle_namespace
             AND lifecycle.team_id=(p_payload->>'team_id')::uuid
             AND lifecycle.data_class='trial' AND lifecycle.owner_kind='trial'
             AND lifecycle.owner_id=v_trial_id::text AND lifecycle.state='active'
             AND lifecycle.created_at=v_submitted_at
             AND NOT lifecycle.pinned AND lifecycle.expires_at=v_submitted_at+interval '7 days'
             AND lifecycle.expires_at>statement_timestamp()
           FOR KEY SHARE NOWAIT;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'legacy trial retention is not its active original authority' USING ERRCODE='55000';
          END IF;

"""
    inner = inner[:start] + existing + inner[end:]
    inner = _replace(inner, f"PERFORM {S}.register_inert_trial_submission(",
                     f"PERFORM {S}.register_adopted_trial_submission(")
    update = "          UPDATE public.trials AS t\n             SET state = 'protected-pending'"
    inner = _replace(inner, update, f"""          v_adoption_permit := {S}.authorize_frozen_trial_adoption(
            v_trial_id, (p_payload->>'protected_attempt_id')::uuid,
            (p_payload->>'execution_generation')::bigint, p_operation_id);
""" + update)
    inner = _replace(inner, "          SELECT a.protected_attempt_id, a.execution_generation,",
                     f"          PERFORM {S}.assert_frozen_trial_mutation_consumed(v_adoption_permit);\n"
                     "          SELECT a.protected_attempt_id, a.execution_generation,")
    op.execute(inner)
    op.execute(f"REVOKE ALL ON FUNCTION {_INNER} FROM PUBLIC")

    outer = _copy(f"{S}.submit_protected_runtime_trial_projection({_RUNTIME_ARGS})",
                  "adopt_protected_runtime_trial_projection", _EXTRA)
    outer = _replace(outer, "          v_updated_rows bigint;",
                     f"          v_updated_rows bigint;\n          v_adoption {S}.trial_adoptions%ROWTYPE;")
    outer = _replace(outer, "          SELECT runtime_role_name INTO v_runtime_role", f"""          IF current_setting('transaction_isolation') <> 'serializable' THEN
            RAISE EXCEPTION 'legacy trial adoption requires SERIALIZABLE' USING ERRCODE='25000';
          END IF;
          PERFORM 1 FROM {S}.agent_runtime_authority WHERE singleton_id=1 FOR UPDATE NOWAIT;
          IF p_operation_id IS NULL OR p_operation_id='00000000-0000-0000-0000-000000000000'::uuid
             OR p_expected_submitted_at IS NULL OR p_expected_lifecycle_authority_id IS NULL THEN
            RAISE EXCEPTION 'legacy trial adoption identity is incomplete' USING ERRCODE='22023';
          END IF;
          SELECT runtime_role_name INTO v_runtime_role""")
    call = f"          v_receipt := {S}.submit_inert_trial_projection("
    evidence = f"""          PERFORM {S}.assert_inert_agent_binding(p_agent_incarnation,p_payload,p_canonical_payload,p_payload_digest);
          PERFORM {S}.assert_inert_agent_binding(p_agent_incarnation,p_protected_payload,p_protected_canonical_payload,p_protected_payload_digest);
          IF p_requirements_payload IS NULL
             OR octet_length(p_requirements_payload) > 8388608
             OR p_requirements_digest IS DISTINCT FROM p_payload->>'requirements_digest'
             OR encode(sha256(p_requirements_payload), 'hex') IS DISTINCT FROM p_requirements_digest THEN
            RAISE EXCEPTION 'legacy adoption requirements payload is invalid' USING ERRCODE='22023';
          END IF;
          SELECT * INTO v_adoption FROM {S}.trial_adoptions
           WHERE operation_id=p_operation_id OR trial_id=(p_payload->>'trial_id')::uuid FOR KEY SHARE NOWAIT;
          IF FOUND THEN
            IF v_adoption.operation_id IS DISTINCT FROM p_operation_id
               OR v_adoption.trial_id IS DISTINCT FROM (p_payload->>'trial_id')::uuid
               OR v_adoption.protected_attempt_id IS DISTINCT FROM (p_payload->>'protected_attempt_id')::uuid
               OR v_adoption.lifecycle_authority_id IS DISTINCT FROM p_expected_lifecycle_authority_id
               OR v_adoption.submitted_at IS DISTINCT FROM p_expected_submitted_at
               OR v_adoption.canonical_payload IS DISTINCT FROM p_canonical_payload
               OR v_adoption.protected_canonical_payload IS DISTINCT FROM p_protected_canonical_payload
               OR v_adoption.public_requires_caps IS DISTINCT FROM p_public_requires_caps THEN
              RAISE EXCEPTION 'conflicting legacy trial adoption replay' USING ERRCODE='55000';
            END IF;
            SELECT jsonb_build_object('schema_version',1,'trial_id',origin.trial_id,
              'protected_attempt_id',origin.protected_attempt_id,'lifecycle_authority_id',origin.lifecycle_authority_id,
              'requirements_digest',origin.requirements_digest,'submitted_at',origin.submitted_at,'replayed',true,'executable',false)
              INTO v_receipt FROM {S}.atomic_trial_submissions AS origin
             WHERE origin.trial_id=v_adoption.trial_id AND origin.protected_attempt_id=v_adoption.protected_attempt_id;
            IF NOT FOUND THEN RAISE EXCEPTION 'legacy adoption origin is missing' USING ERRCODE='55000'; END IF;
            RETURN v_receipt;
          END IF;
          IF EXISTS (SELECT 1 FROM {S}.atomic_trial_submissions WHERE trial_id=(p_payload->>'trial_id')::uuid
                       OR idempotency_key=p_payload->>'idempotency_key') THEN
            RAISE EXCEPTION 'legacy adoption conflicts with existing submission provenance' USING ERRCODE='55000';
          END IF;
          INSERT INTO {S}.trial_adoptions(operation_id,trial_id,protected_attempt_id,lifecycle_authority_id,
            submitted_at,canonical_payload,protected_canonical_payload,public_requires_caps)
          VALUES(p_operation_id,(p_payload->>'trial_id')::uuid,(p_payload->>'protected_attempt_id')::uuid,
            p_expected_lifecycle_authority_id,p_expected_submitted_at,p_canonical_payload,p_protected_canonical_payload,p_public_requires_caps);
"""
    outer = _replace(outer, call, evidence + f"          v_receipt := {S}.adopt_inert_trial_projection(")
    outer = _replace(outer, "            p_requirements_digest\n          );",
                     "            p_requirements_digest, p_expected_submitted_at, p_expected_lifecycle_authority_id, p_operation_id, p_public_requires_caps\n          );")
    # The original capabilities already match. Do not perform an unnecessary
    # second public UPDATE through the frozen fence.
    start = outer.index("          UPDATE public.trials AS trial\n             SET requires_caps")
    end = outer.index(f"          INSERT INTO {S}.protected_runtime_trial_submissions", start)
    outer = outer[:start] + outer[end:]
    op.execute(outer)
    op.execute(f"REVOKE ALL ON FUNCTION {_OUTER} FROM PUBLIC")
    runtime = op.get_bind().execute(sa.text(f"SELECT runtime_role_name FROM {S}.staging_worker_runtime_authority WHERE singleton_id=1")).scalar_one()
    quoted = op.get_bind().dialect.identifier_preparer.quote(runtime)
    op.execute(f"GRANT EXECUTE ON FUNCTION {_OUTER} TO {quoted}")
    rewrite(_VALIDATOR, [(_OLD_ALLOWED, _NEW_ALLOWED)], upgrading=True)


def _install_permission() -> None:
    op.execute(f"""
        CREATE FUNCTION {S}.authorize_frozen_trial_adoption(p_trial uuid,p_attempt uuid,p_generation bigint,p_operation uuid)
        RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE
          v_fence {S}.trial_writer_fence%ROWTYPE;
          v_binding jsonb;
          v_old jsonb;
          v_permit uuid:=gen_random_uuid();
        BEGIN
          IF current_setting('transaction_isolation')<>'serializable' THEN
            RAISE EXCEPTION 'legacy adoption permission requires SERIALIZABLE' USING ERRCODE='25000';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM {S}.staging_worker_runtime_authority
                          WHERE singleton_id=1 AND runtime_role_name=session_user::text)
             OR pg_has_role(session_user,current_user,'MEMBER') THEN
            RAISE EXCEPTION 'legacy adoption runtime is not bound' USING ERRCODE='42501';
          END IF;
          SELECT * INTO STRICT v_fence FROM {S}.trial_writer_fence WHERE singleton_id=1 FOR SHARE NOWAIT;
          IF NOT v_fence.frozen THEN RETURN NULL; END IF;
          SELECT jsonb_build_object('registration',to_jsonb(r),'authority',to_jsonb(f)-'reporter_high_water'-'updated_at')
            INTO v_binding FROM {S}.authority_state f JOIN {S}.agent_registrations r ON r.singleton_id=f.singleton_id
           WHERE f.singleton_id=1 AND r.registration_state='registered'
             AND r.agent_incarnation=(v_fence.registration->>'agent_incarnation')::uuid FOR SHARE OF f,r NOWAIT;
          IF v_binding IS NULL OR v_binding->'registration' IS DISTINCT FROM v_fence.registration
             OR v_binding->'authority' IS DISTINCT FROM v_fence.authority_binding THEN
            RAISE EXCEPTION 'frozen legacy adoption writer binding changed' USING ERRCODE='55000';
          END IF;
          SELECT jsonb_build_object('id',trial.id,'state',trial.state,'worker_id',trial.worker_id,
                   'attempt_count',trial.attempt_count,'lifecycle_authority_id',trial.lifecycle_authority_id)
            INTO v_old FROM public.trials trial JOIN {S}.trial_adoptions event ON event.trial_id=trial.id
            JOIN {S}.trial_attempts attempt ON attempt.trial_id=trial.id AND attempt.protected_attempt_id=event.protected_attempt_id
            JOIN {S}.attempt_lifecycle_heads head ON head.protected_attempt_id=attempt.protected_attempt_id
           WHERE trial.id=p_trial AND event.operation_id=p_operation AND attempt.protected_attempt_id=p_attempt
             AND attempt.execution_generation=p_generation AND attempt.attempt_sequence=0 AND attempt.claim_state='queued'
             AND head.lifecycle_state='pending-unassigned' AND NOT head.executable
             AND event.lifecycle_authority_id=trial.lifecycle_authority_id AND event.submitted_at=trial.submitted_at
             AND event.public_requires_caps=trial.requires_caps AND trial.state='queued'
             AND trial.worker_id IS NULL AND trial.attempt_count=0
             AND NOT EXISTS (SELECT 1 FROM {S}.executable_claim_leases WHERE protected_attempt_id=p_attempt)
           FOR UPDATE OF trial NOWAIT FOR KEY SHARE OF event,attempt,head NOWAIT;
          IF v_old IS NULL THEN RAISE EXCEPTION 'legacy adoption permission lacks exact origin' USING ERRCODE='55000'; END IF;
          INSERT INTO {S}.trial_mutation_permits(permit_id,transaction_id,backend_pid,writer_incarnation,
            writer_epoch,freeze_operation_id,authority_binding,registration,trial_id,protected_attempt_id,
            execution_generation,adoption_operation_id,operation,old_binding,changes)
          VALUES(v_permit,pg_current_xact_id(),pg_backend_pid(),v_fence.writer_incarnation,v_fence.writer_epoch,
            v_fence.freeze_operation_id,v_fence.authority_binding,v_fence.registration,p_trial,p_attempt,p_generation,
            p_operation,'adopt',v_old,jsonb_build_object('state','protected-pending'));
          RETURN v_permit;
        END $function$;
        REVOKE ALL ON FUNCTION {_PERMIT} FROM PUBLIC;
    """)


def uninstall_trial_adoption(rewrite: Callable[..., None]) -> None:
    op.execute(f"""
        DO $retirement$
        DECLARE v_relation oid;
        BEGIN
          SELECT oid INTO v_relation FROM pg_class WHERE oid='{S}.trial_adoptions'::regclass
            AND relkind='r' AND NOT relispartition AND relowner=current_user::regrole::oid;
          IF v_relation IS NULL THEN
            RAISE EXCEPTION 'legacy adoption retirement relation ownership changed' USING ERRCODE='42501';
          END IF;
          IF EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=v_relation OR inhrelid=v_relation) THEN
            RAISE EXCEPTION 'legacy adoption retirement inheritance is unsupported' USING ERRCODE='55000';
          END IF;
          LOCK TABLE ONLY {S}.trial_adoptions IN ACCESS EXCLUSIVE MODE NOWAIT;
          IF '{S}.trial_adoptions'::regclass::oid <> v_relation
             OR NOT EXISTS (SELECT 1 FROM pg_class WHERE oid=v_relation AND relowner=current_user::regrole::oid
                              AND relkind='r' AND NOT relispartition)
             OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent=v_relation OR inhrelid=v_relation) THEN
            RAISE EXCEPTION 'legacy adoption retirement relation changed' USING ERRCODE='55000';
          END IF;
          IF EXISTS(SELECT 1 FROM ONLY {S}.trial_adoptions) THEN
            RAISE EXCEPTION 'legacy adoption evidence requires protected retirement' USING ERRCODE='55000';
          END IF;
        END $retirement$;
    """)
    rewrite(_VALIDATOR, [(_OLD_ALLOWED, _NEW_ALLOWED)], upgrading=False)
    op.execute(f"""
        DROP FUNCTION {_OUTER};
        DROP FUNCTION {_INNER};
        DROP FUNCTION {_REGISTER};
        DROP FUNCTION {_PERMIT};
        DROP TABLE {S}.trial_adoptions;
    """)
