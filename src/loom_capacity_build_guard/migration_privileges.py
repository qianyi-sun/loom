"""Reject unexpected authority instead of silently preserving privilege drift."""

from sqlalchemy import Connection, text

SCHEMA = "loom_capacity_build_guard"


def verify_migration_privileges(connection: Connection, *, owner: str, agent: str) -> None:
    """Check explicit defaults and effective schema/object/column ACLs on every run.

    The agent has schema USAGE and only the revision's exact preparation entrypoint.
    Future callable procedures must amend this verifier with their exact surface.
    The check is also valid before initial creation and after empty downgrade.
    """
    version_table = connection.scalar(text("SELECT to_regclass('loom_capacity_build_guard.alembic_version')"))
    revision = connection.scalar(text("SELECT version_num FROM loom_capacity_build_guard.alembic_version")) if version_table else None
    callables = []
    additional_helpers = []
    if revision == "build_guard_0028":
        callables.append(f"{SCHEMA}.read_source_context(uuid,jsonb,bytea,text,text)")
        revision = "build_guard_0027"
    if revision == "build_guard_0027":
        callables.append(f"{SCHEMA}.authorize_source(uuid,jsonb,bytea,text,text)")
        revision = "build_guard_0026"
    if revision == "build_guard_0026":
        callables.append(f"{SCHEMA}.assert_management_installation(uuid,bytea)")
        revision = "build_guard_0025"
    if revision == "build_guard_0025":
        callables.append(f"{SCHEMA}.read_accepted_artifact(uuid,uuid,jsonb,bytea,text)")
        additional_helpers.append("assert_live_source(uuid,uuid,jsonb,bytea,text)")
        revision = "build_guard_0024"
    if revision == "build_guard_0024":
        callables.append(f"{SCHEMA}.read_pending_publications(uuid,uuid,uuid,integer)")
        revision = "build_guard_0023"
    if revision == "build_guard_0023":
        # Only the missing-plan SQLSTATE changes; callable authority is unchanged.
        revision = "build_guard_0022"
    if revision == "build_guard_0022":
        callables.append(f"{SCHEMA}.read_pending_native_workers(uuid,bigint,bigint,integer)")
        # This read-only extension retains the exact 0021 surface and helpers.
        revision = "build_guard_0021"
    if revision in {"build_guard_0003", "build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.prepare_plan(uuid,jsonb,bytea,text,jsonb)")
    if revision in {"build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.authorize_publication(uuid,uuid)")
    if revision in {"build_guard_0005", "build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.extend((f"{SCHEMA}.close_plan(uuid,jsonb,bytea,text)",
            f"{SCHEMA}.authorize_closure_publication(uuid,uuid)"))
    if revision in {"build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.extend((f"{SCHEMA}.capture_demand(uuid,bigint,jsonb)", f"{SCHEMA}.read_demand(uuid)"))
    if revision in {"build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.read_pending_sources(uuid)")
    if revision in {"build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.extend((f"{SCHEMA}.register_bootstrap(uuid,jsonb,bytea,text)", f"{SCHEMA}.authorize_bootstrap_publication(uuid,uuid)"))
    if revision in {"build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.extend((f"{SCHEMA}.prepare_worker(uuid,jsonb,bytea,text,text)", f"{SCHEMA}.bind_slurm_job(uuid,jsonb,bytea,text)"))
    if revision in {"build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.observe_intent(uuid,jsonb,bytea,text)")
    if revision in {"build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.revoke_prepared_bootstrap(uuid,jsonb,bytea,text)")
    if revision in {"build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.withdraw_unregistered_worker(uuid,jsonb,bytea,text)")
    if revision in {"build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.import_terminal_inventory(uuid,jsonb,bytea,text)")
    if revision in {"build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.extend((f"{SCHEMA}.read_next_protected_release(uuid)",
            f"{SCHEMA}.acknowledge_protected_release(uuid,jsonb,bytea,text,text)"))
    if revision in {"build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.retire_request_hold(uuid,jsonb,bytea,text)")
        callables.append(f"{SCHEMA}.read_pending_retirements(uuid,bigint,bigint,integer)")
    if revision in {"build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.register_worker(uuid,jsonb,bytea,text,text)")
    if revision in {"build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.claim_platform(uuid,jsonb,bytea,text,text)")
    if revision in {"build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.begin_drain(uuid,jsonb,bytea,text)")
    if revision in {"build_guard_0019", "build_guard_0020", "build_guard_0021"}:
        callables.extend((f"{SCHEMA}.record_outcome(uuid,jsonb,bytea,text,text)",
            f"{SCHEMA}.read_outcome(uuid,jsonb,bytea,text)"))
    if revision in {"build_guard_0020", "build_guard_0021"}:
        callables.append(f"{SCHEMA}.settle_interrupted_claim(uuid,jsonb,bytea,text)")
    if revision == "build_guard_0021":
        callables.extend((f"{SCHEMA}.acknowledge_release(uuid,jsonb,bytea,text,text)",
            f"{SCHEMA}.release_terminal_worker(uuid,jsonb,bytea,text,text)"))
    parameters = {"schema": SCHEMA, "owner": owner, "agent": agent, "callables": callables}
    defaults = connection.scalar(text("""
        SELECT EXISTS (
            SELECT 1 FROM pg_default_acl d
            CROSS JOIN LATERAL aclexplode(d.defaclacl) a
            WHERE d.defaclrole = (SELECT oid FROM pg_roles WHERE rolname=:owner)
              AND (d.defaclnamespace=0 OR d.defaclnamespace=
                (SELECT oid FROM pg_namespace WHERE nspname=:schema))
              AND a.grantee <> d.defaclrole
              -- PostgreSQL's global function default includes PUBLIC EXECUTE.
              -- The migration explicitly revokes that default and object grant.
              AND NOT (d.defaclnamespace=0 AND d.defaclobjtype='f'
                AND a.grantee=0 AND a.privilege_type='EXECUTE' AND NOT a.is_grantable)
        )
    """), parameters)
    if defaults:
        raise RuntimeError("build guard has unexpected default privileges")
    drift = connection.scalar(text("""
        WITH namespace AS (
            SELECT * FROM pg_namespace WHERE nspname=:schema
        ), objects AS (
            SELECT c.relowner AS owner_oid, COALESCE(c.relacl,
                acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,
                    c.relowner)) AS acl, false AS agent_callable
            FROM pg_class c JOIN namespace n ON n.oid=c.relnamespace
            UNION ALL
            SELECT p.proowner, COALESCE(p.proacl, acldefault('f', p.proowner)),
                p.oid IN (SELECT to_regprocedure(signature) FROM unnest(CAST(:callables AS text[])) signature)
                AND p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
            FROM pg_proc p JOIN namespace n ON n.oid=p.pronamespace
        )
        SELECT EXISTS (
            SELECT 1 FROM namespace n
            CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) a
            WHERE a.grantee <> n.nspowner AND NOT (
                a.grantee=(SELECT oid FROM pg_roles WHERE rolname=:agent)
                AND a.privilege_type='USAGE' AND NOT a.is_grantable)
        ) OR EXISTS (
            SELECT 1 FROM objects o WHERE
                o.owner_oid <> (SELECT oid FROM pg_roles WHERE rolname=:owner)
                OR EXISTS (SELECT 1 FROM aclexplode(o.acl) a WHERE a.grantee <> o.owner_oid AND NOT (
                    COALESCE(o.agent_callable, false)
                    AND a.grantee=(SELECT oid FROM pg_roles WHERE rolname=:agent)
                    AND a.privilege_type='EXECUTE' AND NOT a.is_grantable))
        ) OR EXISTS (
            SELECT 1 FROM pg_attribute c
            JOIN pg_class t ON t.oid=c.attrelid
            JOIN namespace n ON n.oid=t.relnamespace
            CROSS JOIN LATERAL aclexplode(c.attacl) a
            WHERE a.grantee <> t.relowner
        )
    """), parameters)
    if drift:
        raise RuntimeError("build guard has unexpected schema or object privileges")
    if revision is not None:
        usage = connection.scalar(text("SELECT has_schema_privilege(:agent, :schema, 'USAGE')"), parameters)
        if usage is not True:
            raise RuntimeError("build guard required schema privilege is absent")
        helpers = ["reject_evidence_mutation()", *additional_helpers]
        if revision in {"build_guard_0002", "build_guard_0003", "build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append("assert_current_source(uuid,uuid,jsonb,bytea,text)")
        if revision in {"build_guard_0003", "build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend(("canonical_plan_json(jsonb)",
                "assert_plan_fields(jsonb,text[],text[],text[],text[],text[])", "assert_plan_contract(jsonb,bytea)"))
        if revision in {"build_guard_0005", "build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append("assert_native_closure_plan(jsonb,jsonb)")
        if revision in {"build_guard_0006", "build_guard_0007", "build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append("demand_timestamp(timestamptz)")
        if revision in {"build_guard_0008", "build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append("assert_bootstrap(jsonb,bytea,text,jsonb)")
        if revision in {"build_guard_0009", "build_guard_0010", "build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append(f"execution_receipt({SCHEMA}.execution_events)")
        if revision in {"build_guard_0011", "build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend(("assert_bootstrap_not_revoked(uuid)", f"bootstrap_revocation_receipt({SCHEMA}.bootstrap_revocations)", "observe_bootstrap_revocation(uuid,jsonb)"))
        if revision in {"build_guard_0012", "build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend((f"worker_withdrawal_receipt({SCHEMA}.worker_withdrawals)",
                "observe_worker_withdrawal(uuid,jsonb)"))
        if revision in {"build_guard_0013", "build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append(f"terminal_inventory_receipt({SCHEMA}.terminal_inventory)")
        if revision in {"build_guard_0014", "build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append("protected_release_publication(uuid,bigint)")
        if revision in {"build_guard_0015", "build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.append(f"hold_retirement_receipt({SCHEMA}.hold_retirements)")
        if revision in {"build_guard_0016", "build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend((f"worker_registration_receipt({SCHEMA}.worker_registrations)", "observe_registered_worker(uuid,jsonb)"))
        if revision in {"build_guard_0017", "build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend((f"native_claim_receipt({SCHEMA}.platform_claims)",
                "native_claim_high_water(uuid)", "fixed_native_claims(uuid)"))
        if revision in {"build_guard_0018", "build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend((f"native_drain_receipt({SCHEMA}.worker_drains)", "native_worker_drain(uuid)"))
        if revision in {"build_guard_0019", "build_guard_0020", "build_guard_0021"}:
            helpers.extend((f"native_outcome_receipt({SCHEMA}.platform_outcomes)",
                "native_live_claim_count(uuid)", "native_request_finished(uuid)"))
        if revision == "build_guard_0021":
            helpers.extend((f"native_release_receipt({SCHEMA}.worker_releases)", "native_worker_release(uuid)",
                "native_release(uuid,jsonb,bytea,text,text,text)"))
        for signature in helpers:
            present = connection.scalar(text("""
                SELECT EXISTS (SELECT 1 FROM pg_proc p WHERE p.oid=to_regprocedure(:signature)
                    AND pg_get_userbyid(p.proowner)=:owner AND NOT p.prosecdef
                    AND p.proconfig=ARRAY['search_path=pg_catalog']::text[])
            """), {"signature": f"{SCHEMA}.{signature}", "owner": owner})
            if present is not True:
                raise RuntimeError("build guard required helper surface is absent or changed")
    for signature in callables:
        surface = connection.scalar(text("""
            SELECT EXISTS (SELECT 1 FROM pg_proc p
                WHERE p.oid=to_regprocedure(:signature)
                  AND pg_get_userbyid(p.proowner)=:owner
                  AND p.prosecdef AND p.proconfig=ARRAY['search_path=pg_catalog']::text[]
                  AND EXISTS (SELECT 1 FROM aclexplode(p.proacl) a
                    WHERE a.grantee=(SELECT oid FROM pg_roles WHERE rolname=:agent)
                      AND a.privilege_type='EXECUTE' AND NOT a.is_grantable))
        """), {**parameters, "signature": signature})
        if surface is not True:
            raise RuntimeError("build guard required callable surface is absent or changed")
