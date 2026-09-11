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
    if revision in {"build_guard_0003", "build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007"}:
        callables.append(f"{SCHEMA}.prepare_plan(uuid,jsonb,bytea,text,jsonb)")
    if revision in {"build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007"}:
        callables.append(f"{SCHEMA}.authorize_publication(uuid,uuid)")
    if revision in {"build_guard_0005", "build_guard_0006", "build_guard_0007"}:
        callables.extend((f"{SCHEMA}.close_plan(uuid,jsonb,bytea,text)",
            f"{SCHEMA}.authorize_closure_publication(uuid,uuid)"))
    if revision in {"build_guard_0006", "build_guard_0007"}:
        callables.extend((f"{SCHEMA}.capture_demand(uuid,bigint,jsonb)", f"{SCHEMA}.read_demand(uuid)"))
    if revision == "build_guard_0007":
        callables.append(f"{SCHEMA}.read_pending_sources(uuid)")
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
        helpers = ["reject_evidence_mutation()"]
        if revision in {"build_guard_0002", "build_guard_0003", "build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007"}:
            helpers.append("assert_current_source(uuid,uuid,jsonb,bytea,text)")
        if revision in {"build_guard_0003", "build_guard_0004", "build_guard_0005", "build_guard_0006", "build_guard_0007"}:
            helpers.extend(("canonical_plan_json(jsonb)",
                "assert_plan_fields(jsonb,text[],text[],text[],text[],text[])", "assert_plan_contract(jsonb,bytea)"))
        if revision in {"build_guard_0005", "build_guard_0006", "build_guard_0007"}:
            helpers.append("assert_native_closure_plan(jsonb,jsonb)")
        if revision in {"build_guard_0006", "build_guard_0007"}:
            helpers.append("demand_timestamp(timestamptz)")
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
