"""Reject unexpected authority instead of silently preserving privilege drift."""

from sqlalchemy import Connection, text

SCHEMA = "loom_capacity_build_guard"


def verify_migration_privileges(connection: Connection, *, owner: str, agent: str) -> None:
    """Check explicit defaults and effective schema/object/column ACLs on every run.

    This initial, non-executable schema grants the agent only schema USAGE.
    Future callable procedures must amend this verifier with their exact surface.
    The check is also valid before initial creation and after empty downgrade.
    """
    parameters = {"schema": SCHEMA, "owner": owner, "agent": agent}
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
                    c.relowner)) AS acl
            FROM pg_class c JOIN namespace n ON n.oid=c.relnamespace
            UNION ALL
            SELECT p.proowner, COALESCE(p.proacl, acldefault('f', p.proowner))
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
                OR EXISTS (SELECT 1 FROM aclexplode(o.acl) a WHERE a.grantee <> o.owner_oid)
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
