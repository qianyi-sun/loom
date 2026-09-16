"""Canonical public application catalog observations, not transfer authority.

The protected release must supply an independent trusted reference. This reader
does not bless a live snapshot, pin a release, acquire ownership locks, transfer
objects, or certify credential/session retirement. The eventual handoff must
compare again after taking its exact locks in the ownership transaction.
Relevant DDL must be externally serialized throughout observation; PostgreSQL
deparsers use catalog caches that do not follow the query's MVCC snapshot.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from psycopg.pq import TransactionStatus

from loom.application_database_connection import ApplicationDatabaseConnection, application_sql


class ApplicationSchemaInventoryError(RuntimeError):
    """A complete canonical application catalog observation was unavailable."""


@dataclass(frozen=True, order=True, slots=True)
class ApplicationSchemaObject:
    kind: str
    identity: str
    definition_sha256: str


@dataclass(frozen=True, slots=True)
class ApplicationSchemaDifference:
    kind: str
    identity: str
    change: Literal["missing", "unexpected", "changed"]


@dataclass(frozen=True, slots=True)
class ApplicationSchemaInventory:
    postgres_major: int
    objects: tuple[ApplicationSchemaObject, ...]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            _canonical(
                {
                    "schema_version": 1,
                    "postgres_major": self.postgres_major,
                    "objects": [(o.kind, o.identity, o.definition_sha256) for o in self.objects],
                }
            )
        ).hexdigest()

    def differences_from(
        self, reference: ApplicationSchemaInventory
    ) -> tuple[ApplicationSchemaDifference, ...]:
        observed = {(o.kind, o.identity): o.definition_sha256 for o in self.objects}
        expected = {(o.kind, o.identity): o.definition_sha256 for o in reference.objects}
        differences = []
        if self.postgres_major != reference.postgres_major:
            differences.append(ApplicationSchemaDifference("server", "postgres_major", "changed"))
        for kind, identity in sorted(observed.keys() | expected.keys()):
            key = kind, identity
            if key not in observed:
                differences.append(ApplicationSchemaDifference(kind, identity, "missing"))
            elif key not in expected:
                differences.append(ApplicationSchemaDifference(kind, identity, "unexpected"))
            elif observed[key] != expected[key]:
                differences.append(ApplicationSchemaDifference(kind, identity, "changed"))
        return tuple(differences)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _normalize(value: object, role_bindings: Mapping[str, str]) -> object:
    if isinstance(value, dict):
        if set(value) == {"$role"}:
            role = value["$role"]
            if role is None:
                return {"public": True}
            if not isinstance(role, str):
                raise ApplicationSchemaInventoryError("invalid catalog role identity")
            if role in role_bindings:
                return {"binding": role_bindings[role]}
            return {"unbound_role": role}
        normalized = {}
        for key, item in value.items():
            result = _normalize(item, role_bindings)
            if key in {"acl", "policy_roles"} and isinstance(result, list):
                result.sort(key=_canonical)
            normalized[key] = result
        return normalized
    if isinstance(value, list):
        return [_normalize(item, role_bindings) for item in value]
    return value


def _acl(expression: str) -> str:
    # expression is exclusively a source-code constant, never caller SQL.
    return f"""COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'grantor', jsonb_build_object('$role', pg_get_userbyid(a.grantor)),
      'grantee', jsonb_build_object('$role', CASE WHEN a.grantee=0 THEN NULL
                                              ELSE pg_get_userbyid(a.grantee) END),
      'privilege', a.privilege_type, 'grantable', a.is_grantable))
      FROM aclexplode({expression}) AS a), '[]'::jsonb)"""


_CATALOG_SQL = f"""
WITH RECURSIVE public_relations AS (
  SELECT c.* FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='public'
), namespaced_objects AS (
  SELECT d.classid,d.objid FROM pg_depend d JOIN pg_namespace n ON n.oid=d.refobjid
  WHERE d.refclassid='pg_namespace'::regclass AND n.nspname='public'
), scoped_objects AS (
  SELECT classid,objid FROM namespaced_objects
  UNION SELECT 'pg_namespace'::regclass,n.oid FROM pg_namespace n WHERE n.nspname='public'
  UNION SELECT 'pg_class'::regclass,c.oid FROM public_relations c
  UNION SELECT 'pg_type'::regclass,t.oid FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
    WHERE n.nspname='public'
  UNION SELECT 'pg_attrdef'::regclass,d.oid FROM pg_attrdef d JOIN public_relations c ON c.oid=d.adrelid
  UNION SELECT 'pg_trigger'::regclass,t.oid FROM pg_trigger t JOIN public_relations c ON c.oid=t.tgrelid
  UNION SELECT 'pg_constraint'::regclass,c.oid FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace
    WHERE n.nspname='public'
  UNION SELECT 'pg_rewrite'::regclass,r.oid FROM pg_rewrite r JOIN public_relations c ON c.oid=r.ev_class
  UNION SELECT 'pg_policy'::regclass,p.oid FROM pg_policy p JOIN public_relations c ON c.oid=p.polrelid
), referenced_objects AS (
  SELECT classid,objid FROM scoped_objects
  UNION SELECT d.refclassid,d.refobjid FROM pg_depend d
    JOIN referenced_objects r ON r.classid=d.classid AND r.objid=d.objid
), referenced_access_methods AS (
  SELECT c.relam AS oid FROM referenced_objects r
    JOIN pg_class c ON r.classid='pg_class'::regclass AND r.objid=c.oid WHERE c.relam<>0
  UNION SELECT objid FROM referenced_objects WHERE classid='pg_am'::regclass
), safety AS MATERIALIZED (
  SELECT EXISTS (
    SELECT 1 FROM referenced_objects r JOIN pg_type t ON r.classid='pg_type'::regclass AND r.objid=t.oid
    CROSS JOIN LATERAL unnest(ARRAY[t.typoutput,t.typmodout]) AS callback(function_oid)
    LEFT JOIN pg_proc p ON p.oid=callback.function_oid LEFT JOIN pg_namespace n ON n.oid=p.pronamespace
    LEFT JOIN pg_language l ON l.oid=p.prolang
    WHERE callback.function_oid<>0
      AND (n.nspname IS DISTINCT FROM 'pg_catalog' OR l.lanname IS DISTINCT FROM 'internal')
  ) AS unsafe_output,
  EXISTS (
    SELECT 1 FROM referenced_access_methods r LEFT JOIN pg_am a ON a.oid=r.oid
    LEFT JOIN pg_proc p ON p.oid=a.amhandler LEFT JOIN pg_namespace n ON n.oid=p.pronamespace
    LEFT JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname IS DISTINCT FROM 'pg_catalog' OR l.lanname IS DISTINCT FROM 'internal'
  ) AS unsafe_access_method,
  EXISTS (SELECT 1 FROM namespaced_objects WHERE classid NOT IN (
    'pg_class'::regclass,'pg_proc'::regclass,'pg_type'::regclass,
    'pg_constraint'::regclass,'pg_default_acl'::regclass)) AS unsupported_object,
  EXISTS (SELECT 1 FROM pg_depend d JOIN scoped_objects s ON s.classid=d.classid AND s.objid=d.objid
          WHERE d.refclassid='pg_extension'::regclass AND d.deptype='e') AS extension_member
), observations(kind, identity, definition) AS (
  SELECT 'database', jsonb_build_array('application'), jsonb_build_object(
    'owner', jsonb_build_object('$role', pg_get_userbyid(d.datdba)),
    'acl', {_acl("COALESCE(d.datacl, acldefault('d', d.datdba))")},
    'encoding', pg_encoding_to_char(d.encoding), 'collate', d.datcollate,
    'ctype', d.datctype, 'locale_provider', d.datlocprovider,
    'icu_locale', d.daticulocale, 'icu_rules', d.daticurules,
    'template', d.datistemplate)
  FROM pg_database d WHERE datname=current_database()
  UNION ALL
  SELECT 'schema', jsonb_build_array(n.nspname), jsonb_build_object(
    'owner', jsonb_build_object('$role', pg_get_userbyid(n.nspowner)),
    'acl', {_acl("COALESCE(n.nspacl, acldefault('n', n.nspowner))")})
  FROM pg_namespace n WHERE n.nspname='public'
  UNION ALL
  SELECT 'relation', jsonb_build_array('public', c.relname), jsonb_build_object(
    'owner', jsonb_build_object('$role', pg_get_userbyid(c.relowner)),
    'acl', {_acl("COALESCE(c.relacl, CASE WHEN c.relkind='S' THEN acldefault('s',c.relowner) ELSE acldefault('r',c.relowner) END)")},
    'kind', c.relkind, 'persistence', c.relpersistence,
    'access_method', (SELECT amname FROM pg_am WHERE oid=c.relam),
    'options', c.reloptions, 'replica_identity', c.relreplident,
    'row_security', c.relrowsecurity, 'force_row_security', c.relforcerowsecurity,
    'partition', c.relispartition, 'partition_bound', pg_get_expr(c.relpartbound,c.oid),
    'partition_key', CASE WHEN c.relkind='p' THEN pg_get_partkeydef(c.oid) END,
    'of_type', c.reloftype::regtype::text)
  FROM public_relations c
  UNION ALL
  SELECT 'column', jsonb_build_array('public', c.relname, a.attnum, a.attname),
    jsonb_build_object('type', format_type(a.atttypid,a.atttypmod),
      'collation', a.attcollation::regcollation::text, 'not_null', a.attnotnull,
      'identity', a.attidentity, 'generated', a.attgenerated, 'dropped', a.attisdropped,
      'local', a.attislocal, 'inherit_count', a.attinhcount, 'dimensions', a.attndims,
      'storage', a.attstorage, 'compression', a.attcompression,
      'options', a.attoptions, 'foreign_options', a.attfdwoptions,
      'default', (SELECT pg_get_expr(d.adbin,d.adrelid) FROM pg_attrdef d
                  WHERE d.adrelid=c.oid AND d.adnum=a.attnum),
      'acl', {_acl("a.attacl")})
  FROM public_relations c JOIN pg_attribute a ON a.attrelid=c.oid WHERE a.attnum>0
  UNION ALL
  SELECT 'index', jsonb_build_array('public', c.relname), jsonb_build_object(
    'definition', pg_get_indexdef(c.oid), 'valid', i.indisvalid, 'ready', i.indisready,
    'live', i.indislive, 'replica_identity', i.indisreplident, 'clustered', i.indisclustered)
  FROM public_relations c JOIN pg_index i ON i.indexrelid=c.oid
  UNION ALL
  SELECT 'sequence', jsonb_build_array('public', c.relname), jsonb_build_object(
    'type', s.seqtypid::regtype::text, 'start', s.seqstart, 'increment', s.seqincrement,
    'min', s.seqmin, 'max', s.seqmax, 'cache', s.seqcache, 'cycle', s.seqcycle,
    'owned_by', (SELECT jsonb_agg(jsonb_build_array(d.refobjid::regclass::text,
                                                 d.refobjsubid,d.deptype)
                                ORDER BY d.refobjid::regclass::text,d.refobjsubid,d.deptype)
      FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid
        AND d.refclassid='pg_class'::regclass AND d.deptype IN ('a','i')))
  FROM public_relations c JOIN pg_sequence s ON s.seqrelid=c.oid
  UNION ALL
  SELECT 'constraint', jsonb_build_array('public',
      CASE WHEN c.conrelid<>0 THEN c.conrelid::regclass::text ELSE c.contypid::regtype::text END,
      c.conname), jsonb_build_object('definition',pg_get_constraintdef(c.oid),
        'validated',c.convalidated,'local',c.conislocal,'inherit_count',c.coninhcount,
        'parent', (SELECT jsonb_build_array(p.conrelid::regclass::text,p.conname)
                   FROM pg_constraint p WHERE p.oid=c.conparentid))
  FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public'
  UNION ALL
  SELECT 'trigger', jsonb_build_array(t.tgrelid::regclass::text,
      CASE WHEN t.tgisinternal THEN con.conrelid::regclass::text ELSE t.tgname END,
      CASE WHEN t.tgisinternal THEN con.conname ELSE '' END,
      t.tgfoid::regprocedure::text,t.tgtype), jsonb_build_object(
    'definition', CASE WHEN NOT t.tgisinternal THEN pg_get_triggerdef(t.oid) END,
    'internal',t.tgisinternal,'enabled',t.tgenabled,'type',t.tgtype,
    'function',t.tgfoid::regprocedure::text,'columns',t.tgattr::text,
    'arguments',encode(t.tgargs,'hex'),'argument_count',t.tgnargs,
    'deferrable',t.tgdeferrable,'initially_deferred',t.tginitdeferred,
    'old_table',t.tgoldtable,'new_table',t.tgnewtable,
    'constraint',CASE WHEN t.tgconstraint<>0 THEN pg_get_constraintdef(t.tgconstraint) END)
  FROM pg_trigger t JOIN public_relations c ON c.oid=t.tgrelid
    LEFT JOIN pg_constraint con ON con.oid=t.tgconstraint
  UNION ALL
  SELECT 'routine', jsonb_build_array('public',p.proname,pg_get_function_identity_arguments(p.oid)),
    jsonb_build_object('owner',jsonb_build_object('$role',pg_get_userbyid(p.proowner)),
      'kind',p.prokind,'definition',CASE WHEN p.prokind IN ('f','p') THEN pg_get_functiondef(p.oid) END,
      'acl',{_acl("COALESCE(p.proacl, acldefault('f',p.proowner))")})
  FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
  UNION ALL
  SELECT 'type', jsonb_build_array('public',t.typname), jsonb_build_object(
    'owner',jsonb_build_object('$role',pg_get_userbyid(t.typowner)),
    'acl',{_acl("COALESCE(t.typacl, acldefault('T',t.typowner))")},
    'kind',t.typtype,'category',t.typcategory,'defined',t.typisdefined,
    'length',t.typlen,'by_value',t.typbyval,'alignment',t.typalign,'storage',t.typstorage,
    'not_null',t.typnotnull,'delimiter',t.typdelim,'preferred',t.typispreferred,
    'relation',t.typrelid::regclass::text,'element',t.typelem::regtype::text,
    'array',t.typarray::regtype::text,'base',t.typbasetype::regtype::text,
    'collation',t.typcollation::regcollation::text,'type_modifier',t.typtypmod,
    'dimensions',t.typndims,'default',pg_get_expr(t.typdefaultbin,0),
    'default_text',t.typdefault,'input',t.typinput::regprocedure::text,
    'output',t.typoutput::regprocedure::text,'receive',t.typreceive::regprocedure::text,
    'send',t.typsend::regprocedure::text,'modifier_input',t.typmodin::regprocedure::text,
    'modifier_output',t.typmodout::regprocedure::text,'analyze',t.typanalyze::regprocedure::text,
    'subscript',t.typsubscript::regprocedure::text,
    'enum', (SELECT jsonb_agg(e.enumlabel ORDER BY e.enumsortorder) FROM pg_enum e WHERE e.enumtypid=t.oid),
    'range',(SELECT jsonb_build_object('subtype',r.rngsubtype::regtype::text,
      'multirange',r.rngmultitypid::regtype::text,'collation',r.rngcollation::regcollation::text,
      'canonical',r.rngcanonical::regprocedure::text,'difference',r.rngsubdiff::regprocedure::text,
      'operator_class',(SELECT quote_ident(n.nspname)||'.'||quote_ident(o.opcname) FROM pg_opclass o
                        JOIN pg_namespace n ON n.oid=o.opcnamespace WHERE o.oid=r.rngsubopc))
      FROM pg_range r WHERE r.rngtypid=t.oid))
  FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public'
  UNION ALL
  SELECT 'rule',jsonb_build_array(r.ev_class::regclass::text,r.rulename),
    jsonb_build_object('definition',pg_get_ruledef(r.oid),'enabled',r.ev_enabled)
  FROM pg_rewrite r JOIN public_relations c ON c.oid=r.ev_class
  UNION ALL
  SELECT 'policy',jsonb_build_array(p.polrelid::regclass::text,p.polname),jsonb_build_object(
    'command',p.polcmd,'permissive',p.polpermissive,
    'using',pg_get_expr(p.polqual,p.polrelid),'check',pg_get_expr(p.polwithcheck,p.polrelid),
    'policy_roles',(SELECT jsonb_agg(jsonb_build_object('$role',
      CASE WHEN r=0 THEN NULL ELSE pg_get_userbyid(r) END)) FROM unnest(p.polroles) r))
  FROM pg_policy p JOIN public_relations c ON c.oid=p.polrelid
  UNION ALL
  SELECT 'inheritance',jsonb_build_array(i.inhrelid::regclass::text,i.inhparent::regclass::text),
    jsonb_build_object('sequence',i.inhseqno,'detach_pending',i.inhdetachpending)
  FROM pg_inherits i WHERE i.inhrelid IN (SELECT oid FROM public_relations)
                       OR i.inhparent IN (SELECT oid FROM public_relations)
  UNION ALL
  SELECT 'default_acl',jsonb_build_array(jsonb_build_object('$role',pg_get_userbyid(d.defaclrole)),
      COALESCE(n.nspname,'*'),d.defaclobjtype),jsonb_build_object('acl',{_acl("d.defaclacl")})
  FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace
  WHERE n.nspname='public' OR d.defaclnamespace=0 AND pg_get_userbyid(d.defaclrole)=ANY({{}})
)
SELECT s.unsafe_output,s.unsafe_access_method,s.unsupported_object,s.extension_member,
  CASE WHEN s.unsafe_output OR s.unsafe_access_method OR s.unsupported_object OR s.extension_member THEN NULL
       ELSE (SELECT jsonb_agg(jsonb_build_array(kind,identity,definition) ORDER BY kind,identity)
             FROM (SELECT kind,identity,definition FROM observations ORDER BY kind,identity LIMIT 10001) bounded)
  END FROM safety s
"""

_CATALOG_SETTINGS = {
    # Explicit pg_temp placement prevents its implicit precedence over catalogs.
    "search_path": "pg_catalog, pg_temp",
    "quote_all_identifiers": "off",
    "timezone": "UTC",
    "datestyle": "ISO, YMD",
    "intervalstyle": "postgres",
    "extra_float_digits": "3",
    "bytea_output": "hex",
    "standard_conforming_strings": "on",
    "lc_monetary": "C",
    "lc_numeric": "C",
}


def require_application_event_trigger_policy(connection: ApplicationDatabaseConnection) -> None:
    """Refuse all database-wide callbacks; never disable or adopt an existing policy.

    The caller must serialize event-trigger DDL throughout observation/use. On
    PG17 the privileged connection also needs admitted startup-time LOGIN-trigger
    protection BEFORE entering this helper. This query cannot provide that proof.
    Include disabled triggers and functions outside public; neither belongs to
    the independently provisioned application reference.
    """
    if connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger)"
    ).fetchone() != (False,):
        raise ApplicationSchemaInventoryError("application database event trigger policy is not admitted")


def read_application_schema_inventory(
    connection: ApplicationDatabaseConnection, *, role_bindings: Mapping[str, str]
) -> ApplicationSchemaInventory:
    """Observe catalogs in an active READ COMMITTED transaction with DDL quiesced.

    The caller must externally serialize relevant DDL throughout this operation.
    This is not a safe privileged observer against concurrently writable schemas:
    deparsers can use newer catalog caches than the query's dependency snapshot.
    No locks or session retirement are supplied by this helper.
    Explicit role aliases normalize independent trusted installations; they are
    not an ownership assertion. Definitions and literal SQL bodies are never
    rewritten. Only catalog tables are read, not even alembic_version rows. A
    same-statement safety gate refuses unsupported namespaced/extension objects
    and referenced non-native type-output/access-method functions BEFORE any deparser runs.
    Catalog settings are restored on success and savepoint rollback. Object/byte
    limits are acceptance limits, not resource ceilings for the PostgreSQL query.
    """
    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise ApplicationSchemaInventoryError("catalog observation requires an active transaction")
    postgres_major = connection.info.server_version // 10000
    if postgres_major not in {16, 17}:
        raise ApplicationSchemaInventoryError("catalog observation requires PostgreSQL 16 or 17")
    # PG17 renamed the catalog column. Choose before SQL parsing, keeping the
    # canonical field and PG16 release pin unchanged (a CASE still parses both).
    catalog_sql = (
        _CATALOG_SQL
        if postgres_major == 16
        else _CATALOG_SQL.replace("d.daticulocale", "d.datlocale")
    )
    role_bindings = dict(role_bindings)
    if (
        len(role_bindings) > 16
        or len(set(role_bindings.values())) != len(role_bindings)
        or any(
            re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None
            or re.fullmatch(r"[a-z][a-z0-9-]{0,62}", alias) is None
            for role, alias in role_bindings.items()
        )
    ):
        raise ApplicationSchemaInventoryError("catalog role bindings are invalid")
    try:
        with connection.transaction():
            require_application_event_trigger_policy(connection)
            if connection.execute(
                "SELECT pg_catalog.current_setting('transaction_isolation')"
            ).fetchone() != ("read committed",):
                raise ApplicationSchemaInventoryError(
                    "catalog observation requires READ COMMITTED and externally serialized DDL"
                )
            previous = connection.execute(
                application_sql(
                    "SELECT name,pg_catalog.current_setting(name) "
                    "FROM pg_catalog.unnest({}::pg_catalog.text[]) AS setting(name)",
                    list(_CATALOG_SETTINGS),
                )
            ).fetchall()
            for name, value in _CATALOG_SETTINGS.items():
                connection.execute(
                    application_sql("SELECT pg_catalog.set_config({},{},true)", name, value)
                )
            result = connection.execute(
                application_sql(catalog_sql, list(role_bindings))
            ).fetchone()
            for previous_name, previous_value in reversed(previous):
                connection.execute(
                    application_sql(
                        "SELECT pg_catalog.set_config({},{},true)", previous_name, previous_value
                    )
                )
        if result is None:
            raise ApplicationSchemaInventoryError(
                "application catalog safety observation is missing"
            )
        unsafe_output, unsafe_access_method, unsupported_object, extension_member, rows = result
        if unsafe_output:
            raise ApplicationSchemaInventoryError("application catalog type output is not native")
        if unsafe_access_method:
            raise ApplicationSchemaInventoryError("application catalog access method is not native")
        if unsupported_object:
            raise ApplicationSchemaInventoryError("unsupported application schema object")
        if extension_member:
            raise ApplicationSchemaInventoryError(
                "application catalog has extension-managed objects"
            )
        if not isinstance(rows, list):
            raise ApplicationSchemaInventoryError("application catalog observations are missing")
        if len(rows) > 10000:
            raise ApplicationSchemaInventoryError("application catalog object bound exceeded")
        objects = []
        total_bytes = 0
        for kind, identity, definition in rows:
            if not isinstance(kind, str) or not isinstance(definition, (dict, list)):
                raise ApplicationSchemaInventoryError("application catalog record is incomplete")
            if (
                kind == "routine"
                and isinstance(definition, dict)
                and definition.get("kind") not in {"f", "p"}
            ):
                raise ApplicationSchemaInventoryError("unsupported application routine kind")
            if (
                kind == "relation"
                and isinstance(definition, dict)
                and definition.get("kind") not in {"r", "p", "S", "i", "I", "v", "m", "c"}
            ):
                raise ApplicationSchemaInventoryError("unsupported application relation kind")
            key = _canonical(_normalize(identity, role_bindings)).decode("utf-8")
            payload = _canonical(_normalize(definition, role_bindings))
            total_bytes += len(payload) + len(key.encode("utf-8"))
            if len(payload) > 1024 * 1024 or total_bytes > 16 * 1024 * 1024:
                raise ApplicationSchemaInventoryError("application catalog byte bound exceeded")
            objects.append(ApplicationSchemaObject(kind, key, hashlib.sha256(payload).hexdigest()))
        if len({(o.kind, o.identity) for o in objects}) != len(objects):
            raise ApplicationSchemaInventoryError("application catalog identity is ambiguous")
        return ApplicationSchemaInventory(postgres_major, tuple(sorted(objects)))
    except ApplicationSchemaInventoryError:
        raise
    except Exception:
        raise ApplicationSchemaInventoryError("application catalog observation failed") from None


__all__ = [
    "ApplicationSchemaDifference",
    "ApplicationSchemaInventory",
    "ApplicationSchemaInventoryError",
    "ApplicationSchemaObject",
    "read_application_schema_inventory",
    "require_application_event_trigger_policy",
]
