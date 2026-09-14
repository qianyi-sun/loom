"""Fixed public-table cleanup authority for the trusted guard migration.

This helper is not a standalone retirement API. The caller must check the private
uninitialized fence and perform all cleanup in the same transaction.
"""

import hashlib
import re

from psycopg import sql

from loom.application_database_admission import (
    ApplicationDatabaseCoordinationGuard,
    coordination_guard_handoff_predicate,
)
from loom.application_schema_reference import (
    ApplicationSchemaRevision,
    application_schema_revisions,
)

_BODY = """
BEGIN
  LOCK TABLE ONLY public.trials IN ACCESS EXCLUSIVE MODE NOWAIT;
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_inherits
              WHERE inhparent = 'public.trials'::regclass) THEN
    RAISE EXCEPTION 'trial writer retirement inheritance is unsupported'
      USING ERRCODE = '55000';
  END IF;
  IF (SELECT count(*) FROM pg_catalog.pg_trigger AS t
        JOIN pg_catalog.pg_proc AS p ON p.oid = t.tgfoid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
       WHERE t.tgrelid = 'public.trials'::regclass
         AND n.nspname = 'loom_capacity_guard'
         AND p.pronargs = 0 AND p.prorettype = 'trigger'::regtype
         AND NOT t.tgisinternal AND t.tgenabled = 'O'
         AND t.tgnargs = 0 AND t.tgargs = ''::bytea AND t.tgqual IS NULL
         AND t.tgparentid = 0 AND t.tgconstraint = 0
         AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
         AND t.tgattr = ''::int2vector
         AND ((t.tgname = 'capacity_guard_lock_trial_writer'
               AND p.proname = 'lock_trial_writer_statement' AND t.tgtype = 62)
           OR (t.tgname = 'zz_capacity_guard_account_trial_writer'
               AND p.proname = 'account_trial_writer_mutation' AND t.tgtype = 29))) <> 2 THEN
    RAISE EXCEPTION 'trial writer trigger retirement identity changed'
      USING ERRCODE = '55000';
  END IF;
  DROP TRIGGER zz_capacity_guard_account_trial_writer ON public.trials;
  DROP TRIGGER capacity_guard_lock_trial_writer ON public.trials;
END
"""


def application_trigger_owner_handoff_ddl(
    *, previous_owner: str, application_owner: str, guard_owner: str,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
    schema_revision: ApplicationSchemaRevision = "0142/guard_0033",
) -> sql.Composed:
    """Move the revision-bound canonical definers in a protected ownership transaction.

    The baseline has two application bridges; the current revision also has its
    retirement helper and requires the uninitialized writer fence. Revision
    selection comes from the original protected checkpoint, never live discovery.
    The caller must first transfer public.trials in the SAME transaction and
    complete the rest of application ownership/credential/session convergence.
    This substep neither seals the database/schema owner nor permits activation.
    It requires existing database-administrator authority, not a definer bypass.
    The previous login/memberships must be sealed in a prior committed phase and this database
    quiescent: stale SET ROLE sessions are not identified by their login name.
    The optional durably captured rollout guard must hold its exact lock in the
    still-closed database; its process/role authority is admitted by the caller.
    """
    application_schema_revisions(schema_revision)
    baseline = schema_revision == "0134/guard_0030"
    roles = (previous_owner, application_owner, guard_owner)
    if len(set(roles)) != 3 or any(
        re.fullmatch(r"[a-z][a-z0-9_]{0,62}", role) is None for role in roles
    ):
        raise ValueError("application trigger handoff roles are invalid")
    # Exact prosrc hashes from application migrations 0127 and 0132. Real
    # migration-to-handoff tests verify these pins; the installed wheel must
    # not import application migrations, which are deployment image payload.
    # Never adopt an arbitrary live SECURITY DEFINER body under a stronger owner.
    definers: tuple[tuple[str, str, str], ...] = (
        ("loom_drop_trial_writer_triggers", hashlib.sha256(_BODY.encode()).hexdigest(), "void"),
        (
            "loom_close_protected_runtime_trial_claim",
            "e3dcf5b0e35a49c7c048b8ae298212ee5c13cebc8b6bf9b64444bd2278e15044",
            "trigger",
        ),
        (
            "loom_transform_protected_runtime_trial_requeue",
            "764078e4f54716642d962a293f5291eb76381ed02867e9fa02bfdb5f464e00ef",
            "trigger",
        ),
    )
    if baseline:
        definers = definers[1:]
    expected = sql.SQL(", ").join(
        sql.SQL("({}, {}, {})").format(*(sql.Literal(value) for value in item)) for item in definers
    )
    guarded = (
        "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)",
        "loom_capacity_guard.transform_protected_runtime_trial_requeue"
        "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)",
    )
    fence_admission = sql.SQL("""
          IF pg_catalog.to_regclass('loom_capacity_guard.trial_writer_fence') IS NOT NULL
             OR pg_catalog.to_regprocedure('public.loom_drop_trial_writer_triggers()') IS NOT NULL
             OR (SELECT array_agg(version_num::text ORDER BY version_num)
                 FROM loom_capacity_guard.capacity_guard_alembic_version)
                 IS DISTINCT FROM ARRAY['guard_0030']::text[] THEN
            RAISE EXCEPTION 'application trigger baseline guard revision changed' USING ERRCODE='55000';
          END IF;
    """ if baseline else """
          IF (SELECT relowner FROM pg_catalog.pg_class
               WHERE oid = 'loom_capacity_guard.trial_writer_fence'::regclass)
             IS DISTINCT FROM v_guard THEN
            RAISE EXCEPTION 'application trigger guard owner changed' USING ERRCODE = '55000';
          END IF;
          PERFORM 1 FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 AND writer_incarnation IS NULL FOR UPDATE NOWAIT;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'application trigger handoff requires an uninitialized writer'
              USING ERRCODE = '55000';
          END IF;
    """)
    return sql.SQL(
        """
        DO $handoff$
        DECLARE
          v_previous oid := pg_catalog.to_regrole({previous})::oid;
          v_target oid := pg_catalog.to_regrole({target})::oid;
          v_guard oid := pg_catalog.to_regrole({guard})::oid;
          v_function record;
          v_replay boolean;
          v_guarded text;
          v_lock_timeout text;
        BEGIN
          IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'application trigger handoff requires READ COMMITTED'
              USING ERRCODE = '25001';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                          WHERE rolname = current_user AND rolsuper)
             OR v_previous IS NULL OR v_target IS NULL OR v_guard IS NULL THEN
            RAISE EXCEPTION 'application trigger handoff requires protected administrator authority'
              USING ERRCODE = '42501';
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_authid WHERE oid = v_target
              AND NOT rolcanlogin AND NOT rolinherit AND NOT rolsuper
              AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication
              AND NOT rolbypassrls AND rolpassword IS NULL
          ) OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members
                       WHERE member = v_target OR roleid = v_target) THEN
            RAISE EXCEPTION 'application trigger target owner is not sealed'
              USING ERRCODE = '42501';
          END IF;
          PERFORM pg_catalog.pg_stat_clear_snapshot();
          IF {has_coordination_guard} AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_stat_activity AS a WHERE {coordination_guard_match}
          ) THEN
            RAISE EXCEPTION 'application trigger coordination guard changed' USING ERRCODE='55000';
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE oid=v_previous
                         AND NOT rolcanlogin AND NOT rolsuper AND NOT rolcreaterole
                         AND NOT rolcreatedb AND NOT rolreplication AND NOT rolbypassrls)
             OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members
                        WHERE member=v_previous OR roleid=v_previous)
             OR EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity AS a
                        LEFT JOIN pg_catalog.pg_roles AS r ON r.oid=a.usesysid
                        WHERE a.datname=current_database() AND (NOT r.rolsuper OR {has_coordination_guard})
                          AND NOT COALESCE(({coordination_guard_match}), false)
                          AND a.pid <> pg_catalog.pg_backend_pid()) THEN
            RAISE EXCEPTION 'application trigger handoff requires quiescent legacy authority'
              USING ERRCODE='55000';
          END IF;
          {fence_admission}
          LOCK TABLE ONLY public.trials IN ACCESS EXCLUSIVE MODE NOWAIT;
          IF (SELECT relowner FROM pg_catalog.pg_class WHERE oid = 'public.trials'::regclass)
               IS DISTINCT FROM v_target
             OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits
                        WHERE inhparent = 'public.trials'::regclass
                           OR inhrelid = 'public.trials'::regclass) THEN
            RAISE EXCEPTION 'application trigger target table ownership is not exact'
              USING ERRCODE = '55000';
          END IF;
          IF (SELECT count(*) FROM pg_catalog.pg_proc AS p
              JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
              WHERE n.nspname = 'public' AND p.proname = ANY({names})) <> {definer_count} THEN
            RAISE EXCEPTION 'application trigger definer set changed' USING ERRCODE = '55000';
          END IF;
          -- Retain catalog write locks before reading the definitions to adopt.
          -- An unchanged COST update does not replace code, owner or ACL, but
          -- serializes with concurrent CREATE OR REPLACE on each exact routine.
          -- Bound catalog-lock waits and preserve an already tighter caller
          -- timeout. Any refusal requires rollback of this complete transaction.
          v_lock_timeout := pg_catalog.current_setting('lock_timeout');
          IF (SELECT setting::integer FROM pg_catalog.pg_settings WHERE name='lock_timeout') = 0
             OR (SELECT setting::integer FROM pg_catalog.pg_settings WHERE name='lock_timeout') > 1000 THEN
            PERFORM pg_catalog.set_config('lock_timeout', '1s', true);
          END IF;
          FOR v_function IN
            SELECT expected.name, p.procost
            FROM (VALUES {expected}) AS expected(name, body_sha256, result_type)
            JOIN pg_catalog.pg_proc AS p
              ON p.oid = pg_catalog.to_regprocedure('public.' || expected.name || '()')
            ORDER BY expected.name
          LOOP
            EXECUTE pg_catalog.format('ALTER FUNCTION public.%I() COST %s',
                                      v_function.name, v_function.procost);
          END LOOP;
          IF (SELECT count(*) FROM pg_catalog.pg_trigger AS t
              JOIN pg_catalog.pg_proc AS p ON p.oid=t.tgfoid
              JOIN pg_catalog.pg_namespace AS n ON n.oid=p.pronamespace
              WHERE n.nspname='public' AND p.proname = ANY({names})) <> 2
             OR (SELECT count(*) FROM pg_catalog.pg_trigger AS t
                 JOIN pg_catalog.pg_proc AS p ON p.oid=t.tgfoid
                 JOIN pg_catalog.pg_namespace AS n ON n.oid=p.pronamespace
                 WHERE n.nspname='public' AND t.tgrelid='public.trials'::regclass
                   AND NOT t.tgisinternal AND t.tgenabled='O' AND t.tgnargs=0
                   AND t.tgargs=''::bytea AND t.tgqual IS NULL AND t.tgparentid=0
                   AND t.tgconstraint=0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
                   AND t.tgattr::text = (SELECT attnum::text FROM pg_catalog.pg_attribute
                     WHERE attrelid='public.trials'::regclass AND attname='state' AND NOT attisdropped)
                   AND ((p.proname='loom_close_protected_runtime_trial_claim'
                         AND t.tgname='capacity_guard_close_protected_runtime_trial_claim' AND t.tgtype=17)
                     OR (p.proname='loom_transform_protected_runtime_trial_requeue'
                         AND t.tgname='capacity_guard_transform_protected_runtime_trial_requeue' AND t.tgtype=19))) <> 2 THEN
            RAISE EXCEPTION 'application trigger attachment authority changed' USING ERRCODE='55000';
          END IF;
          SELECT proowner = v_target INTO STRICT v_replay FROM pg_catalog.pg_proc
           WHERE oid = 'public.loom_close_protected_runtime_trial_claim()'::regprocedure;
          FOR v_function IN
            SELECT p.*, expected.body_sha256, expected.result_type
            FROM (VALUES {expected}) AS expected(name, body_sha256, result_type)
            JOIN pg_catalog.pg_namespace AS n ON n.nspname = 'public'
            JOIN pg_catalog.pg_proc AS p ON p.pronamespace = n.oid AND p.proname = expected.name
          LOOP
            IF v_function.proowner <> (CASE WHEN v_replay THEN v_target ELSE v_previous END)
               OR v_function.pronargs <> 0 OR v_function.prokind <> 'f'
               OR v_function.prorettype <> pg_catalog.to_regtype(v_function.result_type)
               OR NOT v_function.prosecdef OR v_function.proretset OR v_function.proisstrict
               OR v_function.proleakproof OR v_function.prosupport <> 0
               OR v_function.provolatile <> 'v' OR v_function.proparallel <> 'u'
               OR v_function.prolang <> (SELECT oid FROM pg_catalog.pg_language WHERE lanname='plpgsql')
               OR v_function.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']
               OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                    v_function.prosrc, 'UTF8')), 'hex') IS DISTINCT FROM v_function.body_sha256
               OR (v_function.proname='loom_drop_trial_writer_triggers'
                   AND (NOT pg_catalog.has_schema_privilege(v_guard, 'public', 'USAGE')
                        OR NOT pg_catalog.has_function_privilege(v_guard, v_function.oid, 'EXECUTE')))
               OR EXISTS (
                 SELECT 1 FROM pg_catalog.aclexplode(COALESCE(
                   v_function.proacl, pg_catalog.acldefault('f', v_function.proowner))) AS a
                 WHERE (a.grantee <> v_function.proowner
                        AND NOT (v_function.proname = 'loom_drop_trial_writer_triggers'
                                 AND a.grantee = v_guard))
                    OR (a.grantee <> v_function.proowner AND a.is_grantable)
               ) THEN
              RAISE EXCEPTION 'application trigger definer identity or privileges changed'
                USING ERRCODE = '55000';
            END IF;
          END LOOP;
          FOREACH v_guarded IN ARRAY ({guarded})::text[] LOOP
            IF NOT EXISTS (
              SELECT 1 FROM pg_catalog.pg_proc WHERE oid = pg_catalog.to_regprocedure(v_guarded)
                AND proowner = v_guard AND prosecdef
                AND proconfig = ARRAY['search_path=pg_catalog']
            ) OR NOT pg_catalog.has_schema_privilege(
              CASE WHEN v_replay THEN v_target ELSE v_previous END, 'public', 'USAGE')
              OR NOT pg_catalog.has_schema_privilege(
              CASE WHEN v_replay THEN v_target ELSE v_previous END, 'loom_capacity_guard', 'USAGE')
              OR pg_catalog.has_function_privilege(v_target, v_guarded, 'EXECUTE WITH GRANT OPTION')
              OR NOT pg_catalog.has_function_privilege(
              CASE WHEN v_replay THEN v_target ELSE v_previous END, v_guarded, 'EXECUTE')
              OR (v_replay AND pg_catalog.has_function_privilege(v_previous, v_guarded, 'EXECUTE')) THEN
              RAISE EXCEPTION 'application trigger private bridge authority changed'
                USING ERRCODE = '55000';
            END IF;
          END LOOP;
          FOR v_function IN SELECT name FROM (VALUES {expected}) AS expected(name, body, result_type)
          LOOP
            EXECUTE pg_catalog.format('ALTER FUNCTION public.%I() OWNER TO %I',
                                      v_function.name, {target});
          END LOOP;
          GRANT USAGE ON SCHEMA public, loom_capacity_guard TO {target_identifier};
          FOREACH v_guarded IN ARRAY ({guarded})::text[] LOOP
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %I', v_guarded, {target});
            EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %I', v_guarded, {previous});
            IF pg_catalog.has_function_privilege(v_previous, v_guarded, 'EXECUTE')
               OR NOT pg_catalog.has_function_privilege(v_target, v_guarded, 'EXECUTE') THEN
              RAISE EXCEPTION 'application trigger bridge authority was not transferred exactly'
                USING ERRCODE = '42501';
            END IF;
          END LOOP;
          PERFORM pg_catalog.set_config('lock_timeout', v_lock_timeout, true);
        END
        $handoff$;
        """
    ).format(
        fence_admission=fence_admission,
        definer_count=sql.Literal(len(definers)),
        previous=sql.Literal(previous_owner),
        target=sql.Literal(application_owner),
        guard=sql.Literal(guard_owner),
        target_identifier=sql.Identifier(application_owner),
        names=sql.Literal([item[0] for item in definers]),
        expected=expected,
        guarded=sql.Literal(list(guarded)),
        has_coordination_guard=sql.Literal(coordination_guard is not None),
        coordination_guard_match=(sql.SQL("false") if coordination_guard is None
                                  else coordination_guard_handoff_predicate(coordination_guard)),
    )


def trial_writer_trigger_retirement_ddl(*, guard_owner: str) -> sql.Composed:
    """Provision only as the verified public-table owner, refusing helper drift."""

    if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", guard_owner) is None:
        raise ValueError("trial writer guard owner is invalid")
    return sql.SQL(
        """
        DO $provision$
        DECLARE v_owner oid;
        BEGIN
          SELECT relowner INTO STRICT v_owner FROM pg_catalog.pg_class
           WHERE oid = 'public.trials'::regclass;
          IF v_owner <> current_user::regrole::oid THEN
            RAISE EXCEPTION 'trial retirement provisioner must be the public table owner'
              USING ERRCODE = '42501';
          END IF;
          IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname = 'loom_drop_trial_writer_triggers'
              AND (p.pronargs <> 0 OR p.proowner <> v_owner
                   OR p.prorettype <> 'void'::regtype OR NOT p.prosecdef
                   OR p.prolang <> (SELECT oid FROM pg_catalog.pg_language
                                    WHERE lanname = 'plpgsql')
                   OR p.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']
                   OR p.prosrc IS DISTINCT FROM {body})
          ) THEN
            RAISE EXCEPTION 'trial retirement helper identity changed' USING ERRCODE = '55000';
          END IF;
        END
        $provision$;
        CREATE OR REPLACE FUNCTION public.loom_drop_trial_writer_triggers()
          RETURNS void LANGUAGE plpgsql SECURITY DEFINER
          SET search_path = pg_catalog AS {body};
        REVOKE ALL ON FUNCTION public.loom_drop_trial_writer_triggers() FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION public.loom_drop_trial_writer_triggers() TO {guard_owner};
        DO $verify$
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc AS p,
              LATERAL pg_catalog.aclexplode(p.proacl) AS a
            WHERE p.oid = 'public.loom_drop_trial_writer_triggers()'::regprocedure
              AND (a.grantee NOT IN (p.proowner, {guard_owner_name}::regrole::oid)
                   OR (a.grantee <> p.proowner AND a.is_grantable))
          ) THEN
            RAISE EXCEPTION 'trial retirement helper privileges changed' USING ERRCODE = '42501';
          END IF;
        END
        $verify$;
        """
    ).format(
        body=sql.Literal(_BODY),
        guard_owner=sql.Identifier(guard_owner),
        guard_owner_name=sql.Literal(guard_owner),
    )
