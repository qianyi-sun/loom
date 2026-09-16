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


def application_public_definer_references() -> tuple[tuple[str, str, str], ...]:
    """Reviewed public definer identities; not callable grants or transfer authority."""
    # Exact prosrc hashes from application migrations 0127 and 0132. Real
    # migration-to-handoff tests verify these pins; the installed wheel must
    # not import application migrations, which are deployment image payload.
    # Never adopt an arbitrary live SECURITY DEFINER body under a stronger owner.
    return (
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


def application_trigger_owner_handoff_ddl(
    *, previous_owner: str, application_owner: str, guard_owner: str,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
    schema_revision: ApplicationSchemaRevision = "0148/guard_0036",
