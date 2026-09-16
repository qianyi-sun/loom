"""Internal ownership transaction, not a deployment or credential-retirement API.

The protected caller must admit its installed release and the private guard
definitions, ownership and ACLs, seal login/membership credentials in a prior committed phase,
reconcile sessions, and externally serialize relevant administrator DDL for the
whole transaction. These prerequisites cannot be inferred from a public catalog
digest. No production caller may use this substep until the durable operation,
credential/Secret lifecycle and recovery workflow supply those guarantees.
Session reconciliation requires an admitted startup barrier: a pre-seal login
can publish its backend statistics after an otherwise empty session observation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _checked_state,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_password import require_sealed_runtime_password
from loom.application_runtime_grants import application_runtime_grants_ddl
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    ApplicationSchemaAclProfile,
    ApplicationSchemaRevision,
    application_schema_profile,
    require_application_migration_revisions,
    require_application_schema_reference,
)
from loom.trial_writer_trigger_authority import application_trigger_owner_handoff_ddl


class ApplicationOwnershipTransferError(RuntimeError):
    """The exact sealed ownership transition could not be admitted."""


_ALIASES = {
    "application-owner",
    "guard-owner",
    "guard-migrator",
    "guard-agent",
    "guard-executor",
    "guard-observer",
    "guard-runtime",
    "provisioner",
}


def transfer_application_ownership(
    connection: ApplicationDatabaseConnection,
    *,
    owner_role: str,
    role_bindings: Mapping[str, str],
    runtime_password: str | None = None,
    admission_target: ApplicationDatabaseAdmissionTarget | None = None,
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
    schema_acl_profile: ApplicationSchemaAclProfile = "application-only",
    schema_revision: ApplicationSchemaRevision = "0148/guard_0036",
