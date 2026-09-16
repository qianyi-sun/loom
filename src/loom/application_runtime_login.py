"""Restore an admitted application's credential after exact ownership separation.

Internal phase only: the protected caller must bind this connection and the
preserved credential to its durable operation/Secret recovery authority, admit
private guard definitions, and serialize credential/DDL writers. No deployment
caller, Secret mutation, ownership transfer or workload restart is provided here.
Only an independently admitted same-original-password refresher may overlap;
that permission never covers LOGIN, membership, ownership or another password.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from enum import StrEnum

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    _require_coordination_guard,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_ownership_transfer import require_application_role_scope
from loom.application_password import application_scram_verifier, matches_application_scram
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    ApplicationSchemaAclProfile,
    ApplicationSchemaRevision,
    application_schema_profile,
    require_application_migration_revisions,
    require_application_schema_reference,
)


class ApplicationRuntimeLoginError(RuntimeError):
    """Runtime login cannot be restored without changing the admitted authority."""


class ApplicationRuntimeLoginState(StrEnum):
    """Validated credential state; neither value proves workload or fence recovery."""

    SEALED = "sealed"
    RESTORED = "restored"


def observe_application_runtime_login(
    connection: ApplicationDatabaseConnection,
    *,
    owner_role: str,
    role_bindings: Mapping[str, str],
    password: str,
    target: ApplicationDatabaseAdmissionTarget,
    schema_acl_profile: ApplicationSchemaAclProfile = "application-only",
    schema_revision: ApplicationSchemaRevision = "0148/guard_0036",
