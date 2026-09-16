"""Rebuild reference metadata using only a fresh, pinned PostgreSQL container.

No database URL, output file, or alternate reference is accepted on the command
line. Output is metadata only. The enclosing trusted source/image release binds
the reviewed pin; running this generator is not a deployment or transfer receipt.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import psycopg
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from loom.application_runtime_grants import application_runtime_grants_ddl
from loom.application_schema_inventory import (
    ApplicationSchemaInventory,
    read_application_schema_inventory,
)
from loom.application_schema_reference import (
    ApplicationSchemaProfile,
    ApplicationSchemaReference,
    ApplicationSchemaRevision,
    application_reference_postgres_image,
    application_schema_revisions,
)
from loom.dev_instance import DevInstanceIdentity, derive_identity
from loom.dev_instance_provision import render_create_database_sql, render_role_convergence_sql
from loom.dev_instance_runtime import PsycopgSharedFixtureSqlExecutor, instance_database_url
from loom.personal_dev_capacity_runtime import (
    ApplicationOwnerBinding,
    PsycopgPersonalDevCapacityDatabase,
    _new_credentials,
)
from loom_cli.rollout.readonly_database_bootstrap import (
    ReadonlyDatabaseCredential,
    render_readonly_role_sql,
)

_ROOT = Path(__file__).resolve().parents[1]


def _head(directory: str) -> str:
    config = Config(str(_ROOT / directory / "alembic.ini"))
    config.set_main_option("script_location", str(_ROOT / directory))
    heads = ScriptDirectory.from_config(config).get_heads()
    if len(heads) != 1:
        raise RuntimeError("reference generation requires one migration head")
    return heads[0]


async def _observe_fresh_database(
    admin_url: str,
    identity: DevInstanceIdentity,
    *,
    profile: ApplicationSchemaProfile = "legacy-owner",
    revision: ApplicationSchemaRevision = "0148/guard_0036",
