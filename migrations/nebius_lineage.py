"""Explicit, transactional conversion of the historical Nebius migration fork.

Run only in the protected migration Job after backup/restore qualification and
writer quiescence. The default command inspects without changing the database.
Ordinary Alembic upgrades retain their ambiguous-lineage rejection.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from migrations.env import _assert_direct_postgres_connection
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.exc import SQLAlchemyError

SOURCE_REVISIONS = ("0133", "0134", "0135")
TARGET_REVISION = "0146"
_DEV_TABLES = (
    "gateway_dispatch_receipts",
    "task_image_publication_keys",
    "task_image_publication_keysets",
    "task_bundle_sources",
    "personal_dev_build_platform_requests",
)


def inspect_lineage(connection: Connection, expected_revision: str) -> str:
    """Read revision and fork markers; reject ambiguous or partly changed state."""
    if expected_revision not in SOURCE_REVISIONS:
        raise ValueError("unsupported source revision")
    if (
        connection.exec_driver_sql("SELECT to_regclass('public.alembic_version')").scalar_one()
        is None
    ):
        raise ValueError("missing source revision table")
    revisions = list(
        connection.exec_driver_sql("SELECT version_num FROM public.alembic_version").scalars()
    )
    if revisions != [expected_revision]:
        raise ValueError("database revision does not match the expected historical revision")
    for table in _DEV_TABLES:
        if (
            connection.execute(
                text("SELECT to_regclass(:name)"), {"name": f"public.{table}"}
            ).scalar_one()
            is not None
        ):
            raise ValueError(
                "dev schema marker present; conversion requires the historical Nebius fork"
            )
    native = connection.exec_driver_sql("""
        SELECT format_type(a.atttypid, a.atttypmod), a.attnotnull, a.atthasdef, a.attgenerated
        FROM pg_attribute a
        WHERE a.attrelid = to_regclass('public.task_image_materialization_attempts')
          AND a.attname = 'native_build' AND NOT a.attisdropped
    """).one_or_none()
    if expected_revision == "0135":
        if native is None or tuple(native) != ("jsonb", False, False, ""):
            raise ValueError("historical native_build column does not match revision 0135")
    elif native is not None:
        raise ValueError("unexpected native_build column before historical revision 0135")
    quota = connection.exec_driver_sql("""
        SELECT pg_get_constraintdef(oid), convalidated
        FROM pg_constraint
        WHERE conrelid = to_regclass('public.execution_capacity_observations')
          AND conname = 'execution_capacity_observations_quota_check' AND contype = 'c'
    """).one_or_none()
    comparison = ">" if expected_revision == "0133" else ">="
    terms = [
        f"provider_quota_{name} {comparison} 0"
        for name in (
            "nodes",
            "vcpu_millis",
            "memory_mib",
            "storage_mib",
        )
    ] + [
        f"provider_used_{name} >= 0"
        for name in (
            "nodes",
            "vcpu_millis",
            "memory_mib",
            "storage_mib",
        )
    ]

    def normalize(value: str) -> str:
        return "".join(value.replace("(", "").replace(")", "").split())

    if (
        quota is None
        or not quota[1]
        or normalize(quota[0]) != normalize("CHECK (" + " AND ".join(terms) + ")")
    ):
        raise ValueError("quota constraint does not match the historical source revision")
    return expected_revision


def convert_lineage(
    connection: Connection, scripts: ScriptDirectory, expected_revision: str
) -> None:
    """Apply every missing migration, then change the revision in one transaction.

    The caller owns the transaction and must roll it back on any exception.
    No downgrade, transient base stamp, data-copy/drop, or trigger bypass occurs.
    """
    if not connection.in_transaction() or connection.get_isolation_level() != "READ COMMITTED":
        raise ValueError("conversion requires an explicit READ COMMITTED transaction")
    if getattr(connection.connection.dbapi_connection, "autocommit", True):
        raise ValueError("conversion refuses driver autocommit")
    if connection.dialect.name != "postgresql":
        raise ValueError("conversion requires PostgreSQL")
    connection.exec_driver_sql("SET LOCAL lock_timeout = '1s'")
    connection.exec_driver_sql("SET LOCAL statement_timeout = '120s'")
    connection.exec_driver_sql("SET LOCAL search_path = public, pg_catalog")
    inspect_lineage(connection, expected_revision)
    # Acquire all existing application/FK parents without waiting behind writers.
    # Locks also fence a concurrent converter and remain held through final stamp.
    tables = (
        connection.exec_driver_sql("""
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind IN ('r','p') ORDER BY c.relname
    """)
        .scalars()
        .all()
    )
    quote = connection.dialect.identifier_preparer.quote_identifier
    connection.exec_driver_sql(
        "LOCK TABLE "
        + ", ".join("public." + quote(table) for table in tables)
        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    inspect_lineage(connection, expected_revision)
    with Operations.context(MigrationContext.configure(connection)):
        for number in range(133, 147):
            revision = f"{number:04}"
            script = scripts.get_revision(revision)
            if script is None or script.down_revision != f"{number - 1:04}":
                raise ValueError("conversion requires the pinned linear dev migration history")
            # Only this DDL already exists at Nebius 0135. Its exact type,
            # nullability and default were verified under the table locks.
            if revision == "0146" and expected_revision == "0135":
                continue
            script.module.upgrade()
    result = connection.execute(
        text("""
        UPDATE public.alembic_version SET version_num=:target WHERE version_num=:source
    """),
        {"target": TARGET_REVISION, "source": expected_revision},
    )
    if result.rowcount != 1:
        raise ValueError("source revision changed during conversion")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-revision", choices=SOURCE_REVISIONS, required=True)
    parser.add_argument(
        "--apply", action="store_true", help="apply inside the protected migration Job"
    )
    args = parser.parse_args()
    try:
        database_url = make_url(os.environ["LOOM_DB_URL"])
        if database_url.get_backend_name() != "postgresql":
            raise ValueError("conversion requires PostgreSQL")
        engine = create_engine(
            database_url.set(drivername="postgresql+psycopg"),
            hide_parameters=True,
            connect_args={"connect_timeout": 10},
        )
        try:
            _assert_direct_postgres_connection(engine)
            with engine.begin() as connection:
                if args.apply:
                    config = Config()
                    config.set_main_option("script_location", str(Path(__file__).resolve().parent))
                    convert_lineage(
                        connection, ScriptDirectory.from_config(config), args.expected_revision
                    )
                else:
                    connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                    connection.exec_driver_sql("SET LOCAL lock_timeout = '1s'")
                    connection.exec_driver_sql("SET LOCAL statement_timeout = '120s'")
                    inspect_lineage(connection, args.expected_revision)
        finally:
            engine.dispose()
    except (KeyError, ValueError, RuntimeError, SQLAlchemyError):
        # Database/driver exceptions can contain protected values or row data.
        print(json.dumps({"passed": False, "failure": "lineage-conversion-rejected"}))
        return 1
    print(
        json.dumps(
            {
                "passed": True,
                "applied": args.apply,
                "source_revision": args.expected_revision,
                "target_revision": TARGET_REVISION,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
