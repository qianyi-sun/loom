"""Migrate and safely bind a new global capacity-management authority."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast
from uuid import UUID

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import Engine, MetaData, RowMapping, Table, create_engine, inspect, select, update

from loom_capacity_manager.config import read_owner_only_secret
from loom_capacity_manager.models import CapacityAuditEvent, CapacityAuthorityState
from loom_capacity_manager.postgres_timeouts import capacity_migration_connect_args


class CapacityAuthorityBootstrapError(RuntimeError):
    """The management database is not safe to bind to a new authority."""


_AUTHORITY_MARKER_ACTOR_KIND = "migration"
_AUTHORITY_MARKER_ACTOR_ID = "capacity-authority-bootstrap"
_AUTHORITY_SEED_EVENT = "authority_incarnation_seeded"
_AUTHORITY_BOUND_EVENT = "authority_incarnation_bound"
_RESERVED_AUTHORITY_EVENTS = frozenset(
    {_AUTHORITY_SEED_EVENT, _AUTHORITY_BOUND_EVENT}
)
_AUTHORITY_EVENT_STATES = {
    _AUTHORITY_SEED_EVENT: "migration-generated-seed",
    _AUTHORITY_BOUND_EVENT: "reviewed-bootstrap-bound",
}


def _authority_marker(event_kind: str, authority: UUID) -> dict[str, object]:
    return {
        "actor_kind": _AUTHORITY_MARKER_ACTOR_KIND,
        "actor_id": _AUTHORITY_MARKER_ACTOR_ID,
        "event_kind": event_kind,
        "object_binding": {"authority_incarnation": str(authority)},
        "detail": {"state": _AUTHORITY_EVENT_STATES[event_kind]},
    }


def _validated_reserved_markers(
    rows: Sequence[RowMapping],
) -> dict[str, tuple[UUID, int]]:
    markers: dict[str, tuple[UUID, int]] = {}
    for row in rows:
        event_kind = row["event_kind"]
        if event_kind not in _RESERVED_AUTHORITY_EVENTS:
            raise CapacityAuthorityBootstrapError(
                "capacity authority reserved audit evidence is invalid"
            )
        if event_kind in markers:
            raise CapacityAuthorityBootstrapError(
                "capacity authority reserved audit evidence is duplicated"
            )
        object_binding = row["object_binding"]
        detail = row["detail"]
        if (
            row["actor_kind"] != _AUTHORITY_MARKER_ACTOR_KIND
            or row["actor_id"] != _AUTHORITY_MARKER_ACTOR_ID
            or not isinstance(object_binding, dict)
            or set(object_binding) != {"authority_incarnation"}
            or not isinstance(detail, dict)
            or detail != {"state": _AUTHORITY_EVENT_STATES[event_kind]}
        ):
            raise CapacityAuthorityBootstrapError(
                "capacity authority reserved audit evidence is invalid"
            )
        raw_authority = object_binding["authority_incarnation"]
        try:
            marker_authority = UUID(raw_authority)
        except (AttributeError, TypeError, ValueError) as exc:
            raise CapacityAuthorityBootstrapError(
                "capacity authority reserved audit evidence is invalid"
            ) from exc
        if marker_authority.int == 0 or str(marker_authority) != raw_authority:
            raise CapacityAuthorityBootstrapError(
                "capacity authority reserved audit evidence is invalid"
            )
        markers[event_kind] = (marker_authority, row["id"])
    return markers


def _validate_expected_authority(expected: UUID) -> None:
    if not isinstance(expected, UUID):
        raise TypeError("expected capacity authority incarnation must be a UUID")
    if expected.int == 0:
        raise ValueError("expected capacity authority incarnation must be non-nil")


def _non_nil_uuid_argument(value: str) -> UUID:
    try:
        expected = UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "expected capacity authority incarnation must be a UUID"
        ) from exc
    if expected.int == 0:
        raise argparse.ArgumentTypeError(
            "expected capacity authority incarnation must be non-nil"
        )
    return expected


def bind_fresh_authority(engine: Engine, expected: UUID) -> None:
    """Bind one reviewed UUID only while the migrated authority is unused."""

    if not isinstance(engine, Engine):
        raise TypeError("capacity authority bootstrap requires a synchronous engine")
    _validate_expected_authority(expected)
    authority_table = cast(Table, CapacityAuthorityState.__table__)
    audit_table = cast(Table, CapacityAuditEvent.__table__)
    try:
        with engine.begin() as connection:
            authority = (
                connection.execute(
                    select(authority_table)
                    .where(authority_table.c.singleton_id == 1)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            reserved_rows = (
                connection.execute(
                    select(
                        audit_table.c.id,
                        audit_table.c.actor_kind,
                        audit_table.c.actor_id,
                        audit_table.c.event_kind,
                        audit_table.c.object_binding,
                        audit_table.c.detail,
                    )
                    .where(audit_table.c.event_kind.in_(_RESERVED_AUTHORITY_EVENTS))
                    .order_by(audit_table.c.id)
                )
                .mappings()
                .all()
            )
            markers = _validated_reserved_markers(reserved_rows)
            seed_marker = markers.get(_AUTHORITY_SEED_EVENT)
            bound_marker = markers.get(_AUTHORITY_BOUND_EVENT)
            if (
                seed_marker is not None
                and bound_marker is not None
                and seed_marker[1] >= bound_marker[1]
            ):
                raise CapacityAuthorityBootstrapError(
                    "capacity authority seed marker must precede binding marker"
                )
            current = authority["authority_incarnation"]
            if bound_marker is not None and bound_marker[0] != current:
                raise CapacityAuthorityBootstrapError(
                    "capacity authority binding marker conflicts with its state"
                )
            if bound_marker is None and seed_marker is not None and seed_marker[0] != current:
                raise CapacityAuthorityBootstrapError(
                    "capacity authority seed marker conflicts with its state"
                )
            binding_marker = _authority_marker(_AUTHORITY_BOUND_EVENT, expected)
            if authority["authority_incarnation"] == expected:
                if bound_marker is None:
                    connection.execute(audit_table.insert().values(**binding_marker))
                return
            if bound_marker is not None or seed_marker is None or seed_marker[0] != current:
                raise CapacityAuthorityBootstrapError(
                    "capacity authority UUID is not the migration-generated seed"
                )
            if (
                authority["writer_epoch"] != 0
                or authority["recovery_state"] != "shadow"
                or authority["increase_freeze"] is not True
                or authority["executable_new_capacity_ceiling"] != 0
                or authority["global_pending_slot_ceiling"] != 0
                or authority["global_pending_job_ceiling"] != 0
                or authority["global_submission_rate_ceiling"] != 0
            ):
                raise CapacityAuthorityBootstrapError(
                    "capacity authority database is not an unused frozen shadow"
                )
            table_names = sorted(
                name
                for name in inspect(connection).get_table_names()
                if name.startswith("capacity_") and name != authority_table.name
            )
            for table_name in table_names:
                table = Table(table_name, MetaData(), autoload_with=connection)
                statement = select(1).select_from(table)
                if table_name == audit_table.name:
                    statement = statement.where(table.c.id != seed_marker[1])
                if connection.execute(statement.limit(1)).first() is not None:
                    raise CapacityAuthorityBootstrapError(
                        "capacity authority database is not empty"
                    )
            connection.execute(
                update(authority_table)
                .where(authority_table.c.singleton_id == 1)
                .values(authority_incarnation=expected)
            )
            connection.execute(audit_table.insert().values(**binding_marker))
    except CapacityAuthorityBootstrapError:
        raise
    except Exception as exc:
        raise CapacityAuthorityBootstrapError(
            "capacity authority bootstrap could not verify the management database"
        ) from exc


def migrate_capacity_database(
    db_url_file: Path,
    expected: UUID,
    *,
    alembic_ini: Path | None = None,
) -> None:
    """Upgrade the independent schema, then bind its reviewed incarnation."""

    _validate_expected_authority(expected)
    database_url = read_owner_only_secret(db_url_file)
    config_path = alembic_ini or Path(__file__).resolve().parents[2] / "capacity_migrations/alembic.ini"
    if not config_path.is_file():
        raise CapacityAuthorityBootstrapError("capacity migration configuration is missing")
    previous_url = os.environ.get("LOOM_CAPACITY_DB_URL")
    try:
        os.environ["LOOM_CAPACITY_DB_URL"] = database_url
        config = AlembicConfig(str(config_path))
        config.set_main_option("script_location", str(config_path.parent))
        command.upgrade(config, "head")
    except Exception as exc:
        raise CapacityAuthorityBootstrapError("capacity schema migration failed") from exc
    finally:
        if previous_url is None:
            os.environ.pop("LOOM_CAPACITY_DB_URL", None)
        else:
            os.environ["LOOM_CAPACITY_DB_URL"] = previous_url

    engine = create_engine(
        database_url,
        isolation_level="SERIALIZABLE",
        connect_args=capacity_migration_connect_args(),
    )
    try:
        bind_fresh_authority(engine, expected)
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate a new frozen global capacity authority"
    )
    parser.add_argument("--db-url-file", type=Path, required=True)
    parser.add_argument(
        "--expected-authority-incarnation",
        type=_non_nil_uuid_argument,
        required=True,
    )
    arguments = parser.parse_args()
    try:
        migrate_capacity_database(
            arguments.db_url_file,
            arguments.expected_authority_incarnation,
        )
    except (CapacityAuthorityBootstrapError, OSError, ValueError):
        sys.stderr.write("error: capacity migration failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = [
    "CapacityAuthorityBootstrapError",
    "bind_fresh_authority",
    "main",
    "migrate_capacity_database",
]
