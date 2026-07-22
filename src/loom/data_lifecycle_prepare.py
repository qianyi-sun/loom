"""Digest-approved staging lifecycle schema preparation.

This is the one maintenance bridge from the deployed pre-lifecycle schema to
the repository-owned lifecycle authority.  It deliberately does not classify
or delete data.  A read-only inventory binds the exact migration source and
live schema; a separate apply rechecks that digest while holding a PostgreSQL
advisory lock, runs the canonical Alembic chain, and initializes only the exact
epoch-zero staging authority.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, Engine, text

from loom.data_lifecycle_bootstrap import (
    LifecycleBootstrapPlan,
    SqlAlchemyLifecycleBootstrap,
)
from loom.data_lifecycle_gc import GcScope

_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_TREE_RE = _SHA_RE
_PRE_LIFECYCLE_REVISIONS = frozenset({"0065", "0066", "0067"})
_MIN_LIFECYCLE_REVISION = 68
_ADVISORY_LOCK_KEY = 0x4C4F4F4D4C494645  # ``LOOMLIFE`` as one signed-safe bigint.
_LIFECYCLE_TABLES = (
    "data_lifecycle_authorities",
    "data_lifecycle_gc_authorities",
    "data_lifecycle_gc_items",
    "data_lifecycle_gc_runs",
    "data_lifecycle_objects",
    "staging_lifecycle_capacity",
    "staging_mutation_epoch_events",
    "staging_mutation_epochs",
)
_EXECUTION_TABLES = ("artifacts", "batches", "llm_calls", "trial_events", "trials")


class LifecyclePrepareError(RuntimeError):
    """The staging lifecycle schema cannot be prepared safely."""


@dataclass(frozen=True, slots=True)
class LifecycleSourceIdentity:
    candidate_sha: str
    candidate_tree: str
    approved_base_sha: str

    def __post_init__(self) -> None:
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or _TREE_RE.fullmatch(self.candidate_tree) is None
            or _SHA_RE.fullmatch(self.approved_base_sha) is None
        ):
            raise ValueError("lifecycle preparation source identity is invalid")


@dataclass(frozen=True, slots=True)
class LifecyclePreparePlan:
    scope: GcScope
    source: LifecycleSourceIdentity
    migration_policy_sha256: str
    migration_plan_sha256: str
    current_revision: str
    target_revision: str
    lifecycle_tables: tuple[str, ...]
    linked_execution_tables: tuple[str, ...]
    bootstrap: LifecycleBootstrapPlan | None
    blockers: tuple[str, ...]
    inventory_digest: str

    @property
    def applicable(self) -> bool:
        return not self.blockers and (
            self.current_revision != self.target_revision
            or (self.bootstrap is not None and self.bootstrap.applicable)
        )

    @property
    def converged(self) -> bool:
        return (
            not self.blockers
            and self.current_revision == self.target_revision
            and self.bootstrap is not None
            and self.bootstrap.converged
        )

    def require_applicable_or_converged(self) -> None:
        if self.blockers:
            raise LifecyclePrepareError("; ".join(self.blockers))
        if not self.applicable and not self.converged:
            raise LifecyclePrepareError("lifecycle schema preparation state is ambiguous")


def _revision_number(value: str) -> int:
    if _REVISION_RE.fullmatch(value) is None:
        raise LifecyclePrepareError("database schema revision authority is invalid")
    return int(value[:4])


def _payload(plan: LifecyclePreparePlan, *, include_digest: bool) -> dict[str, object]:
    bootstrap_digest = plan.bootstrap.inventory_digest if plan.bootstrap is not None else None
    value: dict[str, object] = {
        "schema_version": 1,
        "environment": plan.scope.environment,
        "namespace": plan.scope.namespace,
        "candidate_sha": plan.source.candidate_sha,
        "candidate_tree": plan.source.candidate_tree,
        "approved_base_sha": plan.source.approved_base_sha,
        "migration_policy_sha256": plan.migration_policy_sha256,
        "migration_plan_sha256": plan.migration_plan_sha256,
        "current_revision": plan.current_revision,
        "target_revision": plan.target_revision,
        "lifecycle_tables": list(plan.lifecycle_tables),
        "linked_execution_tables": list(plan.linked_execution_tables),
        "bootstrap_inventory_digest": bootstrap_digest,
        "bootstrap_applicable": bool(plan.bootstrap and plan.bootstrap.applicable),
        "bootstrap_converged": bool(plan.bootstrap and plan.bootstrap.converged),
        "blockers": list(plan.blockers),
    }
    if include_digest:
        value["inventory_digest"] = plan.inventory_digest
        value["applicable"] = plan.applicable
        value["converged"] = plan.converged
    return value


def lifecycle_prepare_plan_document(plan: LifecyclePreparePlan) -> dict[str, object]:
    return _payload(plan, include_digest=True)


def _hash_plan(plan: LifecyclePreparePlan) -> str:
    return hashlib.sha256(
        json.dumps(
            _payload(plan, include_digest=False),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _single_revision(connection: Connection) -> str:
    rows = tuple(
        str(value)
        for value in connection.execute(text("SELECT version_num FROM alembic_version")).scalars()
    )
    if len(rows) != 1:
        raise LifecyclePrepareError("database schema revision authority is ambiguous")
    _revision_number(rows[0])
    return rows[0]


def _existing_tables(connection: Connection) -> tuple[str, ...]:
    return tuple(
        str(value)
        for value in connection.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name = ANY(:names) "
                "ORDER BY table_name"
            ),
            {"names": list(_LIFECYCLE_TABLES)},
        ).scalars()
    )


def _linked_tables(connection: Connection) -> tuple[str, ...]:
    return tuple(
        str(value)
        for value in connection.execute(
            text(
                "SELECT table_name FROM information_schema.columns "
                "WHERE table_schema='public' AND column_name='lifecycle_authority_id' "
                "AND table_name = ANY(:names) ORDER BY table_name"
            ),
            {"names": list(_EXECUTION_TABLES)},
        ).scalars()
    )


class SqlAlchemyLifecyclePreparer:
    """Inventory and apply only the canonical staging lifecycle transition."""

    def __init__(
        self,
        engine: Engine,
        *,
        alembic_config_path: Path,
        source: LifecycleSourceIdentity,
        migration_policy_sha256: str,
        migration_plan_sha256: str,
        migration_target_revision: str,
    ) -> None:
        if (
            not alembic_config_path.is_absolute()
            or not alembic_config_path.is_file()
            or re.fullmatch(r"[0-9a-f]{64}", migration_policy_sha256) is None
            or re.fullmatch(r"[0-9a-f]{64}", migration_plan_sha256) is None
            or _REVISION_RE.fullmatch(migration_target_revision) is None
        ):
            raise ValueError("lifecycle preparation migration authority is invalid")
        self._engine = engine
        self._alembic_config_path = alembic_config_path
        self._source = source
        self._migration_policy_sha256 = migration_policy_sha256
        self._migration_plan_sha256 = migration_plan_sha256
        self._migration_target_revision = migration_target_revision

    def _config(self) -> Config:
        config = Config(str(self._alembic_config_path))
        # The installed maintenance command has no repository working-directory
        # contract.  Bind Alembic to the exact sealed migrations directory just
        # as the shared preflight migration inspector does.
        config.set_main_option("path_separator", "os")
        config.set_main_option("script_location", str(self._alembic_config_path.parent))
        # Alembic's Config interpolation treats percent characters specially.
        database_url = self._engine.url.render_as_string(hide_password=False)
        config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
        return config

    def inventory(self, *, scope: GcScope) -> LifecyclePreparePlan:
        if (scope.environment, scope.namespace) != ("staging", "loom-staging"):
            raise LifecyclePrepareError("lifecycle schema preparation is staging-only")
        target = self._migration_target_revision
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as connection:
            with connection.begin():
                current = _single_revision(connection)
                tables = _existing_tables(connection)
                linked = _linked_tables(connection)
        current_number = _revision_number(current)
        target_number = _revision_number(target)
        blockers: list[str] = []
        bootstrap: LifecycleBootstrapPlan | None = None
        if target_number < _MIN_LIFECYCLE_REVISION:
            blockers.append("migration target does not include lifecycle authority")
        if current_number > target_number:
            blockers.append("database schema is ahead of the exact migration source")
        if current_number < _MIN_LIFECYCLE_REVISION:
            if current not in _PRE_LIFECYCLE_REVISIONS:
                blockers.append(
                    "legacy lifecycle preparation requires exact revision 0065, 0066, or 0067"
                )
            if tables or linked:
                blockers.append("pre-lifecycle database contains partial lifecycle schema")
        else:
            bootstrap = SqlAlchemyLifecycleBootstrap(self._engine).inventory(scope=scope)
            blockers.extend(bootstrap.blockers)
            expected_tables = set(_LIFECYCLE_TABLES)
            if current_number == 68:
                expected_tables.remove("staging_lifecycle_capacity")
            if current_number < 70:
                expected_tables.remove("data_lifecycle_gc_authorities")
            if set(tables) != expected_tables or set(linked) != set(_EXECUTION_TABLES):
                blockers.append("lifecycle schema structure is incomplete")
        provisional = LifecyclePreparePlan(
            scope=scope,
            source=self._source,
            migration_policy_sha256=self._migration_policy_sha256,
            migration_plan_sha256=self._migration_plan_sha256,
            current_revision=current,
            target_revision=target,
            lifecycle_tables=tables,
            linked_execution_tables=linked,
            bootstrap=bootstrap,
            blockers=tuple(sorted(set(blockers))),
            inventory_digest="0" * 64,
        )
        return LifecyclePreparePlan(
            scope=provisional.scope,
            source=provisional.source,
            migration_policy_sha256=provisional.migration_policy_sha256,
            migration_plan_sha256=provisional.migration_plan_sha256,
            current_revision=provisional.current_revision,
            target_revision=provisional.target_revision,
            lifecycle_tables=provisional.lifecycle_tables,
            linked_execution_tables=provisional.linked_execution_tables,
            bootstrap=provisional.bootstrap,
            blockers=provisional.blockers,
            inventory_digest=_hash_plan(provisional),
        )

    def apply(
        self,
        *,
        plan: LifecyclePreparePlan,
        approved_inventory_digest: str,
    ) -> LifecyclePreparePlan:
        plan.require_applicable_or_converged()
        if approved_inventory_digest != plan.inventory_digest:
            raise LifecyclePrepareError("approved lifecycle preparation digest does not match")
        if plan.converged:
            return plan
        with self._engine.connect() as lock_connection:
            locked = bool(
                lock_connection.execute(
                    text("SELECT pg_try_advisory_lock(:key)"), {"key": _ADVISORY_LOCK_KEY}
                ).scalar_one()
            )
            if not locked:
                raise LifecyclePrepareError("another lifecycle preparation holds the advisory lock")
            try:
                live = self.inventory(scope=plan.scope)
                if live.inventory_digest != plan.inventory_digest:
                    raise LifecyclePrepareError("lifecycle preparation inventory drifted")
                if live.current_revision != live.target_revision:
                    command.upgrade(self._config(), "head")
                bootstrap = SqlAlchemyLifecycleBootstrap(self._engine)
                bootstrap_plan = bootstrap.inventory(scope=plan.scope)
                bootstrap.apply(
                    plan=bootstrap_plan,
                    approved_inventory_digest=bootstrap_plan.inventory_digest,
                )
                converged = self.inventory(scope=plan.scope)
                if not converged.converged:
                    raise LifecyclePrepareError("lifecycle schema preparation did not converge")
                return converged
            finally:
                lock_connection.execute(
                    text("SELECT pg_advisory_unlock(:key)"), {"key": _ADVISORY_LOCK_KEY}
                )


def verify_lifecycle_source(root: Path, source: LifecycleSourceIdentity) -> None:
    """Prove the command is executing from the exact clean cumulative source."""

    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise LifecyclePrepareError("lifecycle preparation source root is unsafe")

    def git(*arguments: str) -> str:
        completed = subprocess.run(
            ("git", "-C", str(root), *arguments),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=30,
        )
        if completed.returncode != 0:
            raise LifecyclePrepareError("lifecycle preparation source Git authority failed")
        return completed.stdout.strip()

    if (
        git("rev-parse", "HEAD") != source.candidate_sha
        or git("rev-parse", "HEAD^{tree}") != source.candidate_tree
        or git("merge-base", source.approved_base_sha, source.candidate_sha)
        != source.approved_base_sha
        or git("status", "--porcelain=v1", "--untracked-files=all")
    ):
        raise LifecyclePrepareError("lifecycle preparation source identity drifted")


__all__ = [
    "LifecyclePrepareError",
    "LifecyclePreparePlan",
    "LifecycleSourceIdentity",
    "SqlAlchemyLifecyclePreparer",
    "lifecycle_prepare_plan_document",
    "verify_lifecycle_source",
]
