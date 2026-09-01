"""Cross-process mutation coordination for protected staging operations."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text

STAGING_MUTATION_ADVISORY_LOCK_KEY = 5_498_691_230_183_247_727
STAGING_MUTATION_TRY_LOCK_SQL = f"SELECT pg_try_advisory_lock({STAGING_MUTATION_ADVISORY_LOCK_KEY})"
STAGING_MUTATION_UNLOCK_SQL = f"SELECT pg_advisory_unlock({STAGING_MUTATION_ADVISORY_LOCK_KEY})"
_REQUEST_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_GENERATION_RE = re.compile(r"^[0-9a-f]{32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ROLLOUT_CAPACITY_AUTHORITY_SQL = """
SELECT
  EXISTS (
    SELECT 1
    FROM pg_locks AS locks
    JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
    WHERE locks.locktype = 'advisory'
      AND locks.pid = :guard_backend_pid
      AND locks.classid = 1280263818
      AND locks.objid = 1621151599
      AND locks.objsubid = 1
      AND locks.mode = 'ExclusiveLock'
      AND locks.granted
      AND activity.application_name = :guard_application_name
  ) AS guard_owned,
  EXISTS (
    SELECT 1
    FROM staging_mutation_epochs
    WHERE environment = 'staging'
      AND namespace = 'loom-staging'
      AND epoch = :mutation_epoch
      AND reason = 'rollout_apply'
      AND request_id = :request_id
      AND evidence_sha256 = :plan_digest
  ) AS epoch_owned
""".strip()


def rollout_guard_application_name(
    *,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str,
    generation: str,
) -> str:
    """Derive the bounded PostgreSQL session identity for one rollout guard."""

    if (
        _REQUEST_RE.fullmatch(request_id) is None
        or _SHA_RE.fullmatch(candidate_sha) is None
        or _SHA_RE.fullmatch(candidate_tree) is None
        or _GENERATION_RE.fullmatch(generation) is None
    ):
        raise ValueError("rollout guard application identity is invalid")
    digest = hashlib.sha256(
        b"\0".join(
            value.encode()
            for value in (request_id, candidate_sha, candidate_tree, generation)
        )
    ).hexdigest()
    return f"loom-rollout-guard-{digest[:40]}"


def rollout_guard_bind_sql(application_name: str) -> str:
    if re.fullmatch(r"loom-rollout-guard-[0-9a-f]{40}", application_name) is None:
        raise ValueError("rollout guard application identity is invalid")
    return (
        "SELECT set_config('application_name', "
        f"'{application_name}', false) AS application_name"
    )


@dataclass(frozen=True, slots=True)
class RolloutCapacityAuthority:
    request_id: str
    candidate_sha: str
    candidate_tree: str
    generation: str
    guard_backend_pid: int
    mutation_epoch: int
    plan_digest: str

    def __post_init__(self) -> None:
        if (
            type(self.guard_backend_pid) is not int
            or self.guard_backend_pid < 1
            or type(self.mutation_epoch) is not int
            or self.mutation_epoch < 1
            or _SHA256_RE.fullmatch(self.plan_digest) is None
        ):
            raise ValueError("rollout capacity authority is invalid")
        rollout_guard_application_name(
            request_id=self.request_id,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            generation=self.generation,
        )

    @property
    def guard_application_name(self) -> str:
        return rollout_guard_application_name(
            request_id=self.request_id,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            generation=self.generation,
        )


def _require_rollout_capacity_authority(
    connection: Connection,
    authority: RolloutCapacityAuthority,
) -> None:
    result = connection.execute(
        text(_ROLLOUT_CAPACITY_AUTHORITY_SQL),
        {
            "guard_application_name": authority.guard_application_name,
            "guard_backend_pid": authority.guard_backend_pid,
            "mutation_epoch": authority.mutation_epoch,
            "plan_digest": authority.plan_digest,
            "request_id": authority.request_id,
        },
    )
    row = result.mappings().one_or_none()
    if (
        row is None
        or set(row) != {"epoch_owned", "guard_owned"}
        or row["guard_owned"] is not True
        or row["epoch_owned"] is not True
    ):
        raise RuntimeError("rollout capacity authority is unavailable")


@contextmanager
def hold_rollout_capacity_authority(
    engine: Engine,
    authority: RolloutCapacityAuthority,
) -> Iterator[None]:
    """Require the exact live rollout guard before and after one refresh."""

    connection = engine.connect()
    try:
        _require_rollout_capacity_authority(connection, authority)
        try:
            yield
        finally:
            _require_rollout_capacity_authority(connection, authority)
    finally:
        connection.close()


def hold_staging_mutation_guard(engine: Engine) -> AbstractContextManager[bool]:
    """Try the fixed session lock and hold its connection for the context."""

    return _hold_staging_mutation_guard(engine)


@contextmanager
def _hold_staging_mutation_guard(engine: Engine) -> Iterator[bool]:
    connection = engine.connect()
    acquired = False
    try:
        result = connection.execute(text(STAGING_MUTATION_TRY_LOCK_SQL)).scalar_one()
        if type(result) is not bool:
            raise RuntimeError("staging mutation guard acquisition result is invalid")
        acquired = result
        yield acquired
    finally:
        try:
            if acquired:
                released = connection.execute(text(STAGING_MUTATION_UNLOCK_SQL)).scalar_one()
                if released is not True:
                    raise RuntimeError("staging mutation guard unlock result is invalid")
        finally:
            connection.close()


__all__ = [
    "STAGING_MUTATION_ADVISORY_LOCK_KEY",
    "STAGING_MUTATION_TRY_LOCK_SQL",
    "STAGING_MUTATION_UNLOCK_SQL",
    "RolloutCapacityAuthority",
    "hold_rollout_capacity_authority",
    "hold_staging_mutation_guard",
    "rollout_guard_application_name",
    "rollout_guard_bind_sql",
]
