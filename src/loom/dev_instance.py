"""Per-developer dev environment (``dev-<name>``) identity + guardrails.

Multiple developers each run an isolated dev environment co-located on the
shared fleet with staging/production. Every environment's identity
(namespace, database, buckets, route, worker pool, provider namespace) is a
**pure, non-overridable function of its ``name``** — the core guardrail, so an
instance can never be pointed at another instance's or a base env's resources.

This module is the single source of truth for that derivation and for the
create-time guardrail check. It has no I/O: the guarded provisioning endpoint
and the CLI both call these functions, and they are unit-tested directly.

See ``docs/architecture/multi-dev-env-design.md``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

# ── Envelope defaults (operator-tunable via control-plane config) ────────────
#: Max concurrent Slurm worker slots a single dev instance may request.
PER_INSTANCE_CAP = 2
#: Ceiling on the SUM of ``max_slots`` across all live dev instances, so N
#: developers collectively cannot out-commit the fleet's dev-tier share.
DEV_FLEET_BUDGET = 8

#: Public subdomain base for dev instances: ``<name>.dev.<INGRESS_HOST>``.
INGRESS_HOST = "yylx.world"

#: Worker-pool name prefix for dev instances (``dev-<name>``). The autoscaler
#: policy-upsert admission path uses this to recognize a dev pool and enforce
#: the dev envelope (:func:`validate_dev_instance`) fail-closed. Must stay in
#: lockstep with ``DevInstanceIdentity.worker_pool`` below.
DEV_POOL_PREFIX = "dev-"

# ``<name>`` must be a valid RFC 1123 DNS label that starts with a letter and
# ends with an alphanumeric (so the derived namespace/subdomain are valid),
# 1–20 chars.
_NAME_RE = re.compile(r"^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$")

# Names whose short/derived identity would clash with a base env or a shared
# fixture, reserved so a dev instance can never masquerade as one.
RESERVED_NAMES = frozenset(
    {
        "dev",
        "development",
        "staging",
        "production",
        "prod",
        "local",
        "loom",
        "shared",
        "default",
    }
)

# Base-env identity surface a dev instance must never collide with.
BASE_ENV_NAMESPACES = frozenset({"loom-dev", "loom-staging", "loom-prod"})
BASE_ENV_ROUTE_PATHS = frozenset({"/dev", "/staging", "/prod"})


@dataclass(frozen=True)
class DevInstanceIdentity:
    """The fully-derived identity of a ``dev-<name>`` environment."""

    name: str
    runtime_environment: str  # dev-<name>
    namespace: str  # loom-dev-<name>
    database: str  # loom_dev_<name>  (on the shared dev-Postgres)
    db_role: str  # loom_dev_<name>
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    route_host: str  # <name>.dev.yylx.world
    route_path: str  # /dev-<name>  (interim path route)
    worker_pool: str  # dev-<name>
    provider_connection_namespace: str  # dev-<name>


@dataclass(frozen=True)
class RequestedPolicy:
    """The autoscaler policy a create request asks for (envelope-checked)."""

    actuator: str
    min_slots: int
    max_slots: int


@dataclass(frozen=True)
class DevInstanceRef:
    """A live dev instance as seen by the budget/distinctness checks."""

    name: str
    max_slots: int


class InvalidDevInstanceNameError(ValueError):
    """The requested dev-instance name is malformed or reserved."""


def validate_name(name: str) -> None:
    """Raise :class:`InvalidDevInstanceNameError` if ``name`` is unusable."""
    if not _NAME_RE.fullmatch(name):
        raise InvalidDevInstanceNameError(
            f"invalid dev-instance name {name!r}: must match {_NAME_RE.pattern} "
            "(lowercase, start with a letter, end alphanumeric, 1–20 chars)",
        )
    if name in RESERVED_NAMES:
        raise InvalidDevInstanceNameError(
            f"dev-instance name {name!r} is reserved",
        )


def derive_identity(name: str) -> DevInstanceIdentity:
    """Return the deterministic identity for ``dev-<name>``.

    Pure and injective in ``name`` — distinct names never share any field.
    Validates the name first (raises on a bad name).
    """
    validate_name(name)
    # Postgres identifiers use underscores; instance names only contain
    # dashes (never underscores), so the mapping stays collision-free.
    db_slug = name.replace("-", "_")
    return DevInstanceIdentity(
        name=name,
        runtime_environment=f"dev-{name}",
        namespace=f"loom-dev-{name}",
        database=f"loom_dev_{db_slug}",
        db_role=f"loom_dev_{db_slug}",
        task_bucket=f"loom-dev-{name}-tasks",
        trajectories_bucket=f"loom-dev-{name}-trajectories",
        artifacts_bucket=f"loom-dev-{name}-artifacts",
        route_host=f"{name}.dev.{INGRESS_HOST}",
        route_path=f"/dev-{name}",
        worker_pool=f"{DEV_POOL_PREFIX}{name}",
        provider_connection_namespace=f"dev-{name}",
    )


def dev_pool_instance_name(pool_name: str) -> str | None:
    """Recover the dev-instance ``name`` a ``dev-<name>`` worker pool belongs to.

    Returns ``None`` when ``pool_name`` is not a dev-instance pool (so base
    pools like ``oldlab`` / ``gb10`` are untouched by the dev-envelope
    admission). Pure and cheap; the caller still runs the full
    :func:`validate_dev_instance` guardrail on the recovered name.
    """
    if not pool_name.startswith(DEV_POOL_PREFIX):
        return None
    name = pool_name[len(DEV_POOL_PREFIX) :]
    return name or None


def validate_dev_instance(
    name: str,
    requested_policy: RequestedPolicy,
    other_instances: Sequence[DevInstanceRef],
) -> list[str]:
    """Guardrail check run **fail-closed before any provisioning mutation**.

    ``other_instances`` is every *other* live dev instance (excluding the one
    being created/updated). Returns a list of human-readable errors; empty
    means the request is inside the dev envelope and may be provisioned.
    """
    errors: list[str] = []

    # 1. Name shape + reserved.
    try:
        identity = derive_identity(name)
    except InvalidDevInstanceNameError as exc:
        return [str(exc)]

    # 2. Never collide with a base environment's identity (structurally
    # impossible given the prefix scheme + reserved names, but checked as
    # defense-in-depth in case the derivation ever changes).
    if identity.namespace in BASE_ENV_NAMESPACES:
        errors.append(f"namespace {identity.namespace!r} collides with a base env")
    if identity.route_path in BASE_ENV_ROUTE_PATHS:
        errors.append(f"route path {identity.route_path!r} collides with a base env")

    # 3. Distinct from every other live instance (again injective, so this only
    # trips on a derivation bug or a duplicate name in the caller's list).
    for other in other_instances:
        if other.name == name:
            continue
        other_id = derive_identity(other.name)
        for field in (
            "namespace",
            "database",
            "task_bucket",
            "trajectories_bucket",
            "artifacts_bucket",
            "route_host",
            "route_path",
            "worker_pool",
            "provider_connection_namespace",
        ):
            if getattr(identity, field) == getattr(other_id, field):
                errors.append(
                    f"{field} {getattr(identity, field)!r} collides with instance {other.name!r}",
                )

    # 4. Autoscaler policy envelope: dev instances only run on Slurm, at or
    # below the per-instance cap, and within the fleet-wide dev budget.
    if requested_policy.actuator != "slurm":
        errors.append(
            f"dev instances must use the slurm actuator, got {requested_policy.actuator!r}",
        )
    if requested_policy.min_slots < 0:
        errors.append("min_slots must be >= 0")
    if requested_policy.max_slots < requested_policy.min_slots:
        errors.append("max_slots must be >= min_slots")
    if requested_policy.max_slots > PER_INSTANCE_CAP:
        errors.append(
            f"max_slots {requested_policy.max_slots} exceeds PER_INSTANCE_CAP {PER_INSTANCE_CAP}",
        )
    # 5. Fleet budget: this instance's max_slots + every *other* live dev
    # instance's max_slots must stay within DEV_FLEET_BUDGET.
    other_committed = sum(ref.max_slots for ref in other_instances if ref.name != name)
    if other_committed + requested_policy.max_slots > DEV_FLEET_BUDGET:
        errors.append(
            f"fleet budget exceeded: {other_committed} committed by other dev "
            f"instances + {requested_policy.max_slots} requested > "
            f"DEV_FLEET_BUDGET {DEV_FLEET_BUDGET}",
        )

    return errors
