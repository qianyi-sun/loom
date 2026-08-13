"""One global capacity authority for every development environment.

Environment control planes publish bounded demand snapshots.  This coordinator
is the single writer that turns the complete cohort into fair, candidate-bound
grants.  The durable SQLite lease machinery lives in ``shared_capacity_broker``
as an implementation detail; there is no second broker service to deploy.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from loom_control_plane.global_execution_fence import (
    GlobalExecutionWitness,
    assert_legacy_scale_up_allowed,
)
from loom_control_plane.shared_capacity_broker import (
    AutoscalerGrantHandoff,
    BrokerBudgets,
    BrokerError,
    LeaseObservation,
    RequestState,
    SharedCapacityBroker,
)
from loom_control_plane.slurm_worker_jobs import slurm_cluster_for_pool

_ENVIRONMENT_RE = re.compile(r"[a-z][a-z0-9-]{0,62}")
_POOL_RE = re.compile(r"[a-z][a-z0-9-]{0,31}")
_SHA_RE = re.compile(r"[0-9a-f]{40}")
_MAX_SLOTS = 10_000
_ALLOWED_EXACT_ENVIRONMENTS = frozenset({"development"})
_ALLOWED_ENVIRONMENT_PREFIXES = ("dev-", "sandbox-")


class GlobalDevAutoscalerError(ValueError):
    """A bounded, secret-free global development autoscaler failure."""


@dataclass(frozen=True, slots=True)
class DevCapacityDemand:
    """One registry environment's desired capacity for one worker pool."""

    environment: str
    deployment_generation: int
    candidate_sha: str
    pool_name: str
    min_slots: int
    requested_slots: int
    observed_at: datetime

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> DevCapacityDemand:
        expected = {
            "environment",
            "deployment_generation",
            "candidate_sha",
            "pool_name",
            "min_slots",
            "requested_slots",
            "observed_at",
        }
        if set(value) != expected:
            raise GlobalDevAutoscalerError(
                "demand snapshot fields do not match the versioned contract"
            )
        observed_at = value["observed_at"]
        if not isinstance(observed_at, str):
            raise GlobalDevAutoscalerError("observed_at must be an RFC 3339 timestamp")
        try:
            parsed = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise GlobalDevAutoscalerError("observed_at must be an RFC 3339 timestamp") from exc
        return cls(
            environment=cast(str, value["environment"]),
            deployment_generation=cast(int, value["deployment_generation"]),
            candidate_sha=cast(str, value["candidate_sha"]),
            pool_name=cast(str, value["pool_name"]),
            min_slots=cast(int, value["min_slots"]),
            requested_slots=cast(int, value["requested_slots"]),
            observed_at=parsed,
        )

    def public_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["observed_at"] = _timestamp(self.observed_at)
        return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise GlobalDevAutoscalerError("timestamps must include a timezone")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _record_parts(record: Mapping[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    request = record.get("request")
    lease = record.get("lease")
    if not isinstance(request, dict) or not isinstance(lease, dict):
        raise GlobalDevAutoscalerError("capacity authority returned an invalid record")
    return request, lease


def capacity_grants_from_report(
    document: Mapping[str, object],
) -> dict[tuple[str, str], AutoscalerGrantHandoff]:
    """Parse the versioned global report into exact local handoffs."""
    if document.get("schema_version") != 1 or document.get("authority") != (
        "global-dev-fleet-autoscaler"
    ):
        raise GlobalDevAutoscalerError("capacity grant report authority is invalid")
    raw_grants = document.get("grants")
    if not isinstance(raw_grants, list):
        raise GlobalDevAutoscalerError("capacity grant report grants must be an array")
    grants: dict[tuple[str, str], AutoscalerGrantHandoff] = {}
    fields = set(AutoscalerGrantHandoff.__dataclass_fields__)
    for raw in raw_grants:
        if not isinstance(raw, dict) or set(raw) != fields:
            raise GlobalDevAutoscalerError("capacity grant fields are invalid")
        try:
            grant = AutoscalerGrantHandoff(**raw)
        except TypeError as exc:
            raise GlobalDevAutoscalerError("capacity grant types are invalid") from exc
        if (
            grant.schema_version != 1
            or not isinstance(grant.environment, str)
            or _ENVIRONMENT_RE.fullmatch(grant.environment) is None
            or not isinstance(grant.pool_name, str)
            or _POOL_RE.fullmatch(grant.pool_name) is None
            or not _is_int(grant.deployment_generation)
            or grant.deployment_generation <= 0
            or not isinstance(grant.candidate_sha, str)
            or _SHA_RE.fullmatch(grant.candidate_sha) is None
            or not _is_int(grant.lease_epoch)
            or grant.lease_epoch < 0
            or not _is_int(grant.max_slots)
            or grant.max_slots < 0
            or grant.min_slots != 0
            or grant.preemptible is not True
        ):
            raise GlobalDevAutoscalerError("capacity grant contract is invalid")
        key = (grant.environment, grant.pool_name)
        if key in grants:
            raise GlobalDevAutoscalerError("capacity grant report contains a duplicate scope")
        grants[key] = grant
    return grants


class GlobalDevFleetAutoscaler:
    """Registry-driven global reconciler with one transactional slot ledger."""

    def __init__(
        self,
        broker: SharedCapacityBroker,
        *,
        clock: Any | None = None,
        snapshot_freshness_seconds: int = 120,
        lease_ttl_seconds: int = 300,
    ) -> None:
        if snapshot_freshness_seconds <= 0:
            raise GlobalDevAutoscalerError("snapshot_freshness_seconds must be positive")
        if not 60 <= lease_ttl_seconds <= 86_400:
            raise GlobalDevAutoscalerError("lease_ttl_seconds must be in 60..86400")
        self.broker = broker
        self._clock = clock or (lambda: datetime.now(UTC))
        self.snapshot_freshness_seconds = snapshot_freshness_seconds
        self.lease_ttl_seconds = lease_ttl_seconds

    def reconcile(
        self,
        demands: Sequence[DevCapacityDemand],
        budgets: BrokerBudgets,
        *,
        observations: Sequence[LeaseObservation] = (),
        execution_witness: GlobalExecutionWitness | None = None,
        execution_witness_required: bool = False,
    ) -> dict[str, object]:
        """Converge the complete dynamic cohort and return environment grants."""
        now = _utc(self._clock())
        normalized = tuple(demands)
        if execution_witness_required:
            # A single witness is bound to one physical pool.  Refuse an
            # equivocal mixed-pool legacy request instead of calculating even
            # one new grant against a possibly active manager epoch.
            pool_ids = {slurm_cluster_for_pool(item.pool_name) for item in normalized}
            for pool_id in pool_ids or {"oldlab"}:
                assert_legacy_scale_up_allowed(
                    execution_witness,
                    expected_authority="global-capacity-manager",
                    expected_pool_id=pool_id,
                    now=now,
                    required=True,
                )
        status = self.broker.status()
        self._prevalidate(normalized, observations, budgets, status=status, now=now)

        # Apply observations against their current epochs before lifecycle
        # changes.  Cancelling a superseded request increments its epoch, so
        # reversing this order would reject an otherwise fresh final report.
        current = self.broker.reconcile(budgets, observations=observations)
        desired = {(d.environment, d.pool_name): d for d in normalized}
        kept: dict[tuple[str, str], str] = {}

        for raw_record in cast(list[dict[str, object]], current["requests"]):
            request, _lease = _record_parts(raw_record)
            if request["state"] == RequestState.TERMINAL.value:
                continue
            key = (str(request["sandbox"]), str(request["pool"]))
            demand = desired.get(key)
            reason: str | None = None
            if demand is None:
                reason = "registry_removed"
            elif demand.requested_slots == 0:
                reason = "demand_zero"
            elif not self._request_matches(request, demand):
                reason = "deployment_or_demand_superseded"
            elif bool(request["cancel_requested"]):
                reason = None
            elif key in kept:
                reason = "duplicate_capacity_request"
            else:
                kept[key] = str(request["id"])
            if reason is not None:
                self.broker.cancel(str(request["id"]), reason=reason)

        for demand in normalized:
            if demand.requested_slots == 0:
                continue
            key = (demand.environment, demand.pool_name)
            request_id = kept.get(key)
            if request_id is not None:
                self.broker.renew(request_id, ttl_seconds=self.lease_ttl_seconds)
                continue
            created_request, _created_lease = self.broker.request_capacity(
                sandbox=demand.environment,
                deployment_generation=demand.deployment_generation,
                candidate_sha=demand.candidate_sha,
                pool=demand.pool_name,
                min_slots=demand.min_slots,
                target_slots=demand.requested_slots,
                ttl_seconds=self.lease_ttl_seconds,
                purpose="global-dev-fleet-autoscaler",
                preemptible=True,
                idempotency_key=self._idempotency_key(demand),
            )
            kept[key] = created_request.id

        ledger = self.broker.reconcile(budgets)
        grants = self._current_grants(normalized, ledger)
        return {
            "schema_version": 1,
            "authority": "global-dev-fleet-autoscaler",
            "generated_at": _timestamp(now),
            "demands": [demand.public_dict() for demand in normalized],
            "budgets": ledger["budgets"],
            "aggregate": ledger["aggregate"],
            "grants": grants,
            "ledger": ledger,
        }

    def _prevalidate(
        self,
        demands: Sequence[DevCapacityDemand],
        observations: Sequence[LeaseObservation],
        budgets: BrokerBudgets,
        *,
        status: Mapping[str, object],
        now: datetime,
    ) -> None:
        keys: set[tuple[str, str]] = set()
        binding_by_environment: dict[str, tuple[int, str]] = {}
        pools: set[str] = set()
        for demand in demands:
            self._validate_demand(demand, now=now)
            key = (demand.environment, demand.pool_name)
            if key in keys:
                raise GlobalDevAutoscalerError("duplicate environment/pool demand snapshot")
            keys.add(key)
            pools.add(demand.pool_name)
            binding = (demand.deployment_generation, demand.candidate_sha)
            previous = binding_by_environment.setdefault(demand.environment, binding)
            if previous != binding:
                raise GlobalDevAutoscalerError(
                    "one environment cannot publish multiple deployment bindings"
                )
        try:
            budgets.validate_for_pools(pools)
        except BrokerError as exc:
            raise GlobalDevAutoscalerError(str(exc)) from exc

        records: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
        for raw in cast(list[dict[str, object]], status.get("requests", [])):
            request, lease = _record_parts(raw)
            records[str(request["id"])] = request, lease
        observed_ids: set[str] = set()
        for observation in observations:
            if observation.request_id in observed_ids:
                raise GlobalDevAutoscalerError("duplicate lease observation")
            observed_ids.add(observation.request_id)
            record = records.get(observation.request_id)
            if record is None:
                raise GlobalDevAutoscalerError("lease observation references an unknown request")
            _request, lease = record
            counters = (
                observation.lease_epoch,
                observation.pending_slots,
                observation.active_slots,
                observation.draining_slots,
                observation.terminal_slots,
            )
            if any(not _is_int(value) or value < 0 for value in counters):
                raise GlobalDevAutoscalerError("lease observation counters must be non-negative")
            if observation.lease_epoch != cast(int, lease["lease_epoch"]):
                raise GlobalDevAutoscalerError("lease observation epoch is stale")
            nonterminal = (
                observation.pending_slots + observation.active_slots + observation.draining_slots
            )
            if nonterminal > cast(int, lease["committed_slots"]):
                raise GlobalDevAutoscalerError("lease observation exceeds its commitment")
            if observation.terminal_slots < cast(int, lease["terminal_slots"]):
                raise GlobalDevAutoscalerError("lease observation terminal count regressed")

    def _validate_demand(self, demand: DevCapacityDemand, *, now: datetime) -> None:
        if (
            not isinstance(demand.environment, str)
            or _ENVIRONMENT_RE.fullmatch(demand.environment) is None
            or not (
                demand.environment in _ALLOWED_EXACT_ENVIRONMENTS
                or demand.environment.startswith(_ALLOWED_ENVIRONMENT_PREFIXES)
            )
        ):
            raise GlobalDevAutoscalerError("environment is not a development identity")
        if not _is_int(demand.deployment_generation) or demand.deployment_generation <= 0:
            raise GlobalDevAutoscalerError("deployment_generation must be positive")
        if (
            not isinstance(demand.candidate_sha, str)
            or _SHA_RE.fullmatch(demand.candidate_sha) is None
        ):
            raise GlobalDevAutoscalerError("candidate_sha must be a full lowercase Git SHA")
        if not isinstance(demand.pool_name, str) or _POOL_RE.fullmatch(demand.pool_name) is None:
            raise GlobalDevAutoscalerError("pool_name is invalid")
        if (
            not _is_int(demand.min_slots)
            or not _is_int(demand.requested_slots)
            or not 0 <= demand.min_slots <= demand.requested_slots <= _MAX_SLOTS
        ):
            raise GlobalDevAutoscalerError(
                "capacity demand must satisfy 0 <= min_slots <= requested_slots <= 10000"
            )
        observed_at = _utc(demand.observed_at)
        if observed_at < now - timedelta(seconds=self.snapshot_freshness_seconds):
            raise GlobalDevAutoscalerError("demand snapshot is stale")
        if observed_at > now + timedelta(seconds=30):
            raise GlobalDevAutoscalerError("demand snapshot is from the future")

    @staticmethod
    def _request_matches(request: Mapping[str, object], demand: DevCapacityDemand) -> bool:
        return (
            request["sandbox"] == demand.environment
            and request["deployment_generation"] == demand.deployment_generation
            and request["candidate_sha"] == demand.candidate_sha
            and request["pool"] == demand.pool_name
            and request["min_slots"] == demand.min_slots
            and request["target_slots"] == demand.requested_slots
            and request["preemptible"] is True
        )

    @staticmethod
    def _idempotency_key(demand: DevCapacityDemand) -> str:
        material = "\0".join(
            (
                demand.environment,
                str(demand.deployment_generation),
                demand.candidate_sha,
                demand.pool_name,
                str(demand.min_slots),
                str(demand.requested_slots),
                _timestamp(demand.observed_at),
            )
        ).encode()
        return f"gdfa:{hashlib.sha256(material).hexdigest()}"

    @staticmethod
    def _current_grants(
        demands: Sequence[DevCapacityDemand],
        ledger: Mapping[str, object],
    ) -> list[dict[str, object]]:
        desired = {
            (
                demand.environment,
                demand.pool_name,
                demand.deployment_generation,
                demand.candidate_sha,
            )
            for demand in demands
            if demand.requested_slots > 0
        }
        grants = []
        for raw in cast(list[dict[str, object]], ledger["handoffs"]):
            key = (
                raw["environment"],
                raw["pool_name"],
                raw["deployment_generation"],
                raw["candidate_sha"],
            )
            if key in desired:
                grants.append(raw)
        return sorted(grants, key=lambda item: (str(item["environment"]), str(item["pool_name"])))


__all__ = [
    "DevCapacityDemand",
    "GlobalDevAutoscalerError",
    "GlobalDevFleetAutoscaler",
    "capacity_grants_from_report",
]
