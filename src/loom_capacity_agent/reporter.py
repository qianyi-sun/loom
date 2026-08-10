"""Fail-closed conversion of local protected demand into manager reports."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass

from pydantic import ValidationError

from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    GuardDemandAttemptV1,
    GuardDemandObservationV1,
    ReporterConfigurationV1,
)
from loom_capacity_guard.contracts import SealedRequirementsV1
from loom_capacity_manager.contracts import DemandBucketV1, DemandSnapshotV1


class DemandReportBlockedError(RuntimeError):
    """A complete, exactly bound demand snapshot cannot be produced."""


_BINDING_FIELDS = (
    "environment_id",
    "subject_id",
    "subject_incarnation",
    "authority_incarnation",
    "agent_incarnation",
    "reporter_incarnation",
    "authority_mode",
    "allocation_epoch",
    "reporter_high_water",
    "candidate_digest",
    "deployment_generation",
    "configuration_generation",
)


def _compatible(
    requirements: SealedRequirementsV1,
    pool: AgentPoolCapabilityV1,
) -> bool:
    return (
        (requirements.required_pool is None or requirements.required_pool == pool.pool_id)
        and requirements.os == pool.operating_system
        and (
            requirements.cpu_arch == "any"
            or requirements.cpu_arch == pool.cpu_architecture
        )
        and requirements.gpu_vendor == pool.gpu_vendor
        and set(requirements.network_policies) <= set(pool.network_policies)
    )


def _required_capabilities(requirements: SealedRequirementsV1) -> tuple[str, ...]:
    values = {
        f"os.{requirements.os}",
        f"cpu_arch.{requirements.cpu_arch}",
        f"gpu_vendor.{requirements.gpu_vendor}",
        *(f"network.{item}" for item in requirements.network_policies),
    }
    if requirements.required_pool is not None:
        values.add(f"required_pool.{requirements.required_pool}")
    return tuple(sorted(values))


@dataclass(frozen=True, slots=True)
class _BucketKey:
    requirements_digest: str
    execution_generation: int
    eligible_pool_ids: tuple[str, ...]
    required_capabilities: tuple[str, ...]
    local_priority: int


def _bucket_id(key: _BucketKey) -> str:
    encoded = json.dumps(
        {
            "eligible_pool_ids": key.eligible_pool_ids,
            "execution_generation": key.execution_generation,
            "local_priority": key.local_priority,
            "required_capabilities": key.required_capabilities,
            "requirements_digest": key.requirements_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return f"demand-{hashlib.sha256(encoded).hexdigest()}"


def _binding_mismatches(
    observation: GuardDemandObservationV1,
    configuration: ReporterConfigurationV1,
) -> tuple[str, ...]:
    return tuple(
        field
        for field in _BINDING_FIELDS
        if getattr(observation, field) != getattr(configuration, field)
    )


def build_demand_snapshot(
    observation: GuardDemandObservationV1,
    configuration: ReporterConfigurationV1,
) -> DemandSnapshotV1:
    """Build one exact report, or return no report by raising a blocked error."""

    mismatches = _binding_mismatches(observation, configuration)
    if mismatches:
        raise DemandReportBlockedError(
            f"protected observation binding mismatch: {', '.join(mismatches)}"
        )

    grouped: dict[_BucketKey, list[GuardDemandAttemptV1]] = defaultdict(list)
    for attempt in observation.attempts:
        eligible = tuple(
            sorted(
                {
                    offer.pool_id
                    for offer in configuration.pool_capabilities
                    if _compatible(attempt.requirements, offer)
                }
            )
        )
        if not eligible:
            raise DemandReportBlockedError(
                f"protected attempt {attempt.protected_attempt_id} has no compatible pool"
            )
        grouped[
            _BucketKey(
                requirements_digest=attempt.requirements_digest,
                execution_generation=attempt.execution_generation,
                eligible_pool_ids=eligible,
                required_capabilities=_required_capabilities(attempt.requirements),
                local_priority=attempt.submit_priority,
            )
        ].append(attempt)

    buckets = tuple(
        DemandBucketV1(
            bucket_id=_bucket_id(key),
            requested_slots=len(attempts),
            local_priority=key.local_priority,
            oldest_submitted_at=min(item.submitted_at for item in attempts),
            eligible_pool_ids=key.eligible_pool_ids,
            required_capabilities=key.required_capabilities,
            attempt_ids=tuple(str(item.protected_attempt_id) for item in attempts),
        )
        for key, attempts in sorted(grouped.items(), key=lambda item: _bucket_id(item[0]))
    )
    try:
        return DemandSnapshotV1(
            subject_id=observation.subject_id,
            subject_incarnation=observation.subject_incarnation,
            configuration_generation=observation.configuration_generation,
            deployment_generation=observation.deployment_generation,
            reporter_incarnation=observation.reporter_incarnation,
            sequence=observation.sequence,
            source_observed_at=observation.source_observed_at,
            pending_unassigned=buckets,
            current_assignments=(),
            fixed_claims=(),
        )
    except ValidationError as exc:
        raise DemandReportBlockedError("protected demand snapshot is outside manager bounds") from exc


__all__ = ["DemandReportBlockedError", "build_demand_snapshot"]
