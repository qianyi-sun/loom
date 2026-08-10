"""Canonical test builders for the global capacity manager."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    CurrentAssignmentV1,
    DemandBucketV1,
    DemandSnapshotV1,
    FixedClaimV1,
    FleetManifestV1,
    NodeEnvelopeV1,
    ObservedCommitmentV1,
    PoolObservationV1,
    ProfileReferenceV1,
    ResourceVectorV1,
    ShadowEpochV1,
    SubjectConfigurationV1,
    WorkerShapeV1,
    canonical_digest,
)

AUTHORITY_ID = UUID("00000000-0000-4000-8000-000000000001")
DEMAND_REPORTER_ID = UUID("00000000-0000-4000-8000-000000000002")
POOL_REPORTER_GB10_ID = UUID("00000000-0000-4000-8000-000000000003")
POOL_REPORTER_OLDLAB_ID = UUID("00000000-0000-4000-8000-000000000004")
SUBJECT_ID = UUID("00000000-0000-4000-8000-000000000005")
SUBJECT_INCARNATION = UUID("00000000-0000-4000-8000-000000000006")
CONFIG_KEY_A = UUID("00000000-0000-4000-8000-000000000007")
CONFIG_KEY_B = UUID("00000000-0000-4000-8000-000000000008")
ACTIVATION_KEY = UUID("00000000-0000-4000-8000-000000000009")
FIXED_TIME = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
SHA_A = "a" * 64
SHA_B = "b" * 64


def resource_vector_payload(
    *,
    slots: int = 1,
    cpu_millicores: int = 1_000,
    memory_bytes: int = 1_073_741_824,
    gpu_count: int = 0,
    generic: dict[str, int] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "slots": slots,
        "cpu_millicores": cpu_millicores,
        "memory_bytes": memory_bytes,
        "gpu_count": gpu_count,
        "generic": {} if generic is None else generic,
    }


def resource_vector(**overrides: Any) -> ResourceVectorV1:
    return ResourceVectorV1.model_validate(resource_vector_payload(**overrides))


def node(
    node_id: str = "node-a",
    *,
    cpu: int = 8_000,
    memory: int = 17_179_869_184,
    slots: int = 8,
    gpu_count: int = 0,
    features: tuple[str, ...] = (),
) -> NodeEnvelopeV1:
    return NodeEnvelopeV1(
        node_id=node_id,
        allocatable=resource_vector(
            slots=slots,
            cpu_millicores=cpu,
            memory_bytes=memory,
            gpu_count=gpu_count,
        ),
        features=features,
    )


def shape(
    shape_id: str = "one-slot",
    *,
    concurrency_slots: int = 1,
    total: ResourceVectorV1 | None = None,
    per_node: tuple[ResourceVectorV1, ...] | None = None,
    compatible_domain_ids: tuple[str, ...] = ("gb10-arm", "oldlab-x86"),
    capabilities: tuple[str, ...] = ("cpu",),
    warm_approved: bool = True,
) -> WorkerShapeV1:
    resolved_total = total or resource_vector(slots=concurrency_slots)
    resolved_per_node = per_node or (resolved_total,)
    return WorkerShapeV1(
        shape_id=shape_id,
        concurrency_slots=concurrency_slots,
        total_resources=resolved_total,
        node_resources=resolved_per_node,
        compatible_domain_ids=compatible_domain_ids,
        capabilities=capabilities,
        warm_approved=warm_approved,
    )


def _pool_payload(
    pool_id: str,
    *,
    architecture: str,
    domain_id: str,
    reporter_incarnation: UUID,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "pool_id": pool_id,
        "pool_generation": 1,
        "pool_digest": SHA_A if pool_id == "gb10" else SHA_B,
        "controller": f"{pool_id}-controller",
        "partition": "loom",
        "association": "loom",
        "protocol_generation": 1,
        "protocol_digest": SHA_B,
        "pool_reporter_incarnation": reporter_incarnation,
        "resource_domains": [
            {
                "schema_version": 1,
                "domain_id": domain_id,
                "architecture": architecture,
                "partition": "loom",
                "nodes": [node(f"{pool_id}-node").model_dump(mode="python")],
                "topology_constraints": {},
            }
        ],
        "max_slots": 8,
        "max_pending_slots": 8,
        "max_pending_jobs": 8,
        "submission_rate_per_minute": 8,
        "health": "eligible",
    }


def fleet_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "authority_incarnation": AUTHORITY_ID,
        "fleet_generation": 1,
        "fleet_digest": SHA_A,
        "executable_new_capacity_ceiling": 0,
        "tiers": [
            {
                "schema_version": 1,
                "tier_id": tier_id,
                "priority": priority,
                "max_slots": 16,
                "max_pending_slots": 16,
                "max_pending_jobs": 16,
            }
            for priority, tier_id in enumerate(("production", "staging", "development"))
        ],
        "account_policies": [
            {
                "schema_version": 1,
                "account_id": "shared-development",
                "kind": "service",
                "owner_id": None,
                "min_reservation_slots": 0,
                "max_slots": 16,
                "max_surge_slots": 2,
                "max_pending_slots": 16,
                "max_pending_jobs": 16,
                "max_live_subjects": 16,
            }
        ],
        "pools": [
            _pool_payload(
                "gb10",
                architecture="arm64",
                domain_id="gb10-arm",
                reporter_incarnation=POOL_REPORTER_GB10_ID,
            ),
            _pool_payload(
                "oldlab",
                architecture="x86_64",
                domain_id="oldlab-x86",
                reporter_incarnation=POOL_REPORTER_OLDLAB_ID,
            ),
        ],
        "global_max_pending_slots": 32,
        "global_max_pending_jobs": 32,
        "global_submission_rate_per_minute": 32,
    }
    payload.update(overrides)
    return payload


def fleet_manifest(**overrides: Any) -> FleetManifestV1:
    pool_generation = overrides.pop("pool_generation", None)
    payload = fleet_payload(**overrides)
    if pool_generation is not None:
        for pool in payload["pools"]:
            pool["pool_generation"] = pool_generation
    return FleetManifestV1.model_validate(payload)


def valid_profile_payload(
    manifest: FleetManifestV1 | None = None,
    *,
    pool_id: str = "gb10",
) -> dict[str, Any]:
    resolved = manifest or fleet_manifest()
    pool = next(pool for pool in resolved.pools if pool.pool_id == pool_id)
    return {
        "schema_version": 1,
        "pool_id": pool.pool_id,
        "pool_generation": pool.pool_generation,
        "pool_digest": pool.pool_digest,
        "protocol_generation": pool.protocol_generation,
        "protocol_digest": pool.protocol_digest,
        "eligible_resource_domains": tuple(
            domain.domain_id for domain in pool.resource_domains
        ),
        "worker_shapes": (
            shape(
                compatible_domain_ids=tuple(
                    domain.domain_id for domain in pool.resource_domains
                )
            ).model_dump(mode="python"),
        ),
    }


def profile_reference(
    manifest: FleetManifestV1 | None = None,
    **overrides: Any,
) -> ProfileReferenceV1:
    payload = valid_profile_payload(manifest)
    payload.update(overrides)
    return ProfileReferenceV1.model_validate(payload)


def subject_configuration(
    manifest: FleetManifestV1 | None = None,
    **overrides: Any,
) -> SubjectConfigurationV1:
    resolved = manifest or fleet_manifest()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "subject_id": SUBJECT_ID,
        "subject_incarnation": SUBJECT_INCARNATION,
        "display_name": "development",
        "account_id": "shared-development",
        "tier_id": "development",
        "min_slots": 0,
        "max_slots": 8,
        "rollout_surge_slots": 1,
        "max_pending_slots": 8,
        "max_pending_jobs": 8,
        "lifecycle_state": "active",
        "candidate_generation": 1,
        "deployment_generation": 1,
        "configuration_generation": 1,
        "demand_reporter_incarnation": DEMAND_REPORTER_ID,
        "profiles": tuple(
            ProfileReferenceV1.model_validate(valid_profile_payload(resolved, pool_id=pool_id))
            for pool_id in ("gb10", "oldlab")
        ),
    }
    payload.update(overrides)
    return SubjectConfigurationV1.model_validate(payload)


def configuration_activation(
    *,
    fleet: Any,
    subjects: tuple[Any, ...],
    expected_configuration_epoch: int = 0,
) -> ConfigurationActivationV1:
    return ConfigurationActivationV1(
        expected_configuration_epoch=expected_configuration_epoch,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=fleet.generation,
            digest=fleet.digest,
        ),
        subjects=tuple(
            ConfigurationGenerationRefV1(
                scope="subject",
                generation=subject.generation,
                digest=subject.digest,
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
            )
            for subject in subjects
        ),
    )


def demand_snapshot(
    *,
    sequence: int = 1,
    fixed_claim_ids: tuple[str, ...] = (),
    pending_attempt_ids: tuple[str, ...] = ("attempt-pending",),
    assigned_attempt_ids: tuple[str, ...] = (),
) -> DemandSnapshotV1:
    pending = (
        (
            DemandBucketV1(
                bucket_id="cpu-default",
                requested_slots=len(pending_attempt_ids),
                local_priority=0,
                oldest_submitted_at=FIXED_TIME,
                eligible_pool_ids=("gb10", "oldlab"),
                required_capabilities=("cpu",),
                attempt_ids=pending_attempt_ids,
            ),
        )
        if pending_attempt_ids
        else ()
    )
    assignments = tuple(
        CurrentAssignmentV1(
            attempt_id=attempt_id,
            pool_id="gb10",
            profile_id="one-slot",
            allowance_epoch=1,
            local_priority=0,
            submitted_at=FIXED_TIME,
        )
        for attempt_id in assigned_attempt_ids
    )
    claims = tuple(
        FixedClaimV1(
            claim_id=claim_id,
            attempt_id=f"attempt-{claim_id}",
            worker_identity=f"worker-{claim_id}",
            pool_id="gb10",
            profile_id="one-slot",
            shape_id="one-slot",
            deployment_generation=1,
            concurrency_slots=1,
            resources=resource_vector(),
            state="live",
        )
        for claim_id in fixed_claim_ids
    )
    return DemandSnapshotV1(
        subject_id=SUBJECT_ID,
        subject_incarnation=SUBJECT_INCARNATION,
        configuration_generation=1,
        deployment_generation=1,
        reporter_incarnation=DEMAND_REPORTER_ID,
        sequence=sequence,
        source_observed_at=FIXED_TIME,
        pending_unassigned=pending,
        current_assignments=assignments,
        fixed_claims=claims,
    )


def pool_observation(
    *,
    sequence: int = 1,
    pool_id: str = "gb10",
    commitment_ids: tuple[str, ...] = (),
) -> PoolObservationV1:
    reporter = (
        POOL_REPORTER_GB10_ID if pool_id == "gb10" else POOL_REPORTER_OLDLAB_ID
    )
    return PoolObservationV1(
        pool_id=pool_id,
        pool_generation=1,
        reporter_incarnation=reporter,
        sequence=sequence,
        source_observed_at=FIXED_TIME,
        health="eligible",
        commitments=tuple(
            ObservedCommitmentV1(
                commitment_id=commitment_id,
                physical_identity=f"worker-{commitment_id}",
                subject_id=SUBJECT_ID,
                subject_incarnation=SUBJECT_INCARNATION,
                deployment_generation=1,
                pool_id=pool_id,
                pool_generation=1,
                profile_id="one-slot",
                shape_id="one-slot",
                resources=resource_vector(),
                state="observed",
            )
            for commitment_id in commitment_ids
        ),
    )


def shadow_epoch(allocation_input: Any) -> ShadowEpochV1:
    return ShadowEpochV1(
        configuration=allocation_input.configuration,
        input_digest=canonical_digest(allocation_input),
        allocations=(),
        next_fairness_cursors=allocation_input.fairness_cursors,
        hypothetical_launch_rank=(),
        blockers=(),
    )
