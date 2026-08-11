"""Canonical test builders for the global capacity manager."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    AllocationInputV1,
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    CurrentAssignmentV1,
    DemandBucketV1,
    DemandSnapshotV1,
    DevelopmentSubjectTemplateV1,
    DynamicDevelopmentSubjectProjectionV1,
    FairnessCursorV1,
    FixedClaimV1,
    FleetManifestV1,
    InputFreshnessV1,
    NodeEnvelopeV1,
    ObservedCommitmentV1,
    PackingRequestV1,
    PackingShapeRequestV1,
    PoolAllocationInputV1,
    PoolManifestV1,
    PoolObservationV1,
    ProfileReferenceV1,
    ResourceVectorV1,
    ShadowEpochV1,
    SubjectAllocationInputV1,
    SubjectConfigurationV1,
    WorkerShapeV1,
    canonical_digest,
    canonical_digest_excluding,
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
DEVELOPMENT_OWNER_ID = UUID("00000000-0000-4000-8000-000000000100")
DEVELOPMENT_SUBJECT_ID = UUID("00000000-0000-4000-8000-000000000101")
DEVELOPMENT_SUBJECT_INCARNATION = UUID("00000000-0000-4000-8000-000000000102")
DEVELOPMENT_REPORTER_INCARNATION = UUID("00000000-0000-4000-8000-000000000103")
DEVELOPMENT_OPERATION_ID = UUID("00000000-0000-4000-8000-000000000104")


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
    placement_constraints: dict[str, str] | None = None,
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
        placement_constraints=placement_constraints or {},
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
    for pool_payload in payload["pools"]:
        pool = PoolManifestV1.model_validate(pool_payload)
        pool_payload["pool_digest"] = canonical_digest_excluding(pool, "pool_digest")
    manifest = FleetManifestV1.model_validate(payload)
    payload["fleet_digest"] = canonical_digest_excluding(manifest, "fleet_digest")
    return FleetManifestV1.model_validate(payload)


def valid_profile_payload(
    manifest: FleetManifestV1 | None = None,
    *,
    pool_id: str = "gb10",
) -> dict[str, Any]:
    resolved = manifest or fleet_manifest()
    pool = next(pool for pool in resolved.pools if pool.pool_id == pool_id)
    payload = {
        "schema_version": 1,
        "pool_id": pool.pool_id,
        "pool_generation": pool.pool_generation,
        "pool_digest": pool.pool_digest,
        "profile_generation": 1,
        "profile_digest": SHA_A if pool_id == "gb10" else SHA_B,
        "protocol_generation": pool.protocol_generation,
        "protocol_digest": pool.protocol_digest,
        "eligible_resource_domains": tuple(domain.domain_id for domain in pool.resource_domains),
        "worker_shapes": (
            shape(
                compatible_domain_ids=tuple(domain.domain_id for domain in pool.resource_domains)
            ).model_dump(mode="python"),
        ),
    }
    profile = ProfileReferenceV1.model_validate(payload)
    payload["profile_digest"] = canonical_digest_excluding(profile, "profile_digest")
    return payload


def fleet_with_development_template(
    *,
    owner_min_reservation_slots: int = 4,
    owner_max_slots: int = 8,
    owner_max_live_subjects: int = 2,
    max_slots_per_subject: int = 8,
) -> FleetManifestV1:
    base = fleet_manifest()
    owner_template = AccountPolicyV1(
        account_id="personal-development-owner",
        kind="owner_template",
        owner_id=None,
        min_reservation_slots=owner_min_reservation_slots,
        max_slots=owner_max_slots,
        max_surge_slots=1,
        max_pending_slots=8,
        max_pending_jobs=8,
        max_live_subjects=owner_max_live_subjects,
    )
    template = DevelopmentSubjectTemplateV1(
        owner_account_template_id=owner_template.account_id,
        max_slots_per_subject=max_slots_per_subject,
        rollout_surge_slots=0,
        max_pending_slots_per_subject=8,
        max_pending_jobs_per_subject=8,
        profiles=tuple(
            ProfileReferenceV1.model_validate(valid_profile_payload(base, pool_id=pool_id))
            for pool_id in ("gb10", "oldlab")
        ),
    )
    changed = base.model_copy(
        update={
            "fleet_digest": "f" * 64,
            "account_policies": tuple(
                sorted(
                    (*base.account_policies, owner_template),
                    key=lambda account: account.account_id,
                )
            ),
            "development_subject_template": template,
        }
    )
    return changed.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed, "fleet_digest")}
    )


def development_projection(
    *,
    expected_configuration_epoch: int = 1,
    operation_id: UUID = DEVELOPMENT_OPERATION_ID,
    operation_epoch: int = 1,
    environment_name: str = "alice",
    subject_id: UUID = DEVELOPMENT_SUBJECT_ID,
    subject_incarnation: UUID = DEVELOPMENT_SUBJECT_INCARNATION,
    owner_id: UUID = DEVELOPMENT_OWNER_ID,
    min_slots: int = 0,
    max_slots: int = 2,
    candidate_generation: int = 1,
    deployment_generation: int = 1,
    configuration_generation: int = 1,
    demand_reporter_incarnation: UUID = DEVELOPMENT_REPORTER_INCARNATION,
) -> DynamicDevelopmentSubjectProjectionV1:
    return DynamicDevelopmentSubjectProjectionV1(
        expected_configuration_epoch=expected_configuration_epoch,
        operation_id=operation_id,
        operation_epoch=operation_epoch,
        environment_name=environment_name,
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
        owner_id=owner_id,
        min_slots=min_slots,
        max_slots=max_slots,
        candidate_generation=candidate_generation,
        candidate_sha256="a" * 64,
        candidate_publication_sha256="b" * 64,
        deployment_generation=deployment_generation,
        configuration_generation=configuration_generation,
        demand_reporter_incarnation=demand_reporter_incarnation,
        demand_reporter_token_sha256="c" * 64,
        local_activation_sha256="d" * 64,
        supported_pool_ids=("gb10", "oldlab"),
        supported_architectures=("arm64", "x86_64"),
        protocol_versions={
            "capacity-agent": "v1",
            "claim-guard": "v1",
            "control-plane-worker": "v1",
        },
    )


def profile_reference(
    manifest: FleetManifestV1 | None = None,
    **overrides: Any,
) -> ProfileReferenceV1:
    payload = valid_profile_payload(manifest)
    payload.update(overrides)
    profile = ProfileReferenceV1.model_validate(payload)
    payload["profile_digest"] = canonical_digest_excluding(profile, "profile_digest")
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
    subject_id: UUID = SUBJECT_ID,
    subject_incarnation: UUID = SUBJECT_INCARNATION,
    configuration_generation: int = 1,
    deployment_generation: int = 1,
    reporter_incarnation: UUID = DEMAND_REPORTER_ID,
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
            pool_generation=1,
            profile_id="one-slot",
            profile_generation=1,
            profile_digest=SHA_A,
            shape_id="one-slot",
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
            pool_generation=1,
            profile_id="one-slot",
            profile_generation=1,
            profile_digest=SHA_A,
            shape_id="one-slot",
            deployment_generation=1,
            concurrency_slots=1,
            resources=resource_vector(),
            state="live",
        )
        for claim_id in fixed_claim_ids
    )
    return DemandSnapshotV1(
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
        configuration_generation=configuration_generation,
        deployment_generation=deployment_generation,
        reporter_incarnation=reporter_incarnation,
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
    reporter = POOL_REPORTER_GB10_ID if pool_id == "gb10" else POOL_REPORTER_OLDLAB_ID
    return PoolObservationV1(
        pool_id=pool_id,
        pool_generation=1,
        reporter_incarnation=reporter,
        sequence=sequence,
        source_observed_at=FIXED_TIME,
        health="eligible",
        commitments=tuple(
            ObservedCommitmentV1(
                kind="physical",
                commitment_id=commitment_id,
                physical_identity=f"worker-{commitment_id}",
                subject_id=SUBJECT_ID,
                subject_incarnation=SUBJECT_INCARNATION,
                deployment_generation=1,
                pool_id=pool_id,
                pool_generation=1,
                profile_id="one-slot",
                profile_generation=1,
                profile_digest=SHA_A if pool_id == "gb10" else SHA_B,
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
        pool_witnesses=(),
        blockers=(),
    )


def packing_request(
    *,
    nodes: tuple[NodeEnvelopeV1, ...] | None = None,
    shapes: tuple[WorkerShapeV1, ...] = (),
    fixed_commitments: tuple[ObservedCommitmentV1, ...] = (),
    reverse: bool = False,
) -> PackingRequestV1:
    resolved_nodes = nodes or (node("node-a"), node("node-b"))
    desired = tuple(
        PackingShapeRequestV1(
            instance_id=f"{item.shape_id}-{index:04d}",
            shape=item,
        )
        for index, item in enumerate(shapes)
    )
    if reverse:
        resolved_nodes = tuple(reversed(resolved_nodes))
        desired = tuple(reversed(desired))
    return PackingRequestV1(
        pool_id="gb10",
        domains=(
            {
                "schema_version": 1,
                "domain_id": "gb10-arm",
                "architecture": "arm64",
                "partition": "loom",
                "nodes": resolved_nodes,
                "topology_constraints": {},
            },
        ),
        fixed_commitments=fixed_commitments,
        desired_shapes=desired,
    )


def fragmented_request(*, reverse: bool = False) -> PackingRequestV1:
    first = shape(
        "large",
        total=resource_vector(slots=1, cpu_millicores=8, memory_bytes=4),
        per_node=(resource_vector(slots=1, cpu_millicores=8, memory_bytes=4),),
        compatible_domain_ids=("gb10-arm",),
    )
    second = shape(
        "balanced",
        total=resource_vector(slots=1, cpu_millicores=4, memory_bytes=4),
        per_node=(resource_vector(slots=1, cpu_millicores=4, memory_bytes=4),),
        compatible_domain_ids=("gb10-arm",),
    )
    return packing_request(
        nodes=(
            node("node-a", cpu=8, memory=4, slots=2),
            node("node-b", cpu=4, memory=8, slots=2),
        ),
        shapes=(first, second),
        reverse=reverse,
    )


def request_with_old_generation_commitment_over_limit() -> PackingRequestV1:
    commitment = ObservedCommitmentV1(
        kind="physical",
        commitment_id="old-worker-a",
        physical_identity="old-worker-a",
        subject_id=SUBJECT_ID,
        subject_incarnation=SUBJECT_INCARNATION,
        deployment_generation=1,
        pool_id="gb10",
        pool_generation=1,
        profile_id="old-profile",
        profile_generation=1,
        profile_digest=SHA_A,
        shape_id="old-shape",
        resources=resource_vector(slots=2, cpu_millicores=16, memory_bytes=16),
        state="observed",
        node_ids=("node-a",),
    )
    return packing_request(
        nodes=(node("node-a", cpu=8, memory=8, slots=1),),
        fixed_commitments=(commitment,),
    )


def allocator_subject(
    index: int,
    *,
    account_id: str,
    tier_id: str = "development",
    pending: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (),
    assigned: tuple[tuple[str, str], ...] = (),
    fixed_claims: tuple[FixedClaimV1, ...] = (),
    min_slots: int = 0,
    max_slots: int = 32,
    rollout_surge_slots: int = 0,
    deployment_generation: int = 1,
    freshness: str = "valid",
) -> SubjectAllocationInputV1:
    subject_id = UUID(int=1_000 + index * 3)
    incarnation = UUID(int=1_001 + index * 3)
    reporter = UUID(int=1_002 + index * 3)
    configuration = subject_configuration(
        subject_id=subject_id,
        subject_incarnation=incarnation,
        demand_reporter_incarnation=reporter,
        display_name=f"subject-{index}",
        account_id=account_id,
        tier_id=tier_id,
        min_slots=min_slots,
        max_slots=max_slots,
        rollout_surge_slots=rollout_surge_slots,
        deployment_generation=deployment_generation,
        max_pending_slots=max_slots,
        max_pending_jobs=max_slots,
    )
    buckets = tuple(
        DemandBucketV1(
            bucket_id=f"bucket-{attempt_id}",
            requested_slots=1,
            local_priority=0,
            oldest_submitted_at=FIXED_TIME + timedelta(seconds=offset),
            eligible_pool_ids=pool_ids,
            required_capabilities=capabilities,
            attempt_ids=(attempt_id,),
        )
        for offset, (attempt_id, pool_ids, capabilities) in enumerate(pending)
    )
    assignments = tuple(
        CurrentAssignmentV1(
            attempt_id=attempt_id,
            pool_id=pool_id,
            pool_generation=1,
            profile_id="one-slot",
            profile_generation=1,
            profile_digest=SHA_A if pool_id == "gb10" else SHA_B,
            shape_id="one-slot",
            allowance_epoch=1,
            local_priority=0,
            submitted_at=FIXED_TIME,
        )
        for attempt_id, pool_id in assigned
    )
    snapshot = DemandSnapshotV1(
        subject_id=subject_id,
        subject_incarnation=incarnation,
        configuration_generation=configuration.configuration_generation,
        deployment_generation=configuration.deployment_generation,
        reporter_incarnation=reporter,
        sequence=1,
        source_observed_at=FIXED_TIME,
        pending_unassigned=buckets,
        current_assignments=assignments,
        fixed_claims=fixed_claims,
    )
    freshness_model = InputFreshnessV1(
        state=freshness,
        last_payload_digest=canonical_digest(snapshot),
        database_received_at=FIXED_TIME,
    )
    return SubjectAllocationInputV1(
        configuration=configuration,
        freshness=freshness_model,
        last_demand=snapshot,
    )


def allocator_input(
    subjects: tuple[SubjectAllocationInputV1, ...],
    *,
    gb10_slots: int,
    oldlab_slots: int,
    observed_commitments: tuple[ObservedCommitmentV1, ...] = (),
    fairness_cursors: tuple[FairnessCursorV1, ...] = (),
    existing_pending_slots: int = 0,
    existing_pending_jobs: int = 0,
    global_pending_slots: int = 128,
    global_pending_jobs: int = 128,
) -> AllocationInputV1:
    payload = fleet_payload(
        account_policies=[
            {
                "schema_version": 1,
                "account_id": account_id,
                "kind": "owner",
                "owner_id": None,
                "min_reservation_slots": 0,
                "max_slots": 128,
                "max_surge_slots": max(
                    subject.configuration.rollout_surge_slots
                    for subject in subjects
                    if subject.configuration.account_id == account_id
                ),
                "max_pending_slots": 128,
                "max_pending_jobs": 128,
                "max_live_subjects": 32,
            }
            for account_id in sorted({subject.configuration.account_id for subject in subjects})
        ],
        global_max_pending_slots=global_pending_slots,
        global_max_pending_jobs=global_pending_jobs,
    )
    pool_slots = {"gb10": gb10_slots, "oldlab": oldlab_slots}
    for pool in payload["pools"]:
        slots = pool_slots[pool["pool_id"]]
        pool["max_slots"] = slots
        pool["max_pending_slots"] = max(slots, global_pending_slots)
        pool["max_pending_jobs"] = max(slots, global_pending_jobs)
        allocatable = pool["resource_domains"][0]["nodes"][0]["allocatable"]
        allocatable.update(
            slots=slots,
            cpu_millicores=slots * 1_000,
            memory_bytes=slots * 1_073_741_824,
        )
    total_slots = gb10_slots + oldlab_slots
    for tier in payload["tiers"]:
        tier["max_slots"] = total_slots
        tier["max_pending_slots"] = global_pending_slots
        tier["max_pending_jobs"] = global_pending_jobs
    for pool_payload in payload["pools"]:
        pool = PoolManifestV1.model_validate(pool_payload)
        pool_payload["pool_digest"] = canonical_digest_excluding(pool, "pool_digest")
    manifest = FleetManifestV1.model_validate(payload)
    payload["fleet_digest"] = canonical_digest_excluding(manifest, "fleet_digest")
    manifest = FleetManifestV1.model_validate(payload)
    pools_by_id = {pool.pool_id: pool for pool in manifest.pools}

    resolved_subjects: list[SubjectAllocationInputV1] = []
    for subject_input in subjects:
        profiles: list[ProfileReferenceV1] = []
        for current_profile in subject_input.configuration.profiles:
            pool = pools_by_id[current_profile.pool_id]
            profile = current_profile.model_copy(update={"pool_digest": pool.pool_digest})
            profile = profile.model_copy(
                update={"profile_digest": canonical_digest_excluding(profile, "profile_digest")}
            )
            profiles.append(profile)
        configuration = subject_input.configuration.model_copy(update={"profiles": tuple(profiles)})
        profile_by_pool = {profile.pool_id: profile for profile in profiles}
        demand = subject_input.last_demand
        if demand is not None:
            assignments = tuple(
                assignment.model_copy(
                    update={
                        "pool_generation": profile_by_pool[assignment.pool_id].pool_generation,
                        "profile_generation": profile_by_pool[
                            assignment.pool_id
                        ].profile_generation,
                        "profile_digest": profile_by_pool[assignment.pool_id].profile_digest,
                    }
                )
                for assignment in demand.current_assignments
            )
            claims = tuple(
                claim.model_copy(
                    update={
                        "pool_generation": profile_by_pool[claim.pool_id].pool_generation,
                        "profile_generation": profile_by_pool[claim.pool_id].profile_generation,
                        "profile_digest": profile_by_pool[claim.pool_id].profile_digest,
                    }
                )
                if claim.deployment_generation == configuration.deployment_generation
                else claim
                for claim in demand.fixed_claims
            )
            demand = demand.model_copy(
                update={
                    "current_assignments": assignments,
                    "fixed_claims": claims,
                }
            )
        freshness = subject_input.freshness.model_copy(
            update={
                "last_payload_digest": (canonical_digest(demand) if demand is not None else None)
            }
        )
        resolved_subjects.append(
            subject_input.model_copy(
                update={
                    "configuration": configuration,
                    "last_demand": demand,
                    "freshness": freshness,
                }
            )
        )
    resolved_subject_tuple = tuple(resolved_subjects)
    subject_by_id = {item.configuration.subject_id: item for item in resolved_subject_tuple}
    resolved_observed = tuple(
        evidence.model_copy(
            update={
                "pool_generation": profile.pool_generation,
                "profile_generation": profile.profile_generation,
                "profile_digest": profile.profile_digest,
            }
        )
        if (
            (subject := subject_by_id.get(evidence.subject_id)) is not None
            and evidence.deployment_generation == subject.configuration.deployment_generation
            and (
                profile := next(
                    (
                        item
                        for item in subject.configuration.profiles
                        if item.pool_id == evidence.pool_id
                    ),
                    None,
                )
            )
            is not None
        )
        else evidence
        for evidence in observed_commitments
    )

    pool_inputs = tuple(
        PoolAllocationInputV1(
            configuration=pool,
            freshness=InputFreshnessV1(
                state="valid",
                last_payload_digest=SHA_A,
                database_received_at=FIXED_TIME,
            ),
            last_observation=PoolObservationV1(
                pool_id=pool.pool_id,
                pool_generation=pool.pool_generation,
                reporter_incarnation=pool.pool_reporter_incarnation,
                sequence=1,
                source_observed_at=FIXED_TIME,
                health="eligible",
                commitments=tuple(
                    item
                    for item in resolved_observed
                    if item.pool_id == pool.pool_id and item.kind == "physical"
                ),
            ),
        )
        for pool in manifest.pools
    )
    configuration = ConfigurationSnapshotV1(
        configuration_epoch=1,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=manifest.fleet_generation,
            digest=canonical_digest(manifest),
        ),
        subjects=tuple(
            ConfigurationGenerationRefV1(
                scope="subject",
                generation=subject.configuration.configuration_generation,
                digest=canonical_digest(subject.configuration),
                subject_id=subject.configuration.subject_id,
                subject_incarnation=subject.configuration.subject_incarnation,
            )
            for subject in resolved_subject_tuple
        ),
    )
    return AllocationInputV1(
        configuration=configuration,
        fleet=manifest,
        subjects=resolved_subject_tuple,
        pools=pool_inputs,
        observed_commitments=resolved_observed,
        fairness_cursors=fairness_cursors,
        existing_pending_slots=existing_pending_slots,
        existing_pending_jobs=existing_pending_jobs,
    )
