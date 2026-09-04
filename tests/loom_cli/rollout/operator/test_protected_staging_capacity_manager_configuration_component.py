from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, ClassVar
from uuid import UUID, uuid5

import pytest

import loom_cli.rollout.operator.protected_staging_capacity_manager_configuration_component as manager_configuration_module
from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    FleetManifestV1,
    PoolManifestV1,
    ProfileReferenceV1,
    StaticCandidateProvenanceV1,
    SubjectConfigurationV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClientError,
)
from loom_cli.rollout.operator.protected_capacity_manager_configuration_compensation import (
    CapacityManagerConfigurationCompensationStore,
)
from loom_cli.rollout.operator.protected_staging_capacity_manager_configuration_component import (
    KubernetesProtectedStagingCapacityManagerConfigurationComponent,
)
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    DEMAND_REPORTER_ID,
    SUBJECT_ID,
    fleet_with_development_template,
    node,
    shape,
    subject_configuration,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan

_STAGING_SUBJECT = UUID("00000000-0000-4000-8000-00000000c101")
_STAGING_INCARNATION = UUID("00000000-0000-4000-8000-00000000c102")
_STAGING_REPORTER = UUID("00000000-0000-4000-8000-00000000c103")


def _profile(
    pool: PoolManifestV1,
    *,
    profile_generation: int = 1,
    shape_prefix: str = "template",
    include_two_slot: bool = True,
) -> ProfileReferenceV1:
    domain_ids = tuple(domain.domain_id for domain in pool.resource_domains)
    shapes = [
        shape(
            f"{shape_prefix}-one-slot",
            compatible_domain_ids=domain_ids,
        )
    ]
    if include_two_slot:
        shapes.append(
            shape(
                f"{shape_prefix}-two-slot",
                concurrency_slots=2,
                compatible_domain_ids=domain_ids,
            )
        )
    value = ProfileReferenceV1(
        pool_id=pool.pool_id,
        pool_generation=pool.pool_generation,
        pool_digest=pool.pool_digest,
        profile_generation=profile_generation,
        profile_digest="0" * 64,
        protocol_generation=pool.protocol_generation,
        protocol_digest=pool.protocol_digest,
        eligible_resource_domains=domain_ids,
        worker_shapes=tuple(shapes),
    )
    return value.model_copy(
        update={"profile_digest": canonical_digest_excluding(value, "profile_digest")}
    )


def _live_fleet(*, include_template: bool = True) -> FleetManifestV1:
    base = fleet_with_development_template(
        owner_min_reservation_slots=4,
        owner_max_slots=8,
        owner_max_live_subjects=2,
        max_slots_per_subject=8,
    )
    pools: list[PoolManifestV1] = []
    for current in base.pools:
        domain = current.resource_domains[0]
        if current.pool_id == "gb10":
            exact_nodes = tuple(
                sorted(
                    (node(f"trt-gb10-{index}", slots=10) for index in range(1, 16)),
                    key=lambda item: item.node_id,
                )
            )
            maximum = 150
        else:
            exact_nodes = tuple(node(f"trt-eai-oldlab-{index}", slots=6) for index in range(3, 6))
            maximum = 18
        changed_domain = domain.model_copy(update={"nodes": exact_nodes})
        changed_pool = current.model_copy(
            update={
                "pool_digest": "0" * 64,
                "resource_domains": (changed_domain,),
                "max_slots": maximum,
            }
        )
        pools.append(
            changed_pool.model_copy(
                update={"pool_digest": canonical_digest_excluding(changed_pool, "pool_digest")}
            )
        )
    template = None
    if include_template:
        assert base.development_subject_template is not None
        template = base.development_subject_template.model_copy(
            update={"profiles": tuple(_profile(pool) for pool in pools)}
        )
    fleet = base.model_copy(
        update={
            "fleet_generation": 7,
            "fleet_digest": "0" * 64,
            "pools": tuple(pools),
            "development_subject_template": template,
        }
    )
    return fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
    )


def _active_document(
    fleet: FleetManifestV1,
    subjects: tuple[SubjectConfigurationV1, ...],
    *,
    epoch: int = 3,
) -> dict[str, object]:
    snapshot = ConfigurationSnapshotV1(
        configuration_epoch=epoch,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=fleet.fleet_generation,
            digest=canonical_digest(fleet),
        ),
        subjects=tuple(
            ConfigurationGenerationRefV1(
                scope="subject",
                generation=subject.configuration_generation,
                digest=canonical_digest(subject),
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
            )
            for subject in subjects
        ),
    )
    return {
        "schema_version": 1,
        "configuration": snapshot.model_dump(mode="json"),
        "fleet": fleet.model_dump(mode="json"),
        "subjects": [subject.model_dump(mode="json") for subject in subjects],
    }


class _Client:
    @dataclass(frozen=True, slots=True)
    class MutationOutcome:
        response: object
        mutate_document: bool = True

    def __init__(
        self,
        document: dict[str, object],
        *,
        stale_second_read: bool = False,
        equivocal_fleet_response: bool = False,
        retain_old_readback: bool = False,
        read_sequence: tuple[dict[str, object], ...] | None = None,
        activate_outcomes: tuple[MutationOutcome, ...] = (),
        rollback_outcomes: tuple[MutationOutcome, ...] = (),
        on_get: Callable[[int, dict[str, object]], None] | None = None,
        rollback_document: dict[str, object] | None = None,
        on_rollback: Callable[[dict[str, object], UUID], None] | None = None,
    ) -> None:
        self.document = deepcopy(document)
        self.original = deepcopy(document)
        self.stale_second_read = stale_second_read
        self.equivocal_fleet_response = equivocal_fleet_response
        self.retain_old_readback = retain_old_readback
        self.read_sequence = [deepcopy(item) for item in read_sequence] if read_sequence else None
        self.activate_outcomes = list(activate_outcomes)
        self.rollback_outcomes = list(rollback_outcomes)
        self.on_get = on_get
        self.rollback_document = (
            deepcopy(rollback_document) if rollback_document is not None else None
        )
        self.on_rollback = on_rollback
        self.reads = 0
        self.calls: list[tuple[str, object, UUID | None]] = []
        self.pending_fleet: dict[str, object] | None = None
        self.pending_subjects: dict[UUID, dict[str, object]] = {}

    def get_configuration(self) -> dict[str, object]:
        self.reads += 1
        self.calls.append(("get", {}, None))
        if self.read_sequence and self.reads <= len(self.read_sequence):
            result = deepcopy(self.read_sequence[self.reads - 1])
        else:
            result = deepcopy(self.document)
            if self.stale_second_read and self.reads == 2:
                configuration = result["configuration"]
                assert isinstance(configuration, dict)
                configuration["configuration_epoch"] = int(configuration["configuration_epoch"]) + 1
        if self.on_get is not None:
            self.on_get(self.reads, result)
        return result

    @staticmethod
    def _proposal(
        *,
        scope: str,
        generation: int,
        digest: str,
        subject_id: UUID | None = None,
        subject_incarnation: UUID | None = None,
    ) -> dict[str, object]:
        identity = uuid5(
            UUID("00000000-0000-4000-8000-000000000999"),
            f"{scope}:{generation}:{digest}:{subject_id}",
        )
        return {
            "configuration_id": str(identity),
            "scope": scope,
            "generation": generation,
            "digest": digest,
            "subject_id": None if subject_id is None else str(subject_id),
            "subject_incarnation": (
                None if subject_incarnation is None else str(subject_incarnation)
            ),
        }

    @staticmethod
    def _response_for_document(document: dict[str, object]) -> dict[str, object]:
        snapshot = ConfigurationSnapshotV1.model_validate_json(
            json.dumps(document["configuration"])
        )
        return {
            "configuration_epoch": snapshot.configuration_epoch,
            "digest": canonical_digest(snapshot),
            "snapshot": snapshot.model_dump(mode="json"),
        }

    def propose_fleet(self, payload: dict[str, object], idempotency_key: UUID) -> dict[str, object]:
        self.calls.append(("fleet", deepcopy(payload), idempotency_key))
        self.pending_fleet = deepcopy(payload)
        fleet = FleetManifestV1.model_validate_json(json.dumps(payload))
        digest = canonical_digest(fleet)
        if self.equivocal_fleet_response:
            digest = "f" * 64
        return self._proposal(
            scope="fleet",
            generation=fleet.fleet_generation,
            digest=digest,
        )

    def propose_subject(
        self,
        subject_id: UUID,
        payload: dict[str, object],
        idempotency_key: UUID,
    ) -> dict[str, object]:
        self.calls.append(("subject", deepcopy(payload), idempotency_key))
        self.pending_subjects[subject_id] = deepcopy(payload)
        subject = SubjectConfigurationV1.model_validate_json(json.dumps(payload))
        return self._proposal(
            scope="subject",
            generation=subject.configuration_generation,
            digest=canonical_digest(subject),
            subject_id=subject.subject_id,
            subject_incarnation=subject.subject_incarnation,
        )

    def activate(self, payload: dict[str, object], idempotency_key: UUID) -> dict[str, object]:
        self.calls.append(("activate", deepcopy(payload), idempotency_key))
        activation = ConfigurationActivationV1.model_validate_json(json.dumps(payload))
        assert self.pending_fleet is not None
        fleet = FleetManifestV1.model_validate_json(json.dumps(self.pending_fleet))
        current_subjects = {
            SubjectConfigurationV1.model_validate_json(
                json.dumps(item)
            ).subject_id: SubjectConfigurationV1.model_validate_json(json.dumps(item))
            for item in self.document["subjects"]  # type: ignore[union-attr]
        }
        current_subjects.update(
            {
                subject_id: SubjectConfigurationV1.model_validate_json(json.dumps(item))
                for subject_id, item in self.pending_subjects.items()
            }
        )
        subjects = tuple(
            current_subjects[reference.subject_id] for reference in activation.subjects
        )
        next_epoch = activation.expected_configuration_epoch + 1
        next_document = _active_document(fleet, subjects, epoch=next_epoch)
        snapshot = ConfigurationSnapshotV1.model_validate_json(
            json.dumps(next_document["configuration"])
        )
        response = {
            "configuration_epoch": next_epoch,
            "digest": canonical_digest(snapshot),
            "snapshot": snapshot.model_dump(mode="json"),
        }
        outcome = (
            self.activate_outcomes.pop(0)
            if self.activate_outcomes
            else _Client.MutationOutcome(
                response=response, mutate_document=not self.retain_old_readback
            )
        )
        if outcome.mutate_document:
            self.document = next_document
        if isinstance(outcome.response, Exception):
            raise outcome.response
        return deepcopy(outcome.response)

    def rollback(self, payload: dict[str, object], idempotency_key: UUID) -> dict[str, object]:
        self.calls.append(("rollback", deepcopy(payload), idempotency_key))
        request = payload
        restore_epoch = int(request["restore_configuration_epoch"])
        if restore_epoch != int(self.original["configuration"]["configuration_epoch"]):  # type: ignore[index]
            raise AssertionError("rollback restore epoch drifted")
        if self.rollback_document is None:
            current_fleet = FleetManifestV1.model_validate_json(json.dumps(self.document["fleet"]))
            predecessor_fleet = FleetManifestV1.model_validate_json(
                json.dumps(self.original["fleet"])
            )
            cloned_fleet = predecessor_fleet.model_copy(
                update={
                    "fleet_generation": current_fleet.fleet_generation + 1,
                    "fleet_digest": "0" * 64,
                }
            )
            cloned_fleet = cloned_fleet.model_copy(
                update={"fleet_digest": canonical_digest_excluding(cloned_fleet, "fleet_digest")}
            )
            current_subjects = {
                subject.subject_id: subject
                for subject in (
                    SubjectConfigurationV1.model_validate_json(json.dumps(item))
                    for item in self.document["subjects"]  # type: ignore[union-attr]
                )
            }
            predecessor_subjects = tuple(
                SubjectConfigurationV1.model_validate_json(json.dumps(item))
                for item in self.original["subjects"]  # type: ignore[union-attr]
            )
            rollback_document = _active_document(
                cloned_fleet,
                tuple(
                    subject.model_copy(
                        update={
                            "configuration_generation": (
                                current_subjects[subject.subject_id].configuration_generation + 1
                            )
                        }
                    )
                    for subject in predecessor_subjects
                ),
                epoch=int(request["expected_configuration_epoch"]) + 1,
            )
        else:
            configuration = self.rollback_document["configuration"]
            assert isinstance(configuration, dict)
            rollback_document = deepcopy(self.rollback_document)
            rollback_configuration = deepcopy(configuration)
            rollback_configuration["configuration_epoch"] = (
                int(request["expected_configuration_epoch"]) + 1
            )
            rollback_document["configuration"] = rollback_configuration
        snapshot = ConfigurationSnapshotV1.model_validate_json(
            json.dumps(rollback_document["configuration"])
        )
        response = self._response_for_document(
            {
                "schema_version": 1,
                "configuration": snapshot.model_dump(mode="json"),
                "fleet": rollback_document["fleet"],
                "subjects": rollback_document["subjects"],
            }
        )
        outcome = (
            self.rollback_outcomes.pop(0)
            if self.rollback_outcomes
            else _Client.MutationOutcome(response=response, mutate_document=True)
        )
        if self.on_rollback is not None:
            self.on_rollback(deepcopy(payload), idempotency_key)
        if outcome.mutate_document:
            self.document = rollback_document
        if isinstance(outcome.response, Exception):
            raise outcome.response
        return deepcopy(outcome.response)


class _Runner:
    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/protected/kubeconfig"}


def _component(
    client: _Client,
    seed: dict[str, object],
    *,
    root: Path | None = None,
    seed_reader: Any | None = None,
):
    @contextmanager
    def client_context(**_kwargs: object):
        yield client

    return KubernetesProtectedStagingCapacityManagerConfigurationComponent(
        runner=_Runner(),  # type: ignore[arg-type]
        credentials_root=(
            Path("/protected/credentials") if root is None else (root / "credentials").resolve()
        ),
        service_uid=1000 if root is None else os.geteuid(),
        service_gid=1000 if root is None else os.getegid(),
        seed_reader=(lambda: deepcopy(seed)) if seed_reader is None else seed_reader,
        client_context=client_context,
    )


def _component_with_credentials_root(
    client: _Client,
    seed: dict[str, object],
    *,
    credentials_root: Path,
    seed_reader: Any | None = None,
):
    @contextmanager
    def client_context(**_kwargs: object):
        yield client

    return KubernetesProtectedStagingCapacityManagerConfigurationComponent(
        runner=_Runner(),  # type: ignore[arg-type]
        credentials_root=credentials_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        seed_reader=(lambda: deepcopy(seed)) if seed_reader is None else seed_reader,
        client_context=client_context,
    )


def _seed() -> dict[str, object]:
    return {
        "authority_incarnation": str(AUTHORITY_ID),
        "subject_id": str(_STAGING_SUBJECT),
        "subject_incarnation": str(_STAGING_INCARNATION),
        "reporter_incarnation": str(_STAGING_REPORTER),
    }


def _drifted_seed(seed: dict[str, object]) -> dict[str, object]:
    drifted = deepcopy(seed)
    drifted["reporter_incarnation"] = "00000000-0000-4000-8000-00000000c1ff"
    return drifted


def _plan_with_manager_configuration(
    tmp_path: Path,
    *,
    manager_configuration_epoch: int,
    manager_configuration_digest: str,
):
    plan = _plan(tmp_path)
    authority = DatabaseAuthorityEvidence(
        public_schema_revision=plan.public_schema_revision,  # type: ignore[arg-type]
        capacity_guard_schema_revision=plan.capacity_guard_schema_revision,
        configuration_epoch=manager_configuration_epoch,
        configuration_digest=manager_configuration_digest,
        authority_incarnation=UUID(str(plan.manager_authority_incarnation)),
        writer_epoch=plan.manager_writer_epoch,  # type: ignore[arg-type]
        execution_state=plan.manager_execution_state,  # type: ignore[arg-type]
        execution_epoch=plan.manager_execution_epoch,  # type: ignore[arg-type]
        execution_manifest_sha256=plan.manager_execution_manifest_sha256,  # type: ignore[arg-type]
        executable_new_capacity_ceiling=(
            plan.manager_executable_new_capacity_ceiling  # type: ignore[arg-type]
        ),
        increase_freeze=plan.manager_increase_freeze,  # type: ignore[arg-type]
    )
    checkpoint_component_sha256 = dict(plan.checkpoint_component_sha256 or {})
    checkpoint_component_sha256["database_authority"] = authority.digest
    return replace(
        plan,
        checkpoint_component_sha256=checkpoint_component_sha256,
        database_authority_digest=authority.digest,
        manager_configuration_epoch=manager_configuration_epoch,
        manager_configuration_digest=manager_configuration_digest,
    )


def _backup_bound_plan(tmp_path: Path, document: dict[str, object]):
    snapshot = ConfigurationSnapshotV1.model_validate_json(json.dumps(document["configuration"]))
    return _plan_with_manager_configuration(
        tmp_path,
        manager_configuration_epoch=snapshot.configuration_epoch,
        manager_configuration_digest=canonical_digest(snapshot),
    )


def _find_pool(payload: dict[str, object], pool_id: str) -> dict[str, object]:
    return next(pool for pool in payload["pools"] if pool["pool_id"] == pool_id)  # type: ignore[index,union-attr,no-any-return]


def _find_subject(
    calls: list[tuple[str, object, UUID | None]], subject_id: UUID
) -> dict[str, object]:
    return next(
        payload
        for kind, payload, _key in calls
        if kind == "subject" and payload["subject_id"] == str(subject_id)  # type: ignore[index]
    )  # type: ignore[return-value]


def test_shared_desired_configuration_derivation_uses_authenticated_live_state(
    tmp_path: Path,
) -> None:
    """Catch preflight or apply replacing the shared live-state derivation."""
    fleet = _live_fleet()
    existing = subject_configuration(fleet)
    active = _active_document(fleet, (existing,))

    derive = manager_configuration_module.derive_protected_staging_capacity_configuration
    desired = derive(
        active_document=active,
        seed_values=_seed(),
        target_generation=_plan(tmp_path).starting_mutation_epoch + 1,
    )

    assert desired.original.snapshot.configuration_epoch == 3
    assert (
        desired.original.evidence_digest
        == hashlib.sha256(
            json.dumps(active, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert desired.fleet.executable_new_capacity_ceiling == 0
    assert {pool.pool_id: pool.max_slots for pool in desired.fleet.pools} == {
        "gb10": 140,
        "oldlab": 18,
    }
    assert {
        node.node_id
        for pool in desired.fleet.pools
        if pool.pool_id == "gb10"
        for domain in pool.resource_domains
        for node in domain.nodes
    } == {"trt-gb10-1", *{f"trt-gb10-{index}" for index in range(3, 16)}}
    assert desired.staging_subject.min_slots == 0
    assert desired.staging_subject.subject_id == _STAGING_SUBJECT


def test_component_preserves_live_state_and_converges_exact_staging_subject(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    existing = subject_configuration(fleet)
    client = _Client(_active_document(fleet, (existing,)))
    component = _component(client, _seed())
    plan = _plan(tmp_path)

    state, _evidence = component.classify(plan)
    assert state is ComponentState.READY
    client.calls.clear()
    client.reads = 0

    component.apply(plan)

    assert [kind for kind, _payload, _key in client.calls] == [
        "get",
        "fleet",
        "subject",
        "subject",
        "get",
        "activate",
        "get",
    ]
    fleet_payload = next(payload for kind, payload, _key in client.calls if kind == "fleet")
    assert isinstance(fleet_payload, dict)
    desired_fleet = FleetManifestV1.model_validate_json(json.dumps(fleet_payload))
    assert desired_fleet.fleet_generation == fleet.fleet_generation + 1
    assert desired_fleet.executable_new_capacity_ceiling == 0
    assert desired_fleet.fleet_digest == canonical_digest_excluding(desired_fleet, "fleet_digest")
    desired_gb10 = next(pool for pool in desired_fleet.pools if pool.pool_id == "gb10")
    desired_oldlab = next(pool for pool in desired_fleet.pools if pool.pool_id == "oldlab")
    assert desired_gb10.pool_generation == desired_oldlab.pool_generation == 1
    assert desired_gb10.max_slots == 140
    assert desired_oldlab.max_slots == 18
    assert {item.node_id for domain in desired_gb10.resource_domains for item in domain.nodes} == {
        "trt-gb10-1",
        *[f"trt-gb10-{index}" for index in range(3, 16)],
    }
    assert {
        item.node_id for domain in desired_oldlab.resource_domains for item in domain.nodes
    } == {f"trt-eai-oldlab-{index}" for index in range(3, 6)}
    assert desired_gb10.pool_digest == canonical_digest_excluding(desired_gb10, "pool_digest")
    assert desired_oldlab == next(pool for pool in fleet.pools if pool.pool_id == "oldlab")
    assert desired_fleet.tiers == fleet.tiers
    assert desired_fleet.account_policies == fleet.account_policies
    assert desired_fleet.global_max_pending_slots == fleet.global_max_pending_slots
    assert desired_fleet.global_max_pending_jobs == fleet.global_max_pending_jobs
    assert (
        desired_fleet.global_submission_rate_per_minute == fleet.global_submission_rate_per_minute
    )
    assert desired_fleet.development_subject_template is not None
    template_profiles = {
        profile.pool_id: profile for profile in desired_fleet.development_subject_template.profiles
    }
    assert template_profiles["gb10"].profile_generation == 2
    assert template_profiles["gb10"].pool_digest == desired_gb10.pool_digest
    assert template_profiles["oldlab"] == fleet.development_subject_template.profiles[1]  # type: ignore[union-attr]

    changed_existing = SubjectConfigurationV1.model_validate_json(
        json.dumps(_find_subject(client.calls, SUBJECT_ID))
    )
    assert (
        changed_existing.model_copy(
            update={
                "configuration_generation": existing.configuration_generation,
                "profiles": existing.profiles,
            }
        )
        == existing
    )
    assert changed_existing.configuration_generation == existing.configuration_generation + 1
    existing_profiles = {profile.pool_id: profile for profile in changed_existing.profiles}
    assert existing_profiles["gb10"].profile_generation == 2
    assert existing_profiles["gb10"].pool_digest == desired_gb10.pool_digest
    assert existing_profiles["oldlab"] == existing.profiles[1]

    staging = SubjectConfigurationV1.model_validate_json(
        json.dumps(_find_subject(client.calls, _STAGING_SUBJECT))
    )
    target_generation = plan.starting_mutation_epoch + 1
    assert staging.subject_incarnation == _STAGING_INCARNATION
    assert staging.demand_reporter_incarnation == _STAGING_REPORTER
    assert staging.display_name == "staging"
    assert staging.account_id == "shared-development"
    assert staging.tier_id == "staging"
    assert staging.min_slots == 0
    assert staging.max_slots == 16
    assert staging.rollout_surge_slots == 2
    assert staging.max_pending_slots == 16
    assert staging.max_pending_jobs == 16
    assert staging.submission_rate_per_minute == 8
    assert staging.lifecycle_state == "active"
    assert staging.candidate_generation == target_generation
    assert staging.deployment_generation == target_generation
    assert staging.configuration_generation == target_generation
    assert {profile.pool_id for profile in staging.profiles} == {"gb10", "oldlab"}
    assert all(
        tuple(shape.concurrency_slots for shape in profile.worker_shapes) == (1,)
        for profile in staging.profiles
    )
    assert all(
        profile.profile_digest == canonical_digest_excluding(profile, "profile_digest")
        for profile in staging.profiles
    )

    activation_payload = next(payload for kind, payload, _key in client.calls if kind == "activate")
    activation = ConfigurationActivationV1.model_validate_json(json.dumps(activation_payload))
    assert activation.expected_configuration_epoch == 3
    assert {reference.subject_id for reference in activation.subjects} == {
        existing.subject_id,
        _STAGING_SUBJECT,
    }
    assert activation.static_candidate_provenance == (
        StaticCandidateProvenanceV1(
            subject_id=_STAGING_SUBJECT,
            subject_incarnation=_STAGING_INCARNATION,
            candidate_generation=target_generation,
            algorithm="git-sha1",
            identity=plan.candidate_sha,
            publication_sha256=plan.artifact_bundle_digest,
        ),
    )
    mutation_keys = [key for kind, _payload, key in client.calls if kind != "get"]
    assert all(isinstance(key, UUID) for key in mutation_keys)
    assert len(set(mutation_keys)) == len(mutation_keys)
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_component_reuses_deterministic_keys_for_the_same_fresh_target(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    document = _active_document(fleet, (subject_configuration(fleet),))
    plan = _plan(tmp_path)
    observed: list[tuple[UUID, ...]] = []

    for _ in range(2):
        client = _Client(document)
        _component(client, _seed()).apply(plan)
        observed.append(
            tuple(key for kind, _payload, key in client.calls if kind != "get" and key is not None)
        )

    assert observed[0]
    assert observed[0] == observed[1]


def test_component_prefers_an_existing_staging_subject_as_profile_authority(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    staging_profiles = tuple(
        _profile(
            pool,
            shape_prefix="existing-staging",
            include_two_slot=False,
        )
        for pool in fleet.pools
    )
    staging = subject_configuration(
        fleet,
        subject_id=_STAGING_SUBJECT,
        subject_incarnation=_STAGING_INCARNATION,
        demand_reporter_incarnation=DEMAND_REPORTER_ID,
        display_name="staging",
        tier_id="staging",
        min_slots=1,
        max_slots=7,
        rollout_surge_slots=1,
        max_pending_slots=6,
        max_pending_jobs=5,
        submission_rate_per_minute=4,
        profiles=staging_profiles,
    )
    client = _Client(_active_document(fleet, (staging,)))
    component = _component(client, _seed())

    component.apply(_plan(tmp_path))

    desired = SubjectConfigurationV1.model_validate_json(
        json.dumps(_find_subject(client.calls, _STAGING_SUBJECT))
    )
    assert [shape.shape_id for profile in desired.profiles for shape in profile.worker_shapes] == [
        "existing-staging-one-slot",
        "existing-staging-one-slot",
    ]
    assert desired.account_id == staging.account_id
    assert desired.max_slots == staging.max_slots
    assert desired.rollout_surge_slots == staging.rollout_surge_slots
    assert desired.max_pending_slots == staging.max_pending_slots
    assert desired.max_pending_jobs == staging.max_pending_jobs
    assert desired.submission_rate_per_minute == staging.submission_rate_per_minute
    assert desired.min_slots == 0


@pytest.mark.parametrize("failure", ["missing-template", "ambiguous-service-account"])
def test_component_fails_closed_without_unambiguous_live_staging_authority(
    tmp_path: Path,
    failure: str,
) -> None:
    fleet = _live_fleet(include_template=failure != "missing-template")
    if failure == "ambiguous-service-account":
        service = next(account for account in fleet.account_policies if account.kind == "service")
        fleet = fleet.model_copy(
            update={
                "fleet_digest": "0" * 64,
                "account_policies": (
                    *fleet.account_policies,
                    service.model_copy(update={"account_id": "another-service"}),
                ),
            }
        )
        fleet = fleet.model_copy(
            update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
        )
    client = _Client(_active_document(fleet, ()))

    assert _component(client, _seed()).classify(_plan(tmp_path))[0] is ComponentState.DRIFTED
    assert [kind for kind, _payload, _key in client.calls] == ["get"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda nodes: (*nodes, node("trt-gb10-16", slots=10)),
        lambda nodes: tuple(item for item in nodes if item.node_id != "trt-gb10-4"),
        lambda nodes: tuple(
            node(item.node_id, slots=9) if item.node_id == "trt-gb10-4" else item for item in nodes
        ),
    ],
)
def test_component_rejects_unsafe_live_gb10_topology(
    tmp_path: Path,
    mutate,
) -> None:
    fleet = _live_fleet()
    pools = list(fleet.pools)
    position = next(index for index, pool in enumerate(pools) if pool.pool_id == "gb10")
    pool = pools[position]
    domain = pool.resource_domains[0]
    domain = domain.model_copy(update={"nodes": mutate(domain.nodes)})
    pool = pool.model_copy(update={"pool_digest": "0" * 64, "resource_domains": (domain,)})
    pools[position] = pool.model_copy(
        update={"pool_digest": canonical_digest_excluding(pool, "pool_digest")}
    )
    fleet = fleet.model_copy(update={"fleet_digest": "0" * 64, "pools": tuple(pools)})
    assert fleet.development_subject_template is not None
    template = fleet.development_subject_template.model_copy(
        update={"profiles": tuple(_profile(pool) for pool in fleet.pools)}
    )
    fleet = fleet.model_copy(update={"development_subject_template": template})
    fleet = fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(fleet, "fleet_digest")}
    )
    client = _Client(_active_document(fleet, ()))

    assert _component(client, _seed()).classify(_plan(tmp_path))[0] is ComponentState.DRIFTED


def test_component_refuses_activation_when_second_read_is_stale(tmp_path: Path) -> None:
    fleet = _live_fleet()
    client = _Client(_active_document(fleet, ()), stale_second_read=True)
    component = _component(client, _seed())

    with pytest.raises(RuntimeError, match="active configuration changed"):
        component.apply(_plan(tmp_path))

    assert "activate" not in [kind for kind, _payload, _key in client.calls]


def test_component_rejects_equivocal_mutation_response(tmp_path: Path) -> None:
    fleet = _live_fleet()
    client = _Client(
        _active_document(fleet, ()),
        equivocal_fleet_response=True,
    )

    with pytest.raises(RuntimeError, match="fleet proposal response"):
        _component(client, _seed()).apply(_plan(tmp_path))

    assert "activate" not in [kind for kind, _payload, _key in client.calls]


def test_component_reports_no_mutation_when_activation_readback_is_the_exact_predecessor(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    client = _Client(_active_document(fleet, ()), retain_old_readback=True)

    with pytest.raises(RuntimeError, match="failed without mutation"):
        _component(client, _seed()).apply(_plan(tmp_path))


@pytest.mark.parametrize(
    ("label", "response"),
    (
        (
            "lost",
            _Client.MutationOutcome(
                response=ProtectedCapacityManagerClientError("transport"),
                mutate_document=True,
            ),
        ),
        (
            "redirected",
            _Client.MutationOutcome(
                response=ProtectedCapacityManagerClientError("redirected"),
                mutate_document=True,
            ),
        ),
        (
            "oversized",
            _Client.MutationOutcome(
                response=ProtectedCapacityManagerClientError("oversized"),
                mutate_document=True,
            ),
        ),
        (
            "malformed",
            _Client.MutationOutcome(
                response={"digest": "broken"},
                mutate_document=True,
            ),
        ),
    ),
)
def test_component_recovers_forward_from_unusable_activation_response(
    tmp_path: Path,
    label: str,
    response: _Client.MutationOutcome,
) -> None:
    del label
    fleet = _live_fleet()
    client = _Client(
        _active_document(fleet, ()),
        activate_outcomes=(response,),
    )
    component = _component(client, _seed())
    plan = _plan(tmp_path)

    component.apply(plan)

    assert [kind for kind, _payload, _key in client.calls] == [
        "get",
        "fleet",
        "subject",
        "get",
        "activate",
        "get",
    ]
    assert "rollback" not in [kind for kind, _payload, _key in client.calls]
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_component_reports_activation_failure_without_additional_mutation_when_predecessor_is_exact(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    client = _Client(
        _active_document(fleet, ()),
        activate_outcomes=(
            _Client.MutationOutcome(
                response=ProtectedCapacityManagerClientError("transport"),
                mutate_document=False,
            ),
        ),
    )

    with pytest.raises(RuntimeError, match="failed without mutation"):
        _component(client, _seed()).apply(_plan(tmp_path))

    assert [kind for kind, _payload, _key in client.calls] == [
        "get",
        "fleet",
        "subject",
        "get",
        "activate",
        "get",
    ]
    assert "rollback" not in [kind for kind, _payload, _key in client.calls]


def test_component_retries_the_same_activation_after_equivocal_readback(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original = _active_document(fleet, ())
    newer = deepcopy(original)
    configuration = deepcopy(newer["configuration"])
    assert isinstance(configuration, dict)
    configuration["configuration_epoch"] = 5
    newer["configuration"] = configuration
    client = _Client(
        original,
        read_sequence=(original, original, newer),
        activate_outcomes=(
            _Client.MutationOutcome(
                response=ProtectedCapacityManagerClientError("transport"),
                mutate_document=False,
            ),
        ),
    )
    component = _component(client, _seed())
    plan = _plan(tmp_path)

    component.apply(plan)

    activation_keys = [key for kind, _payload, key in client.calls if kind == "activate"]
    assert len(activation_keys) == 2
    assert activation_keys[0] == activation_keys[1]
    assert "rollback" not in [kind for kind, _payload, _key in client.calls]
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_component_rejects_second_activation_retry_when_seed_drifted(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original = _active_document(fleet, ())
    newer = deepcopy(original)
    configuration = deepcopy(newer["configuration"])
    assert isinstance(configuration, dict)
    configuration["configuration_epoch"] = 5
    newer["configuration"] = configuration
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_ambiguous_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    client = _Client(
        original,
        read_sequence=(original, original, newer),
        activate_outcomes=(
            _Client.MutationOutcome(
                response=ProtectedCapacityManagerClientError("transport"),
                mutate_document=False,
            ),
        ),
        on_get=drift_after_ambiguous_readback,
    )

    with pytest.raises(RuntimeError, match="seed changed before mutation"):
        _component(client, original_seed, root=root, seed_reader=seed_reader).apply(_plan(tmp_path))

    activation_keys = [key for kind, _payload, key in client.calls if kind == "activate"]
    assert len(activation_keys) == 1
    assert "rollback" not in [kind for kind, _payload, _key in client.calls]


def test_component_rolls_back_after_target_converges_and_seed_drift_is_detected(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original = _active_document(fleet, ())
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    root = (tmp_path / "protected-capacity-state").resolve()
    client = _Client(
        original,
        on_get=drift_after_target_readback,
    )
    component = _component(client, original_seed, root=root, seed_reader=seed_reader)
    plan = _backup_bound_plan(tmp_path, original)

    with pytest.raises(RuntimeError, match="rolled back after credential seed changed"):
        component.apply(plan)

    kinds = [kind for kind, _payload, _key in client.calls]
    assert kinds == ["get", "fleet", "subject", "get", "activate", "get", "rollback", "get"]
    activation_key = next(key for kind, _payload, key in client.calls if kind == "activate")
    rollback_key = next(key for kind, _payload, key in client.calls if kind == "rollback")
    assert activation_key != rollback_key
    rollback_payload = next(payload for kind, payload, _key in client.calls if kind == "rollback")
    assert rollback_payload["expected_configuration_epoch"] == 4
    assert rollback_payload["restore_configuration_epoch"] == 3
    snapshot = ConfigurationSnapshotV1.model_validate_json(
        json.dumps(client.original["configuration"])
    )
    predecessor_digest = canonical_digest(snapshot)
    assert rollback_payload["restore_configuration_digest"] == predecessor_digest
    assert (
        rollback_payload["rollback_evidence_sha256"]
        == hashlib.sha256(
            json.dumps(
                {
                    "backup_component_set_digest": plan.backup_component_set_digest,
                    "backup_lease_digest": plan.backup_lease_digest,
                    "backup_lease_id": plan.backup_lease_id,
                    "backup_source_request_id": plan.backup_source_request_id,
                    "checkpoint_schema_version": plan.checkpoint_schema_version,
                    "predecessor_configuration_digest": predecessor_digest,
                    "predecessor_configuration_epoch": 3,
                    "restore_report_sha256": plan.restore_report_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
    )
    record = CapacityManagerConfigurationCompensationStore(
        (root / "capacity-manager-configuration-compensations").resolve(),
        service_uid=os.geteuid(),
    ).read(activation_key)
    assert record.request_id == plan.request_id
    assert record.attempt_number == plan.attempt_number
    assert record.plan_digest == plan.plan_digest
    assert record.backup_lease_digest == plan.backup_lease_digest
    assert record.predecessor_configuration_epoch == 3
    assert record.predecessor_configuration_digest == predecessor_digest
    assert record.activation_idempotency_key == activation_key
    assert record.rollback_idempotency_key == rollback_key


def test_component_refuses_rollback_when_live_predecessor_is_not_backup_bound(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    client = _Client(
        _active_document(fleet, ()),
        on_get=drift_after_target_readback,
    )
    plan = _plan_with_manager_configuration(
        tmp_path,
        manager_configuration_epoch=2,
        manager_configuration_digest="f" * 64,
    )

    with pytest.raises(RuntimeError, match="backup-bound"):
        _component(client, original_seed, root=root, seed_reader=seed_reader).apply(plan)

    assert "rollback" not in [kind for kind, _payload, _key in client.calls]


def test_component_records_compensation_intent_before_rollback_and_terminal_after_success(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original = _active_document(fleet, ())
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    def assert_intent_written_before_rollback(_payload: dict[str, object], key: UUID) -> None:
        activation_key = next(
            observed
            for kind, _observed_payload, observed in client.calls
            if kind == "activate" and observed is not None
        )
        store = CapacityManagerConfigurationCompensationStore(
            (root / "capacity-manager-configuration-compensations").resolve(),
            service_uid=os.geteuid(),
        )
        assert store.read_intent(activation_key).rollback_idempotency_key == key
        with pytest.raises(FileNotFoundError):
            store.read(activation_key)

    client = _Client(
        original,
        on_get=drift_after_target_readback,
        on_rollback=assert_intent_written_before_rollback,
    )
    component = _component(client, original_seed, root=root, seed_reader=seed_reader)
    plan = _backup_bound_plan(tmp_path, original)

    with pytest.raises(RuntimeError, match="rolled back after credential seed changed"):
        component.apply(plan)

    activation_key = next(key for kind, _payload, key in client.calls if kind == "activate")
    store = CapacityManagerConfigurationCompensationStore(
        (root / "capacity-manager-configuration-compensations").resolve(),
        service_uid=os.geteuid(),
    )
    assert store.read_intent(activation_key).activation_idempotency_key == activation_key
    assert store.read(activation_key).activation_idempotency_key == activation_key


def test_component_rejects_symlinked_compensation_root_before_rollback(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original = _active_document(fleet, ())
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()
    root.mkdir(mode=0o700)
    compensation_target = (tmp_path / "redirected-compensations").resolve()
    compensation_target.mkdir(mode=0o700)
    os.symlink(compensation_target, root / "capacity-manager-configuration-compensations")

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    client = _Client(original, on_get=drift_after_target_readback)
    component = _component(client, original_seed, root=root, seed_reader=seed_reader)

    with pytest.raises(RuntimeError, match="unsafe"):
        component.apply(_backup_bound_plan(tmp_path, original))

    assert "rollback" not in [kind for kind, _payload, _key in client.calls]


def test_component_accepts_exact_rollback_with_fresher_server_generations(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    existing = subject_configuration(fleet)
    original = _active_document(fleet, (existing,))
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    rollback_fleet = fleet.model_copy(
        update={
            "fleet_generation": 17,
            "fleet_digest": "0" * 64,
        }
    )
    rollback_fleet = rollback_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(rollback_fleet, "fleet_digest")}
    )
    rollback_subject = existing.model_copy(update={"configuration_generation": 13})
    rollback_document = _active_document(rollback_fleet, (rollback_subject,), epoch=5)
    client = _Client(
        original,
        on_get=drift_after_target_readback,
        rollback_document=rollback_document,
    )

    with pytest.raises(RuntimeError, match="rolled back after credential seed changed"):
        _component(client, original_seed, root=root, seed_reader=seed_reader).apply(
            _backup_bound_plan(tmp_path, original)
        )

    activation_key = next(key for kind, _payload, key in client.calls if kind == "activate")
    record = CapacityManagerConfigurationCompensationStore(
        (root / "capacity-manager-configuration-compensations").resolve(),
        service_uid=os.geteuid(),
    ).read(activation_key)
    assert record.resulting_configuration_epoch == 5
    assert record.resulting_configuration_digest == canonical_digest(
        ConfigurationSnapshotV1.model_validate_json(json.dumps(rollback_document["configuration"]))
    )


@pytest.mark.parametrize("kind", ("digest", "reference"))
def test_component_rejects_rollback_response_digest_or_reference_drift(
    tmp_path: Path,
    kind: str,
) -> None:
    fleet = _live_fleet()
    existing = subject_configuration(fleet)
    original = _active_document(fleet, (existing,))
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    rollback_fleet = fleet.model_copy(
        update={
            "fleet_generation": 17,
            "fleet_digest": "0" * 64,
        }
    )
    rollback_fleet = rollback_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(rollback_fleet, "fleet_digest")}
    )
    rollback_subject = existing.model_copy(update={"configuration_generation": 13})
    rollback_document = _active_document(rollback_fleet, (rollback_subject,), epoch=5)
    response = _Client._response_for_document(rollback_document)
    if kind == "digest":
        response["digest"] = "0" * 64
    else:
        snapshot = dict(response["snapshot"])
        assert isinstance(snapshot["subjects"], list)
        subject = dict(snapshot["subjects"][0])
        subject["subject_id"] = "00000000-0000-4000-8000-00000000dead"
        snapshot["subjects"] = [subject]
        response["snapshot"] = snapshot
        response["digest"] = canonical_digest(
            ConfigurationSnapshotV1.model_validate_json(json.dumps(response["snapshot"]))
        )
    client = _Client(
        original,
        on_get=drift_after_target_readback,
        rollback_document=rollback_document,
        rollback_outcomes=(_Client.MutationOutcome(response=response, mutate_document=True),),
    )

    with pytest.raises(RuntimeError, match="rollback response is not exact"):
        _component(client, original_seed, root=root, seed_reader=seed_reader).apply(
            _backup_bound_plan(tmp_path, original)
        )

    activation_key = next(key for kind2, _payload, key in client.calls if kind2 == "activate")
    store = CapacityManagerConfigurationCompensationStore(
        (root / "capacity-manager-configuration-compensations").resolve(),
        service_uid=os.geteuid(),
    )
    assert store.read_intent(activation_key).activation_idempotency_key == activation_key
    with pytest.raises(FileNotFoundError):
        store.read(activation_key)


def test_component_rejects_rollback_that_does_not_restore_predecessor_policy(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    original = _active_document(fleet, ())
    original_seed = _seed()
    drifted = _drifted_seed(original_seed)
    seed_state = {"drifted": False}
    root = (tmp_path / "protected-capacity-state").resolve()

    def seed_reader() -> dict[str, object]:
        return deepcopy(drifted if seed_state["drifted"] else original_seed)

    def drift_after_target_readback(reads: int, _document: dict[str, object]) -> None:
        if reads == 3:
            seed_state["drifted"] = True

    rollback_document = _active_document(_live_fleet(include_template=False), ())
    client = _Client(
        original,
        on_get=drift_after_target_readback,
        rollback_document=rollback_document,
    )
    plan = _backup_bound_plan(tmp_path, original)

    with pytest.raises(RuntimeError, match="rollback response is not exact"):
        _component(client, original_seed, root=root, seed_reader=seed_reader).apply(plan)

    activation_key = next(key for kind, _payload, key in client.calls if kind == "activate")
    store = CapacityManagerConfigurationCompensationStore(
        (root / "capacity-manager-configuration-compensations").resolve(),
        service_uid=os.geteuid(),
    )
    assert store.read_intent(activation_key).activation_idempotency_key == activation_key
    with pytest.raises(FileNotFoundError):
        store.read(activation_key)


def test_component_classification_rejects_malformed_live_configuration(
    tmp_path: Path,
) -> None:
    fleet = _live_fleet()
    document = _active_document(fleet, ())
    document["unexpected"] = True

    assert (
        _component(_Client(document), _seed()).classify(_plan(tmp_path))[0]
        is ComponentState.DRIFTED
    )
