"""Protected convergence of the live staging capacity configuration."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, TypeVar
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import ValidationError

from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    ConfigurationRollbackV1,
    ConfigurationSnapshotV1,
    DevelopmentSubjectTemplateV1,
    FleetManifestV1,
    PoolManifestV1,
    ProfileReferenceV1,
    StaticCandidateProvenanceV1,
    SubjectConfigurationV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.fleet_state import (
    FleetStateError,
    validate_fleet_manifest_digests,
    validate_profile_narrowing,
)

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentState
from .protected_capacity_manager_client import (
    ManagerCommandRunner,
    ProtectedCapacityManagerClientError,
    open_protected_capacity_manager_client,
)
from .protected_capacity_manager_configuration_compensation import (
    CapacityManagerConfigurationCompensationIntentRecord,
    CapacityManagerConfigurationCompensationRecord,
    CapacityManagerConfigurationCompensationStore,
)

_GB10_SOURCE_NODES = frozenset(f"trt-gb10-{index}" for index in range(1, 16))
_GB10_TARGET_NODES = _GB10_SOURCE_NODES - {"trt-gb10-2"}
_OLDLAB_NODES = frozenset(f"trt-eai-oldlab-{index}" for index in range(3, 6))
_POOL_IDS = frozenset({"gb10", "oldlab"})
_ROOT_FIELDS = frozenset({"schema_version", "configuration", "fleet", "subjects"})
_PROPOSAL_FIELDS = frozenset(
    {
        "configuration_id",
        "scope",
        "generation",
        "digest",
        "subject_id",
        "subject_incarnation",
    }
)
_ACTIVATION_RESPONSE_FIELDS = frozenset({"configuration_epoch", "digest", "snapshot"})


class ManagerConfigurationClient(Protocol):
    def get_configuration(self) -> dict[str, object]: ...

    def get_status(self) -> dict[str, object]: ...

    def propose_fleet(
        self, payload: Mapping[str, object], idempotency_key: UUID
    ) -> dict[str, object]: ...

    def propose_subject(
        self,
        subject_id: UUID,
        payload: Mapping[str, object],
        idempotency_key: UUID,
    ) -> dict[str, object]: ...

    def activate(
        self, payload: Mapping[str, object], idempotency_key: UUID
    ) -> dict[str, object]: ...

    def rollback(
        self, payload: Mapping[str, object], idempotency_key: UUID
    ) -> dict[str, object]: ...


ClientContext = Callable[..., AbstractContextManager[ManagerConfigurationClient]]


@dataclass(frozen=True, slots=True)
class _Seed:
    values: Mapping[str, object]
    authority_incarnation: UUID
    subject_id: UUID
    subject_incarnation: UUID
    reporter_incarnation: UUID
    digest: str


@dataclass(frozen=True, slots=True)
class _ActiveConfiguration:
    snapshot: ConfigurationSnapshotV1
    fleet: FleetManifestV1
    subjects: tuple[SubjectConfigurationV1, ...]
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class _DesiredConfiguration:
    original: _ActiveConfiguration
    fleet: FleetManifestV1
    subjects: tuple[SubjectConfigurationV1, ...]
    changed_subject_ids: frozenset[UUID]
    staging_subject: SubjectConfigurationV1
    exact: bool


# Public shared type for preflight/apply desired-state convergence.  The
# leading-underscore implementation name remains as a compatibility alias for
# this module's existing helpers while external producers depend only on the
# stable public name.
ProtectedStagingDesiredConfiguration = _DesiredConfiguration


@dataclass(frozen=True, slots=True)
class KubernetesProtectedStagingCapacityManagerConfigurationComponent:
    """Preserve authenticated live state while narrowing the protected fleet."""

    runner: ManagerCommandRunner
    credentials_root: Path
    service_uid: int
    service_gid: int
    seed_reader: Callable[[], dict[str, object]]
    client_context: ClientContext = open_protected_capacity_manager_client

    def __post_init__(self) -> None:
        if (
            not self.credentials_root.is_absolute()
            or ".." in self.credentials_root.parts
            or type(self.service_uid) is not int
            or type(self.service_gid) is not int
            or self.service_uid < 0
            or self.service_gid < 0
            or not callable(self.seed_reader)
            or not callable(self.client_context)
            or not isinstance(self.runner.environment.get("KUBECONFIG"), str)
        ):
            raise ValueError("protected capacity configuration authority is invalid")

    def classify(self, plan: FinalGatePlan) -> tuple[ComponentState, str]:
        try:
            seed = self._read_seed()
            with self._client() as client:
                desired = derive_protected_staging_capacity_configuration(
                    active_document=client.get_configuration(),
                    seed_values=seed.values,
                    target_generation=plan.starting_mutation_epoch + 1,
                )
        except (
            FleetStateError,
            KeyError,
            OSError,
            ProtectedCapacityManagerClientError,
            RuntimeError,
            TypeError,
            UnicodeError,
            ValidationError,
            ValueError,
        ):
            return ComponentState.DRIFTED, _hash_json({"status": "observation-failed"})
        state = ComponentState.EXACT if desired.exact else ComponentState.READY
        return state, _hash_json(
            {
                "configuration_epoch": desired.original.snapshot.configuration_epoch,
                "desired_fleet": canonical_digest(desired.fleet),
                "desired_subjects": [canonical_digest(subject) for subject in desired.subjects],
                "live": desired.original.evidence_digest,
                "state": state.value,
            }
        )

    def apply(self, plan: FinalGatePlan) -> None:
        seed = self._read_seed()
        with self._client() as client:
            desired = derive_protected_staging_capacity_configuration(
                active_document=client.get_configuration(),
                seed_values=seed.values,
                target_generation=plan.starting_mutation_epoch + 1,
            )
            original = desired.original
            if desired.exact:
                raise RuntimeError("protected capacity configuration changed before apply")
            self._require_seed(seed)
            fleet_reference = _validate_proposal_response(
                client.propose_fleet(
                    desired.fleet.model_dump(mode="json", exclude_none=False),
                    _idempotency_key(
                        plan,
                        "fleet",
                        canonical_digest(desired.fleet),
                    ),
                ),
                expected_scope="fleet",
                expected_generation=desired.fleet.fleet_generation,
                expected_digest=canonical_digest(desired.fleet),
            )
            original_references = {
                reference.subject_id: reference
                for reference in original.snapshot.subjects
                if reference.subject_id is not None
            }
            subject_references: list[ConfigurationGenerationRefV1] = []
            for subject in desired.subjects:
                if subject.subject_id in desired.changed_subject_ids:
                    self._require_seed(seed)
                    response = client.propose_subject(
                        subject.subject_id,
                        subject.model_dump(mode="json", exclude_none=False),
                        _idempotency_key(
                            plan,
                            f"subject:{subject.subject_id}",
                            canonical_digest(subject),
                        ),
                    )
                    reference = _validate_proposal_response(
                        response,
                        expected_scope="subject",
                        expected_generation=subject.configuration_generation,
                        expected_digest=canonical_digest(subject),
                        expected_subject_id=subject.subject_id,
                        expected_subject_incarnation=subject.subject_incarnation,
                    )
                else:
                    existing_reference = original_references.get(subject.subject_id)
                    if existing_reference is None:
                        raise RuntimeError(
                            "protected capacity subject reference disappeared before activation"
                        )
                    reference = existing_reference
                subject_references.append(reference)
            activation = ConfigurationActivationV1(
                expected_configuration_epoch=original.snapshot.configuration_epoch,
                fleet=fleet_reference,
                subjects=tuple(subject_references),
                static_candidate_provenance=(
                    StaticCandidateProvenanceV1(
                        subject_id=seed.subject_id,
                        subject_incarnation=seed.subject_incarnation,
                        candidate_generation=desired.staging_subject.candidate_generation,
                        algorithm="git-sha1",
                        identity=plan.candidate_sha,
                        publication_sha256=plan.artifact_bundle_digest,
                    ),
                ),
            )
            self._require_seed(seed)
            before_activation = _parse_active_configuration(client.get_configuration())
            if before_activation != original:
                raise RuntimeError(
                    "protected capacity active configuration changed before activation"
                )
            self._converge_activation(
                client=client,
                plan=plan,
                seed=seed,
                original=original,
                desired=desired,
                activation=activation,
            )

    def _client(self) -> AbstractContextManager[ManagerConfigurationClient]:
        return self.client_context(
            runner=self.runner,
            credentials_root=self.credentials_root,
            service_uid=self.service_uid,
            service_gid=self.service_gid,
        )

    def _read_seed(self) -> _Seed:
        return _parse_configuration_seed(self.seed_reader())

    def _require_seed(self, expected: _Seed) -> None:
        if not self._seed_is_unchanged(expected):
            raise RuntimeError("protected capacity configuration seed changed before mutation")

    def _seed_is_unchanged(self, expected: _Seed) -> bool:
        try:
            observed = self._read_seed()
        except (KeyError, OSError, TypeError, UnicodeError, ValueError) as exc:
            raise RuntimeError(
                "protected capacity configuration seed changed before mutation"
            ) from exc
        return observed.digest == expected.digest

    def _converge_activation(
        self,
        *,
        client: ManagerConfigurationClient,
        plan: FinalGatePlan,
        seed: _Seed,
        original: _ActiveConfiguration,
        desired: _DesiredConfiguration,
        activation: ConfigurationActivationV1,
    ) -> None:
        activation_payload = activation.model_dump(mode="json", exclude_none=False)
        activation_key = _idempotency_key(plan, "activation", canonical_digest(activation))
        attempt = 0
        while True:
            attempt += 1
            if attempt > 1:
                self._require_seed(seed)
            activated: ConfigurationSnapshotV1 | None = None
            try:
                activated = _validate_activation_response(
                    client.activate(activation_payload, activation_key),
                    activation=activation,
                )
            except ProtectedCapacityManagerClientError as exc:
                if exc.reason == "credential":
                    raise
            except RuntimeError as exc:
                if not str(exc).startswith("protected capacity activation response"):
                    raise
            readback = _read_active_configuration_or_none(client)
            if readback is not None and _is_exact_target(readback, desired, activation=activation):
                if activated is not None and readback.snapshot != activated:
                    raise RuntimeError(
                        "protected capacity activation response and readback diverged"
                    )
                if self._seed_is_unchanged(seed):
                    return
                self._rollback_after_seed_drift(
                    client=client,
                    plan=plan,
                    activation=activation,
                    activation_key=activation_key,
                    target=readback,
                    predecessor=original,
                )
                raise RuntimeError(
                    "protected capacity configuration rolled back after credential seed changed"
                )
            if readback is not None and _is_exact_predecessor(readback, original):
                raise RuntimeError("protected capacity activation failed without mutation")
            if attempt >= 2:
                raise RuntimeError("protected capacity activation outcome is equivocal")

    def _rollback_after_seed_drift(
        self,
        *,
        client: ManagerConfigurationClient,
        plan: FinalGatePlan,
        activation: ConfigurationActivationV1,
        activation_key: UUID,
        target: _ActiveConfiguration,
        predecessor: _ActiveConfiguration,
    ) -> None:
        predecessor_digest = canonical_digest(predecessor.snapshot)
        _require_backup_bound_predecessor(
            plan,
            predecessor_epoch=predecessor.snapshot.configuration_epoch,
            predecessor_digest=predecessor_digest,
        )
        rollback = ConfigurationRollbackV1(
            expected_configuration_epoch=target.snapshot.configuration_epoch,
            expected_configuration_digest=canonical_digest(target.snapshot),
            restore_configuration_epoch=predecessor.snapshot.configuration_epoch,
            restore_configuration_digest=predecessor_digest,
            rollback_evidence_sha256=_rollback_evidence_sha256(
                plan,
                predecessor_epoch=predecessor.snapshot.configuration_epoch,
                predecessor_digest=predecessor_digest,
            ),
        )
        rollback_key = _idempotency_key(plan, "rollback", canonical_digest(rollback))
        store = self._compensation_store()
        store.record_intent(
            CapacityManagerConfigurationCompensationIntentRecord.build(
                request_id=plan.request_id,
                attempt_number=plan.attempt_number,
                plan_digest=plan.plan_digest,
                activation_idempotency_key=activation_key,
                activation_request_digest=canonical_digest(activation),
                target_configuration_epoch=target.snapshot.configuration_epoch,
                target_configuration_digest=canonical_digest(target.snapshot),
                target_configuration_evidence_digest=target.evidence_digest,
                predecessor_configuration_epoch=predecessor.snapshot.configuration_epoch,
                predecessor_configuration_digest=predecessor_digest,
                predecessor_configuration_evidence_digest=predecessor.evidence_digest,
                backup_lease_digest=plan.backup_lease_digest,
                rollback_idempotency_key=rollback_key,
                rollback_request_digest=canonical_digest(rollback),
                rollback_evidence_sha256=rollback.rollback_evidence_sha256,
            )
        )
        rolled = _validate_rollback_response(
            client.rollback(
                rollback.model_dump(mode="json", exclude_none=False),
                rollback_key,
            ),
            target=target,
            predecessor=predecessor,
        )
        readback = _read_active_configuration_exact(client)
        _validate_rollback_readback(
            readback,
            target=target,
            predecessor=predecessor,
            response=rolled,
        )
        store.record(
            CapacityManagerConfigurationCompensationRecord.build(
                request_id=plan.request_id,
                attempt_number=plan.attempt_number,
                plan_digest=plan.plan_digest,
                activation_idempotency_key=activation_key,
                activation_request_digest=canonical_digest(activation),
                target_configuration_epoch=target.snapshot.configuration_epoch,
                target_configuration_digest=canonical_digest(target.snapshot),
                target_configuration_evidence_digest=target.evidence_digest,
                predecessor_configuration_epoch=predecessor.snapshot.configuration_epoch,
                predecessor_configuration_digest=predecessor_digest,
                predecessor_configuration_evidence_digest=predecessor.evidence_digest,
                backup_lease_digest=plan.backup_lease_digest,
                rollback_idempotency_key=rollback_key,
                rollback_request_digest=canonical_digest(rollback),
                rollback_evidence_sha256=rollback.rollback_evidence_sha256,
                resulting_configuration_epoch=readback.snapshot.configuration_epoch,
                resulting_configuration_digest=canonical_digest(readback.snapshot),
                resulting_configuration_evidence_digest=readback.evidence_digest,
            )
        )

    def _compensation_store(self) -> CapacityManagerConfigurationCompensationStore:
        return CapacityManagerConfigurationCompensationStore(
            self.credentials_root.parent / "capacity-manager-configuration-compensations",
            service_uid=self.service_uid,
        )


def _parse_active_configuration(value: object) -> _ActiveConfiguration:
    if not isinstance(value, dict) or set(value) != _ROOT_FIELDS:
        raise ValueError("active capacity configuration fields are invalid")
    if value.get("schema_version") != 1:
        raise ValueError("active capacity configuration schema is invalid")
    raw_subjects = value.get("subjects")
    if not isinstance(raw_subjects, list):
        raise ValueError("active capacity subjects are invalid")
    snapshot = _parse_contract(ConfigurationSnapshotV1, value.get("configuration"))
    fleet = _parse_contract(FleetManifestV1, value.get("fleet"))
    subjects = tuple(
        sorted(
            (_parse_contract(SubjectConfigurationV1, item) for item in raw_subjects),
            key=lambda subject: subject.subject_id.hex,
        )
    )
    if snapshot.configuration_epoch < 1:
        raise ValueError("active capacity configuration epoch is invalid")
    validate_fleet_manifest_digests(fleet)
    fleet_digest = canonical_digest(fleet)
    if (
        snapshot.fleet.scope != "fleet"
        or snapshot.fleet.generation != fleet.fleet_generation
        or snapshot.fleet.digest != fleet_digest
        or snapshot.fleet.subject_id is not None
        or snapshot.fleet.subject_incarnation is not None
    ):
        raise ValueError("active capacity fleet reference is invalid")
    if len({subject.subject_id for subject in subjects}) != len(subjects):
        raise ValueError("active capacity subjects contain duplicate identities")
    by_id = {subject.subject_id: subject for subject in subjects}
    if len(snapshot.subjects) != len(subjects):
        raise ValueError("active capacity subject manifest is incomplete")
    for reference in snapshot.subjects:
        subject_id = reference.subject_id
        if subject_id is None:
            raise ValueError("active capacity subject reference is invalid")
        subject = by_id.get(subject_id)
        if (
            subject is None
            or reference.subject_incarnation != subject.subject_incarnation
            or reference.generation != subject.configuration_generation
            or reference.digest != canonical_digest(subject)
        ):
            raise ValueError("active capacity subject reference is invalid")
    for subject in subjects:
        for profile in subject.profiles:
            validate_profile_narrowing(fleet, profile)
    canonical = {
        "schema_version": 1,
        "configuration": snapshot.model_dump(mode="json", exclude_none=False),
        "fleet": fleet.model_dump(mode="json", exclude_none=False),
        "subjects": [subject.model_dump(mode="json", exclude_none=False) for subject in subjects],
    }
    return _ActiveConfiguration(
        snapshot=snapshot,
        fleet=fleet,
        subjects=subjects,
        evidence_digest=_hash_json(canonical),
    )


def _parse_configuration_seed(value: object) -> _Seed:
    if not isinstance(value, dict):
        raise ValueError("protected capacity configuration seed is invalid")
    copied = copy.deepcopy(value)

    def identity(field: str) -> UUID:
        raw = copied.get(field)
        if not isinstance(raw, str):
            raise ValueError("protected capacity configuration identity is invalid")
        parsed = UUID(raw)
        if parsed.int == 0 or str(parsed) != raw:
            raise ValueError("protected capacity configuration identity is invalid")
        return parsed

    identities = {
        field: identity(field)
        for field in (
            "authority_incarnation",
            "subject_id",
            "subject_incarnation",
            "reporter_incarnation",
        )
    }
    if len(set(identities.values())) != len(identities):
        raise ValueError("protected capacity configuration identities overlap")
    return _Seed(
        values=copied,
        authority_incarnation=identities["authority_incarnation"],
        subject_id=identities["subject_id"],
        subject_incarnation=identities["subject_incarnation"],
        reporter_incarnation=identities["reporter_incarnation"],
        digest=_hash_json(copied),
    )


def derive_protected_staging_capacity_configuration(
    *,
    active_document: object,
    seed_values: Mapping[str, object],
    target_generation: int,
) -> _DesiredConfiguration:
    """Derive the one target shared by protected preflight and apply."""

    return _derive_desired_configuration(
        _parse_active_configuration(active_document),
        seed=_parse_configuration_seed(dict(seed_values)),
        target_generation=target_generation,
    )


def _derive_desired_configuration(
    active: _ActiveConfiguration,
    *,
    seed: _Seed,
    target_generation: int,
) -> _DesiredConfiguration:
    if active.fleet.authority_incarnation != seed.authority_incarnation:
        raise ValueError("active capacity authority does not match the staging seed")
    _validate_live_topology(active.fleet)
    target_pools = tuple(_target_pool(pool) for pool in active.fleet.pools)
    pool_by_id = {pool.pool_id: pool for pool in target_pools}
    template = active.fleet.development_subject_template
    target_template: DevelopmentSubjectTemplateV1 | None = None
    if template is not None:
        template_payload = template.model_dump(mode="python")
        template_payload["profiles"] = tuple(
            _retarget_profile(
                profile,
                source_fleet=active.fleet,
                target_pool=pool_by_id[profile.pool_id],
                one_slot_only=False,
                bump_on_change=True,
            )
            for profile in template.profiles
        )
        target_template = DevelopmentSubjectTemplateV1.model_validate(template_payload)
    base_fleet = _fleet_with_digest(
        active.fleet,
        fleet_generation=active.fleet.fleet_generation,
        pools=target_pools,
        template=target_template,
    )
    existing_staging = next(
        (subject for subject in active.subjects if subject.subject_id == seed.subject_id),
        None,
    )
    if existing_staging is not None and (
        existing_staging.subject_incarnation != seed.subject_incarnation
    ):
        raise ValueError("active staging subject incarnation conflicts with the seed")
    if any(
        subject.subject_id != seed.subject_id
        and subject.subject_incarnation == seed.subject_incarnation
        for subject in active.subjects
    ):
        raise ValueError("active subject incarnation conflicts with the staging seed")
    if existing_staging is None and any(
        subject.display_name == "staging" for subject in active.subjects
    ):
        raise ValueError("active staging display name belongs to another subject")

    desired_subjects: list[SubjectConfigurationV1] = []
    for subject in active.subjects:
        if subject.subject_id == seed.subject_id:
            continue
        profiles = tuple(
            _retarget_profile(
                profile,
                source_fleet=active.fleet,
                target_pool=pool_by_id[profile.pool_id],
                one_slot_only=False,
                bump_on_change=True,
            )
            for profile in subject.profiles
        )
        if profiles == subject.profiles:
            desired_subjects.append(subject)
        else:
            payload = subject.model_dump(mode="python")
            payload["profiles"] = profiles
            payload["configuration_generation"] = subject.configuration_generation + 1
            desired_subjects.append(SubjectConfigurationV1.model_validate(payload))

    staging = _staging_subject(
        active=active,
        base_fleet=base_fleet,
        existing=existing_staging,
        seed=seed,
        target_generation=target_generation,
    )
    desired_subjects.append(staging)
    desired_tuple = tuple(sorted(desired_subjects, key=lambda subject: subject.subject_id.hex))
    for subject in desired_tuple:
        for profile in subject.profiles:
            validate_profile_narrowing(base_fleet, profile)

    same_subjects = desired_tuple == active.subjects
    same_fleet = base_fleet == active.fleet
    exact = same_subjects and same_fleet
    desired_fleet = (
        active.fleet
        if exact
        else _fleet_with_digest(
            base_fleet,
            fleet_generation=active.fleet.fleet_generation + 1,
            pools=base_fleet.pools,
            template=base_fleet.development_subject_template,
        )
    )
    current_by_id = {subject.subject_id: subject for subject in active.subjects}
    changed_subject_ids = frozenset(
        subject.subject_id
        for subject in desired_tuple
        if current_by_id.get(subject.subject_id) != subject
    )
    return _DesiredConfiguration(
        original=active,
        fleet=desired_fleet,
        subjects=desired_tuple,
        changed_subject_ids=changed_subject_ids,
        staging_subject=staging,
        exact=exact,
    )


def _validate_live_topology(fleet: FleetManifestV1) -> None:
    if {pool.pool_id for pool in fleet.pools} != _POOL_IDS:
        raise ValueError("active capacity pool coverage is invalid")
    for pool in fleet.pools:
        if pool.pool_generation != 1 or pool.health != "eligible":
            raise ValueError("active capacity pool generation or health is unsafe")
        expected_architecture = "arm64" if pool.pool_id == "gb10" else "x86_64"
        if any(domain.architecture != expected_architecture for domain in pool.resource_domains):
            raise ValueError("active capacity pool architecture is unsafe")
        nodes = {item.node_id: item for domain in pool.resource_domains for item in domain.nodes}
        if pool.pool_id == "gb10":
            if set(nodes) == _GB10_SOURCE_NODES:
                expected_maximum = 150
            elif set(nodes) == _GB10_TARGET_NODES:
                expected_maximum = 140
            else:
                raise ValueError("active GB10 topology is unsafe")
            expected_slots = 10
        else:
            if set(nodes) != _OLDLAB_NODES:
                raise ValueError("active OLDLAB topology is unsafe")
            expected_maximum = 18
            expected_slots = 6
        if (
            pool.max_slots != expected_maximum
            or any(item.allocatable.slots != expected_slots for item in nodes.values())
            or sum(item.allocatable.slots for item in nodes.values()) != expected_maximum
        ):
            raise ValueError("active capacity node slot topology is unsafe")


def _target_pool(pool: PoolManifestV1) -> PoolManifestV1:
    payload = pool.model_dump(mode="python")
    if pool.pool_id == "gb10":
        domains: list[dict[str, object]] = []
        for domain in pool.resource_domains:
            domain_payload = domain.model_dump(mode="python")
            domain_payload["nodes"] = tuple(
                node for node in domain.nodes if node.node_id != "trt-gb10-2"
            )
            domains.append(domain_payload)
        payload["resource_domains"] = tuple(domains)
        payload["max_slots"] = 140
    elif pool.pool_id == "oldlab":
        payload["max_slots"] = 18
    else:  # guarded by _validate_live_topology
        raise ValueError("active capacity pool coverage is invalid")
    payload["pool_digest"] = "0" * 64
    provisional = PoolManifestV1.model_validate(payload)
    payload["pool_digest"] = canonical_digest_excluding(provisional, "pool_digest")
    return PoolManifestV1.model_validate(payload)


def _retarget_profile(
    profile: ProfileReferenceV1,
    *,
    source_fleet: FleetManifestV1,
    target_pool: PoolManifestV1,
    one_slot_only: bool,
    bump_on_change: bool,
) -> ProfileReferenceV1:
    validate_profile_narrowing(source_fleet, profile)
    if profile.pool_id != target_pool.pool_id:
        raise ValueError("capacity profile pool binding is invalid")
    shapes = tuple(
        shape
        for shape in profile.worker_shapes
        if not one_slot_only or shape.concurrency_slots == 1
    )
    if not shapes:
        raise ValueError("capacity profile has no authenticated one-slot shape")
    changed = (
        profile.pool_generation != target_pool.pool_generation
        or profile.pool_digest != target_pool.pool_digest
        or profile.protocol_generation != target_pool.protocol_generation
        or profile.protocol_digest != target_pool.protocol_digest
        or shapes != profile.worker_shapes
    )
    payload = profile.model_dump(mode="python")
    payload.update(
        {
            "pool_generation": target_pool.pool_generation,
            "pool_digest": target_pool.pool_digest,
            "protocol_generation": target_pool.protocol_generation,
            "protocol_digest": target_pool.protocol_digest,
            "worker_shapes": shapes,
            "profile_generation": (
                profile.profile_generation + 1
                if changed and bump_on_change
                else profile.profile_generation
            ),
            "profile_digest": "0" * 64,
        }
    )
    provisional = ProfileReferenceV1.model_validate(payload)
    payload["profile_digest"] = canonical_digest_excluding(provisional, "profile_digest")
    return ProfileReferenceV1.model_validate(payload)


def _fleet_with_digest(
    source: FleetManifestV1,
    *,
    fleet_generation: int,
    pools: tuple[PoolManifestV1, ...],
    template: DevelopmentSubjectTemplateV1 | None,
) -> FleetManifestV1:
    payload = source.model_dump(mode="python")
    payload.update(
        {
            "fleet_generation": fleet_generation,
            "fleet_digest": "0" * 64,
            "pools": pools,
            "development_subject_template": template,
            "executable_new_capacity_ceiling": 0,
        }
    )
    provisional = FleetManifestV1.model_validate(payload)
    payload["fleet_digest"] = canonical_digest_excluding(provisional, "fleet_digest")
    fleet = FleetManifestV1.model_validate(payload)
    validate_fleet_manifest_digests(fleet)
    return fleet


def _staging_subject(
    *,
    active: _ActiveConfiguration,
    base_fleet: FleetManifestV1,
    existing: SubjectConfigurationV1 | None,
    seed: _Seed,
    target_generation: int,
) -> SubjectConfigurationV1:
    target_pools = {pool.pool_id: pool for pool in base_fleet.pools}
    if existing is not None:
        if {profile.pool_id for profile in existing.profiles} != _POOL_IDS:
            raise ValueError("existing staging profile coverage is invalid")
        profiles = tuple(
            _retarget_profile(
                profile,
                source_fleet=active.fleet,
                target_pool=target_pools[profile.pool_id],
                one_slot_only=True,
                bump_on_change=True,
            )
            for profile in existing.profiles
        )
        payload = existing.model_dump(mode="python")
        payload.update(
            {
                "min_slots": 0,
                "lifecycle_state": "active",
                "candidate_generation": target_generation,
                "deployment_generation": target_generation,
                "configuration_generation": target_generation,
                "demand_reporter_incarnation": seed.reporter_incarnation,
                "profiles": profiles,
            }
        )
        desired = SubjectConfigurationV1.model_validate(payload)
        if desired != existing and target_generation <= existing.configuration_generation:
            raise ValueError("staging configuration generation is not monotonic")
        return desired

    template = base_fleet.development_subject_template
    if template is None:
        raise ValueError("authenticated staging profile authority is unavailable")
    services = [account for account in base_fleet.account_policies if account.kind == "service"]
    if len(services) != 1:
        raise ValueError("authenticated staging service account is ambiguous")
    account = services[0]
    tier = next((tier for tier in base_fleet.tiers if tier.tier_id == "staging"), None)
    if tier is None:
        raise ValueError("authenticated staging tier is unavailable")
    profiles = tuple(
        _retarget_profile(
            profile,
            source_fleet=base_fleet,
            target_pool=target_pools[profile.pool_id],
            one_slot_only=True,
            bump_on_change=False,
        )
        for profile in template.profiles
    )
    if {profile.pool_id for profile in profiles} != _POOL_IDS:
        raise ValueError("authenticated staging profile coverage is incomplete")
    pool_slot_limit = sum(pool.max_slots for pool in base_fleet.pools)
    pool_pending_slots = sum(pool.max_pending_slots for pool in base_fleet.pools)
    pool_pending_jobs = sum(pool.max_pending_jobs for pool in base_fleet.pools)
    pool_submission_rate = sum(pool.submission_rate_per_minute for pool in base_fleet.pools)
    return SubjectConfigurationV1(
        subject_id=seed.subject_id,
        subject_incarnation=seed.subject_incarnation,
        display_name="staging",
        account_id=account.account_id,
        tier_id="staging",
        min_slots=0,
        max_slots=min(account.max_slots, tier.max_slots, pool_slot_limit),
        rollout_surge_slots=account.max_surge_slots,
        max_pending_slots=min(
            account.max_pending_slots,
            tier.max_pending_slots,
            base_fleet.global_max_pending_slots,
            pool_pending_slots,
        ),
        max_pending_jobs=min(
            account.max_pending_jobs,
            tier.max_pending_jobs,
            base_fleet.global_max_pending_jobs,
            pool_pending_jobs,
        ),
        submission_rate_per_minute=min(
            account.submission_rate_per_minute,
            base_fleet.global_submission_rate_per_minute,
            pool_submission_rate,
        ),
        lifecycle_state="active",
        candidate_generation=target_generation,
        deployment_generation=target_generation,
        configuration_generation=target_generation,
        demand_reporter_incarnation=seed.reporter_incarnation,
        profiles=profiles,
    )


def _validate_proposal_response(
    value: object,
    *,
    expected_scope: Literal["fleet", "subject"],
    expected_generation: int,
    expected_digest: str,
    expected_subject_id: UUID | None = None,
    expected_subject_incarnation: UUID | None = None,
) -> ConfigurationGenerationRefV1:
    if not isinstance(value, dict) or set(value) != _PROPOSAL_FIELDS:
        raise RuntimeError("protected capacity proposal response fields are invalid")
    try:
        configuration_id = UUID(str(value["configuration_id"]))
    except (KeyError, ValueError) as exc:
        raise RuntimeError("protected capacity proposal response identity is invalid") from exc
    expected_subject = None if expected_subject_id is None else str(expected_subject_id)
    expected_incarnation = (
        None if expected_subject_incarnation is None else str(expected_subject_incarnation)
    )
    if (
        configuration_id.int == 0
        or str(configuration_id) != value["configuration_id"]
        or value["scope"] != expected_scope
        or type(value["generation"]) is not int
        or value["generation"] != expected_generation
        or value["digest"] != expected_digest
        or value["subject_id"] != expected_subject
        or value["subject_incarnation"] != expected_incarnation
    ):
        label = "fleet" if expected_scope == "fleet" else "subject"
        raise RuntimeError(f"protected capacity {label} proposal response is equivocal")
    return ConfigurationGenerationRefV1(
        scope=expected_scope,
        generation=expected_generation,
        digest=expected_digest,
        subject_id=expected_subject_id,
        subject_incarnation=expected_subject_incarnation,
    )


def _validate_activation_response(
    value: object,
    *,
    activation: ConfigurationActivationV1,
) -> ConfigurationSnapshotV1:
    if not isinstance(value, dict) or set(value) != _ACTIVATION_RESPONSE_FIELDS:
        raise RuntimeError("protected capacity activation response fields are invalid")
    try:
        snapshot = _parse_contract(ConfigurationSnapshotV1, value["snapshot"])
    except (KeyError, ValidationError, ValueError) as exc:
        raise RuntimeError("protected capacity activation response is invalid") from exc
    if (
        type(value["configuration_epoch"]) is not int
        or value["configuration_epoch"] != activation.expected_configuration_epoch + 1
        or snapshot.configuration_epoch != value["configuration_epoch"]
        or snapshot.fleet != activation.fleet
        or snapshot.subjects != activation.subjects
        or value["digest"] != canonical_digest(snapshot)
    ):
        raise RuntimeError("protected capacity activation response is equivocal")
    return snapshot


def _validate_rollback_response(
    value: object,
    *,
    target: _ActiveConfiguration,
    predecessor: _ActiveConfiguration,
) -> ConfigurationSnapshotV1:
    if not isinstance(value, dict) or set(value) != _ACTIVATION_RESPONSE_FIELDS:
        raise RuntimeError("protected capacity rollback response fields are invalid")
    try:
        snapshot = _parse_contract(ConfigurationSnapshotV1, value["snapshot"])
    except (KeyError, ValidationError, ValueError) as exc:
        raise RuntimeError("protected capacity rollback response is invalid") from exc
    if (
        type(value["configuration_epoch"]) is not int
        or value["configuration_epoch"] != target.snapshot.configuration_epoch + 1
        or snapshot.configuration_epoch != value["configuration_epoch"]
        or value["digest"] != canonical_digest(snapshot)
    ):
        raise RuntimeError("protected capacity rollback response is not exact")
    _validate_rollback_snapshot(snapshot, target=target, predecessor=predecessor)
    return snapshot


def _read_active_configuration_or_none(
    client: ManagerConfigurationClient,
) -> _ActiveConfiguration | None:
    try:
        return _parse_active_configuration(client.get_configuration())
    except (
        FleetStateError,
        KeyError,
        OSError,
        ProtectedCapacityManagerClientError,
        RuntimeError,
        TypeError,
        UnicodeError,
        ValidationError,
        ValueError,
    ):
        return None


def _read_active_configuration_exact(client: ManagerConfigurationClient) -> _ActiveConfiguration:
    readback = _read_active_configuration_or_none(client)
    if readback is None:
        raise RuntimeError("protected capacity rollback readback is not exact")
    return readback


def _is_exact_target(
    readback: _ActiveConfiguration,
    desired: _DesiredConfiguration,
    *,
    activation: ConfigurationActivationV1,
) -> bool:
    return (
        readback.snapshot.configuration_epoch == activation.expected_configuration_epoch + 1
        and readback.snapshot.fleet == activation.fleet
        and readback.snapshot.subjects == activation.subjects
        and readback.fleet == desired.fleet
        and readback.subjects == desired.subjects
    )


def _is_exact_predecessor(
    readback: _ActiveConfiguration,
    original: _ActiveConfiguration,
) -> bool:
    return (
        readback.snapshot == original.snapshot
        and readback.fleet == original.fleet
        and readback.subjects == original.subjects
    )


def _rollback_evidence_sha256(
    plan: FinalGatePlan,
    *,
    predecessor_epoch: int,
    predecessor_digest: str,
) -> str:
    if plan.checkpoint_schema_version != 3 or plan.restore_report_sha256 is None:
        raise RuntimeError("protected capacity rollback evidence is unavailable")
    return _hash_json(
        {
            "backup_component_set_digest": plan.backup_component_set_digest,
            "backup_lease_digest": plan.backup_lease_digest,
            "backup_lease_id": plan.backup_lease_id,
            "backup_source_request_id": plan.backup_source_request_id,
            "checkpoint_schema_version": plan.checkpoint_schema_version,
            "predecessor_configuration_digest": predecessor_digest,
            "predecessor_configuration_epoch": predecessor_epoch,
            "restore_report_sha256": plan.restore_report_sha256,
        }
    )


def _require_backup_bound_predecessor(
    plan: FinalGatePlan,
    *,
    predecessor_epoch: int,
    predecessor_digest: str,
) -> None:
    if (
        plan.manager_configuration_epoch != predecessor_epoch
        or plan.manager_configuration_digest != predecessor_digest
    ):
        raise RuntimeError("protected capacity rollback predecessor is not backup-bound")


def _validate_rollback_readback(
    readback: _ActiveConfiguration,
    *,
    target: _ActiveConfiguration,
    predecessor: _ActiveConfiguration,
    response: ConfigurationSnapshotV1,
) -> None:
    if readback.snapshot != response:
        raise RuntimeError("protected capacity rollback readback is not exact")
    _validate_rollback_snapshot(readback.snapshot, target=target, predecessor=predecessor)
    if readback.fleet.fleet_generation <= max(
        target.fleet.fleet_generation,
        predecessor.fleet.fleet_generation,
    ):
        raise RuntimeError("protected capacity rollback readback is not exact")
    expected_fleet = _fleet_with_digest(
        predecessor.fleet,
        fleet_generation=readback.fleet.fleet_generation,
        pools=predecessor.fleet.pools,
        template=predecessor.fleet.development_subject_template,
    )
    if readback.fleet != expected_fleet:
        raise RuntimeError("protected capacity rollback readback is not exact")
    predecessor_subjects = {subject.subject_id: subject for subject in predecessor.subjects}
    target_subjects = {subject.subject_id: subject for subject in target.subjects}
    observed_subjects = {subject.subject_id: subject for subject in readback.subjects}
    if set(observed_subjects) != set(predecessor_subjects):
        raise RuntimeError("protected capacity rollback readback is not exact")
    for subject_id, previous in predecessor_subjects.items():
        current = target_subjects.get(subject_id)
        observed = observed_subjects.get(subject_id)
        if (
            current is None
            or observed is None
            or current.subject_incarnation != previous.subject_incarnation
            or observed.subject_incarnation != previous.subject_incarnation
            or observed.configuration_generation
            <= max(current.configuration_generation, previous.configuration_generation)
            or observed
            != previous.model_copy(
                update={"configuration_generation": observed.configuration_generation}
            )
        ):
            raise RuntimeError("protected capacity rollback readback is not exact")


def _validate_rollback_snapshot(
    snapshot: ConfigurationSnapshotV1,
    *,
    target: _ActiveConfiguration,
    predecessor: _ActiveConfiguration,
) -> None:
    if (
        snapshot.fleet.scope != "fleet"
        or snapshot.fleet.subject_id is not None
        or snapshot.fleet.subject_incarnation is not None
        or snapshot.fleet.generation
        <= max(target.fleet.fleet_generation, predecessor.fleet.fleet_generation)
    ):
        raise RuntimeError("protected capacity rollback response is not exact")
    predecessor_subjects = {subject.subject_id: subject for subject in predecessor.subjects}
    target_subjects = {subject.subject_id: subject for subject in target.subjects}
    if len(snapshot.subjects) != len(predecessor_subjects):
        raise RuntimeError("protected capacity rollback response is not exact")
    identities = {
        (reference.subject_id, reference.subject_incarnation) for reference in snapshot.subjects
    }
    if len(identities) != len(snapshot.subjects):
        raise RuntimeError("protected capacity rollback response is not exact")
    for reference in snapshot.subjects:
        subject_id = reference.subject_id
        subject_incarnation = reference.subject_incarnation
        if subject_id is None or subject_incarnation is None or reference.scope != "subject":
            raise RuntimeError("protected capacity rollback response is not exact")
        previous = predecessor_subjects.get(subject_id)
        current = target_subjects.get(subject_id)
        if (
            previous is None
            or current is None
            or previous.subject_incarnation != subject_incarnation
            or current.subject_incarnation != previous.subject_incarnation
            or reference.generation
            <= max(current.configuration_generation, previous.configuration_generation)
        ):
            raise RuntimeError("protected capacity rollback response is not exact")


def _expected_rollback_configuration(
    *,
    target: _ActiveConfiguration,
    predecessor: _ActiveConfiguration,
) -> _ActiveConfiguration:
    target_subjects = {subject.subject_id: subject for subject in target.subjects}
    cloned_fleet = _fleet_with_digest(
        predecessor.fleet,
        fleet_generation=target.fleet.fleet_generation + 1,
        pools=predecessor.fleet.pools,
        template=predecessor.fleet.development_subject_template,
    )
    cloned_subjects = tuple(
        sorted(
            (
                _clone_predecessor_subject_for_rollback(
                    predecessor=subject,
                    current=target_subjects.get(subject.subject_id),
                )
                for subject in predecessor.subjects
            ),
            key=lambda subject: subject.subject_id.hex,
        )
    )
    snapshot = ConfigurationSnapshotV1(
        configuration_epoch=target.snapshot.configuration_epoch + 1,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=cloned_fleet.fleet_generation,
            digest=canonical_digest(cloned_fleet),
        ),
        subjects=tuple(
            ConfigurationGenerationRefV1(
                scope="subject",
                generation=subject.configuration_generation,
                digest=canonical_digest(subject),
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
            )
            for subject in cloned_subjects
        ),
    )
    canonical = {
        "schema_version": 1,
        "configuration": snapshot.model_dump(mode="json", exclude_none=False),
        "fleet": cloned_fleet.model_dump(mode="json", exclude_none=False),
        "subjects": [
            subject.model_dump(mode="json", exclude_none=False) for subject in cloned_subjects
        ],
    }
    return _ActiveConfiguration(
        snapshot=snapshot,
        fleet=cloned_fleet,
        subjects=cloned_subjects,
        evidence_digest=_hash_json(canonical),
    )


def _clone_predecessor_subject_for_rollback(
    *,
    predecessor: SubjectConfigurationV1,
    current: SubjectConfigurationV1 | None,
) -> SubjectConfigurationV1:
    if current is None or current.subject_incarnation != predecessor.subject_incarnation:
        raise RuntimeError("protected capacity rollback predecessor is not exact")
    return predecessor.model_copy(
        update={"configuration_generation": current.configuration_generation + 1}
    )


def _idempotency_key(plan: FinalGatePlan, purpose: str, digest: str) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        f"loom:protected-staging-capacity-configuration:v1:{plan.plan_digest}:{purpose}:{digest}",
    )


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


_ContractT = TypeVar(
    "_ContractT",
    ConfigurationSnapshotV1,
    FleetManifestV1,
    SubjectConfigurationV1,
)


def _parse_contract(model: type[_ContractT], value: object) -> _ContractT:
    try:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("active capacity contract is not JSON") from exc
    return model.model_validate_json(payload)


__all__ = [
    "KubernetesProtectedStagingCapacityManagerConfigurationComponent",
    "ProtectedStagingDesiredConfiguration",
    "derive_protected_staging_capacity_configuration",
]
