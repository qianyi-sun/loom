"""Authoritative production of immutable protected execution prerequisites."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from urllib.parse import urlsplit
from uuid import UUID

from loom_capacity_manager.contracts import SubjectConfigurationV1, canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationPolicyV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_cli.capacity_control_plane import CapacityPoolExecutorBinding

from .backup_lease import BackupLease, component_set_digest
from .protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisitePublication,
    ProtectedExecutionPrerequisiteStore,
)
from .protected_execution_prerequisites import (
    CapacityPoolExecutorProfileSeed,
    ProtectedExecutionPrerequisiteArtifact,
)
from .protected_staging_capacity_manager_configuration_component import (
    ProtectedStagingDesiredConfiguration,
    derive_protected_staging_capacity_configuration,
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REGISTRY_RE = re.compile(
    r"^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?(?::[1-9][0-9]{0,4})?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$"
)
_MANAGER_EXTERNAL_ORIGIN = "https://192.168.50.103:31443"
_TARGET_NODES = {
    "gb10": frozenset({"trt-gb10-1", *(f"trt-gb10-{index}" for index in range(3, 16))}),
    "oldlab": frozenset(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
}
_TARGET_POOL_SLOTS = {"gb10": 140, "oldlab": 18}


def _valid_registry(value: object) -> bool:
    if not isinstance(value, str) or _REGISTRY_RE.fullmatch(value) is None:
        return False
    try:
        parsed = urlsplit(f"//{value}")
        port = parsed.port
    except ValueError:
        return False
    return parsed.hostname is not None and (port is None or port <= 65535)


class ProtectedExecutionPrerequisiteSourceError(RuntimeError):
    """The complete prerequisite source was unavailable, stale, or contradictory."""


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPrerequisiteAuthority:
    """One secret-free, typed observation from controllers and frozen writers."""

    executor_profile_seed: CapacityPoolExecutorProfileSeed
    subject_acknowledgements: tuple[SubjectExecutionAcknowledgementV2, ...]
    manager_client_cidrs: Mapping[str, str]
    credential_metadata_sha256: Mapping[str, str]
    coexistence_witness_sha256: Mapping[str, str]
    legacy_writer_fences: tuple[LegacyWriterFenceV2, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.executor_profile_seed, CapacityPoolExecutorProfileSeed)
            or not isinstance(self.subject_acknowledgements, tuple)
            or not self.subject_acknowledgements
            or any(
                not isinstance(item, SubjectExecutionAcknowledgementV2)
                for item in self.subject_acknowledgements
            )
            or not isinstance(self.legacy_writer_fences, tuple)
            or not self.legacy_writer_fences
            or any(not isinstance(item, LegacyWriterFenceV2) for item in self.legacy_writer_fences)
        ):
            raise ValueError("execution prerequisite typed authority is invalid")
        mappings = (
            self.manager_client_cidrs,
            self.credential_metadata_sha256,
            self.coexistence_witness_sha256,
        )
        if any(
            not isinstance(value, Mapping)
            or not value
            or any(
                not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
            )
            for value in mappings
        ):
            raise ValueError("execution prerequisite typed authority is invalid")
        object.__setattr__(
            self,
            "manager_client_cidrs",
            MappingProxyType(dict(sorted(self.manager_client_cidrs.items()))),
        )
        object.__setattr__(
            self,
            "credential_metadata_sha256",
            MappingProxyType(dict(sorted(self.credential_metadata_sha256.items()))),
        )
        object.__setattr__(
            self,
            "coexistence_witness_sha256",
            MappingProxyType(dict(sorted(self.coexistence_witness_sha256.items()))),
        )


ManagerConfigurationSource = Callable[[], Mapping[str, object]]
ConfigurationSeedSource = Callable[[], Mapping[str, object]]
StagingProtectedAdmissionSource = Callable[[Mapping[str, object]], str]
ExecutionAuthoritySource = Callable[
    [ProtectedStagingDesiredConfiguration],
    ProtectedExecutionPrerequisiteAuthority,
]


@dataclass(frozen=True, slots=True)
class _CapturedAuthority:
    desired: ProtectedStagingDesiredConfiguration
    seed_sha256: str
    staging_protected_admission_sha256: str
    authority: ProtectedExecutionPrerequisiteAuthority


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPrerequisiteRuntimeSource:
    """Build one artifact only after every independent source agrees twice."""

    store: ProtectedExecutionPrerequisiteStore
    candidate_sha: str
    candidate_tree: str
    core_artifact_bundle_sha256: str
    mutation_epoch: int
    executor_image_sha256: str
    container_registry: str
    manager_configuration_source: ManagerConfigurationSource
    configuration_seed_source: ConfigurationSeedSource
    staging_protected_admission_source: StagingProtectedAdmissionSource
    authority_source: ExecutionAuthoritySource
    now: Callable[[], datetime] = lambda: datetime.now(UTC)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.store, ProtectedExecutionPrerequisiteStore)
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _SHA256_RE.fullmatch(self.core_artifact_bundle_sha256) is None
            or type(self.mutation_epoch) is not int
            or self.mutation_epoch < 0
            or _SHA256_RE.fullmatch(self.executor_image_sha256) is None
            or not _valid_registry(self.container_registry)
            or self.container_registry.endswith("/")
            or not all(
                callable(source)
                for source in (
                    self.manager_configuration_source,
                    self.configuration_seed_source,
                    self.staging_protected_admission_source,
                    self.authority_source,
                    self.now,
                )
            )
        ):
            raise ValueError("execution prerequisite runtime source is invalid")

    @property
    def executor_image(self) -> str:
        return (
            f"{self.container_registry}/loom-capacity-executor@sha256:{self.executor_image_sha256}"
        )

    def publish(self, lease: BackupLease) -> ProtectedExecutionPrerequisitePublication:
        """Publish only a stable, lease-bound, zero-ceiling prerequisite snapshot."""

        try:
            first = self._capture()
            second = self._capture()
            if first != second:
                raise ValueError("execution prerequisite source changed during capture")
            artifact = self._artifact(first, lease=lease)
            publication = self.store.publish(artifact)
            if self._capture() != first:
                raise ValueError("execution prerequisite source changed during publication")
            if self.store.read(publication) != artifact:
                raise ValueError("execution prerequisite publication changed after write")
        except Exception:
            pass
        else:
            return publication
        raise ProtectedExecutionPrerequisiteSourceError(
            "protected execution prerequisite authority is unavailable"
        )

    def _capture(self) -> _CapturedAuthority:
        active = self.manager_configuration_source()
        seed = self.configuration_seed_source()
        if not isinstance(active, Mapping) or not isinstance(seed, Mapping):
            raise ValueError("execution prerequisite source is invalid")
        active_copy = copy.deepcopy(dict(active))
        seed_copy = copy.deepcopy(dict(seed))
        desired = derive_protected_staging_capacity_configuration(
            active_document=active_copy,
            seed_values=seed_copy,
            target_generation=self.mutation_epoch + 1,
        )
        protected_admission = self.staging_protected_admission_source(seed_copy)
        authority = self.authority_source(desired)
        if _SHA256_RE.fullmatch(protected_admission) is None or not isinstance(
            authority, ProtectedExecutionPrerequisiteAuthority
        ):
            raise ValueError("execution prerequisite source is invalid")
        self._validate_desired_authority(
            desired,
            protected_admission_sha256=protected_admission,
            authority=authority,
        )
        return _CapturedAuthority(
            desired=desired,
            seed_sha256=_hash_json(seed_copy),
            staging_protected_admission_sha256=protected_admission,
            authority=authority,
        )

    def _validate_desired_authority(
        self,
        desired: ProtectedStagingDesiredConfiguration,
        *,
        protected_admission_sha256: str,
        authority: ProtectedExecutionPrerequisiteAuthority,
    ) -> None:
        pools = {pool.pool_id: pool for pool in desired.fleet.pools}
        if (
            set(pools) != set(_TARGET_NODES)
            or desired.fleet.executable_new_capacity_ceiling != 0
            or any(
                pools[pool_id].max_slots != slots for pool_id, slots in _TARGET_POOL_SLOTS.items()
            )
            or desired.staging_subject.min_slots != 0
            or authority.executor_profile_seed.authority_incarnation
            != str(desired.fleet.authority_incarnation)
            or authority.executor_profile_seed.executor_image != self.executor_image
        ):
            raise ValueError("execution prerequisite desired fleet authority drifted")
        bindings: dict[str, CapacityPoolExecutorBinding] = {
            pool.pool_id: pool for pool in authority.executor_profile_seed.pools
        }
        if set(bindings) != set(pools):
            raise ValueError("execution prerequisite executor coverage is incomplete")
        for pool_id, pool in pools.items():
            binding = bindings[pool_id]
            configured_nodes = {
                node.node_id: node.allocatable
                for domain in pool.resource_domains
                for node in domain.nodes
            }
            observed_nodes = {node.node_id: node.allocatable for node in binding.inventory.nodes}
            if (
                frozenset(configured_nodes) != _TARGET_NODES[pool_id]
                or observed_nodes != configured_nodes
                or binding.pool_generation != pool.pool_generation
                or binding.partition != pool.partition
                or binding.association != pool.association
                or binding.inventory.reporter_incarnation != str(pool.pool_reporter_incarnation)
            ):
                raise ValueError("execution prerequisite controller inventory drifted")
        acknowledgements = {item.subject_id: item for item in authority.subject_acknowledgements}
        subjects = {subject.subject_id: subject for subject in desired.subjects}
        if len(acknowledgements) != len(authority.subject_acknowledgements) or set(
            acknowledgements
        ) != set(subjects):
            raise ValueError("execution prerequisite subject authority is incomplete")
        for subject_id, subject in subjects.items():
            acknowledgement = acknowledgements[subject_id]
            if not _acknowledgement_matches_subject(acknowledgement, subject):
                raise ValueError("execution prerequisite subject authority drifted")
        staging_acknowledgement = acknowledgements[desired.staging_subject.subject_id]
        if (
            staging_acknowledgement.candidate.algorithm != "git-sha1"
            or staging_acknowledgement.candidate.identity != self.candidate_sha
            or staging_acknowledgement.candidate.publication_sha256
            != self.core_artifact_bundle_sha256
            or staging_acknowledgement.protected_admission_sha256 != protected_admission_sha256
        ):
            raise ValueError("execution prerequisite staging admission drifted")
        if any(
            acknowledgement.legacy_writer_high_water < 1
            or acknowledgement.acknowledgement_sha256 == "0" * 64
            for acknowledgement in authority.subject_acknowledgements
        ) or any(
            fence.high_water < 1 or fence.freeze_evidence_sha256 == "0" * 64
            for fence in authority.legacy_writer_fences
        ):
            raise ValueError("execution prerequisite legacy high-water authority drifted")
        if any(
            digest == "0" * 64
            for evidence in (
                authority.credential_metadata_sha256,
                authority.coexistence_witness_sha256,
            )
            for digest in evidence.values()
        ):
            raise ValueError("execution prerequisite access authority is a placeholder")

    def _artifact(
        self,
        captured: _CapturedAuthority,
        *,
        lease: BackupLease,
    ) -> ProtectedExecutionPrerequisiteArtifact:
        desired = captured.desired
        self._validate_lease(lease, desired=desired)
        authority = captured.authority
        requested_rate = min(
            desired.fleet.global_submission_rate_per_minute,
            sum(pool.submission_rate_per_minute for pool in desired.fleet.pools),
        )
        if requested_rate < 1:
            raise ValueError("execution prerequisite fleet rate authority is zero")
        rollback = backup_lease_rollback_evidence_sha256(
            lease,
            predecessor_configuration_epoch=desired.original.snapshot.configuration_epoch,
            predecessor_configuration_digest=canonical_digest(desired.original.snapshot),
        )
        policy = ExecutionPreparationPolicyV2(
            trusted_fleet_release_sha256=(
                authority.executor_profile_seed.trusted_fleet_release_sha256
            ),
            executable_new_capacity_ceiling=sum(pool.max_slots for pool in desired.fleet.pools),
            executable_new_capacity_rate_per_minute=requested_rate,
            executors=tuple(
                PreparedExecutorBindingV2(
                    pool_id=pool.pool_id,
                    pool_generation=pool.pool_generation,
                    executor_id=pool.executor_id,
                    executor_incarnation=UUID(pool.executor_incarnation),
                    signing_key_sha256=pool.signing_key_sha256,
                    local_authority_sha256=pool.local_authority_sha256,
                    controller_authority_sha256=pool.controller_authority_sha256,
                )
                for pool in authority.executor_profile_seed.pools
            ),
            subject_acknowledgements=authority.subject_acknowledgements,
            rollback_evidence_sha256=rollback,
            controller_authorities=tuple(
                PoolControllerAuthorityV2(
                    pool_id=pool.pool_id,
                    controller_authority_sha256=pool.controller_authority_sha256,
                )
                for pool in authority.executor_profile_seed.pools
            ),
            legacy_writer_fences=authority.legacy_writer_fences,
        )
        return ProtectedExecutionPrerequisiteArtifact(
            schema_version=1,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            core_artifact_bundle_sha256=self.core_artifact_bundle_sha256,
            source_configuration_epoch=desired.original.snapshot.configuration_epoch,
            source_configuration_sha256=canonical_digest(desired.original.snapshot),
            desired_fleet_generation=desired.fleet.fleet_generation,
            desired_fleet_sha256=canonical_digest(desired.fleet),
            desired_subject_sha256={
                str(subject.subject_id): canonical_digest(subject) for subject in desired.subjects
            },
            subject_protected_admission_sha256={
                str(item.subject_id): item.protected_admission_sha256
                for item in authority.subject_acknowledgements
            },
            staging_subject_id=desired.staging_subject.subject_id,
            backup_lease_sha256=lease.evidence_digest,
            rollback_evidence_sha256=rollback,
            manager_external_origin=_MANAGER_EXTERNAL_ORIGIN,
            manager_client_cidrs=authority.manager_client_cidrs,
            credential_metadata_sha256=authority.credential_metadata_sha256,
            coexistence_witness_sha256=authority.coexistence_witness_sha256,
            legacy_writer_evidence_sha256={
                _legacy_writer_key(item): item.freeze_evidence_sha256
                for item in authority.legacy_writer_fences
            },
            execution_policy=policy,
            executor_profile_seed=authority.executor_profile_seed,
        )

    def _validate_lease(
        self,
        lease: BackupLease,
        *,
        desired: ProtectedStagingDesiredConfiguration,
    ) -> None:
        observed_at = self.now()
        source_configuration_digest = canonical_digest(desired.original.snapshot)
        if (
            not isinstance(lease, BackupLease)
            or observed_at.tzinfo is None
            or observed_at.utcoffset() is None
            or lease.checkpoint_schema_version != 3
            or lease.environment != "staging"
            or lease.namespace != "loom-staging"
            or lease.mutation_epoch != self.mutation_epoch
            or not lease.restore_verified_at <= observed_at < lease.expires_at
            or lease.manager_configuration_epoch != desired.original.snapshot.configuration_epoch
            or lease.manager_configuration_digest != source_configuration_digest
            or lease.manager_authority_incarnation != desired.fleet.authority_incarnation
            or lease.manager_execution_state != "shadow"
            or lease.manager_execution_epoch != 0
            or lease.manager_execution_manifest_sha256 is not None
            or lease.manager_executable_new_capacity_ceiling != 0
            or lease.manager_increase_freeze is not True
            or lease.restore_report_sha256 is None
        ):
            raise ValueError("execution prerequisite backup lease authority drifted")


def _acknowledgement_matches_subject(
    acknowledgement: SubjectExecutionAcknowledgementV2,
    subject: SubjectConfigurationV1,
) -> bool:
    return bool(
        acknowledgement.subject_incarnation == subject.subject_incarnation
        and acknowledgement.configuration_generation == subject.configuration_generation
        and acknowledgement.deployment_generation == subject.deployment_generation
        and acknowledgement.reporter_incarnation == subject.demand_reporter_incarnation
    )


def _legacy_writer_key(fence: LegacyWriterFenceV2) -> str:
    return f"{fence.scope_kind}/{fence.scope_id}/{fence.writer_kind}/{fence.writer_id}"


def backup_lease_rollback_evidence_sha256(
    lease: BackupLease,
    *,
    predecessor_configuration_epoch: int,
    predecessor_configuration_digest: str,
) -> str:
    """Derive rollback authority from one exact schema-3 restore lease."""

    if (
        not isinstance(lease, BackupLease)
        or lease.checkpoint_schema_version != 3
        or type(predecessor_configuration_epoch) is not int
        or predecessor_configuration_epoch < 1
        or _SHA256_RE.fullmatch(predecessor_configuration_digest) is None
        or lease.restore_report_sha256 is None
    ):
        raise ValueError("execution prerequisite rollback authority is invalid")
    return _hash_json(
        {
            "backup_component_set_digest": component_set_digest(lease.component_sha256),
            "backup_lease_digest": lease.evidence_digest,
            "backup_lease_id": lease.lease_id,
            "backup_source_request_id": lease.source_request_id,
            "checkpoint_schema_version": lease.checkpoint_schema_version,
            "predecessor_configuration_digest": predecessor_configuration_digest,
            "predecessor_configuration_epoch": predecessor_configuration_epoch,
            "restore_report_sha256": lease.restore_report_sha256,
        }
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


__all__ = [
    "ExecutionAuthoritySource",
    "ManagerConfigurationSource",
    "ProtectedExecutionPrerequisiteAuthority",
    "ProtectedExecutionPrerequisiteRuntimeSource",
    "ProtectedExecutionPrerequisiteSourceError",
    "backup_lease_rollback_evidence_sha256",
]
