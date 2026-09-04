"""Immutable, secret-free authority for protected execution prerequisites."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal
from uuid import UUID

from pydantic import ValidationError

from loom_capacity_manager.executable_contracts import (
    ExecutionContextV2,
    ExecutionPreparationPolicyV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    canonical_executable_digest,
)
from loom_cli.capacity_control_plane import (
    CapacityPoolExecutorBinding,
    CapacityPoolExecutorProfile,
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
_MANAGER_EXTERNAL_ORIGIN = "https://192.168.50.103:31443"
_POOL_IDS = ("gb10", "oldlab")
_CLIENT_ROLES = {"gb10", "oldlab", "operator"}
_PRIVATE_IPV4_NETWORKS: tuple[ipaddress.IPv4Network, ...] = tuple(
    ipaddress.IPv4Network(value) for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
_CREDENTIAL_NAMES = {
    "manager-abort",
    "manager-activate",
    "manager-drain",
    "manager-prepare",
    "manager-read",
    "manager-retire",
    "pool-executor-gb10",
    "pool-executor-oldlab",
    "pool-ownership-gb10",
    "pool-ownership-oldlab",
}


def _digest(value: str, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"execution prerequisite {label} digest is invalid")
    return value


def _digest_map(
    value: Mapping[str, str],
    label: str,
    *,
    keys: set[str] | None = None,
) -> Mapping[str, str]:
    copied = dict(value)
    if (keys is not None and set(copied) != keys) or any(
        not isinstance(name, str)
        or not name
        or not isinstance(item, str)
        or _SHA256_RE.fullmatch(item) is None
        for name, item in copied.items()
    ):
        raise ValueError(f"execution prerequisite {label} is invalid")
    return MappingProxyType(dict(sorted(copied.items())))


def _uuid_digest_map(value: Mapping[str, str], label: str) -> Mapping[str, str]:
    copied = _digest_map(value, label)
    for name in copied:
        try:
            parsed = UUID(name)
        except ValueError as exc:
            raise ValueError(f"execution prerequisite {label} is invalid") from exc
        if parsed.int == 0 or str(parsed) != name:
            raise ValueError(f"execution prerequisite {label} is invalid")
    return copied


def _legacy_writer_key(scope_kind: str, scope_id: str, writer_kind: str, writer_id: str) -> str:
    return f"{scope_kind}/{scope_id}/{writer_kind}/{writer_id}"


@dataclass(frozen=True, slots=True)
class CapacityPoolExecutorProfileSeed:
    """All controller-profile facts known before an execution epoch exists."""

    schema_version: Literal[1]
    namespace: Literal["loom-dev"]
    executable_new_capacity_ceiling: Literal[0]
    executor_image: str
    service_user: str
    authority_incarnation: str
    trusted_fleet_release_sha256: str
    manager_origin: str
    pools: tuple[CapacityPoolExecutorBinding, CapacityPoolExecutorBinding]

    def __post_init__(self) -> None:
        if not isinstance(self.pools, tuple) or len(self.pools) != 2:
            raise ValueError("execution prerequisite executor profile seed is invalid")
        pools = (self.pools[0], self.pools[1])
        try:
            # The existing profile remains the validator for shared static
            # facts. Sentinels validate only; they never leave this method.
            CapacityPoolExecutorProfile(
                schema_version=self.schema_version,
                namespace=self.namespace,
                executable_new_capacity_ceiling=self.executable_new_capacity_ceiling,
                executor_image=self.executor_image,
                service_user=self.service_user,
                authority_incarnation=self.authority_incarnation,
                writer_epoch=1,
                configuration_epoch=1,
                execution_epoch=1,
                execution_manifest_sha256="1" * 64,
                trusted_fleet_release_sha256=self.trusted_fleet_release_sha256,
                manager_origin=self.manager_origin,
                pools=pools,
            )
        except (TypeError, ValidationError, ValueError) as exc:
            raise ValueError("execution prerequisite executor profile seed is invalid") from exc
        object.__setattr__(self, "pools", pools)

    @classmethod
    def from_profile(
        cls,
        profile: CapacityPoolExecutorProfile,
    ) -> CapacityPoolExecutorProfileSeed:
        if not isinstance(profile, CapacityPoolExecutorProfile):
            raise TypeError("executor profile seed source is invalid")
        return cls(
            schema_version=profile.schema_version,
            namespace=profile.namespace,
            executable_new_capacity_ceiling=profile.executable_new_capacity_ceiling,
            executor_image=profile.executor_image,
            service_user=profile.service_user,
            authority_incarnation=profile.authority_incarnation,
            trusted_fleet_release_sha256=profile.trusted_fleet_release_sha256,
            manager_origin=profile.manager_origin,
            pools=profile.pools,
        )

    def realize(self, execution: ExecutionContextV2) -> CapacityPoolExecutorProfile:
        if (
            not isinstance(execution, ExecutionContextV2)
            or execution.execution_state != "prepared"
            or execution.executable_new_capacity_ceiling != 0
            or execution.executable_new_capacity_rate_per_minute != 0
            or str(execution.authority_incarnation) != self.authority_incarnation
            or execution.trusted_fleet_release_sha256 != self.trusted_fleet_release_sha256
        ):
            raise ValueError("prepared execution context differs from executor profile seed")
        return CapacityPoolExecutorProfile(
            schema_version=self.schema_version,
            namespace=self.namespace,
            executable_new_capacity_ceiling=self.executable_new_capacity_ceiling,
            executor_image=self.executor_image,
            service_user=self.service_user,
            authority_incarnation=self.authority_incarnation,
            writer_epoch=execution.writer_epoch,
            configuration_epoch=execution.configuration_epoch,
            execution_epoch=execution.execution_epoch,
            execution_manifest_sha256=execution.execution_manifest_sha256,
            trusted_fleet_release_sha256=self.trusted_fleet_release_sha256,
            manager_origin=self.manager_origin,
            pools=self.pools,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "namespace": self.namespace,
            "executable_new_capacity_ceiling": self.executable_new_capacity_ceiling,
            "executor_image": self.executor_image,
            "service_user": self.service_user,
            "authority_incarnation": self.authority_incarnation,
            "trusted_fleet_release_sha256": self.trusted_fleet_release_sha256,
            "manager_origin": self.manager_origin,
            "pools": [pool.model_dump(mode="json") for pool in self.pools],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CapacityPoolExecutorProfileSeed:
        expected = {
            "schema_version",
            "namespace",
            "executable_new_capacity_ceiling",
            "executor_image",
            "service_user",
            "authority_incarnation",
            "trusted_fleet_release_sha256",
            "manager_origin",
            "pools",
        }
        pools = value.get("pools")
        if set(value) != expected or not isinstance(pools, list) or len(pools) != 2:
            raise ValueError("execution prerequisite executor profile seed is invalid")
        try:
            parsed_pools = tuple(CapacityPoolExecutorBinding.model_validate(pool) for pool in pools)
            return cls(
                schema_version=value["schema_version"],  # type: ignore[arg-type]
                namespace=value["namespace"],  # type: ignore[arg-type]
                executable_new_capacity_ceiling=value["executable_new_capacity_ceiling"],  # type: ignore[arg-type]
                executor_image=value["executor_image"],  # type: ignore[arg-type]
                service_user=value["service_user"],  # type: ignore[arg-type]
                authority_incarnation=value["authority_incarnation"],  # type: ignore[arg-type]
                trusted_fleet_release_sha256=value["trusted_fleet_release_sha256"],  # type: ignore[arg-type]
                manager_origin=value["manager_origin"],  # type: ignore[arg-type]
                pools=parsed_pools,  # type: ignore[arg-type]
            )
        except (TypeError, ValidationError, ValueError) as exc:
            raise ValueError("execution prerequisite executor profile seed is invalid") from exc


@dataclass(frozen=True, slots=True)
class ProtectedExecutionPrerequisiteArtifact:
    """One complete pre-mutation authority for global execution preparation."""

    schema_version: int
    candidate_sha: str
    candidate_tree: str
    core_artifact_bundle_sha256: str
    source_configuration_epoch: int
    source_configuration_sha256: str
    desired_fleet_generation: int
    desired_fleet_sha256: str
    desired_subject_sha256: Mapping[str, str]
    subject_protected_admission_sha256: Mapping[str, str]
    staging_subject_id: UUID
    backup_lease_sha256: str
    rollback_evidence_sha256: str
    manager_external_origin: str
    manager_client_cidrs: Mapping[str, str]
    credential_metadata_sha256: Mapping[str, str]
    coexistence_witness_sha256: Mapping[str, str]
    legacy_writer_evidence_sha256: Mapping[str, str]
    execution_policy: ExecutionPreparationPolicyV2
    executor_profile_seed: CapacityPoolExecutorProfileSeed

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or type(self.source_configuration_epoch) is not int
            or self.source_configuration_epoch < 1
            or type(self.desired_fleet_generation) is not int
            or self.desired_fleet_generation < 1
            or not isinstance(self.staging_subject_id, UUID)
            or self.staging_subject_id.int == 0
        ):
            raise ValueError("execution prerequisite identity is invalid")
        for value, label in (
            (self.core_artifact_bundle_sha256, "core artifact bundle"),
            (self.source_configuration_sha256, "source configuration"),
            (self.desired_fleet_sha256, "desired fleet"),
            (self.backup_lease_sha256, "backup lease"),
            (self.rollback_evidence_sha256, "rollback"),
        ):
            _digest(value, label)
        desired_subjects = _uuid_digest_map(
            self.desired_subject_sha256,
            "desired subject manifest",
        )
        protected_admission = _uuid_digest_map(
            self.subject_protected_admission_sha256,
            "protected admission manifest",
        )
        if set(desired_subjects) != set(protected_admission) or not desired_subjects:
            raise ValueError("execution prerequisite subject authority is invalid")
        if self.manager_external_origin != _MANAGER_EXTERNAL_ORIGIN:
            raise ValueError("execution prerequisite manager route is invalid")
        client_cidrs = dict(self.manager_client_cidrs)
        if set(client_cidrs) != _CLIENT_ROLES:
            raise ValueError("execution prerequisite manager route inventory is invalid")
        try:
            parsed_routes = {
                name: ipaddress.ip_network(route, strict=True)
                for name, route in client_cidrs.items()
            }
        except ValueError as exc:
            raise ValueError("execution prerequisite manager route is invalid") from exc
        if any(
            not isinstance(network, ipaddress.IPv4Network)
            or network.prefixlen != 32
            or not any(network.subnet_of(allowed) for allowed in _PRIVATE_IPV4_NETWORKS)
            or client_cidrs[name] != network.with_prefixlen
            for name, network in parsed_routes.items()
        ):
            raise ValueError("execution prerequisite manager route is invalid")
        credentials = _digest_map(
            self.credential_metadata_sha256,
            "credential metadata",
            keys=_CREDENTIAL_NAMES,
        )
        witnesses = _digest_map(
            self.coexistence_witness_sha256,
            "coexistence witness manifest",
            keys=set(_POOL_IDS),
        )
        legacy_evidence = _digest_map(
            self.legacy_writer_evidence_sha256,
            "legacy writer evidence",
        )
        if not isinstance(self.execution_policy, ExecutionPreparationPolicyV2) or not isinstance(
            self.executor_profile_seed,
            CapacityPoolExecutorProfileSeed,
        ):
            raise ValueError("execution prerequisite typed authority is invalid")
        seed = self.executor_profile_seed
        policy = self.execution_policy
        expected_executors = tuple(
            PreparedExecutorBindingV2(
                pool_id=pool.pool_id,
                pool_generation=pool.pool_generation,
                executor_id=pool.executor_id,
                executor_incarnation=UUID(pool.executor_incarnation),
                signing_key_sha256=pool.signing_key_sha256,
                local_authority_sha256=pool.local_authority_sha256,
                controller_authority_sha256=pool.controller_authority_sha256,
            )
            for pool in seed.pools
        )
        expected_controllers = tuple(
            PoolControllerAuthorityV2(
                pool_id=pool.pool_id,
                controller_authority_sha256=pool.controller_authority_sha256,
            )
            for pool in seed.pools
        )
        policy_subjects = {str(item.subject_id): item for item in policy.subject_acknowledgements}
        staging = policy_subjects.get(str(self.staging_subject_id))
        expected_legacy_evidence = {
            _legacy_writer_key(
                item.scope_kind,
                item.scope_id,
                item.writer_kind,
                item.writer_id,
            ): item.freeze_evidence_sha256
            for item in policy.legacy_writer_fences
        }
        if policy.rollback_evidence_sha256 != self.rollback_evidence_sha256:
            raise ValueError("execution prerequisite rollback authority drifted")
        if (
            seed.manager_origin != self.manager_external_origin
            or seed.trusted_fleet_release_sha256 != policy.trusted_fleet_release_sha256
            or policy.executors != expected_executors
            or policy.controller_authorities != expected_controllers
            or set(policy_subjects) != set(desired_subjects)
            or staging is None
            or staging.protected_admission_sha256
            != protected_admission[str(self.staging_subject_id)]
            or staging.candidate.algorithm != "git-sha1"
            or staging.candidate.identity != self.candidate_sha
            or staging.candidate.publication_sha256 != self.core_artifact_bundle_sha256
            or any(
                item.protected_admission_sha256 != protected_admission[str(item.subject_id)]
                for item in policy.subject_acknowledgements
            )
            or dict(legacy_evidence) != expected_legacy_evidence
        ):
            raise ValueError("execution prerequisite policy authority drifted")
        object.__setattr__(self, "desired_subject_sha256", desired_subjects)
        object.__setattr__(self, "subject_protected_admission_sha256", protected_admission)
        object.__setattr__(
            self,
            "manager_client_cidrs",
            MappingProxyType(dict(sorted(client_cidrs.items()))),
        )
        object.__setattr__(self, "credential_metadata_sha256", credentials)
        object.__setattr__(self, "coexistence_witness_sha256", witnesses)
        object.__setattr__(self, "legacy_writer_evidence_sha256", legacy_evidence)

    @property
    def artifact_sha256(self) -> str:
        return hashlib.sha256(canonical_execution_prerequisite_bytes(self)).hexdigest()

    @property
    def execution_policy_sha256(self) -> str:
        return canonical_executable_digest(self.execution_policy)

    @property
    def executor_profile_seed_sha256(self) -> str:
        return _hash_json(self.executor_profile_seed.to_dict())

    @property
    def manager_route_sha256(self) -> str:
        return _hash_json(
            {
                "manager_client_cidrs": dict(self.manager_client_cidrs),
                "manager_external_origin": self.manager_external_origin,
            }
        )

    @property
    def credential_metadata_manifest_sha256(self) -> str:
        return _hash_json(dict(self.credential_metadata_sha256))

    @property
    def witness_manifest_sha256(self) -> str:
        return _hash_json(dict(self.coexistence_witness_sha256))

    @property
    def legacy_writer_manifest_sha256(self) -> str:
        return _hash_json(dict(self.legacy_writer_evidence_sha256))

    def attestation_evidence(self) -> dict[str, object]:
        """Return only the bounded identities admitted into preflight evidence."""

        return {
            "schema-version": self.schema_version,
            "artifact-sha256": self.artifact_sha256,
            "core-artifact-bundle-sha256": self.core_artifact_bundle_sha256,
            "execution-policy-sha256": self.execution_policy_sha256,
            "executor-profile-seed-sha256": self.executor_profile_seed_sha256,
            "manager-route-sha256": self.manager_route_sha256,
            "access-metadata-sha256": self.credential_metadata_manifest_sha256,
            "coexistence-witness-sha256": self.witness_manifest_sha256,
            "legacy-writer-sha256": self.legacy_writer_manifest_sha256,
            "rollback-evidence-sha256": self.rollback_evidence_sha256,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "core_artifact_bundle_sha256": self.core_artifact_bundle_sha256,
            "source_configuration_epoch": self.source_configuration_epoch,
            "source_configuration_sha256": self.source_configuration_sha256,
            "desired_fleet_generation": self.desired_fleet_generation,
            "desired_fleet_sha256": self.desired_fleet_sha256,
            "desired_subject_sha256": dict(self.desired_subject_sha256),
            "subject_protected_admission_sha256": dict(self.subject_protected_admission_sha256),
            "staging_subject_id": str(self.staging_subject_id),
            "backup_lease_sha256": self.backup_lease_sha256,
            "rollback_evidence_sha256": self.rollback_evidence_sha256,
            "manager_external_origin": self.manager_external_origin,
            "manager_client_cidrs": dict(self.manager_client_cidrs),
            "credential_metadata_sha256": dict(self.credential_metadata_sha256),
            "coexistence_witness_sha256": dict(self.coexistence_witness_sha256),
            "legacy_writer_evidence_sha256": dict(self.legacy_writer_evidence_sha256),
            "execution_policy": self.execution_policy.model_dump(
                mode="json",
                exclude_none=False,
            ),
            "executor_profile_seed": self.executor_profile_seed.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ProtectedExecutionPrerequisiteArtifact:
        expected = {
            "schema_version",
            "candidate_sha",
            "candidate_tree",
            "core_artifact_bundle_sha256",
            "source_configuration_epoch",
            "source_configuration_sha256",
            "desired_fleet_generation",
            "desired_fleet_sha256",
            "desired_subject_sha256",
            "subject_protected_admission_sha256",
            "staging_subject_id",
            "backup_lease_sha256",
            "rollback_evidence_sha256",
            "manager_external_origin",
            "manager_client_cidrs",
            "credential_metadata_sha256",
            "coexistence_witness_sha256",
            "legacy_writer_evidence_sha256",
            "execution_policy",
            "executor_profile_seed",
        }
        if set(value) != expected:
            raise ValueError("execution prerequisite artifact fields are invalid")

        def string(name: str) -> str:
            found = value[name]
            if not isinstance(found, str):
                raise ValueError("execution prerequisite artifact fields are invalid")
            return found

        def string_map(name: str) -> dict[str, str]:
            found = value[name]
            if not isinstance(found, Mapping) or not all(
                isinstance(key, str) and isinstance(item, str) for key, item in found.items()
            ):
                raise ValueError("execution prerequisite artifact fields are invalid")
            return dict(found)

        try:
            return cls(
                schema_version=value["schema_version"],  # type: ignore[arg-type]
                candidate_sha=string("candidate_sha"),
                candidate_tree=string("candidate_tree"),
                core_artifact_bundle_sha256=string("core_artifact_bundle_sha256"),
                source_configuration_epoch=value["source_configuration_epoch"],  # type: ignore[arg-type]
                source_configuration_sha256=string("source_configuration_sha256"),
                desired_fleet_generation=value["desired_fleet_generation"],  # type: ignore[arg-type]
                desired_fleet_sha256=string("desired_fleet_sha256"),
                desired_subject_sha256=string_map("desired_subject_sha256"),
                subject_protected_admission_sha256=string_map("subject_protected_admission_sha256"),
                staging_subject_id=UUID(string("staging_subject_id")),
                backup_lease_sha256=string("backup_lease_sha256"),
                rollback_evidence_sha256=string("rollback_evidence_sha256"),
                manager_external_origin=string("manager_external_origin"),
                manager_client_cidrs=string_map("manager_client_cidrs"),
                credential_metadata_sha256=string_map("credential_metadata_sha256"),
                coexistence_witness_sha256=string_map("coexistence_witness_sha256"),
                legacy_writer_evidence_sha256=string_map("legacy_writer_evidence_sha256"),
                execution_policy=ExecutionPreparationPolicyV2.model_validate_json(
                    json.dumps(
                        value["execution_policy"],
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                        allow_nan=False,
                    )
                ),
                executor_profile_seed=CapacityPoolExecutorProfileSeed.from_dict(
                    value["executor_profile_seed"]  # type: ignore[arg-type]
                ),
            )
        except (TypeError, ValidationError, ValueError) as exc:
            raise ValueError("execution prerequisite artifact is invalid") from exc


def canonical_execution_prerequisite_bytes(
    artifact: ProtectedExecutionPrerequisiteArtifact,
) -> bytes:
    if not isinstance(artifact, ProtectedExecutionPrerequisiteArtifact):
        raise TypeError("execution prerequisite artifact is invalid")
    return (
        json.dumps(
            artifact.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def parse_execution_prerequisite_bytes(
    payload: bytes,
) -> ProtectedExecutionPrerequisiteArtifact:
    if not isinstance(payload, bytes) or not payload or len(payload) > _MAX_ARTIFACT_BYTES:
        raise ValueError("execution prerequisite artifact is invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("execution prerequisite artifact is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("execution prerequisite artifact is invalid")
    artifact = ProtectedExecutionPrerequisiteArtifact.from_dict(value)
    if canonical_execution_prerequisite_bytes(artifact) != payload:
        raise ValueError("execution prerequisite artifact is not canonical")
    return artifact


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("execution prerequisite artifact has duplicate fields")
        value[key] = item
    return value


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
    "CapacityPoolExecutorProfileSeed",
    "ProtectedExecutionPrerequisiteArtifact",
    "canonical_execution_prerequisite_bytes",
    "parse_execution_prerequisite_bytes",
]
