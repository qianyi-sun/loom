"""Installed adapter for late protected execution-prerequisite publication."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from loom_cli.rollout.image_readiness import ImageArtifactSet
from loom_cli.rollout.preflight_artifact_store import LoadedPreflightArtifacts
from loom_cli.rollout.preflight_contract import EvidenceValue
from loom_cli.rollout.preflight_runtime_sources import ExecutionPrerequisitePublisher

from .backup_lease import BackupLease
from .deep_preflight_authority import RuntimePurpose
from .model import CandidateBinding
from .protected_execution_prerequisite_source import (
    ExecutionAuthoritySource,
    ManagerConfigurationSource,
    ProtectedExecutionPrerequisiteRuntimeSource,
    ProtectedExecutionPrerequisiteSourceError,
)
from .protected_execution_prerequisite_store import ProtectedExecutionPrerequisiteStore

ConfigurationSeedSource = Callable[[], Mapping[str, object]]
StagingProtectedAdmissionSource = Callable[
    [CandidateBinding, str, int, Mapping[str, object]],
    str,
]
ExecutionAuthoritySourceFactory = Callable[[CandidateBinding, str], ExecutionAuthoritySource]
_REGISTRY_DIGEST_RE = re.compile(r"^sha256:([0-9a-f]{64})$")


@dataclass(frozen=True, slots=True)
class InstalledExecutionPrerequisitePublisherFactory:
    """Bind a late publisher to one exact detached artifact publication."""

    store: ProtectedExecutionPrerequisiteStore
    container_registry: str
    manager_configuration_source: ManagerConfigurationSource
    configuration_seed_source: ConfigurationSeedSource
    staging_protected_admission_source: StagingProtectedAdmissionSource
    authority_source_factory: ExecutionAuthoritySourceFactory
    now: Callable[[], datetime]

    def __call__(
        self,
        candidate: CandidateBinding,
        mutation_epoch: int,
        purpose: RuntimePurpose,
        loaded: LoadedPreflightArtifacts | None,
    ) -> ExecutionPrerequisitePublisher:
        if purpose is RuntimePurpose.ADMISSION:
            if loaded is not None:
                raise ValueError("execution prerequisite artifact phase is invalid")

            def unavailable(
                _lease: BackupLease,
                _images: ImageArtifactSet,
            ) -> Mapping[str, EvidenceValue]:
                raise ProtectedExecutionPrerequisiteSourceError(
                    "protected execution prerequisites are unavailable before detached artifacts"
                )

            return unavailable
        if purpose is not RuntimePurpose.DETACHED_REHEARSAL or loaded is None:
            raise ValueError("execution prerequisite artifact phase is invalid")
        publication = loaded.publication
        candidate_tree = candidate.resolved_tree
        if (
            candidate_tree is None
            or publication.candidate_sha != candidate.resolved_sha
            or publication.candidate_tree != candidate_tree
            or publication.mutation_epoch != mutation_epoch
            or publication.container_registry != self.container_registry
        ):
            raise ValueError("execution prerequisite core publication drifted")

        def publish(
            _lease: BackupLease,
            _images: ImageArtifactSet,
        ) -> Mapping[str, EvidenceValue]:
            try:
                if _images != loaded.images:
                    raise ValueError("execution prerequisite image artifacts drifted")
                executor_digest = _images.registry_digests.get("loom-capacity-executor")
                matched = (
                    _REGISTRY_DIGEST_RE.fullmatch(executor_digest)
                    if isinstance(executor_digest, str)
                    else None
                )
                if matched is None:
                    raise ValueError("execution prerequisite executor image is unpublished")
                executor_image = (
                    f"{self.container_registry}/loom-capacity-executor@sha256:{matched.group(1)}"
                )
                authority_source = self.authority_source_factory(candidate, executor_image)
                if not callable(authority_source):
                    raise ValueError("execution prerequisite authority source is invalid")
                source = ProtectedExecutionPrerequisiteRuntimeSource(
                    store=self.store,
                    candidate_sha=candidate.resolved_sha,
                    candidate_tree=candidate_tree,
                    core_artifact_bundle_sha256=publication.bundle_digest,
                    mutation_epoch=mutation_epoch,
                    executor_image_sha256=matched.group(1),
                    container_registry=self.container_registry,
                    manager_configuration_source=self.manager_configuration_source,
                    configuration_seed_source=self.configuration_seed_source,
                    staging_protected_admission_source=lambda seed: (
                        self.staging_protected_admission_source(
                            candidate,
                            publication.bundle_digest,
                            mutation_epoch,
                            seed,
                        )
                    ),
                    authority_source=authority_source,
                    now=self.now,
                )
                prerequisite_publication = source.publish(_lease)
                artifact = self.store.read(prerequisite_publication)
                evidence = cast(dict[str, EvidenceValue], artifact.attestation_evidence())
                evidence["artifact-path"] = str(prerequisite_publication.path)
                return evidence
            except ProtectedExecutionPrerequisiteSourceError:
                raise
            except Exception:
                raise ProtectedExecutionPrerequisiteSourceError(
                    "protected execution prerequisite authority is unavailable"
                ) from None

        return publish


__all__ = ["InstalledExecutionPrerequisitePublisherFactory"]
