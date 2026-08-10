"""Compose broker and detached worker from one installed preflight source graph."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout.browser_runtime_readiness import CommandRunner as BrowserCommandRunner
from loom_cli.rollout.docker_readiness import CommandRunner as DockerCommandRunner
from loom_cli.rollout.final_gate_command_runner import CommandRunner as FinalGateCommandRunner
from loom_cli.rollout.gb10_readiness import GB10SharedMountReadiness
from loom_cli.rollout.image_readiness import ROLLOUT_IMAGES
from loom_cli.rollout.image_readiness import DockerRunner as ImageDockerRunner
from loom_cli.rollout.kubernetes_readiness import CommandRunner as KubernetesCommandRunner
from loom_cli.rollout.manifest_readiness import RenderManifest, ServerDryRun
from loom_cli.rollout.operator.candidate import GitRunner
from loom_cli.rollout.preflight_artifact_store import (
    LoadedPreflightArtifacts,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_registered_checks import ExternalSupervisorPredecessorSource
from loom_cli.rollout.preflight_runtime import RehearsalActionFactory, RehearsalIdentityFactory
from loom_cli.rollout.preflight_runtime_sources import (
    BackupAdmissionAuthority,
    PreflightRuntimeSources,
)
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from loom_cli.rollout.runtime_readiness import ExecutableLookup, ModuleImporter
from loom_cli.rollout.staging_baseline_readiness import ReadonlyProbe
from loom_cli.rollout.systemd_readiness import CommandRunner as SystemdCommandRunner
from loom_cli.rollout.systemd_unit_readiness import CommandRunner as SystemdAnalyzeRunner

from .config import OperatorConfig
from .deep_preflight_authority import DeepPreflightAuthority, RuntimePurpose
from .installed_preflight_inputs import InstalledPreflightInputs
from .model import CandidateBinding

BackupAuthorityFactory = Callable[[int], BackupAdmissionAuthority]
ManifestFactory = Callable[[CandidateBinding], RenderManifest]
RehearsalFactory = Callable[
    [CandidateBinding, int, RuntimePurpose, LoadedPreflightArtifacts | None],
    tuple[RehearsalActionFactory, RehearsalIdentityFactory],
]


@dataclass(frozen=True, slots=True)
class InstalledDeepPreflightComposition:
    """All installed low-level adapters shared by admission and detached rehearsal."""

    config: OperatorConfig
    service_uid: int
    service_gid: int
    inputs: InstalledPreflightInputs
    artifact_store: PreflightArtifactStore
    attestation_store: PreflightAttestationStore
    git_run: GitRunner
    executable_lookup: ExecutableLookup
    docker_runtime_run: DockerCommandRunner
    kubernetes_run: KubernetesCommandRunner
    readonly_authority_source: Callable[[], ReadonlyAuthorityEvidence]
    capacity_source: Callable[[], StagingCapacity]
    backup_authority_factory: BackupAuthorityFactory
    external_supervisor_predecessor_source: ExternalSupervisorPredecessorSource
    systemd_run: SystemdCommandRunner
    gb10_run: SystemdCommandRunner
    gb10_mount_source: Callable[[], GB10SharedMountReadiness]
    systemd_analyze_run: SystemdAnalyzeRunner
    image_run: ImageDockerRunner
    render_manifest_factory: ManifestFactory
    manifest_image_names: frozenset[str]
    server_schema_dry_run: ServerDryRun
    server_dry_run: ServerDryRun
    browser_run: BrowserCommandRunner
    baseline_probe_factory: Callable[[int], Mapping[str, ReadonlyProbe]]
    route: str
    baseline_probe_route: str
    rehearsal_factory: RehearsalFactory
    final_gate_run: FinalGateCommandRunner
    read_mutation_epoch: Callable[[], int]
    read_database_schema_revision: Callable[[], str]
    now: Callable[[], datetime]
    importer: ModuleImporter = importlib.import_module
    max_concurrency: int = 8
    gb10_candidate_source_run: SystemdCommandRunner | None = None
    container_registry: str = ""
    container_registry_push: str = ""

    def __post_init__(self) -> None:
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.service_uid < 0
            or self.service_gid < 0
            or not self.route.startswith("https://")
            or not self.manifest_image_names
            or not self.manifest_image_names <= {name for name, _path in ROLLOUT_IMAGES}
            or not 1 <= self.max_concurrency <= 32
        ):
            raise ValueError("installed deep preflight composition is invalid")

    def sources(
        self,
        candidate: CandidateBinding,
        mutation_epoch: int,
        purpose: RuntimePurpose,
    ) -> PreflightRuntimeSources:
        """Rebuild one exact source graph; detached mode must load published outputs."""
        if mutation_epoch < 0:
            raise ValueError("installed deep preflight mutation epoch is invalid")
        loaded = None
        if purpose is RuntimePurpose.DETACHED_REHEARSAL:
            if candidate.resolved_tree is None:
                raise ValueError("detached preflight candidate tree is unavailable")
            loaded = self.artifact_store.load_exact(
                candidate_sha=candidate.resolved_sha,
                candidate_tree=candidate.resolved_tree,
                mutation_epoch=mutation_epoch,
                image_tag=candidate.image_tag,
                namespace=self.config.namespace,
                image_run=self.image_run,
                container_registry_push=self.container_registry_push,
            )
        rehearsal_actions, rehearsal_identity = self.rehearsal_factory(
            candidate, mutation_epoch, purpose, loaded
        )
        database_schema_revision = self.read_database_schema_revision()
        return PreflightRuntimeSources(
            config=self.config,
            candidate=candidate,
            candidate_root=self.config.runner_repo,
            artifact_store=self.artifact_store,
            service_uid=self.service_uid,
            service_gid=self.service_gid,
            runner_install_digest=self.inputs.runner_install_digest,
            git_run=self.git_run,
            credential_sources=self.inputs.credential_sources,
            executable_lookup=self.executable_lookup,
            docker_runtime_run=self.docker_runtime_run,
            kubernetes_run=self.kubernetes_run,
            kubeconfig_metadata_digest=self.inputs.kubeconfig_metadata_digest,
            readonly_authority_source=self.readonly_authority_source,
            capacity_source=self.capacity_source,
            backup_authority=self.backup_authority_factory(mutation_epoch),
            database_schema_revision=database_schema_revision,
            external_supervisor_predecessor_source=(self.external_supervisor_predecessor_source),
            systemd_run=self.systemd_run,
            gb10_run=self.gb10_run,
            gb10_targets=self.inputs.gb10_targets,
            gb10_ssh_config=self.inputs.gb10_ssh_config,
            gb10_identity=self.inputs.gb10_identity,
            gb10_ssh_config_sha256=self.inputs.gb10_ssh_config_sha256,
            gb10_identity_metadata_fingerprint=(self.inputs.gb10_identity_metadata_fingerprint),
            gb10_mount_source=self.gb10_mount_source,
            gb10_mount_binding_digest=self.inputs.gb10_mount_binding_digest,
            alembic_ini=self.config.runner_repo / "migrations/alembic.ini",
            migration_policy_path=self.inputs.migration_policy_path,
            migration_policy_digest=self.inputs.migration_policy_digest,
            systemd_analyze_run=self.systemd_analyze_run,
            image_run=self.image_run,
            render_manifest=self.render_manifest_factory(candidate),
            manifest_image_names=self.manifest_image_names,
            server_schema_dry_run=self.server_schema_dry_run,
            server_dry_run=self.server_dry_run,
            browser_run=self.browser_run,
            browser_token_path=self.inputs.browser_token_path,
            baseline_probe_factory=self.baseline_probe_factory,
            route=self.route,
            baseline_probe_route=self.baseline_probe_route,
            rehearsal_actions=rehearsal_actions,
            rehearsal_identity=rehearsal_identity,
            now=self.now,
            importer=self.importer,
            loaded_artifacts=loaded,
            permit_reserved_rotation_candidate=(
                purpose is RuntimePurpose.DETACHED_REHEARSAL
            ),
            gb10_candidate_source_run=self.gb10_candidate_source_run,
            container_registry=self.container_registry,
            container_registry_push=self.container_registry_push,
        )

    def authority(self) -> DeepPreflightAuthority:
        """Return the one authority injected into both broker and detached worker."""
        return DeepPreflightAuthority(
            sources_factory=self.sources,
            attestation_store=self.attestation_store,
            read_mutation_epoch=self.read_mutation_epoch,
            now=self.now,
            max_concurrency=self.max_concurrency,
        )


__all__ = ["InstalledDeepPreflightComposition"]
