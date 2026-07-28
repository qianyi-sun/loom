"""Construct the complete preflight registry from typed runtime sources."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom_cli.rollout.browser_runtime_readiness import (
    CommandRunner as BrowserCommandRunner,
)
from loom_cli.rollout.browser_runtime_readiness import browser_report_schema_digest
from loom_cli.rollout.docker_readiness import CommandRunner as DockerCommandRunner
from loom_cli.rollout.gb10_readiness import (
    GB10ProbeTarget,
    GB10SharedMountReadiness,
)
from loom_cli.rollout.image_readiness import (
    DockerRunner as ImageDockerRunner,
)
from loom_cli.rollout.image_readiness import ImageBuildSession, image_plan_digest
from loom_cli.rollout.kubernetes_readiness import CommandRunner as KubernetesCommandRunner
from loom_cli.rollout.lifecycle_protocol import (
    LifecycleSelfTestEvidence,
    lifecycle_protocol_digest,
    run_lifecycle_self_test,
)
from loom_cli.rollout.manifest_readiness import (
    ManifestArtifact,
    ManifestRenderSession,
    RenderManifest,
    ServerDryRun,
)
from loom_cli.rollout.migration_manifest_readiness import MigrationManifestArtifact
from loom_cli.rollout.migration_readiness import MigrationPlanEvidence
from loom_cli.rollout.operator.backup_lease import BackupLease, component_set_digest
from loom_cli.rollout.operator.backup_rotation import BackupRotationState
from loom_cli.rollout.operator.candidate import GitRunner
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.operator.systemd import (
    SystemdLaunchCancelEvidence,
    probe_transient_launch_cancel,
)
from loom_cli.rollout.preflight_artifact_store import (
    LoadedPreflightArtifacts,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_UNIT_DIRECTORY,
    RegisteredCheck,
    SafeValue,
)
from loom_cli.rollout.preflight_registered_checks import (
    CredentialProbeSource,
    ExternalSupervisorPredecessorSource,
    build_backup_lease_eligibility_check,
    build_backup_rotation_capacity_check,
    build_browser_runtime_check,
    build_candidate_identity_check,
    build_capacity_high_water_check,
    build_credentials_metadata_check,
    build_docker_runtime_check,
    build_external_supervisor_predecessor_check,
    build_gb10_candidate_source_check,
    build_gb10_host_readiness_check,
    build_gb10_shared_mount_check,
    build_gb10_ssh_topology_check,
    build_image_preflight_checks,
    build_kubernetes_client_check,
    build_lifecycle_launch_cancel_check,
    build_manifest_preflight_checks,
    build_migration_manifest_check,
    build_migration_plan_check,
    build_preflight_artifact_publication_check,
    build_production_defaults_plan_check,
    build_readonly_authority_check,
    build_runner_install_check,
    build_staging_baseline_checks,
    build_systemd_render_check,
    build_systemd_user_manager_check,
    build_tools_runtime_check,
    credential_source_set_digest,
    gb10_target_inventory_digest,
)
from loom_cli.rollout.preflight_runtime import (
    CandidatePreflightRuntime,
    RehearsalActionFactory,
    RehearsalIdentityFactory,
)
from loom_cli.rollout.production_defaults_readiness import ProductionDefaultsArtifact
from loom_cli.rollout.readonly_authority import (
    ReadonlyAuthorityEvidence,
    readonly_authority_policy_digest,
)
from loom_cli.rollout.runtime_readiness import ExecutableLookup, ModuleImporter
from loom_cli.rollout.staging_baseline_readiness import ReadonlyProbe
from loom_cli.rollout.systemd_readiness import CommandRunner as SystemdCommandRunner
from loom_cli.rollout.systemd_unit_readiness import (
    CommandRunner as SystemdAnalyzeRunner,
)

_SHA256_ZERO = "0" * 64
GB10_PREFLIGHT_FLEET_CONCURRENCY = 4
# Every candidate-source probe walks the same NFS-backed Git checkout.  Running
# those full-tree reads concurrently amplifies one backend rather than adding
# independent work; keep this resource class serialized while other GB10
# readiness checks retain their bounded fleet pool.
GB10_CANDIDATE_SOURCE_CONCURRENCY = 1


@dataclass(frozen=True, slots=True)
class BackupAdmissionAuthority:
    """The exact active lease identity, or bounded fresh-checkpoint sentinel."""

    lease_source: Callable[[], BackupLease | None]
    expected_lease_digest: str
    source_request_id: str
    db_snapshot_identity: str
    schema_revision: str
    object_inventory_root: str
    manifest_sha256: str
    component_sha256: Mapping[str, str]
    rotation_source: Callable[[], BackupRotationState]
    expected_rotation_digest: str

    def __post_init__(self) -> None:
        digests = (
            self.expected_lease_digest,
            self.expected_rotation_digest,
            self.object_inventory_root,
            self.manifest_sha256,
            *self.component_sha256.values(),
        )
        if (
            any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for value in digests
            )
            or not self.source_request_id
            or not self.db_snapshot_identity
            or not self.schema_revision
        ):
            raise ValueError("backup admission authority is invalid")
        component_set_digest(self.component_sha256)

    @classmethod
    def fresh(
        cls,
        *,
        schema_revision: str,
        object_inventory_root: str,
        rotation_source: Callable[[], BackupRotationState] | None = None,
        expected_rotation_digest: str | None = None,
    ) -> BackupAdmissionAuthority:
        """Select a fresh checkpoint without claiming a reusable payload."""
        empty_rotation = BackupRotationState()
        source = rotation_source or (lambda: empty_rotation)
        return cls(
            lease_source=lambda: None,
            expected_lease_digest=_SHA256_ZERO,
            source_request_id="fresh-checkpoint",
            db_snapshot_identity="pending-fresh-checkpoint",
            schema_revision=schema_revision,
            object_inventory_root=object_inventory_root,
            manifest_sha256=_SHA256_ZERO,
            component_sha256={
                "k8s_secrets": _SHA256_ZERO,
                "object_inventory": _SHA256_ZERO,
                "postgres": _SHA256_ZERO,
            },
            rotation_source=source,
            expected_rotation_digest=(expected_rotation_digest or empty_rotation.evidence_digest),
        )

    @classmethod
    def unavailable(cls, *, schema_revision: str) -> BackupAdmissionAuthority:
        """Keep unreadable authority inside its registered fail-closed check."""

        def unavailable_source() -> BackupLease | None:
            raise RuntimeError("backup admission authority is unavailable")

        def unavailable_rotation() -> BackupRotationState:
            raise RuntimeError("backup rotation authority is unavailable")

        return cls(
            lease_source=unavailable_source,
            expected_lease_digest=_SHA256_ZERO,
            source_request_id="unavailable-authority",
            db_snapshot_identity="unavailable-authority",
            schema_revision=schema_revision,
            object_inventory_root=_SHA256_ZERO,
            manifest_sha256=_SHA256_ZERO,
            component_sha256={
                "k8s_secrets": _SHA256_ZERO,
                "object_inventory": _SHA256_ZERO,
                "postgres": _SHA256_ZERO,
            },
            rotation_source=unavailable_rotation,
            expected_rotation_digest=_SHA256_ZERO,
        )


@dataclass(frozen=True, slots=True)
class PreflightRuntimeSources:
    """All low-level sources used by the single registered-check implementations."""

    config: OperatorConfig
    candidate: CandidateBinding
    candidate_root: Path
    artifact_store: PreflightArtifactStore
    service_uid: int
    service_gid: int
    runner_install_digest: str
    git_run: GitRunner
    credential_sources: tuple[CredentialProbeSource, ...]
    executable_lookup: ExecutableLookup
    docker_runtime_run: DockerCommandRunner
    kubernetes_run: KubernetesCommandRunner
    kubeconfig_metadata_digest: str
    readonly_authority_source: Callable[[], ReadonlyAuthorityEvidence]
    capacity_source: Callable[[], StagingCapacity]
    backup_authority: BackupAdmissionAuthority
    database_schema_revision: str
    external_supervisor_predecessor_source: ExternalSupervisorPredecessorSource
    systemd_run: SystemdCommandRunner
    gb10_run: SystemdCommandRunner
    gb10_targets: tuple[GB10ProbeTarget, ...]
    gb10_ssh_config: Path
    gb10_identity: Path
    gb10_ssh_config_sha256: str
    gb10_identity_metadata_fingerprint: str
    gb10_mount_source: Callable[[], GB10SharedMountReadiness]
    gb10_mount_binding_digest: str
    alembic_ini: Path
    migration_policy_path: Path
    migration_policy_digest: str
    systemd_analyze_run: SystemdAnalyzeRunner
    image_run: ImageDockerRunner
    render_manifest: RenderManifest
    manifest_image_names: frozenset[str]
    server_schema_dry_run: ServerDryRun
    server_dry_run: ServerDryRun
    browser_run: BrowserCommandRunner
    browser_token_path: Path
    baseline_probe_factory: Callable[[int], Mapping[str, ReadonlyProbe]]
    route: str
    # Live route the Tier-2 baseline probes hit. Equals ``route`` except during a
    # declared route transition, when the live service still serves the
    # predecessor route (see derive_staging_baseline_probe_route).
    baseline_probe_route: str
    rehearsal_actions: RehearsalActionFactory
    rehearsal_identity: RehearsalIdentityFactory
    now: Callable[[], datetime]
    importer: ModuleImporter = importlib.import_module
    lifecycle_self_test: Callable[[], LifecycleSelfTestEvidence] = run_lifecycle_self_test
    lifecycle_runtime_test: Callable[[], SystemdLaunchCancelEvidence] | None = None
    monotonic: Callable[[], float] | None = None
    # Set only for the isolated restore rehearsal, which runs after the backup's
    # own rotation candidate is reserved; lets backup.rotation-capacity tolerate
    # that expected in-progress candidate (never set for gating/final admission).
    permit_reserved_rotation_candidate: bool = False
    loaded_artifacts: LoadedPreflightArtifacts | None = None
    gb10_candidate_source_run: SystemdCommandRunner | None = None

    def __post_init__(self) -> None:
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.service_uid < 0
            or self.service_gid < 0
            or not self.candidate_root.is_absolute()
            or not self.migration_policy_path.is_absolute()
            or not self.route.startswith("https://")
            or not self.baseline_probe_route.startswith("https://")
            or not self.manifest_image_names
            or len(self.database_schema_revision) != 4
            or not self.database_schema_revision.isascii()
            or not self.database_schema_revision.isdecimal()
        ):
            raise ValueError("preflight runtime sources are outside staging authority")

    def build(
        self,
        *,
        mutation_epoch: int,
    ) -> CandidatePreflightRuntime:
        """Build refreshable exact check groups with one build-once image session."""
        if mutation_epoch < 0:
            raise ValueError("preflight runtime mutation epoch is invalid")
        image_session = ImageBuildSession(
            self.image_run,
            candidate_root=self.candidate_root,
            image_tag=self.candidate.image_tag,
            resolved_sha=self.candidate.resolved_sha,
            artifact=(None if self.loaded_artifacts is None else self.loaded_artifacts.images),
        )
        manifest_artifacts: dict[str, ManifestArtifact] = {}
        migration_artifacts: dict[str, MigrationPlanEvidence] = {}
        migration_manifest_artifacts: dict[str, MigrationManifestArtifact] = {}
        production_defaults_artifacts: dict[str, ProductionDefaultsArtifact] = {}
        manifest_session: ManifestRenderSession | None = None
        if self.loaded_artifacts is not None:
            manifest_artifacts["exact"] = self.loaded_artifacts.manifests
            migration_manifest_artifacts["exact"] = self.loaded_artifacts.migration
            production_defaults_artifacts["exact"] = self.loaded_artifacts.production_defaults
            manifest_session = ManifestRenderSession(
                self.render_manifest,
                self.server_schema_dry_run,
                field_ownership_dry_run=self.server_dry_run,
                image_tag=self.candidate.image_tag,
                namespace=self.config.namespace,
                image_digests=self.loaded_artifacts.images.image_digests,
                expected_image_names=self.manifest_image_names,
                artifact=self.loaded_artifacts.manifests,
            )

        def capture_manifest(artifact: ManifestArtifact) -> None:
            manifest_artifacts["exact"] = artifact

        def exact_manifest() -> ManifestArtifact:
            try:
                return manifest_artifacts["exact"]
            except KeyError as exc:
                raise ValueError("preflight manifest artifact was not produced") from exc

        def capture_migration(artifact: MigrationPlanEvidence) -> None:
            migration_artifacts["exact"] = artifact

        def exact_migration() -> tuple[str, str]:
            try:
                artifact = migration_artifacts["exact"]
            except KeyError:
                if self.loaded_artifacts is None:
                    raise ValueError("preflight migration artifact was not produced") from None
                publication = self.loaded_artifacts.publication
                return (
                    publication.migration_plan_sha256,
                    publication.migration_target_revision,
                )
            return artifact.plan_digest, artifact.head

        def capture_migration_manifest(artifact: MigrationManifestArtifact) -> None:
            migration_manifest_artifacts["exact"] = artifact

        def exact_migration_manifest() -> MigrationManifestArtifact:
            try:
                return migration_manifest_artifacts["exact"]
            except KeyError as exc:
                raise ValueError("preflight migration manifest was not produced") from exc

        def capture_production_defaults(artifact: ProductionDefaultsArtifact) -> None:
            production_defaults_artifacts["exact"] = artifact

        def exact_production_defaults() -> ProductionDefaultsArtifact:
            try:
                return production_defaults_artifacts["exact"]
            except KeyError as exc:
                raise ValueError("preflight production defaults were not produced") from exc

        def groups() -> tuple[
            tuple[RegisteredCheck, ...],
            tuple[RegisteredCheck, ...],
            tuple[RegisteredCheck, ...],
        ]:
            tier0 = self._tier0(mutation_epoch=mutation_epoch)
            tier1 = self._tier1(
                image_session=image_session,
                capture_manifest=capture_manifest,
                exact_manifest=exact_manifest,
                manifest_session=manifest_session,
                capture_migration=capture_migration,
                exact_migration=exact_migration,
                capture_migration_manifest=capture_migration_manifest,
                exact_migration_manifest=exact_migration_manifest,
                capture_production_defaults=capture_production_defaults,
                exact_production_defaults=exact_production_defaults,
                mutation_epoch=mutation_epoch,
            )
            tier2 = build_staging_baseline_checks(
                self.baseline_probe_factory(mutation_epoch),
                environment=self.config.environment,
                namespace=self.config.namespace,
                route=self.route,
                baseline_probe_route=self.baseline_probe_route,
                mutation_epoch=mutation_epoch,
            )
            return tier0, tier1, tier2

        tier0, tier1, tier2 = groups()
        return CandidatePreflightRuntime(
            candidate=self.candidate,
            tier0=tier0,
            tier1=tier1,
            tier2=tier2,
            bindings=self._bindings(mutation_epoch=mutation_epoch),
            rehearsal_actions=self.rehearsal_actions,
            rehearsal_identity=self.rehearsal_identity,
            refresh_static_checks=groups,
        )

    def _tier0(self, *, mutation_epoch: int) -> tuple[RegisteredCheck, ...]:
        authority = self.backup_authority
        systemd_check = (
            build_systemd_user_manager_check(self.systemd_run, service_uid=self.service_uid)
            if self.monotonic is None
            else build_systemd_user_manager_check(
                self.systemd_run,
                service_uid=self.service_uid,
                monotonic=self.monotonic,
            )
        )
        return (
            build_candidate_identity_check(
                config=self.config,
                candidate=self.candidate,
                run=self.git_run,
            ),
            build_runner_install_check(
                config=self.config,
                candidate=self.candidate,
                service_uid=self.service_uid,
                expected_attestation_digest=self.runner_install_digest,
            ),
            build_credentials_metadata_check(
                sources=self.credential_sources,
                service_uid=self.service_uid,
            ),
            build_tools_runtime_check(
                runner_install_hash=self.runner_install_digest,
                executable_lookup=self.executable_lookup,
                importer=self.importer,
            ),
            build_docker_runtime_check(self.docker_runtime_run),
            build_kubernetes_client_check(
                self.kubernetes_run,
                config=self.config,
                expected_kubeconfig_metadata_digest=self.kubeconfig_metadata_digest,
            ),
            build_readonly_authority_check(self.readonly_authority_source),
            build_capacity_high_water_check(self.capacity_source),
            build_lifecycle_launch_cancel_check(
                self.lifecycle_self_test,
                self.lifecycle_runtime_test
                or (
                    lambda: probe_transient_launch_cancel(
                        lambda argv: self.systemd_run(argv),
                        candidate_sha=self.candidate.resolved_sha,
                        working_directory=self.candidate_root,
                    )
                ),
            ),
            build_backup_rotation_capacity_check(
                authority.rotation_source,
                expected_rotation_digest=authority.expected_rotation_digest,
                permit_reserved_candidate=self.permit_reserved_rotation_candidate,
            ),
            systemd_check,
            build_external_supervisor_predecessor_check(
                self.external_supervisor_predecessor_source
            ),
            build_gb10_ssh_topology_check(
                self.gb10_run,
                targets=self.gb10_targets,
                ssh_config=self.gb10_ssh_config,
                identity=self.gb10_identity,
                service_uid=self.service_uid,
                expected_ssh_config_sha256=self.gb10_ssh_config_sha256,
                expected_identity_metadata_fingerprint=self.gb10_identity_metadata_fingerprint,
                max_concurrency=GB10_PREFLIGHT_FLEET_CONCURRENCY,
            ),
            build_gb10_shared_mount_check(
                self.gb10_mount_source,
                targets=self.gb10_targets,
                expected_binding_digest=self.gb10_mount_binding_digest,
            ),
            build_gb10_candidate_source_check(
                self.gb10_candidate_source_run or self.gb10_run,
                targets=self.gb10_targets,
                ssh_config=self.gb10_ssh_config,
                identity=self.gb10_identity,
                candidate_root=self.candidate_root,
                expected_candidate_sha=self.candidate.resolved_sha,
                expected_candidate_tree=self.candidate.resolved_tree or "",
                image_tag=self.candidate.image_tag,
                max_concurrency=GB10_CANDIDATE_SOURCE_CONCURRENCY,
                settle_attempts=2,
                settle_interval_seconds=1.0,
            ),
            build_gb10_host_readiness_check(
                self.gb10_run,
                targets=self.gb10_targets,
                ssh_config=self.gb10_ssh_config,
                identity=self.gb10_identity,
                max_concurrency=GB10_PREFLIGHT_FLEET_CONCURRENCY,
            ),
            build_backup_lease_eligibility_check(
                authority.lease_source,
                now=self.now,
                expected_lease_digest=authority.expected_lease_digest,
                source_request_id=authority.source_request_id,
                environment=self.config.environment,
                namespace=self.config.namespace,
                mutation_epoch=mutation_epoch,
                db_snapshot_identity=authority.db_snapshot_identity,
                schema_revision=authority.schema_revision,
                object_inventory_root=authority.object_inventory_root,
                manifest_sha256=authority.manifest_sha256,
                component_sha256=authority.component_sha256,
            ),
        )

    def _tier1(
        self,
        *,
        image_session: ImageBuildSession,
        capture_manifest: Callable[[ManifestArtifact], None],
        exact_manifest: Callable[[], ManifestArtifact],
        manifest_session: ManifestRenderSession | None,
        capture_migration: Callable[[MigrationPlanEvidence], None],
        exact_migration: Callable[[], tuple[str, str]],
        capture_migration_manifest: Callable[[MigrationManifestArtifact], None],
        exact_migration_manifest: Callable[[], MigrationManifestArtifact],
        capture_production_defaults: Callable[[ProductionDefaultsArtifact], None],
        exact_production_defaults: Callable[[], ProductionDefaultsArtifact],
        mutation_epoch: int,
    ) -> tuple[RegisteredCheck, ...]:
        image_checks = build_image_preflight_checks(
            self.image_run,
            candidate_root=self.candidate_root,
            image_tag=self.candidate.image_tag,
            expected_candidate_sha=self.candidate.resolved_sha,
            session=image_session,
        )
        manifest_checks = build_manifest_preflight_checks(
            self.render_manifest,
            self.server_schema_dry_run,
            image_session.verify,
            field_ownership_dry_run=self.server_dry_run,
            image_tag=self.candidate.image_tag,
            namespace=self.config.namespace,
            expected_candidate_sha=self.candidate.resolved_sha,
            expected_config_digest=self.config.config_sha256,
            expected_image_names=self.manifest_image_names,
            session=manifest_session,
            artifact_sink=capture_manifest,
        )
        return (
            build_migration_plan_check(
                alembic_ini=self.alembic_ini,
                policy_path=self.migration_policy_path,
                expected_candidate_sha=self.candidate.resolved_sha,
                expected_policy_digest=self.migration_policy_digest,
                artifact_sink=capture_migration,
            ),
            build_systemd_render_check(
                self.systemd_analyze_run,
                candidate_root=self.candidate_root,
                expected_candidate_sha=self.candidate.resolved_sha,
                expected_candidate_tree=self.candidate.resolved_tree or "",
                expected_image_tag=self.candidate.image_tag,
                expected_environment=self.config.environment,
            ),
            build_production_defaults_plan_check(
                profile_path=self.candidate_root / "deploy/environment-state/staging.toml",
                candidate_sha=self.candidate.resolved_sha,
                candidate_tree=self.candidate.resolved_tree or "",
                image_tag=self.candidate.image_tag,
                environment=self.config.environment,
                artifact_sink=capture_production_defaults,
            ),
            *image_checks,
            *manifest_checks,
            build_migration_manifest_check(
                self.server_dry_run,
                image_artifact=image_session.verify,
                migration_plan=exact_migration,
                candidate_sha=self.candidate.resolved_sha,
                candidate_tree=self.candidate.resolved_tree or "",
                image_tag=self.candidate.image_tag,
                namespace=self.config.namespace,
                artifact_sink=capture_migration_manifest,
            ),
            build_browser_runtime_check(
                self.browser_run,
                image_session.verify,
                token_path=self.browser_token_path,
                service_uid=self.service_uid,
                service_gid=self.service_gid,
                expected_candidate_sha=self.candidate.resolved_sha,
                expected_source_set_digest=credential_source_set_digest(self.credential_sources),
            ),
            build_preflight_artifact_publication_check(
                store=self.artifact_store,
                image_artifact=image_session.verify,
                manifest_artifact=exact_manifest,
                migration_manifest_artifact=exact_migration_manifest,
                production_defaults_artifact=exact_production_defaults,
                candidate_sha=self.candidate.resolved_sha,
                candidate_tree=self.candidate.resolved_tree or "",
                mutation_epoch=mutation_epoch,
                migration_artifact=exact_migration,
                expected_migration_policy_sha256=self.migration_policy_digest,
                browser_report_schema_sha256=browser_report_schema_digest(),
            ),
        )

    def _bindings(self, *, mutation_epoch: int) -> Mapping[str, SafeValue]:
        authority = self.backup_authority
        return {
            "backup.component-set.sha256": component_set_digest(authority.component_sha256),
            "backup.lease.sha256": authority.expected_lease_digest,
            "backup.manifest.sha256": authority.manifest_sha256,
            "backup.rotation.sha256": authority.expected_rotation_digest,
            "backup.source-request": authority.source_request_id,
            "browser.report-schema.sha256": browser_report_schema_digest(),
            "candidate.base.sha": self.candidate.approved_base_sha or "none",
            "candidate.image-tag": self.candidate.image_tag,
            "candidate.sha": self.candidate.resolved_sha,
            "candidate.tree": self.candidate.resolved_tree or "none",
            "candidate.source-mode": self.candidate.source_mode,
            "capacity.policy.sha256": staging_capacity_policy_digest(),
            "db.snapshot-identity": authority.db_snapshot_identity,
            "environment": self.config.environment,
            "external-supervisor.unit-directory": EXTERNAL_SUPERVISOR_UNIT_DIRECTORY,
            "gb10.identity.metadata-fingerprint": self.gb10_identity_metadata_fingerprint,
            "gb10.inventory-digest": gb10_target_inventory_digest(self.gb10_targets),
            "gb10.mount-binding.sha256": self.gb10_mount_binding_digest,
            "gb10.ssh-config.sha256": self.gb10_ssh_config_sha256,
            "image.plan.sha256": image_plan_digest(),
            "kubeconfig.metadata.sha256": self.kubeconfig_metadata_digest,
            "lifecycle.protocol.sha256": lifecycle_protocol_digest(),
            "migration.policy.sha256": self.migration_policy_digest,
            "namespace": self.config.namespace,
            "object.inventory-root": authority.object_inventory_root,
            "protected-inputs.sha256": credential_source_set_digest(self.credential_sources),
            "readonly.principal.sha256": readonly_authority_policy_digest(),
            "route": self.route,
            "runner.config.sha256": self.config.config_sha256,
            "runner.install.sha256": self.runner_install_digest,
            "schema.revision": authority.schema_revision,
            "database.schema.revision": self.database_schema_revision,
            "secret-fingerprints": {
                source.label: source.expected_content_fingerprint
                for source in self.credential_sources
                if source.expected_content_fingerprint is not None
            },
            "service.uid": self.service_uid,
            "staging.mutation-epoch": mutation_epoch,
        }


__all__ = [
    "GB10_CANDIDATE_SOURCE_CONCURRENCY",
    "GB10_PREFLIGHT_FLEET_CONCURRENCY",
    "BackupAdmissionAuthority",
    "PreflightRuntimeSources",
]
