"""Build the production deep-preflight composition from installed authority."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.gb10_readiness import GB10SharedMountReadiness
from loom_cli.rollout.gb10_rehearsal import GB10RehearsalAuthority
from loom_cli.rollout.image_readiness import ROLLOUT_IMAGES
from loom_cli.rollout.manifest_readiness import RenderManifest
from loom_cli.rollout.preflight_artifact_store import (
    LoadedPreflightArtifacts,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_runtime import RehearsalActionFactory, RehearsalIdentityFactory
from loom_cli.rollout.preflight_runtime_sources import BackupAdmissionAuthority
from loom_cli.rollout.rehearsal_action_source import RehearsalActionSource
from loom_cli.rollout.rehearsal_command_runner import InstalledRehearsalStepRunner
from loom_cli.rollout.rehearsal_journal_backend import JournaledRehearsalBackend
from loom_cli.rollout.rehearsal_readiness import RehearsalAction

from .checkpoint_inventory_provider import ReadonlyLifecycleInventoryProvider
from .checkpoint_lease import CriticalCheckpointEvidence
from .config import OperatorConfig
from .deep_preflight_authority import RuntimePurpose
from .installed_backup_authority import build_installed_backup_authority
from .installed_deep_preflight import InstalledDeepPreflightComposition
from .installed_preflight_commands import InstalledPreflightCommands
from .installed_preflight_inputs import InstalledPreflightInputs
from .model import CandidateBinding
from .policy import sanitized_child_environment
from .preflight import probe_gb10_shared_mount_readonly
from .readonly_capacity_client import (
    InstalledReadonlyCapacitySource,
    probe_installed_readonly_object_store_health,
)
from .readonly_database_client import InstalledReadonlyDatabaseEvidenceSource
from .readonly_preflight_authority import JsonRunner, ReadonlyPreflightAuthority
from .staging_smoke_authority import staging_smoke_authority
from .store import RequestStore


def build_installed_deep_preflight_composition(
    config: OperatorConfig,
    *,
    service_uid: int,
    service_gid: int,
    store: RequestStore,
    now: Callable[[], datetime],
) -> InstalledDeepPreflightComposition:
    """Return one fail-closed production graph shared by broker and worker."""
    if service_uid < 0 or service_gid < 0:
        raise ValueError("installed preflight service identity is invalid")
    child_environment = sanitized_child_environment(config, service_uid=service_uid)
    commands = InstalledPreflightCommands(config, child_environment)
    inputs = InstalledPreflightInputs.load(config, service_uid=service_uid)
    database_evidence = InstalledReadonlyDatabaseEvidenceSource(
        service_uid=service_uid,
    )
    cluster = load_cluster_config(config.cluster_config_path)
    minio_filesystem_path = Path(cluster.persistent_storage_host_path_root) / "minio"
    if minio_filesystem_path != Path("/data/loom-staging/minio"):
        raise ValueError("installed staging MinIO filesystem authority drifted")
    capacity_source = InstalledReadonlyCapacitySource(
        service_uid=service_uid,
        filesystem_paths=(minio_filesystem_path,),
        buckets=tuple(sorted((cluster.artifacts_bucket, cluster.trajectories_bucket))),
    )
    inventory_source = ReadonlyLifecycleInventoryProvider(
        config,
        evidence_source=database_evidence,
    )
    readonly = ReadonlyPreflightAuthority(
        config,
        service_uid=service_uid,
        kubernetes_run=cast(JsonRunner, commands.readonly_json),
        database_evidence=database_evidence,
        capacity_source=capacity_source,
        object_store_probe=lambda: probe_installed_readonly_object_store_health(
            service_uid=service_uid,
        ),
    )
    artifact_store = PreflightArtifactStore(config.state_root, service_uid=service_uid)
    attestation_store = PreflightAttestationStore(config.state_root)

    def backup_authority(mutation_epoch: int) -> BackupAdmissionAuthority:
        return build_installed_backup_authority(
            store,
            inventory_source,
            mutation_epoch=mutation_epoch,
            now=now(),
        )

    manifest_image_names = frozenset(
        name
        for name, _path in ROLLOUT_IMAGES
        if name != "loom-worker" or cluster.k8s_worker.enabled
    )

    def manifest_factory(candidate: CandidateBinding) -> RenderManifest:
        def render() -> str:
            return render_manifests(replace(cluster, image_tag=candidate.image_tag))

        return render

    def shared_mount() -> GB10SharedMountReadiness:
        return probe_gb10_shared_mount_readonly(
            commands.simple,
            ssh_config=inputs.gb10_ssh_config,
            identity=inputs.gb10_identity,
            hosts=tuple(target.ssh_target for target in inputs.gb10_targets),
            binding=inputs.gb10_mount_binding,
        )

    route = readonly.route
    parsed_route = urlsplit(route)
    route_origin = f"{parsed_route.scheme}://{parsed_route.netloc}"
    if not parsed_route.path or route_origin + parsed_route.path != route:
        raise ValueError("installed staging route is not canonical")
    smoke_authority = staging_smoke_authority(config)
    rehearsal_root = config.state_root / "rehearsals"
    rehearsal_runner = InstalledRehearsalStepRunner(
        state_root=rehearsal_root,
        service_uid=service_uid,
        run=commands.rehearsal_helper,
    )
    rehearsal_backend = JournaledRehearsalBackend(
        state_root=rehearsal_root,
        service_uid=service_uid,
        run_step=rehearsal_runner,
    )

    def rehearsal_factory(
        candidate: CandidateBinding,
        mutation_epoch: int,
        purpose: RuntimePurpose,
        loaded: LoadedPreflightArtifacts | None,
    ) -> tuple[RehearsalActionFactory, RehearsalIdentityFactory]:
        if mutation_epoch < 0:
            raise ValueError("rehearsal mutation epoch is invalid")
        if purpose is RuntimePurpose.DETACHED_REHEARSAL:
            if loaded is None:
                raise ValueError("detached rehearsal artifacts are unavailable")
            source = RehearsalActionSource(
                image_artifacts=lambda: loaded.images,
                manifest_artifacts=lambda: loaded.manifests,
                migration_artifacts=lambda: loaded.migration,
                production_defaults_artifacts=lambda: loaded.production_defaults,
                artifact_store=artifact_store,
                migration_plan_sha256=loaded.publication.migration_plan_sha256,
                migration_target_revision=loaded.publication.migration_target_revision,
                browser_report_schema_sha256=loaded.publication.browser_report_schema_sha256,
                cluster_name=config.cluster_name,
                route_origin=route_origin,
                smoke_authority=smoke_authority,
                gb10_authority=GB10RehearsalAuthority(
                    hosts=tuple(target.ssh_target for target in inputs.gb10_targets),
                    ssh_config=inputs.gb10_ssh_config,
                    identity=inputs.gb10_identity,
                    ssh_config_sha256=inputs.gb10_ssh_config_sha256,
                    identity_metadata_fingerprint=inputs.gb10_identity_metadata_fingerprint,
                ),
                backend=rehearsal_backend,
            )
            return source.actions, source.identity

        def unavailable_actions(
            _candidate: CandidateBinding,
            _checkpoint: CriticalCheckpointEvidence,
            _isolation_id: str,
        ) -> Mapping[str, RehearsalAction]:
            raise ValueError("rehearsal actions require an immutable checkpoint")

        def unavailable_identity(
            _candidate: CandidateBinding,
            _checkpoint: CriticalCheckpointEvidence,
        ) -> tuple[str, str]:
            raise ValueError("rehearsal identity requires an immutable checkpoint")

        return unavailable_actions, unavailable_identity

    return InstalledDeepPreflightComposition(
        config=config,
        service_uid=service_uid,
        service_gid=service_gid,
        inputs=inputs,
        artifact_store=artifact_store,
        attestation_store=attestation_store,
        git_run=commands.git,
        executable_lookup=commands.executable,
        docker_runtime_run=commands.simple,
        kubernetes_run=commands.simple,
        readonly_authority_source=readonly.capabilities,
        capacity_source=readonly.capacity,
        backup_authority_factory=backup_authority,
        systemd_run=commands.simple,
        gb10_run=commands.simple,
        gb10_mount_source=shared_mount,
        systemd_analyze_run=commands.simple,
        image_run=commands.image,
        render_manifest_factory=manifest_factory,
        manifest_image_names=manifest_image_names,
        server_dry_run=commands.manifest_server_dry_run,
        browser_run=commands.simple,
        baseline_probe_factory=readonly.baseline_probes,
        route=route,
        rehearsal_factory=rehearsal_factory,
        final_gate_run=commands.final_gate_helper,
        read_mutation_epoch=readonly.mutation_epoch,
        now=now,
    )


__all__ = ["build_installed_deep_preflight_composition"]
