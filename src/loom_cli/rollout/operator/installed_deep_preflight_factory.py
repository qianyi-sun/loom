"""Build the production deep-preflight composition from installed authority."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime
from typing import cast
from urllib.parse import urlsplit

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.admin_smoke_contract import AdminSmokeAuthority
from loom_cli.rollout.gb10_readiness import GB10SharedMountReadiness
from loom_cli.rollout.manifest_readiness import RenderManifest
from loom_cli.rollout.operator.backup import SubprocessBackupCommandRunner
from loom_cli.rollout.preflight_artifact_store import (
    LoadedPreflightArtifacts,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_runtime import RehearsalActionFactory, RehearsalIdentityFactory
from loom_cli.rollout.preflight_runtime_sources import BackupAdmissionAuthority
from loom_cli.rollout.readonly_authority_source import probe_readonly_object_store_health
from loom_cli.rollout.rehearsal_action_source import RehearsalActionSource
from loom_cli.rollout.rehearsal_command_runner import InstalledRehearsalStepRunner
from loom_cli.rollout.rehearsal_journal_backend import JournaledRehearsalBackend
from loom_cli.rollout.rehearsal_readiness import RehearsalAction
from loom_cli.rollout.steps.s13_smoke import (
    DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL,
    DEFAULT_CURRENT_GB10_SMOKE_TASK_ID,
    DEFAULT_SMOKE_AGENT,
)

from .checkpoint_inventory_provider import KubernetesLifecycleInventoryProvider
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
from .readonly_database_client import InstalledReadonlyDatabaseEvidenceSource
from .readonly_preflight_authority import JsonRunner, ReadonlyPreflightAuthority
from .store import RequestStore

_STAGING_SMOKE_ADMIN_ACTOR = "codex-v1-release-gate"


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
    command_runner = SubprocessBackupCommandRunner()
    inventory_source = KubernetesLifecycleInventoryProvider(
        config,
        runner=command_runner,
        environment=child_environment,
    )
    database_evidence = InstalledReadonlyDatabaseEvidenceSource(
        service_uid=service_uid,
    )
    readonly = ReadonlyPreflightAuthority(
        config,
        service_uid=service_uid,
        kubernetes_run=cast(JsonRunner, commands.readonly_json),
        database_evidence=database_evidence,
        object_store_probe=lambda: probe_readonly_object_store_health(
            cast(JsonRunner, commands.readonly_json),
            kubeconfig=readonly.kubeconfig_path,
            namespace=config.namespace,
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

    def manifest_factory(candidate: CandidateBinding) -> RenderManifest:
        def render() -> str:
            cluster = load_cluster_config(config.cluster_config_path)
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
    smoke_authority = AdminSmokeAuthority(
        represented_username=config.smoke_on_behalf_username,
        team_id=config.smoke_on_behalf_team_id,
        admin_actor=_STAGING_SMOKE_ADMIN_ACTOR,
        task_id=DEFAULT_CURRENT_GB10_SMOKE_TASK_ID,
        required_worker_pool=DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL,
        agent=DEFAULT_SMOKE_AGENT,
    )
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
