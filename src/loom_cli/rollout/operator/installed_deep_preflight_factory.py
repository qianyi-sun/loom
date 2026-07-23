"""Build the production deep-preflight composition from installed authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config
from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    ExternalSupervisorCanonicalPointer,
    ExternalSupervisorPoolIdentity,
    load_predecessor_manifest,
)
from loom_cli.rollout.external_supervisor_readiness import build_external_supervisor_artifact
from loom_cli.rollout.gb10_readiness import GB10SharedMountReadiness
from loom_cli.rollout.gb10_rehearsal import GB10RehearsalAuthority
from loom_cli.rollout.image_readiness import ROLLOUT_IMAGES
from loom_cli.rollout.manifest_readiness import RenderManifest
from loom_cli.rollout.preflight_artifact_store import (
    LoadedPreflightArtifacts,
    PreflightArtifactStore,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import CheckContext
from loom_cli.rollout.preflight_registered_checks import (
    ExternalSupervisorPredecessorSnapshot,
    ExternalSupervisorPredecessorSource,
)
from loom_cli.rollout.preflight_runtime import RehearsalActionFactory, RehearsalIdentityFactory
from loom_cli.rollout.preflight_runtime_sources import (
    GB10_PREFLIGHT_FLEET_CONCURRENCY,
    BackupAdmissionAuthority,
)
from loom_cli.rollout.readonly_database_authority import DatabaseQuery
from loom_cli.rollout.rehearsal_action_source import RehearsalActionSource
from loom_cli.rollout.rehearsal_command_runner import InstalledRehearsalStepRunner
from loom_cli.rollout.rehearsal_journal_backend import JournaledRehearsalBackend
from loom_cli.rollout.rehearsal_readiness import RehearsalAction

from .candidate import GitRunner
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
from .protected_external_supervisor_transport import (
    PROTECTED_USER_UNIT_ANCHOR,
    PROTECTED_USER_UNIT_DIR,
    AtomicUserUnitStore,
    ExternalSupervisorLiveObservation,
    FixedUserSystemdControl,
)
from .readonly_capacity_client import (
    InstalledReadonlyCapacitySource,
    probe_installed_readonly_object_store_health,
    verify_installed_immutable_objects,
)
from .readonly_database_client import (
    InstalledReadonlyDatabaseEvidenceSource,
    InstalledReadonlyMutationEpochSource,
    open_readonly_database_query,
    probe_installed_readonly_database_baseline,
)
from .readonly_preflight_authority import JsonRunner, ReadonlyPreflightAuthority
from .staging_smoke_authority import staging_smoke_authority
from .store import RequestStore


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


_EXTERNAL_SUPERVISOR_POOL_IDENTITY_SQL = """
SELECT 'workers' AS table_name,
       count(*) FILTER (WHERE pool_name = 'gb10-arm64') AS legacy_rows,
       count(*) FILTER (WHERE pool_name = 'gb10') AS target_rows
FROM workers
UNION ALL
SELECT 'gb10_worker_pool_desired_states',
       count(*) FILTER (WHERE pool_name = 'gb10-arm64'),
       count(*) FILTER (WHERE pool_name = 'gb10')
FROM gb10_worker_pool_desired_states
UNION ALL
SELECT 'worker_pool_autoscaler_policies',
       count(*) FILTER (WHERE pool_name = 'gb10-arm64'),
       count(*) FILTER (WHERE pool_name = 'gb10')
FROM worker_pool_autoscaler_policies
UNION ALL
SELECT 'gb10_worker_node_statuses',
       count(*) FILTER (WHERE pool_name = 'gb10-arm64'),
       count(*) FILTER (WHERE pool_name = 'gb10')
FROM gb10_worker_node_statuses
UNION ALL
SELECT 'slurm_worker_jobs',
       count(*) FILTER (WHERE pool_name = 'gb10-arm64'),
       count(*) FILTER (WHERE pool_name = 'gb10')
FROM slurm_worker_jobs
""".strip()


def _probe_external_supervisor_pool_identity(
    query: DatabaseQuery,
) -> ExternalSupervisorPoolIdentity:
    revision_rows = query("SELECT version_num AS schema_revision FROM alembic_version")
    if len(revision_rows) != 1 or set(revision_rows[0]) != {"schema_revision"}:
        raise ValueError("external supervisor database schema identity is incomplete")
    schema_revision = revision_rows[0]["schema_revision"]
    if not isinstance(schema_revision, str):
        raise ValueError("external supervisor database schema identity is invalid")

    rows = query(_EXTERNAL_SUPERVISOR_POOL_IDENTITY_SQL)
    legacy_rows: dict[str, int] = {}
    target_rows: dict[str, int] = {}
    for row in rows:
        if set(row) != {"table_name", "legacy_rows", "target_rows"}:
            raise ValueError("external supervisor database pool identity is incomplete")
        table_name = row["table_name"]
        legacy_count = row["legacy_rows"]
        target_count = row["target_rows"]
        if (
            not isinstance(table_name, str)
            or table_name in legacy_rows
            or type(legacy_count) is not int
            or type(target_count) is not int
        ):
            raise ValueError("external supervisor database pool identity is invalid")
        legacy_rows[table_name] = legacy_count
        target_rows[table_name] = target_count
    return ExternalSupervisorPoolIdentity.build(
        schema_revision=schema_revision,
        legacy_rows=legacy_rows,
        target_rows=target_rows,
    )


def _probe_installed_external_supervisor_pool_identity(
    *,
    service_uid: int,
    query_context: Callable[..., AbstractContextManager[DatabaseQuery]] = (
        open_readonly_database_query
    ),
) -> ExternalSupervisorPoolIdentity:
    with query_context(service_uid=service_uid) as query:
        return _probe_external_supervisor_pool_identity(query)


def _external_supervisor_predecessor_source(
    *,
    candidate_root: Path,
    git_run: GitRunner,
    service_uid: int,
    pool_identity_source: Callable[[], ExternalSupervisorPoolIdentity],
) -> ExternalSupervisorPredecessorSource:
    """Return a no-write adapter over the fixed protected user-systemd store."""

    store = AtomicUserUnitStore(
        unit_dir=PROTECTED_USER_UNIT_DIR,
        service_uid=service_uid,
        creation_anchor=PROTECTED_USER_UNIT_ANCHOR,
    )
    control = FixedUserSystemdControl(service_uid=service_uid)

    def git_output(*arguments: str) -> str:
        result = git_run(["git", "-C", str(candidate_root), *arguments])
        if result.returncode != 0:
            raise ValueError("external supervisor predecessor Git provenance is unavailable")
        return result.stdout

    def source(context: CheckContext) -> ExternalSupervisorPredecessorSnapshot:
        legacy = load_predecessor_manifest()
        candidate_sha = context.bindings.get("candidate.sha")
        candidate_tree = context.bindings.get("candidate.tree")
        if not isinstance(candidate_sha, str) or not isinstance(candidate_tree, str):
            raise ValueError("external supervisor predecessor candidate identity is missing")
        ancestor = git_run(
            [
                "git",
                "-C",
                str(candidate_root),
                "merge-base",
                "--is-ancestor",
                legacy.source_commit,
                candidate_sha,
            ]
        )
        source_tree = git_output("rev-parse", f"{legacy.source_commit}^{{tree}}").strip()
        observed_candidate_tree = git_output("rev-parse", f"{candidate_sha}^{{tree}}").strip()
        source_file_sha256 = {
            path: hashlib.sha256(
                git_output("cat-file", "blob", f"{legacy.source_commit}:{path}").encode()
            ).hexdigest()
            for path in legacy.source_file_sha256
        }
        if (
            ancestor.returncode != 0
            or ancestor.stdout
            or legacy.source_tree != source_tree
            or observed_candidate_tree != candidate_tree
            or source_file_sha256 != dict(legacy.source_file_sha256)
        ):
            raise ValueError("external supervisor predecessor Git provenance drifted")
        canonical = store.read_canonical()
        unit_names = set(store.list_units()) | set(
            legacy.unit_sha256 if canonical is None else canonical.unit_sha256
        )
        units = {name: store.read_unit(name) for name in sorted(unit_names)}
        timers = {
            name: control.timer_status(name)
            for name in sorted(unit_names)
            if name.endswith(".timer")
        }
        services = {
            name: control.service_status(name)
            for name in sorted(unit_names)
            if name.endswith(".service")
        }
        observation = ExternalSupervisorLiveObservation(
            unit_payloads=units,
            timer_statuses=timers,
            service_statuses=services,
            canonical_identity=canonical,
            compensation_blockers=store.compensation_blockers(),
        )
        authority = observation.predecessor_authority
        assert authority is not None
        pool_identity = pool_identity_source()
        bound_schema_revision = context.bindings.get("schema.revision")
        if (
            not isinstance(bound_schema_revision, str)
            or bound_schema_revision != pool_identity.schema_revision
        ):
            raise ValueError("external supervisor database schema binding drifted")
        pool_identity.require_predecessor_kind(authority.kind)
        live_unit_sha256 = {
            name: hashlib.sha256(payload).hexdigest()
            for name, payload in units.items()
            if payload is not None
        }
        runtime_ready = live_unit_sha256 == dict(authority.unit_sha256)
        for name, payload in units.items():
            expected = authority.unit_sha256.get(name)
            if name.endswith(".timer"):
                timer_status = timers[name]
                if expected is None:
                    runtime_ready = runtime_ready and (
                        payload is None
                        and timer_status.load_state == "not-found"
                        and timer_status.unit_file_state in {"", "disabled", "not-found"}
                        and timer_status.active_state == "inactive"
                        and timer_status.fragment_path == ""
                        and timer_status.need_daemon_reload == "no"
                    )
                else:
                    runtime_ready = runtime_ready and (
                        timer_status.load_state == "loaded"
                        and timer_status.unit_file_state == "enabled"
                        and timer_status.active_state == "active"
                        and timer_status.fragment_path == str(PROTECTED_USER_UNIT_DIR / name)
                        and timer_status.need_daemon_reload == "no"
                    )
            else:
                service_status = services[name]
                if expected is None:
                    runtime_ready = runtime_ready and (
                        payload is None
                        and service_status.load_state == "not-found"
                        and service_status.result == ""
                        and service_status.exec_main_status is None
                        and service_status.fragment_path == ""
                        and service_status.need_daemon_reload == "no"
                    )
                else:
                    runtime_ready = runtime_ready and (
                        service_status.load_state == "loaded"
                        and service_status.result == "success"
                        and service_status.exec_main_status == 0
                        and service_status.fragment_path == str(PROTECTED_USER_UNIT_DIR / name)
                        and service_status.need_daemon_reload == "no"
                    )
        if canonical is not None:
            runtime_digest = _hash_json(
                {
                    "unit_dir": str(PROTECTED_USER_UNIT_DIR),
                    "services": {
                        name: status.to_dict() for name, status in sorted(services.items())
                    },
                    "timers": {name: status.to_dict() for name, status in sorted(timers.items())},
                    "unit_sha256": dict(authority.unit_sha256),
                }
            )
            runtime_ready = runtime_ready and (canonical.runtime_evidence_digest == runtime_digest)
            pointer_digest = ExternalSupervisorCanonicalPointer.build(canonical).pointer_digest
        else:
            pointer_digest = ABSENT_PREDECESSOR_DIGEST
        return ExternalSupervisorPredecessorSnapshot(
            kind=authority.kind,
            authority_digest=authority.authority_digest,
            pointer_digest=pointer_digest,
            unit_sha256=authority.unit_sha256,
            live_evidence_digest=observation.evidence_digest,
            pending_transition_digest=observation.transition_digest,
            transition_clear=not observation.compensation_blockers,
            runtime_ready=runtime_ready,
            pool_identity_digest=pool_identity.evidence_digest,
        )

    return source


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
        probe=probe_installed_readonly_database_baseline,
    )
    checkpoint_database_evidence = InstalledReadonlyDatabaseEvidenceSource(
        service_uid=service_uid,
    )
    mutation_epoch_evidence = InstalledReadonlyMutationEpochSource(
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
        evidence_source=checkpoint_database_evidence,
        object_verifier=lambda objects: verify_installed_immutable_objects(
            objects,
            service_uid=service_uid,
        ),
    )
    readonly = ReadonlyPreflightAuthority(
        config,
        service_uid=service_uid,
        kubernetes_run=cast(JsonRunner, commands.readonly_json),
        database_evidence=database_evidence,
        mutation_epoch_evidence=mutation_epoch_evidence,
        capacity_source=capacity_source,
        object_store_probe=lambda: probe_installed_readonly_object_store_health(
            service_uid=service_uid,
        ),
    )
    artifact_store = PreflightArtifactStore(config.state_root, service_uid=service_uid)
    attestation_store = PreflightAttestationStore(config.state_root)

    def backup_authority(mutation_epoch: int) -> BackupAdmissionAuthority:
        try:
            return build_installed_backup_authority(
                store,
                inventory_source,
                mutation_epoch=mutation_epoch,
                now=now(),
            )
        except (OSError, RuntimeError, ValueError) as exc:
            epoch = mutation_epoch_evidence()
            if epoch.mutation_epoch != mutation_epoch:
                raise ValueError("backup authority mutation epoch drifted") from exc
            return BackupAdmissionAuthority.unavailable(
                schema_revision=epoch.schema_revision,
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
            max_concurrency=GB10_PREFLIGHT_FLEET_CONCURRENCY,
        )

    route = readonly.route
    parsed_route = urlsplit(route)
    canonical_origin = f"{parsed_route.scheme}://{parsed_route.netloc}"
    # `readonly.route` is derived and fully validated from the exact trusted
    # staging.cluster.toml by derive_staging_route (namespace/runtime_environment
    # match, DNS host, route == api_route, _ROUTE_RE). Pin only that the route is
    # well-formed with no extra URL components. The environment-correct path
    # (e.g. "/staging" after #897 migrated it from "/dev") comes from the trusted
    # config, so do NOT hardcode "/dev" here. See #927 for the remaining
    # attestation-chain route checks that still hardcode "/dev".
    if not parsed_route.path or canonical_origin + parsed_route.path != route:
        raise ValueError("installed staging route is not canonical")
    route_origin = route
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
                external_supervisor_artifacts=lambda: build_external_supervisor_artifact(
                    config.runner_repo,
                    candidate_sha=candidate.resolved_sha,
                    candidate_tree=candidate.resolved_tree or "",
                    image_tag=candidate.image_tag,
                    environment=config.environment,
                ),
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
        external_supervisor_predecessor_source=(
            _external_supervisor_predecessor_source(
                candidate_root=config.runner_repo,
                git_run=commands.git,
                service_uid=service_uid,
                pool_identity_source=lambda: _probe_installed_external_supervisor_pool_identity(
                    service_uid=service_uid
                ),
            )
        ),
        systemd_run=commands.systemd_preflight,
        gb10_run=commands.simple,
        gb10_mount_source=shared_mount,
        systemd_analyze_run=commands.simple,
        image_run=commands.image,
        render_manifest_factory=manifest_factory,
        manifest_image_names=manifest_image_names,
        server_schema_dry_run=commands.manifest_schema_dry_run,
        server_dry_run=commands.manifest_server_dry_run,
        browser_run=commands.simple,
        baseline_probe_factory=readonly.baseline_probes,
        route=route,
        baseline_probe_route=readonly.baseline_probe_route,
        rehearsal_factory=rehearsal_factory,
        final_gate_run=commands.final_gate_helper,
        read_mutation_epoch=readonly.mutation_epoch,
        now=now,
    )


__all__ = ["build_installed_deep_preflight_composition"]
