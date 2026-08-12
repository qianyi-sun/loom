from __future__ import annotations

import hashlib
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom.data_lifecycle import StagingCapacity
from loom_cli.rollout import preflight_contract
from loom_cli.rollout.external_supervisor_predecessor import (
    GB10_CANONICAL_UNIT_DIR,
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorPoolIdentity,
    load_predecessor_manifest,
)
from loom_cli.rollout.external_supervisor_readiness import build_external_supervisor_artifact
from loom_cli.rollout.gb10_readiness import (
    ACTIVE_GB10_HOSTS,
    GB10ProbeTarget,
    GB10SharedMountReadiness,
)
from loom_cli.rollout.operator import installed_deep_preflight_factory
from loom_cli.rollout.operator import protected_external_supervisor_transport as transport_module
from loom_cli.rollout.operator.deep_preflight_authority import RuntimePurpose
from loom_cli.rollout.operator.installed_deep_preflight import InstalledDeepPreflightComposition
from loom_cli.rollout.operator.installed_preflight_inputs import InstalledPreflightInputs
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    PROTECTED_USER_UNIT_DIR,
    ServiceRuntimeStatus,
    TimerRuntimeStatus,
)
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import EXTERNAL_SUPERVISOR_ABSENT_DIGEST, CheckContext
from loom_cli.rollout.preflight_registered_checks import (
    CredentialProbeSource,
    ExternalSupervisorPredecessorSnapshot,
    build_external_supervisor_predecessor_check,
)
from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config
from tests.loom_cli.rollout.operator.test_protected_external_supervisor_component import (
    _observation,
)


def _candidate() -> CandidateBinding:
    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )


def test_multinode_capacity_inputs_select_live_minio_admin_authority() -> None:
    cluster = SimpleNamespace(
        persistent_storage_host_path_root="/data/loom-staging",
        topology=SimpleNamespace(multi_node=True, minio_replicas=4),
    )

    assert installed_deep_preflight_factory._readonly_capacity_probe_inputs(cluster) == (
        "minio-admin",
        (),
        4,
    )


def test_single_node_capacity_inputs_keep_exact_host_path_authority() -> None:
    cluster = SimpleNamespace(
        persistent_storage_host_path_root="/data/loom-staging",
        topology=SimpleNamespace(multi_node=False, minio_replicas=1),
    )

    assert installed_deep_preflight_factory._readonly_capacity_probe_inputs(cluster) == (
        "filesystem",
        (Path("/data/loom-staging/minio"),),
        None,
    )


def test_installed_rehearsal_source_rebuilds_aggregate_supervisor_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    candidate = _candidate()
    inputs = SimpleNamespace(
        gb10_targets=tuple(SimpleNamespace(ssh_target=host) for host in ACTIVE_GB10_HOSTS),
        gb10_ssh_config=tmp_path / "ssh-config",
        gb10_identity=tmp_path / "identity",
        gb10_ssh_config_sha256="1" * 64,
        gb10_identity_metadata_fingerprint="2" * 64,
    )
    cluster = SimpleNamespace(
        artifacts_bucket="artifacts",
        container_registry="registry.invalid",
        container_registry_push="registry-push.invalid",
        k8s_worker=SimpleNamespace(enabled=True),
        minio_replicas=1,
        persistent_storage_host_path_root="/data/loom-staging",
        topology=SimpleNamespace(multi_node=False, minio_replicas=1),
        trajectories_bucket="trajectories",
    )
    readonly = SimpleNamespace(
        baseline_probe_route="https://staging.example.invalid/staging",
        baseline_probes=lambda _epoch: {},
        capabilities=lambda: SimpleNamespace(),
        capacity=lambda: StagingCapacity(0, 0, 100, 100),
        route="https://staging.example.invalid/staging",
    )
    commands = SimpleNamespace(
        candidate_source=lambda *_args: SimpleNamespace(),
        executable=lambda *_args: "/fixed/tool",
        final_gate_helper=lambda *_args: SimpleNamespace(),
        gb10_fleet=lambda *_args: SimpleNamespace(),
        gb10_supervisor_controller=lambda *_args: SimpleNamespace(),
        git=lambda *_args: SimpleNamespace(),
        image=lambda *_args: SimpleNamespace(),
        manifest_schema_dry_run=lambda *_args: SimpleNamespace(),
        manifest_server_dry_run=lambda *_args: SimpleNamespace(),
        readonly_json=lambda *_args: SimpleNamespace(),
        rehearsal_helper=lambda *_args: SimpleNamespace(),
        simple=lambda *_args: SimpleNamespace(),
        systemd_preflight=lambda *_args: SimpleNamespace(),
    )
    loaded = SimpleNamespace(
        images=SimpleNamespace(),
        manifests=SimpleNamespace(),
        migration=SimpleNamespace(),
        production_defaults=SimpleNamespace(),
        publication=SimpleNamespace(
            browser_report_schema_sha256="3" * 64,
            migration_plan_sha256="4" * 64,
            migration_target_revision="0074",
        ),
    )
    captured: dict[str, object] = {}

    class Source:
        def __init__(self, **kwargs: object) -> None:
            captured["source"] = kwargs

        actions = "actions"
        identity = "identity"

    def composition(**kwargs: object) -> SimpleNamespace:
        captured["composition"] = kwargs
        return SimpleNamespace(**kwargs)

    artifact = SimpleNamespace()
    builds: list[dict[str, object]] = []

    def build(candidate_root: Path, **kwargs: object) -> SimpleNamespace:
        builds.append({"candidate_root": candidate_root, **kwargs})
        return artifact

    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "sanitized_child_environment",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "InstalledPreflightCommands",
        lambda *_args, **_kwargs: commands,
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory.InstalledPreflightInputs,
        "load",
        lambda *_args, **_kwargs: inputs,
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "InstalledReadonlyDatabaseEvidenceSource",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "InstalledReadonlyMutationEpochSource",
        lambda **_kwargs: SimpleNamespace(
            refresh=lambda: SimpleNamespace(mutation_epoch=8),
            __call__=lambda: SimpleNamespace(schema_revision="0074"),
        ),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory, "load_cluster_config", lambda _path: cluster
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "InstalledReadonlyCapacitySource",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "ReadonlyLifecycleInventoryProvider",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "ReadonlyPreflightAuthority",
        lambda *_args, **_kwargs: readonly,
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "PreflightArtifactStore",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "PreflightAttestationStore",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "staging_smoke_authority",
        lambda _config: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "InstalledRehearsalStepRunner",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "JournaledRehearsalBackend",
        lambda **_kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(installed_deep_preflight_factory, "RehearsalActionSource", Source)
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "_external_supervisor_predecessor_source",
        lambda **_kwargs: lambda *_args: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "_gb10_external_supervisor_observation_source",
        lambda **_kwargs: lambda *_args: SimpleNamespace(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory, "InstalledDeepPreflightComposition", composition
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory, "build_external_supervisor_artifact", build
    )

    installed_deep_preflight_factory.build_installed_deep_preflight_composition(
        config,
        service_uid=995,
        service_gid=2007,
        store=SimpleNamespace(),  # type: ignore[arg-type]
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )
    rehearsal_factory = captured["composition"]["rehearsal_factory"]  # type: ignore[index]
    assert callable(rehearsal_factory)
    assert rehearsal_factory(candidate, 8, RuntimePurpose.DETACHED_REHEARSAL, loaded) == (  # type: ignore[operator]
        "actions",
        "identity",
    )
    source = captured["source"]
    assert isinstance(source, dict)
    external_supervisor_artifacts = source["external_supervisor_artifacts"]
    assert callable(external_supervisor_artifacts)
    assert external_supervisor_artifacts() is artifact
    assert builds == [
        {
            "candidate_root": config.runner_repo,
            "candidate_sha": candidate.resolved_sha,
            "candidate_tree": candidate.resolved_tree,
            "environment": config.environment,
            "image_tag": candidate.image_tag,
        }
    ]


class _Artifacts:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def load_exact(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            publication=SimpleNamespace(
                candidate_sha=kwargs["candidate_sha"],
                candidate_tree=kwargs["candidate_tree"],
                mutation_epoch=kwargs["mutation_epoch"],
            )
        )


def test_composition_uses_one_source_graph_and_loads_outputs_only_for_detached(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    candidate = _candidate()
    artifacts = _Artifacts()
    inputs = InstalledPreflightInputs(
        runner_install_digest="1" * 64,
        credential_sources=(CredentialProbeSource(label="admin", path=tmp_path / "admin"),),
        kubeconfig_metadata_digest="2" * 64,
        gb10_targets=(GB10ProbeTarget("trt-gb10-1", "loom-gb10-node-agent.service"),),
        gb10_ssh_config=tmp_path / "ssh-config",
        gb10_identity=tmp_path / "identity",
        gb10_ssh_config_sha256="3" * 64,
        gb10_identity_metadata_fingerprint="4" * 64,
        gb10_mount_binding={"service_uid": 501},
        gb10_mount_binding_digest="5" * 64,
        migration_policy_path=tmp_path / "migration-policy.json",
        migration_policy_digest="6" * 64,
    )

    def command(*_args, **_kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    composition = InstalledDeepPreflightComposition(
        config=config,
        service_uid=501,
        service_gid=20,
        inputs=inputs,
        artifact_store=artifacts,  # type: ignore[arg-type]
        attestation_store=PreflightAttestationStore(tmp_path / "state"),
        git_run=command,
        executable_lookup=lambda _name: "/fixed/tool",
        docker_runtime_run=command,
        kubernetes_run=command,
        readonly_authority_source=lambda: ReadonlyAuthorityEvidence(
            principal="loom-rollout-readonly",
            environment="staging",
            namespace="loom-staging",
            kubernetes_verbs=("get", "list", "watch"),
            kubernetes_resources=("deployments", "pods", "services"),
            http_methods=("GET", "HEAD"),
            capability_source_digest="7" * 64,
        ),
        capacity_source=lambda: StagingCapacity(0, 0, 100, 100),
        backup_authority_factory=lambda _epoch: None,  # type: ignore[arg-type,return-value]
        external_supervisor_predecessor_source=lambda _context: (
            ExternalSupervisorPredecessorSnapshot(
                kind="legacy-manifest",
                authority_digest="a" * 64,
                pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
                unit_sha256={
                    "loom-autoscaler-gb10-staging.service": "b" * 64,
                    "loom-autoscaler-gb10-staging.timer": "c" * 64,
                },
                live_evidence_digest="d" * 64,
                pending_transition_digest="e" * 64,
                transition_clear=False,
                runtime_ready=False,
                pool_identity_digest="f" * 64,
            )
        ),
        systemd_run=command,
        gb10_run=command,
        gb10_mount_source=lambda: GB10SharedMountReadiness(
            host_digests={"trt-gb10-1": "8" * 64},
            failed_hosts=(),
        ),
        systemd_analyze_run=command,
        image_run=command,
        render_manifest_factory=lambda _candidate: lambda: "",
        manifest_image_names=frozenset({"loom-service"}),
        server_schema_dry_run=lambda _rendered: command(),
        server_dry_run=lambda _rendered: command(),
        browser_run=command,
        baseline_probe_factory=lambda _epoch: {},
        route="https://staging.example.invalid/dev",
        baseline_probe_route="https://staging.example.invalid/dev",
        rehearsal_factory=lambda *_args: (
            lambda *_inner: {},
            lambda *_inner: ("rehearsal-exact", "9" * 64),
        ),
        final_gate_run=lambda *_args: command(),
        read_mutation_epoch=lambda: 9,
        read_database_schema_revision=lambda: "0074",
        now=lambda: datetime(2026, 7, 19, 12, tzinfo=UTC),
    )

    admission = composition.sources(candidate, 9, RuntimePurpose.ADMISSION)
    detached = composition.sources(candidate, 9, RuntimePurpose.DETACHED_REHEARSAL)

    assert admission.loaded_artifacts is None
    assert detached.loaded_artifacts is not None
    assert artifacts.calls == [
        {
            "candidate_sha": candidate.resolved_sha,
            "candidate_tree": candidate.resolved_tree,
            "mutation_epoch": 9,
            "image_tag": candidate.image_tag,
            "namespace": "loom-staging",
            "image_run": command,
            "container_registry_push": "",
        }
    ]
    assert composition.authority().current_mutation_epoch() == 9
    assert admission.database_schema_revision == "0074"


class _LegacyExternalSupervisorStore:
    def __init__(self) -> None:
        self.manifest = load_predecessor_manifest()

    def list_units(self) -> tuple[str, ...]:
        return tuple(self.manifest.unit_sha256)

    def read_unit(self, name: str) -> bytes:
        return self.manifest.unit_payloads[name].encode()

    def read_canonical(self):
        return None

    def compensation_blockers(self) -> dict[str, str]:
        return {}


class _OldlabLegacyExternalSupervisorStore:
    """Exact live #1197 OLDLAB predecessor with no canonical pointer yet."""

    def __init__(self) -> None:
        self.manifest = load_predecessor_manifest(
            execution_host="TRT-EAI-OLDLAB-1",
        )

    def list_units(self) -> tuple[str, ...]:
        return tuple(self.manifest.unit_sha256)

    def read_unit(self, name: str) -> bytes:
        return self.manifest.unit_payloads[name].encode()

    def read_canonical(self):
        return None

    def compensation_blockers(self) -> dict[str, str]:
        return {}


class _ReadyLegacyExternalSupervisorControl:
    def timer_status(self, name: str) -> TimerRuntimeStatus:
        return TimerRuntimeStatus(
            load_state="loaded",
            unit_file_state="enabled",
            active_state="active",
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name),
            need_daemon_reload="no",
        )

    def service_status(self, name: str) -> ServiceRuntimeStatus:
        return ServiceRuntimeStatus(
            load_state="loaded",
            result="success",
            exec_main_status=0,
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name),
            need_daemon_reload="no",
        )


class _ReadyDisabledExternalSupervisorControl:
    def timer_status(self, name: str) -> TimerRuntimeStatus:
        return TimerRuntimeStatus(
            load_state="loaded",
            unit_file_state="disabled",
            active_state="inactive",
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name),
            need_daemon_reload="no",
        )

    def service_status(self, name: str) -> ServiceRuntimeStatus:
        return ServiceRuntimeStatus(
            load_state="loaded",
            result="success",
            exec_main_status=0,
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name),
            need_daemon_reload="no",
        )


class _ReadyCanonicalExternalSupervisorControl:
    def __init__(self, canonical: ExternalSupervisorCanonicalIdentity) -> None:
        self.canonical = canonical

    def timer_status(self, name: str) -> TimerRuntimeStatus:
        service_name = f"{name.removesuffix('.timer')}.service"
        active = transport_module._identity_pair_desired_active(
            self.canonical,
            service_name,
            name,
        )
        return TimerRuntimeStatus(
            load_state="loaded",
            unit_file_state="enabled" if active else "disabled",
            active_state="active" if active else "inactive",
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name),
            need_daemon_reload="no",
        )

    def service_status(self, name: str) -> ServiceRuntimeStatus:
        return ServiceRuntimeStatus(
            load_state="loaded",
            result="success",
            exec_main_status=0,
            fragment_path=str(PROTECTED_USER_UNIT_DIR / name),
            need_daemon_reload="no",
        )


class _CanonicalExternalSupervisorStore:
    def __init__(self, canonical: ExternalSupervisorCanonicalIdentity) -> None:
        self.canonical = canonical

    def list_units(self) -> tuple[str, ...]:
        return tuple(self.canonical.unit_sha256)

    def read_unit(self, name: str) -> bytes:
        return self.canonical.unit_payloads[name].encode()

    def read_canonical(self) -> ExternalSupervisorCanonicalIdentity:
        return self.canonical

    def compensation_blockers(self) -> dict[str, str]:
        return {}


class _AbsentExternalSupervisorStore:
    """First introduction: the manifest unit names exist but nothing is live."""

    def __init__(self) -> None:
        self.manifest = load_predecessor_manifest()

    def list_units(self) -> tuple[str, ...]:
        return ()

    def read_unit(self, name: str) -> bytes | None:
        return None

    def read_canonical(self):
        return None

    def compensation_blockers(self) -> dict[str, str]:
        return {}


class _AbsentExternalSupervisorControl:
    def timer_status(self, name: str) -> TimerRuntimeStatus:
        return TimerRuntimeStatus(
            load_state="not-found",
            unit_file_state="not-found",
            active_state="inactive",
            fragment_path="",
            need_daemon_reload="no",
        )

    def service_status(self, name: str) -> ServiceRuntimeStatus:
        return ServiceRuntimeStatus(
            load_state="not-found",
            result="",
            exec_main_status=None,
            fragment_path="",
            need_daemon_reload="no",
        )


def _git_run(arguments: list[str]):
    return subprocess.run(arguments, capture_output=True, check=False, text=True)


def _installed_predecessor_context(
    candidate_root: Path,
    *,
    backup_schema_revision: str = "0066",
    database_schema_revision: str = "0066",
) -> CheckContext:
    candidate_sha = _git_run(["git", "-C", str(candidate_root), "rev-parse", "HEAD"])
    candidate_tree = _git_run(["git", "-C", str(candidate_root), "rev-parse", "HEAD^{tree}"])
    assert candidate_sha.returncode == 0
    assert candidate_tree.returncode == 0
    return CheckContext(
        {
            "candidate.sha": candidate_sha.stdout.strip(),
            "candidate.tree": candidate_tree.stdout.strip(),
            "schema.revision": backup_schema_revision,
            "database.schema.revision": database_schema_revision,
        }
    )


_POOL_IDENTITY_TABLES = (
    "gb10_worker_node_statuses",
    "gb10_worker_pool_desired_states",
    "slurm_worker_jobs",
    "worker_pool_autoscaler_policies",
    "workers",
)


def _pool_identity(
    schema_revision: str = "0066",
    *,
    legacy_count: int = 1,
    target_count: int = 0,
) -> ExternalSupervisorPoolIdentity:
    return ExternalSupervisorPoolIdentity.build(
        schema_revision=schema_revision,
        legacy_rows={name: legacy_count for name in _POOL_IDENTITY_TABLES},
        target_rows={name: target_count for name in _POOL_IDENTITY_TABLES},
    )


def test_installed_external_supervisor_predecessor_source_binds_merged_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    store = _LegacyExternalSupervisorStore()
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: store,
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _ReadyLegacyExternalSupervisorControl(),
    )

    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=_pool_identity,
    )
    snapshot = source(_installed_predecessor_context(candidate_root))

    assert snapshot.kind == "legacy-manifest"
    assert snapshot.authority_digest == store.manifest.manifest_digest
    assert dict(snapshot.unit_sha256) == dict(store.manifest.unit_sha256)
    assert snapshot.transition_clear is True
    assert snapshot.runtime_ready is True


def test_installed_predecessor_recognizes_exact_oldlab_activation_on_oldlab_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    store = _OldlabLegacyExternalSupervisorStore()
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: store,
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _ReadyLegacyExternalSupervisorControl(),
    )

    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=lambda: _pool_identity(
            "0077",
            legacy_count=0,
            target_count=1,
        ),
        execution_host="TRT-EAI-OLDLAB-1",
    )
    snapshot = source(
        _installed_predecessor_context(
            candidate_root,
            backup_schema_revision="0077",
            database_schema_revision="0077",
        )
    )

    assert snapshot.kind == "legacy-manifest"
    assert snapshot.authority_digest == store.manifest.manifest_digest
    assert dict(snapshot.unit_sha256) == dict(store.manifest.unit_sha256)
    assert snapshot.transition_clear is True
    assert snapshot.runtime_ready is True


def test_installed_gb10_predecessor_source_uses_remote_typed_observation_and_unit_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    candidate_sha = _git_run(["git", "-C", str(candidate_root), "rev-parse", "HEAD"]).stdout.strip()
    candidate_tree = _git_run(
        ["git", "-C", str(candidate_root), "rev-parse", "HEAD^{tree}"]
    ).stdout.strip()
    artifact = build_external_supervisor_artifact(
        candidate_root,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=f"staging-{candidate_sha[:7]}",
        environment="staging",
        execution_host="gx10-01c7",
    )
    observation = _observation(
        artifact,
        files="legacy",
        runtime="exact",
        unit_dir=Path(GB10_CANONICAL_UNIT_DIR),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: pytest.fail("remote source constructed a local unit store"),
    )

    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=_pool_identity,
        execution_host="gx10-01c7",
        unit_dir=Path(GB10_CANONICAL_UNIT_DIR),
        observation_source=lambda _context: observation,
    )

    snapshot = source(_installed_predecessor_context(candidate_root))

    assert snapshot.kind == "legacy-manifest"
    assert snapshot.runtime_ready is True


def test_installed_gb10_canonical_predecessor_rejects_oldlab_unit_directory() -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    candidate_sha = _git_run(["git", "-C", str(candidate_root), "rev-parse", "HEAD"]).stdout.strip()
    candidate_tree = _git_run(
        ["git", "-C", str(candidate_root), "rev-parse", "HEAD^{tree}"]
    ).stdout.strip()
    artifact = build_external_supervisor_artifact(
        candidate_root,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=f"staging-{candidate_sha[:7]}",
        environment="staging",
        execution_host="gx10-01c7",
    )
    observation = _observation(
        artifact,
        files="exact",
        runtime="exact",
        unit_dir=PROTECTED_USER_UNIT_DIR,
    )
    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=lambda: _pool_identity(
            "0067",
            legacy_count=0,
            target_count=1,
        ),
        execution_host="gx10-01c7",
        unit_dir=Path(GB10_CANONICAL_UNIT_DIR),
        observation_source=lambda _context: observation,
    )

    snapshot = source(
        _installed_predecessor_context(
            candidate_root,
            backup_schema_revision="0067",
            database_schema_revision="0067",
        )
    )

    assert snapshot.kind == "canonical"
    assert snapshot.runtime_ready is False


def test_installed_predecessor_sources_require_exact_two_controller_map() -> None:
    snapshot = ExternalSupervisorPredecessorSnapshot(
        kind="absent",
        authority_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        unit_sha256={},
        live_evidence_digest="a" * 64,
        pending_transition_digest=hashlib.sha256(b"{}").hexdigest(),
        transition_clear=True,
        runtime_ready=True,
        pool_identity_digest="c" * 64,
    )
    combined = installed_deep_preflight_factory._controller_predecessor_sources(
        {
            "gx10-01c7": lambda _context: snapshot,
            "TRT-EAI-OLDLAB-1": lambda _context: snapshot,
        }
    )

    assert combined(SimpleNamespace()) == {
        "gx10-01c7": snapshot,
        "TRT-EAI-OLDLAB-1": snapshot,
    }

    with pytest.raises(ValueError, match="controller coverage"):
        installed_deep_preflight_factory._controller_predecessor_sources(
            {"TRT-EAI-OLDLAB-1": lambda _context: snapshot}
        )


def test_installed_predecessor_sources_share_one_pool_identity_snapshot() -> None:
    identity = _pool_identity()
    calls = 0
    seen: list[ExternalSupervisorPoolIdentity] = []

    def pool_identity_source() -> ExternalSupervisorPoolIdentity:
        nonlocal calls
        calls += 1
        return identity

    def source(
        _context: CheckContext,
        pool_identity: ExternalSupervisorPoolIdentity,
    ) -> ExternalSupervisorPredecessorSnapshot:
        seen.append(pool_identity)
        return ExternalSupervisorPredecessorSnapshot(
            kind="absent",
            authority_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            unit_sha256={},
            live_evidence_digest="a" * 64,
            pending_transition_digest=hashlib.sha256(b"{}").hexdigest(),
            transition_clear=True,
            runtime_ready=True,
            pool_identity_digest=pool_identity.evidence_digest,
        )

    combined = installed_deep_preflight_factory._controller_predecessor_sources(
        {
            "gx10-01c7": source,
            "TRT-EAI-OLDLAB-1": source,
        },
        pool_identity_source=pool_identity_source,
    )

    snapshots = combined(SimpleNamespace())

    assert calls == 1
    assert seen == [identity, identity]
    assert {snapshot.pool_identity_digest for snapshot in snapshots.values()} == {
        identity.evidence_digest
    }


@pytest.mark.parametrize(
    ("elapsed_prework", "first_attempt_elapsed", "expected_timeouts", "expected_success"),
    (
        (200.0, 400, [1740, 1740], True),
        (0.0, 1740, [1740], False),
    ),
)
def test_installed_gb10_absent_retry_fits_inside_predecessor_check_deadline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    elapsed_prework: float,
    first_attempt_elapsed: int,
    expected_timeouts: list[int],
    expected_success: bool,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    context = _installed_predecessor_context(candidate_root)
    expected = SimpleNamespace()
    attempts: list[dict[str, object]] = []
    elapsed_seconds = elapsed_prework
    check_timeout = build_external_supervisor_predecessor_check(
        lambda _context: {}
    ).spec.timeout_seconds
    cancellation = preflight_contract._CheckCancellation(check_timeout)
    monkeypatch.setattr(preflight_contract, "monotonic", lambda: 0.0)
    cancellation.mark_started()
    context = CheckContext(context.bindings, _cancellation=cancellation)
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "monotonic",
        lambda: elapsed_seconds,
        raising=False,
    )

    def run_subprocess(argv, **kwargs):
        nonlocal elapsed_seconds
        attempts.append({"argv": tuple(argv), **kwargs})
        elapsed_seconds += first_attempt_elapsed if len(attempts) == 1 else int(kwargs["timeout"])
        return SimpleNamespace(
            returncode=1 if len(attempts) == 1 else 0,
            stdout="",
            stderr="",
        )

    commands = installed_deep_preflight_factory.InstalledPreflightCommands(
        _config(tmp_path),
        {
            "HOME": "/var/lib/loom-staging-rollout",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "USER": "loom-rollout",
        },
        run_subprocess=run_subprocess,
    )

    class Transport:
        def __init__(self, run):
            self.run = run

        def observe(self, _artifact, authority=None):
            result = self.run(
                ("ssh", "fixed-controller"),
                '{"schema_version":1}\n',
            )
            if result.returncode != 0:
                raise RuntimeError("delayed first attempt failed")
            assert authority is not None and authority.kind == "absent"
            return expected

    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "build_fixed_gb10_external_supervisor_transport",
        lambda **kwargs: Transport(kwargs["run"]),
    )
    source = installed_deep_preflight_factory._gb10_external_supervisor_observation_source(
        candidate_root=candidate_root,
        run=commands.gb10_supervisor_controller,
    )

    if expected_success:
        assert source(context) is expected
        assert elapsed_seconds + 600 <= check_timeout
    else:
        with pytest.raises(RuntimeError, match="deadline"):
            source(context)
        assert elapsed_seconds + 1740 + 600 > check_timeout
    assert [attempt["timeout"] for attempt in attempts] == expected_timeouts


def test_installed_gb10_observation_binds_context_candidate_to_typed_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    context = _installed_predecessor_context(candidate_root)
    expected = SimpleNamespace()
    captured: dict[str, object] = {}

    class Transport:
        def observe(self, artifact, authority=None):
            captured["artifact"] = artifact
            captured["authority"] = authority
            return expected

    def build(**kwargs):
        captured["builder"] = kwargs
        return Transport()

    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "build_fixed_gb10_external_supervisor_transport",
        build,
    )

    def run(*_args):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    source = installed_deep_preflight_factory._gb10_external_supervisor_observation_source(
        candidate_root=candidate_root,
        run=run,
    )

    assert source(context) is expected
    builder = captured["builder"]
    assert isinstance(builder, dict)
    bounded_run = builder.pop("run")
    assert callable(bounded_run)
    assert builder == {
        "candidate_sha": context.bindings["candidate.sha"],
        "candidate_tree": context.bindings["candidate.tree"],
    }
    artifact = captured["artifact"]
    assert {item.execution_host for item in artifact.supervisors} == {"gx10-01c7"}
    assert captured["authority"] is None


def test_installed_external_supervisor_predecessor_uses_live_schema_after_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    candidate_sha = _git_run(["git", "-C", str(candidate_root), "rev-parse", "HEAD"]).stdout.strip()
    candidate_tree = _git_run(
        ["git", "-C", str(candidate_root), "rev-parse", "HEAD^{tree}"]
    ).stdout.strip()
    artifact = build_external_supervisor_artifact(
        candidate_root,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=f"staging-{candidate_sha[:7]}",
        environment="staging",
    )
    canonical = ExternalSupervisorCanonicalIdentity.build(
        artifact,
        plan_digest="a" * 64,
        attestation_digest="b" * 64,
        transition_group_id="c" * 32,
        runtime_evidence_digest=transport_module._expected_activation_runtime_digest(artifact),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: _CanonicalExternalSupervisorStore(canonical),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _ReadyCanonicalExternalSupervisorControl(canonical),
    )
    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=lambda: _pool_identity(
            "0074",
            legacy_count=0,
            target_count=1,
        ),
    )

    snapshot = source(
        _installed_predecessor_context(
            candidate_root,
            backup_schema_revision="0073",
            database_schema_revision="0074",
        )
    )

    assert snapshot.kind == "canonical"
    assert snapshot.runtime_ready is True
    assert (
        snapshot.pool_identity_digest
        == _pool_identity(
            "0074",
            legacy_count=0,
            target_count=1,
        ).evidence_digest
    )


def test_installed_external_supervisor_predecessor_source_declares_safe_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The root installer owns the candidate repo; every git invocation must
    # declare the safe.directory exception or git fails-closed as the service
    # user (dubious ownership), which the check masks as provenance-unavailable.
    candidate_root = Path(__file__).resolve().parents[4]
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: _LegacyExternalSupervisorStore(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _ReadyLegacyExternalSupervisorControl(),
    )

    seen: list[list[str]] = []

    def spy_git_run(arguments: list[str]):
        seen.append(list(arguments))
        return _git_run(arguments)

    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=spy_git_run,
        service_uid=501,
        pool_identity_source=_pool_identity,
    )
    source(_installed_predecessor_context(candidate_root))

    assert seen
    for argv in seen:
        assert argv[0] == "git"
        assert f"safe.directory={candidate_root}" in argv


def test_installed_external_supervisor_predecessor_source_bootstraps_absent_predecessor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # First introduction of the external supervisor: no canonical record and no
    # live units. The predecessor is genuinely absent (nothing to clobber), so
    # the source binds an absent authority instead of failing not-authoritative.
    candidate_root = Path(__file__).resolve().parents[4]
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: _AbsentExternalSupervisorStore(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _AbsentExternalSupervisorControl(),
    )

    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=_pool_identity,
    )
    snapshot = source(_installed_predecessor_context(candidate_root))

    assert snapshot.kind == "absent"
    assert dict(snapshot.unit_sha256) == {}
    assert snapshot.transition_clear is True
    assert snapshot.runtime_ready is True


def test_absent_predecessor_kind_skips_pool_identity_but_present_kinds_still_gate() -> None:
    # An absent predecessor has no supervisor to place in the pool, so it does
    # not gate on the gb10-arm64->gb10 rename state -- it is accepted even on a
    # lineage-diverged live database (post-0067 revision still carrying legacy
    # rows), which the rollout migration reconciles during the deploy.
    diverged = ExternalSupervisorPoolIdentity.build(
        schema_revision="0069",
        legacy_rows={name: 1 for name in _POOL_IDENTITY_TABLES},
        target_rows={name: 0 for name in _POOL_IDENTITY_TABLES},
    )
    diverged.require_predecessor_kind("absent")  # no raise

    # A *present* predecessor still gates on the rename state so drift is caught:
    with pytest.raises(ValueError, match="post-0067 pool identity drifted"):
        diverged.require_predecessor_kind("canonical")
    pre = ExternalSupervisorPoolIdentity.build(
        schema_revision="0066",
        legacy_rows={name: 1 for name in _POOL_IDENTITY_TABLES},
        target_rows={name: 1 for name in _POOL_IDENTITY_TABLES},
    )
    with pytest.raises(ValueError, match="pre-0067 pool identity drifted"):
        pre.require_predecessor_kind("legacy-manifest")


def test_installed_external_supervisor_predecessor_source_rejects_source_blob_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: _LegacyExternalSupervisorStore(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _ReadyLegacyExternalSupervisorControl(),
    )

    def drifted_git_run(arguments: list[str]):
        if "cat-file" in arguments:
            return SimpleNamespace(returncode=0, stdout="drifted\n", stderr="")
        return _git_run(arguments)

    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=drifted_git_run,
        service_uid=501,
        pool_identity_source=_pool_identity,
    )

    with pytest.raises(ValueError, match="Git provenance drifted"):
        source(_installed_predecessor_context(candidate_root))


def test_external_supervisor_pool_identity_is_one_way_across_0067() -> None:
    _pool_identity("0066").require_predecessor_kind("legacy-manifest")
    _pool_identity("0067", legacy_count=0, target_count=1).require_predecessor_kind("canonical")

    with pytest.raises(ValueError, match="pre-0067 pool identity drifted"):
        _pool_identity("0066", target_count=1).require_predecessor_kind("legacy-manifest")
    with pytest.raises(ValueError, match="post-0067 pool identity drifted"):
        _pool_identity("0067", legacy_count=1, target_count=1).require_predecessor_kind("canonical")
    with pytest.raises(ValueError, match="post-0067 pool identity drifted"):
        _pool_identity("0067", legacy_count=0, target_count=1).require_predecessor_kind(
            "legacy-manifest"
        )


def test_installed_predecessor_source_rejects_legacy_after_0067(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = Path(__file__).resolve().parents[4]
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "AtomicUserUnitStore",
        lambda **_kwargs: _LegacyExternalSupervisorStore(),
    )
    monkeypatch.setattr(
        installed_deep_preflight_factory,
        "FixedUserSystemdControl",
        lambda **_kwargs: _ReadyLegacyExternalSupervisorControl(),
    )
    source = installed_deep_preflight_factory._external_supervisor_predecessor_source(
        candidate_root=candidate_root,
        git_run=_git_run,
        service_uid=501,
        pool_identity_source=lambda: _pool_identity(
            "0067",
            legacy_count=0,
            target_count=1,
        ),
    )

    with pytest.raises(ValueError, match="post-0067 pool identity drifted"):
        source(
            _installed_predecessor_context(
                candidate_root,
                backup_schema_revision="0066",
                database_schema_revision="0067",
            )
        )


def test_installed_pool_identity_probe_rejects_missing_or_duplicate_tables() -> None:
    def query(sql: str):
        if "alembic_version" in sql:
            return ({"schema_revision": "0067"},)
        return tuple(
            {
                "table_name": name,
                "legacy_rows": 0,
                "target_rows": 1,
            }
            for name in _POOL_IDENTITY_TABLES
        )

    identity = installed_deep_preflight_factory._probe_external_supervisor_pool_identity(query)
    assert identity.schema_revision == "0067"
    assert identity.legacy_rows == {name: 0 for name in _POOL_IDENTITY_TABLES}
    assert identity.target_rows == {name: 1 for name in _POOL_IDENTITY_TABLES}

    def duplicate_query(sql: str):
        if "alembic_version" in sql:
            return ({"schema_revision": "0067"},)
        return (
            {"table_name": "workers", "legacy_rows": 0, "target_rows": 1},
            {"table_name": "workers", "legacy_rows": 0, "target_rows": 1},
        )

    with pytest.raises(ValueError, match="pool identity is invalid"):
        installed_deep_preflight_factory._probe_external_supervisor_pool_identity(duplicate_query)
