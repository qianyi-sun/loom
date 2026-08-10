from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_cli import admin_cmd, cluster_cmd
from loom_cli.__main__ import main
from loom_cli.cluster_cmd import ApplyResult, ClusterStatus, ComponentStatus
from loom_cli.cluster_config import ClusterConfig, cluster_config_from_mapping
from loom_cli.environment_state import EnvironmentStateProfile
from loom_cli.rollout.operator.model import DriverEnvelope
from loom_cli.rollout_lock import RolloutLeaseManager

_SHA = "abcdef1234567890abcdef1234567890abcdef12"
_ROLLOUT_ID = "staging-abcdef1"
_REQUEST_ID = "request-20260713-hongjian"
_ATTRIBUTION = {
    "request_id": _REQUEST_ID,
    "initiating_operator": "hongjian",
    "initiating_uid": 2011,
    "attempt_number": 2,
    "attempt_operator": "devansh",
    "attempt_uid": 2501,
}


def test_cluster_and_admin_share_rollout_lock_cli_contracts() -> None:
    assert admin_cmd._add_rollout_lock_args is cluster_cmd._add_rollout_lock_args
    assert admin_cmd._load_broker_rollout_envelope is cluster_cmd._load_broker_rollout_envelope
    assert admin_cmd._require_real_file is cluster_cmd._require_real_file
    assert (
        admin_cmd._fixed_rollout_lock_evidence_path is cluster_cmd._fixed_rollout_lock_evidence_path
    )


@dataclass(frozen=True)
class _BrokerAttemptFixture:
    config: Any
    envelope: DriverEnvelope
    envelope_path: Path
    rollout_dir: Path
    rollout_config: Path
    environment_profile: Path
    backup_manifest: Path


def _broker_attempt_fixture(tmp_path: Path) -> _BrokerAttemptFixture:
    rollout_root = tmp_path / "data" / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / _ROLLOUT_ID
    (rollout_dir / "07-render").mkdir(parents=True)
    (rollout_dir / "10-cluster-up").mkdir(parents=True)
    (rollout_dir / "11-env-state").mkdir()

    runner_repo = tmp_path / "runner"
    cluster_relative = Path("deploy/environments/staging.cluster.toml")
    cluster_config = runner_repo / cluster_relative
    cluster_config.parent.mkdir(parents=True)
    cluster_config.write_text(
        'namespace = "loom-staging"\n'
        'runtime_environment = "staging"\n'
        'env_state_profile = "../environment-state/staging.toml"\n'
        'container_registry = "192.168.50.13:5000"\n'
        'container_registry_push = "localhost:5000"\n'
        'persistent_storage_backend = "dynamic"\n'
        f'persistent_storage_host_path_root = "{rollout_root}"\n'
        '[topology]\n'
        'multi_node = true\n'
        'storage_backend = "longhorn"\n'
        'postgres_replicas = 3\n'
        'minio_replicas = 4\n'
        'anti_affinity = "required"\n'
        'min_available = 3\n',
        encoding="utf-8",
    )

    candidate_root = rollout_dir / "01-worktree" / "src"
    candidate_config = candidate_root / cluster_relative
    candidate_config.parent.mkdir(parents=True)
    candidate_config.write_text(cluster_config.read_text(encoding="utf-8"), encoding="utf-8")
    environment_profile = candidate_root / "deploy/environment-state/staging.toml"
    environment_profile.parent.mkdir(parents=True)
    environment_profile.write_text('environment = "staging"\n', encoding="utf-8")

    rollout_config = rollout_dir / "rollout-cluster-config.toml"
    rollout_config.write_text(
        f'image_tag = "staging-{_SHA[:7]}"\n'
        'namespace = "loom-staging"\n'
        'runtime_environment = "staging"\n'
        'container_registry = "192.168.50.13:5000"\n'
        'container_registry_push = "localhost:5000"\n'
        'persistent_storage_backend = "dynamic"\n'
        f'persistent_storage_host_path_root = "{rollout_root}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n"
        "[k8s_worker]\n"
        "enabled = false\n"
        "[topology]\n"
        "multi_node = true\n"
        'storage_backend = "longhorn"\n'
        "postgres_replicas = 3\n"
        "minio_replicas = 4\n"
        'anti_affinity = "required"\n'
        "min_available = 3\n",
        encoding="utf-8",
    )
    (rollout_dir / "07-render" / "rendered.yaml").write_text(
        "apiVersion: v1\nkind: List\nitems: []\n",
        encoding="utf-8",
    )

    backup_manifest = tmp_path / "backups" / "backup-manifest.json"
    backup_manifest.parent.mkdir()
    backup_manifest.write_text("{}\n", encoding="utf-8")
    secret_root = tmp_path / "secrets"
    secret_root.mkdir()
    for name in ("admin", "worker", "service"):
        (secret_root / name).write_text(f"{name}-secret\n", encoding="utf-8")
    kubeconfig_path = tmp_path / "private" / "kubeconfig"
    kubeconfig_path.parent.mkdir()
    kubeconfig_path.write_text("apiVersion: v1\n", encoding="utf-8")

    config = SimpleNamespace(
        runtime_root=tmp_path / "run" / "loom-staging-rollout",
        rollout_root=rollout_root,
        runner_repo=runner_repo,
        kubeconfig_path=kubeconfig_path,
        cluster_config_path=cluster_config,
        environment="staging",
        namespace="loom-staging",
        cluster_name="loom-staging",
        cp_url="http://127.0.0.1:18081",
        admin_token_source=f"file:{secret_root / 'admin'}",
        worker_token_source=f"file:{secret_root / 'worker'}",
        service_token_source=f"file:{secret_root / 'service'}",
        backup_max_objects=1_000_000,
        backup_max_entries=16_000_000,
        expect_admin_token_fingerprint=(
            "sha256:" + hashlib.sha256(b"admin-secret").hexdigest()[:12] + " len=12"
        ),
    )
    envelope = DriverEnvelope(
        schema_version=1,
        request_id=_REQUEST_ID,
        rollout_id=_ROLLOUT_ID,
        initiating_operator="hongjian",
        initiating_uid=2011,
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2501,
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="origin/dev",
        resolved_sha=_SHA,
        image_tag=f"staging-{_SHA[:7]}",
        fetched_at="2026-07-13T15:00:00+00:00",
        backup_manifest_path=str(backup_manifest),
        backup_manifest_sha256="2" * 64,
        runner_config_sha256="1" * 64,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url=config.cp_url,
        cluster_config_path=str(cluster_config),
        rollout_root=str(rollout_root),
        admin_token_source=config.admin_token_source,
        worker_token_source=config.worker_token_source,
        service_token_source=config.service_token_source,
        expect_admin_token_fingerprint=config.expect_admin_token_fingerprint,
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="team-agentic-rl",
        scope="current-gb10",
        gb10_prep_concurrency=5,
        resume=True,
    )
    envelope_path = tmp_path / "private" / "envelope.json"
    return _BrokerAttemptFixture(
        config=config,
        envelope=envelope,
        envelope_path=envelope_path,
        rollout_dir=rollout_dir,
        rollout_config=rollout_config,
        environment_profile=environment_profile,
        backup_manifest=backup_manifest,
    )


def _patch_broker_attempt(
    monkeypatch: pytest.MonkeyPatch,
    fixture: _BrokerAttemptFixture,
) -> None:
    monkeypatch.setenv("KUBECONFIG", str(fixture.config.kubeconfig_path))
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_broker_rollout_envelope",
        lambda _path: (fixture.config, fixture.envelope),
        raising=False,
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_broker_rollout_envelope",
        lambda _path: (fixture.config, fixture.envelope),
        raising=False,
    )


def _cluster_broker_argv(fixture: _BrokerAttemptFixture) -> list[str]:
    from loom_cli.rollout.operator.backup_limits import (
        operator_backup_traversal_limits,
    )

    limits = operator_backup_traversal_limits(fixture.config)
    return [
        "cluster",
        "up",
        "--namespace",
        "loom-staging",
        "--config",
        str(fixture.rollout_config),
        "--rendered-manifest",
        str(fixture.rollout_dir / "07-render" / "rendered.yaml"),
        "--backup-manifest",
        str(fixture.backup_manifest),
        "--recover-sandbox-deadlines",
        "--sandbox-deadline-max-pods",
        "4",
        "--backup-max-files",
        str(limits.max_files),
        "--backup-max-entries",
        str(limits.max_entries),
        "--backup-max-total-bytes",
        str(limits.max_total_bytes),
        "--rollout-request-envelope",
        str(fixture.envelope_path),
    ]


def _admin_broker_argv(
    fixture: _BrokerAttemptFixture,
    operation: str,
) -> list[str]:
    argv = [
        "admin",
        "environment-state",
        operation,
        "--cp-url",
        fixture.envelope.cp_url,
        "--admin-token",
        fixture.envelope.admin_token_source,
        "--expect-admin-token-fingerprint",
        fixture.envelope.expect_admin_token_fingerprint,
        "--file",
        str(fixture.environment_profile),
        "--environment",
        "staging",
        "--var",
        f"IMAGE_TAG={fixture.envelope.image_tag}",
        "--var",
        f"ENV_CONFIG_VERSION={fixture.envelope.image_tag}",
        "--var",
        f"GIT_SHA={fixture.envelope.resolved_sha}",
        "--rollout-request-envelope",
        str(fixture.envelope_path),
    ]
    if operation == "check":
        argv.extend(
            [
                "--worker-token",
                fixture.envelope.worker_token_source,
                "--format",
                "json",
            ]
        )
    return argv


def _main_without_parser_exit(argv: list[str]) -> int:
    try:
        return main(argv)
    except SystemExit as exc:
        return int(exc.code)


def _ready_status() -> ClusterStatus:
    return ClusterStatus(
        namespace="loom",
        context=None,
        components=[
            ComponentStatus(
                name="loom-service",
                kind="Deployment",
                ready=1,
                desired=1,
                available=True,
                generation=1,
                observed_generation=1,
                updated=1,
            ),
        ],
        ingresses=[],
        warnings=[],
    )


def _patch_cluster_up_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    k8s_worker_cls = type(ClusterConfig().k8s_worker)
    config = ClusterConfig(k8s_worker=k8s_worker_cls(enabled=True))
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )
    monkeypatch.setattr("loom_cli.cluster_cmd.load_cluster_config", lambda _path: config)
    monkeypatch.setattr("loom_cli.cluster_cmd.cluster_config_from_mapping", lambda _raw: config)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.render_manifests",
        lambda _config: "apiVersion: v1\nkind: List\nitems: []\n",
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_preflight",
        lambda *_args, **_kwargs: type("Report", (), {"any_fail": False})(),
    )
    monkeypatch.setattr("loom_cli.cluster_cmd._effective_kube_context", lambda _context: None)
    monkeypatch.setattr("loom_cli.cluster_cmd._read_kind_node_mounts", lambda _context: {})
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._append_target_schema_doctor_check",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.apply_manifests",
        lambda *_args, **_kwargs: ApplyResult(
            returncode=0,
            summary_lines=["deployment.apps/loom-service configured"],
            stderr="",
        ),
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.wait_for_ready", lambda *_args, **_kwargs: _ready_status()
    )
    monkeypatch.setattr("loom_cli.cluster_cmd.rendered_image_checks", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.prune_disabled_profile_resources",
        lambda *_args, **_kwargs: SimpleNamespace(
            has_evidence=False,
            ok=True,
            deleted=[],
            retained=[],
            not_found=[],
            failed=[],
        ),
    )


def _empty_environment_profile(
    *,
    environment: str = "staging",
    control_plane_environment: str = "staging",
) -> EnvironmentStateProfile:
    return EnvironmentStateProfile(
        environment=environment,
        control_plane_environment=control_plane_environment,
        autoscaler_policies=[],
        gb10_desired_states=[],
        catalog_provisioning={},
        rate_card_sync={},
        hosted_provider_pricing_defaults=[],
        external_slurm_runner_prerequisites={},
        external_slurm_autoscaler_supervisors=[],
    )


def _protected_v1_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "protected-v1.toml"
    config_path.write_text(
        """[workload_contract]
workload_trust_mode = "internal_trusted"
taskset_transforms_enabled = false
taskset_transform_network_isolated = false
untrusted_workload_isolation = false
""",
        encoding="utf-8",
    )
    return config_path


def test_cluster_up_protected_conflict_fails_before_loading_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = _protected_v1_config(tmp_path)
    RolloutLeaseManager(tmp_path).acquire(
        environment="production",
        owner_id="owner-a",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    def _load_clients(_context: str | None) -> tuple[object, object, object, object]:
        raise AssertionError("cluster clients should not load when lock is held")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _load_clients)

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "production",
            "--namespace",
            "loom-production",
            "--config",
            str(config_path),
            "--rollout-lock-dir",
            str(tmp_path),
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "active rollout mutation lease" in err
    assert "owner-a" in err
    assert "--force-rollout-lock" in err


def test_cluster_up_protected_records_lock_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)
    config_path = _protected_v1_config(tmp_path)
    evidence_path = tmp_path / "lock-evidence.json"

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "production",
            "--namespace",
            "loom-production",
            "--config",
            str(config_path),
            "--rollout-lock-dir",
            str(tmp_path),
            "--rollout-id",
            "production-d46a16c",
            "--rollout-lock-evidence",
            str(evidence_path),
        ]
    )

    assert rc == 0
    active_record = json.loads((tmp_path / "production.lock").read_text(encoding="utf-8"))
    assert active_record["release_status"] == "released"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert [event["event"] for event in evidence["events"]] == [
        "acquired",
        "released",
    ]
    assert evidence["events"][0]["owner_id"] == "production-d46a16c"


def test_environment_state_apply_protected_conflict_reports_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    RolloutLeaseManager(tmp_path).acquire(
        environment="production",
        owner_id="cluster-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        lambda _args: _empty_environment_profile(
            environment="production",
            control_plane_environment="production",
        ),
    )
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "environment-state",
            "apply",
            "--file",
            "deploy/environment-state/staging.toml",
            "--environment",
            "production",
            "--rollout-lock-dir",
            str(tmp_path),
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "cluster-owner" in err
    assert "active rollout mutation lease" in err


def test_environment_state_check_records_lock_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence_path = tmp_path / "env-state-lock.json"
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        lambda _args: _empty_environment_profile(
            environment="production",
            control_plane_environment="production",
        ),
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._fetch_environment_state",
        lambda **_kwargs: (
            0,
            {
                "autoscaler_status": {"policies": []},
                "gb10_status": {"desired_states": []},
                "slurm_status": {"jobs": []},
            },
        ),
    )
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "environment-state",
            "check",
            "--file",
            "deploy/environment-state/staging.toml",
            "--environment",
            "production",
            "--rollout-id",
            "env-state-check-production-d46a16c",
            "--rollout-lock-dir",
            str(tmp_path),
            "--rollout-lock-evidence",
            str(evidence_path),
            "--format",
            "json",
        ]
    )

    assert rc == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    events = json.loads(evidence_path.read_text(encoding="utf-8"))["events"]
    assert events[0]["event"] == "acquired"
    assert events[0]["owner_id"] == "env-state-check-production-d46a16c"
    assert events[1]["event"] == "released"


def test_manual_staging_cluster_up_requires_envelope_before_config_lock_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)
    config_path = tmp_path / "must-not-be-read.cluster.toml"

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "staging",
            "--namespace",
            "loom-staging",
            "--config",
            str(config_path),
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


def test_manual_cluster_up_cannot_hide_staging_behind_development_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)
    staging_config = tmp_path / "staging.cluster.toml"
    staging_config.write_text(
        'namespace = "loom-staging"\n'
        'runtime_environment = "staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(staging_config),
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


@pytest.mark.parametrize(
    "staging_root_alias",
    [
        "/data//loom-staging",
        "/data/./loom-staging",
        "//data/loom-staging",
        "/data/loom-staging/custom",
    ],
)
def test_manual_cluster_up_cannot_hide_staging_behind_host_root_alias(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    staging_root_alias: str,
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)
    staging_config = tmp_path / "staging-alias.cluster.toml"
    staging_config.write_text(
        f'persistent_storage_host_path_root = "{staging_root_alias}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(staging_config),
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


@pytest.mark.parametrize(
    "symlink_target",
    ["/data/loom-staging", "/data/loom-staging/custom"],
)
def test_manual_cluster_up_cannot_hide_staging_behind_symlinked_host_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    symlink_target: str,
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)
    staging_root_alias = tmp_path / "staging-root-alias"
    staging_root_alias.symlink_to(symlink_target, target_is_directory=True)
    staging_config = tmp_path / "staging-symlink.cluster.toml"
    staging_config.write_text(
        f'persistent_storage_host_path_root = "{staging_root_alias}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(staging_config),
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


def test_cluster_up_rejects_unresolvable_host_root_before_lock_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)
    loop_a = tmp_path / "loop-a"
    loop_b = tmp_path / "loop-b"
    loop_a.symlink_to(loop_b, target_is_directory=True)
    loop_b.symlink_to(loop_a, target_is_directory=True)
    config_path = tmp_path / "loop.cluster.toml"
    config_path.write_text(
        f'persistent_storage_host_path_root = "{loop_a}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 2
    assert "preflight config invalid" in capsys.readouterr().err
    assert not (tmp_path / "locks").exists()


def test_cluster_up_uses_one_config_snapshot_when_file_changes_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli import cluster_cmd
    from loom_cli.cluster_config import (
        cluster_config_from_mapping as real_cluster_config_from_mapping,
    )
    from loom_cli.cluster_config import load_cluster_config as real_load_cluster_config

    config_path = tmp_path / "mutable.cluster.toml"
    config_path.write_text(
        'namespace = "loom"\n'
        'runtime_environment = "development"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/tmp/loom-development"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n"
        "[k8s_worker]\n"
        "enabled = true\n",
        encoding="utf-8",
    )
    _patch_cluster_up_happy_path(monkeypatch)
    monkeypatch.setattr(cluster_cmd, "load_cluster_config", real_load_cluster_config)
    monkeypatch.setattr(
        cluster_cmd,
        "cluster_config_from_mapping",
        real_cluster_config_from_mapping,
    )
    config_reads = 0
    read_text = Path.read_text

    def _read_text_once(path: Path, *args: Any, **kwargs: Any) -> str:
        nonlocal config_reads
        if path == config_path.resolve():
            config_reads += 1
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _read_text_once)
    rendered_environments: list[str] = []

    def _render(config: ClusterConfig) -> str:
        rendered_environments.append(config.runtime_environment)
        return "apiVersion: v1\nkind: List\nitems: []\n"

    monkeypatch.setattr("loom_cli.cluster_cmd.render_manifests", _render)
    acquire = cluster_cmd._acquire_protected_rollout_lock

    def _swap_after_admission(*args: Any, **kwargs: Any) -> Any:
        config_path.write_text(
            'namespace = "loom-staging"\n'
            'runtime_environment = "staging"\n'
            'persistent_storage_backend = "static-host-path"\n'
            'persistent_storage_host_path_root = "/data/loom-staging"\n'
            "[workload_contract]\n"
            'workload_trust_mode = "internal_trusted"\n'
            "taskset_transforms_enabled = false\n"
            "taskset_transform_network_isolated = false\n"
            "untrusted_workload_isolation = false\n"
            "[k8s_worker]\n"
            "enabled = true\n",
            encoding="utf-8",
        )
        return acquire(*args, **kwargs)

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._acquire_protected_rollout_lock",
        _swap_after_admission,
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
        ]
    )

    assert rc == 0
    assert config_reads == 1
    assert rendered_environments == ["development"]


@pytest.mark.parametrize(
    "production_declaration",
    ["runtime", "namespace", "host-root"],
)
def test_cluster_up_rejects_production_config_hidden_behind_development_argv_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    production_declaration: str,
) -> None:
    values = {
        "namespace": "loom",
        "runtime_environment": "development",
        "persistent_storage_host_path_root": "/tmp/loom-development",
    }
    if production_declaration == "runtime":
        values["runtime_environment"] = " production "
    elif production_declaration == "namespace":
        values["namespace"] = "loom-prod"
    else:
        values["persistent_storage_host_path_root"] = "/data//loom-prod"
    config_path = tmp_path / "hidden-production.cluster.toml"
    config_path.write_text(
        f'namespace = "{values["namespace"]}"\n'
        f'runtime_environment = "{values["runtime_environment"]}"\n'
        'persistent_storage_backend = "static-host-path"\n'
        "persistent_storage_host_path_root = "
        f'"{values["persistent_storage_host_path_root"]}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    _patch_cluster_up_happy_path(monkeypatch)
    client_loads = 0

    def _track_clients(_context: str | None) -> tuple[object, object, object, object]:
        nonlocal client_loads
        client_loads += 1
        return object(), object(), object(), object()

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _track_clients)

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "protected cluster config target production conflicts" in err
    assert client_loads == 0
    assert not (tmp_path / "locks").exists()


def test_cluster_up_rejects_conflicting_protected_config_declarations_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "conflicting.cluster.toml"
    config_path.write_text(
        'namespace = "loom-prod"\n'
        'runtime_environment = "staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    client_loads = 0

    def _track_clients(_context: str | None) -> tuple[object, object, object, object]:
        nonlocal client_loads
        client_loads += 1
        return object(), object(), object(), object()

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _track_clients)

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
        ]
    )

    assert rc == 2
    assert "conflicting protected cluster config targets" in capsys.readouterr().err
    assert client_loads == 0


def test_cluster_up_preserves_relative_host_root_render_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path = tmp_path / "relative-root.cluster.toml"
    config_path.write_text(
        'namespace = "loom"\n'
        'runtime_environment = "development"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "data/loom-staging"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom",
            "--config",
            str(config_path),
            "--skip-preflight",
        ]
    )

    err = capsys.readouterr().err
    assert rc == 2
    assert "render failed" in err
    assert "must be an absolute host path" in err
    assert "broker-created request envelope" not in err


@pytest.mark.parametrize("operation", ["apply", "check"])
def test_manual_staging_environment_state_requires_envelope_before_profile_or_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
) -> None:
    calls = {"profile": 0, "token": 0, "network": 0, "lock": 0}

    def _load_profile(_args: Any) -> EnvironmentStateProfile:
        calls["profile"] += 1
        return _empty_environment_profile()

    def _track_token(_source: str) -> str:
        calls["token"] += 1
        return "admin-secret"

    def _track_network(*_args: Any, **_kwargs: Any) -> Any:
        calls["network"] += 1
        return SimpleNamespace(status_code=200, text="", json=lambda: {})

    def _track_lock(*_args: Any, **_kwargs: Any) -> Any:
        calls["lock"] += 1
        return SimpleNamespace(
            owner_id="unexpected-lock",
            release=lambda **_release_kwargs: None,
        )

    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        _load_profile,
    )
    monkeypatch.setattr("loom_cli.admin_cmd._resolve_admin_token", _track_token)
    monkeypatch.setattr("loom_cli.admin_cmd.httpx.put", _track_network)
    monkeypatch.setattr("loom_cli.admin_cmd._fetch_environment_state", _track_network)
    monkeypatch.setattr(RolloutLeaseManager, "acquire", _track_lock)

    rc = main(
        [
            "admin",
            "environment-state",
            operation,
            "--file",
            str(tmp_path / "must-not-be-read.toml"),
            "--environment",
            "staging",
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert calls == {"profile": 0, "token": 0, "network": 0, "lock": 0}
    assert not (tmp_path / "locks").exists()


@pytest.mark.parametrize("operation", ["apply", "check"])
def test_environment_state_cannot_hide_staging_control_plane_behind_development_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
) -> None:
    from loom_cli import admin_cmd

    profile_path = tmp_path / "disguised-staging.toml"
    profile_path.write_text(
        'environment = "development"\n'
        'control_plane_environment = "staging"\n'
        "[[worker_pool_autoscaler_policies]]\n"
        'pool_name = "gb10"\n'
        'actuator = "slurm"\n'
        "max_slots = 1\n",
        encoding="utf-8",
    )
    calls = {"profile": 0, "token": 0, "network": 0, "lock": 0}
    load_profile = admin_cmd._load_environment_state_profile_from_args

    def _load_profile_once(args: Any) -> EnvironmentStateProfile | None:
        calls["profile"] += 1
        return load_profile(args)

    def _resolve_token(_source: str) -> str:
        calls["token"] += 1
        return "admin-secret"

    def _fake_put(*_args: Any, **_kwargs: Any) -> Any:
        calls["network"] += 1
        return SimpleNamespace(status_code=200, text="", json=lambda: {})

    def _fake_fetch(**_kwargs: Any) -> tuple[int, dict[str, Any]]:
        calls["network"] += 1
        return (
            0,
            {
                "autoscaler_status": {"policies": []},
                "gb10_status": {"desired_states": []},
                "slurm_status": {"jobs": []},
            },
        )

    def _track_acquire(*_args: Any, **_kwargs: Any) -> Any:
        calls["lock"] += 1
        return SimpleNamespace(
            owner_id="unexpected-lock",
            release=lambda **_release_kwargs: None,
        )

    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        _load_profile_once,
    )
    monkeypatch.setattr("loom_cli.admin_cmd._resolve_admin_token", _resolve_token)
    monkeypatch.setattr("loom_cli.admin_cmd.httpx.put", _fake_put)
    monkeypatch.setattr("loom_cli.admin_cmd._fetch_environment_state", _fake_fetch)
    monkeypatch.setattr(RolloutLeaseManager, "acquire", _track_acquire)

    rc = main(
        [
            "admin",
            "environment-state",
            operation,
            "--cp-url",
            "http://cp:8080",
            "--admin-token",
            "env:LOOM_ADMIN_TOKEN",
            "--file",
            str(profile_path),
            "--environment",
            "development",
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    assert "broker-created request envelope is required" in capsys.readouterr().err
    assert calls == {"profile": 1, "token": 0, "network": 0, "lock": 0}
    assert not (tmp_path / "locks").exists()


def test_broker_cluster_up_records_exact_attribution_and_fixed_lock_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    _patch_cluster_up_happy_path(monkeypatch)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.cluster_config_from_mapping",
        cluster_config_from_mapping,
    )

    rc = _main_without_parser_exit(_cluster_broker_argv(fixture))

    assert rc == 0
    lock_path = fixture.config.runtime_root / "mutation-locks" / "staging.lock"
    active = json.loads(lock_path.read_text(encoding="utf-8"))
    assert active["owner_id"] == fixture.envelope.rollout_id
    assert {field: active[field] for field in _ATTRIBUTION} == _ATTRIBUTION
    evidence_path = fixture.rollout_dir / "10-cluster-up" / "rollout-lock.json"
    events = json.loads(evidence_path.read_text(encoding="utf-8"))["events"]
    assert [event["event"] for event in events] == ["acquired", "released"]
    assert all({field: event[field] for field in _ATTRIBUTION} == _ATTRIBUTION for event in events)
    serialized = json.dumps({"active": active, "events": events})
    assert str(fixture.envelope_path) not in serialized


def test_broker_cluster_up_rejects_traversal_limit_policy_drift_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    argv = _cluster_broker_argv(fixture)
    argv[argv.index("--backup-max-files") + 1] = "1000005"
    calls = {"network": 0}

    def track_clients(_context: str | None) -> tuple[object, object, object, object]:
        calls["network"] += 1
        return object(), object(), object(), object()

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", track_clients)

    rc = _main_without_parser_exit(argv)

    assert rc == 1
    assert "backup traversal limits do not match fixed broker policy" in capsys.readouterr().err
    assert calls == {"network": 0}


def test_broker_cluster_up_requires_exact_rendered_artifact_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    argv = _cluster_broker_argv(fixture)
    index = argv.index("--rendered-manifest")
    del argv[index : index + 2]
    calls = {"network": 0}
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: calls.__setitem__("network", calls["network"] + 1),
    )

    rc = _main_without_parser_exit(argv)

    assert rc == 1
    assert "rendered manifest path does not match broker rollout" in capsys.readouterr().err
    assert calls == {"network": 0}


@pytest.mark.parametrize(
    "mutation",
    [
        "namespace",
        "runtime",
        "image",
        "backend",
        "descendant-root",
        "lexical-root-alias",
        "symlink-root-alias",
    ],
)
def test_broker_cluster_up_rejects_non_exact_protected_config_before_lock_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    mutation: str,
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    _patch_cluster_up_happy_path(monkeypatch)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd.cluster_config_from_mapping",
        cluster_config_from_mapping,
    )
    values = {
        "namespace": fixture.envelope.namespace,
        "runtime": fixture.envelope.environment,
        "image": fixture.envelope.image_tag,
        "backend": "dynamic",
        "root": str(fixture.config.rollout_root),
    }
    if mutation == "namespace":
        values["namespace"] = "loom"
    elif mutation == "runtime":
        values["runtime"] = "development"
    elif mutation == "image":
        values["image"] = "staging-deadbee"
    elif mutation == "backend":
        values["backend"] = "static-host-path"
    elif mutation == "descendant-root":
        values["root"] = str(fixture.config.rollout_root / "custom")
    elif mutation == "lexical-root-alias":
        values["root"] = (
            f"{fixture.config.rollout_root.parent}/./{fixture.config.rollout_root.name}"
        )
    else:
        alias = tmp_path / "staging-root-alias"
        alias.symlink_to(fixture.config.rollout_root, target_is_directory=True)
        values["root"] = str(alias)
    fixture.rollout_config.write_text(
        f'image_tag = "{values["image"]}"\n'
        f'namespace = "{values["namespace"]}"\n'
        f'runtime_environment = "{values["runtime"]}"\n'
        'container_registry = "192.168.50.13:5000"\n'
        'container_registry_push = "localhost:5000"\n'
        f'persistent_storage_backend = "{values["backend"]}"\n'
        f'persistent_storage_host_path_root = "{values["root"]}"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n"
        "[topology]\n"
        "multi_node = true\n"
        'storage_backend = "longhorn"\n'
        "postgres_replicas = 3\n"
        "minio_replicas = 4\n"
        'anti_affinity = "required"\n'
        "min_available = 3\n",
        encoding="utf-8",
    )
    calls = {"lock": 0, "network": 0}

    def _track_lock(*_args: Any, **_kwargs: Any) -> Any:
        calls["lock"] += 1
        return SimpleNamespace(
            owner_id="unexpected-lock",
            release=lambda **_release_kwargs: None,
        )

    def _track_clients(_context: str | None) -> tuple[object, object, object, object]:
        calls["network"] += 1
        return object(), object(), object(), object()

    monkeypatch.setattr(RolloutLeaseManager, "acquire", _track_lock)
    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _track_clients)

    rc = _main_without_parser_exit(_cluster_broker_argv(fixture))

    assert rc == 1
    assert "broker rollout cluster config" in capsys.readouterr().err
    assert calls == {"lock": 0, "network": 0}
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


def test_broker_environment_state_records_exact_attribution_and_fixed_lock_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        lambda _args: _empty_environment_profile(),
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._fetch_environment_state",
        lambda **_kwargs: (
            0,
            {
                "autoscaler_status": {"policies": []},
                "gb10_status": {"desired_states": []},
                "slurm_status": {"jobs": []},
            },
        ),
    )

    rc = _main_without_parser_exit(_admin_broker_argv(fixture, "check"))

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
    lock_path = fixture.config.runtime_root / "mutation-locks" / "staging.lock"
    active = json.loads(lock_path.read_text(encoding="utf-8"))
    assert active["owner_id"] == fixture.envelope.rollout_id
    assert {field: active[field] for field in _ATTRIBUTION} == _ATTRIBUTION
    evidence_path = fixture.rollout_dir / "11-env-state" / "rollout-lock.json"
    events = json.loads(evidence_path.read_text(encoding="utf-8"))["events"]
    assert all({field: event[field] for field in _ATTRIBUTION} == _ATTRIBUTION for event in events)


@pytest.mark.parametrize("operation", ["apply", "check"])
def test_broker_environment_state_rejects_cross_environment_control_plane_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    calls = {"token": 0, "network": 0, "lock": 0}
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        lambda _args: _empty_environment_profile(
            environment="staging",
            control_plane_environment="production",
        ),
    )

    def _resolve_token(_source: str) -> str:
        calls["token"] += 1
        return "admin-secret"

    def _fake_put(*_args: Any, **_kwargs: Any) -> Any:
        calls["network"] += 1
        return SimpleNamespace(status_code=200, text="", json=lambda: {})

    def _fake_fetch(**_kwargs: Any) -> tuple[int, dict[str, Any]]:
        calls["network"] += 1
        return (
            0,
            {
                "autoscaler_status": {"policies": []},
                "gb10_status": {"desired_states": []},
                "slurm_status": {"jobs": []},
            },
        )

    def _track_acquire(*_args: Any, **_kwargs: Any) -> Any:
        calls["lock"] += 1
        return SimpleNamespace(
            owner_id="unexpected-lock",
            release=lambda **_release_kwargs: None,
        )

    monkeypatch.setattr("loom_cli.admin_cmd._resolve_admin_token", _resolve_token)
    monkeypatch.setattr("loom_cli.admin_cmd.httpx.put", _fake_put)
    monkeypatch.setattr("loom_cli.admin_cmd._fetch_environment_state", _fake_fetch)
    monkeypatch.setattr(RolloutLeaseManager, "acquire", _track_acquire)

    rc = _main_without_parser_exit(_admin_broker_argv(fixture, operation))

    assert rc == 1
    assert "profile targets do not match broker envelope" in capsys.readouterr().err
    assert calls == {"token": 0, "network": 0, "lock": 0}
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


@pytest.mark.parametrize(
    "override",
    [
        ["--rollout-id", _ROLLOUT_ID],
        ["--rollout-lock-dir", "/tmp/ignored-rollout-locks"],
        ["--rollout-lock-ttl-seconds", "14400"],
        ["--rollout-lock-evidence", "/tmp/ignored/../escape.json"],
        ["--force-rollout-lock"],
    ],
)
def test_broker_cluster_up_rejects_explicit_lock_override_even_when_value_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    override: list[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    _patch_cluster_up_happy_path(monkeypatch)

    rc = _main_without_parser_exit(_cluster_broker_argv(fixture) + override)

    assert rc == 1
    assert "manual rollout lock overrides are forbidden" in capsys.readouterr().err
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


@pytest.mark.parametrize("operation", ["apply", "check"])
@pytest.mark.parametrize(
    "override",
    [
        ["--rollout-id", _ROLLOUT_ID],
        ["--rollout-lock-dir", "/tmp/ignored-rollout-locks"],
        ["--rollout-lock-ttl-seconds", "14400"],
        ["--rollout-lock-evidence", "/tmp/ignored/../escape.json"],
        ["--force-rollout-lock"],
    ],
)
def test_broker_environment_state_rejects_every_manual_lock_override_before_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
    override: list[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        lambda _args: (_ for _ in ()).throw(
            AssertionError("profile must not load before override rejection")
        ),
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._resolve_admin_token",
        lambda _source: (_ for _ in ()).throw(
            AssertionError("token must not resolve before override rejection")
        ),
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._fetch_environment_state",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("network must not run before override rejection")
        ),
    )

    rc = _main_without_parser_exit(
        [
            *_admin_broker_argv(fixture, operation),
            *override,
        ]
    )

    assert rc == 1
    assert "manual rollout lock overrides are forbidden" in capsys.readouterr().err
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


def test_broker_cluster_up_rejects_symlinked_evidence_parent_before_lock_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    outside = tmp_path / "outside"
    outside.mkdir()
    step_dir = fixture.rollout_dir / "10-cluster-up"
    step_dir.rmdir()
    step_dir.symlink_to(outside, target_is_directory=True)

    def fail_network(_context: str | None) -> tuple[object, object, object, object]:
        raise AssertionError("cluster clients must not load before evidence validation")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", fail_network)

    rc = _main_without_parser_exit(_cluster_broker_argv(fixture))

    assert rc == 1
    assert "rollout lock evidence parent" in capsys.readouterr().err
    assert not (fixture.config.runtime_root / "mutation-locks").exists()
    assert not (outside / "rollout-lock.json").exists()


@pytest.mark.parametrize("configured", [None, "/tmp/wrong-kubeconfig"])
def test_broker_cluster_up_rejects_missing_or_mismatched_fixed_kubeconfig_before_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    configured: str | None,
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    _patch_cluster_up_happy_path(monkeypatch)
    if configured is None:
        monkeypatch.delenv("KUBECONFIG", raising=False)
    else:
        monkeypatch.setenv("KUBECONFIG", configured)

    rc = _main_without_parser_exit(_cluster_broker_argv(fixture))

    assert rc == 1
    assert "KUBECONFIG" in capsys.readouterr().err
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


def test_broker_cluster_up_rejects_non_service_owned_rollout_evidence_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    _patch_cluster_up_happy_path(monkeypatch)
    actual_uid = os.geteuid()
    monkeypatch.setattr("loom_cli.cluster_cmd.os.geteuid", lambda: actual_uid + 1)

    rc = _main_without_parser_exit(_cluster_broker_argv(fixture))

    assert rc == 1
    assert "service-owned" in capsys.readouterr().err
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


def test_broker_environment_state_rejects_envelope_before_token_or_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    profile_loads = 0

    def reject(_path: Path) -> tuple[Any, DriverEnvelope]:
        raise ValueError("invalid private envelope")

    def _load_profile(_args: Any) -> EnvironmentStateProfile:
        nonlocal profile_loads
        profile_loads += 1
        return _empty_environment_profile()

    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_broker_rollout_envelope",
        reject,
        raising=False,
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        _load_profile,
    )

    rc = _main_without_parser_exit(_admin_broker_argv(fixture, "apply"))

    assert rc == 1
    assert "invalid private envelope" in capsys.readouterr().err
    assert profile_loads == 0
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


def test_broker_environment_state_rejects_wrong_profile_path_before_reading_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = _broker_attempt_fixture(tmp_path)
    _patch_broker_attempt(monkeypatch, fixture)
    profile_loads = 0

    def _load_profile(_args: Any) -> EnvironmentStateProfile:
        nonlocal profile_loads
        profile_loads += 1
        return _empty_environment_profile()

    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        _load_profile,
    )
    argv = _admin_broker_argv(fixture, "apply")
    argv[argv.index("--file") + 1] = str(tmp_path / "attacker-controlled.toml")

    rc = _main_without_parser_exit(argv)

    assert rc == 1
    assert "profile path does not match broker rollout" in capsys.readouterr().err
    assert profile_loads == 0
    assert not (fixture.config.runtime_root / "mutation-locks").exists()


@pytest.mark.parametrize("operation", ["apply", "check"])
def test_environment_state_hidden_production_target_keeps_production_lock_diagnostic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
) -> None:
    profile_path = tmp_path / "production-control-plane.toml"
    profile_path.write_text(
        'environment = "development"\ncontrol_plane_environment = "production"\n',
        encoding="utf-8",
    )
    RolloutLeaseManager(tmp_path / "locks").acquire(
        environment="production",
        owner_id="production-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    def _unexpected_token(_source: str) -> str:
        raise AssertionError("token must not resolve while the production lock is held")

    def _unexpected_network(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("network must not run while the production lock is held")

    monkeypatch.setattr("loom_cli.admin_cmd._resolve_admin_token", _unexpected_token)
    monkeypatch.setattr("loom_cli.admin_cmd.httpx.put", _unexpected_network)
    monkeypatch.setattr("loom_cli.admin_cmd._fetch_environment_state", _unexpected_network)

    rc = main(
        [
            "admin",
            "environment-state",
            operation,
            "--file",
            str(profile_path),
            "--environment",
            "development",
            "--rollout-lock-dir",
            str(tmp_path / "locks"),
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "active rollout mutation lease" in err
    assert "production-owner" in err


def test_development_cluster_up_does_not_create_rollout_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)

    rc = main(
        [
            "cluster",
            "up",
            "--environment",
            "development",
            "--namespace",
            "loom-dev",
            "--rollout-lock-dir",
            str(tmp_path),
        ]
    )

    assert rc == 0
    assert not list(tmp_path.glob("*.lock"))
