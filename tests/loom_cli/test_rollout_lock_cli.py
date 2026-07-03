from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import ApplyResult, ClusterStatus, ComponentStatus
from loom_cli.environment_state import EnvironmentStateProfile
from loom_cli.rollout_lock import RolloutLeaseManager


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
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _context: (object(), object(), object(), object()),
    )
    monkeypatch.setattr("loom_cli.cluster_cmd.load_cluster_config", lambda _path: object())
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
    monkeypatch.setattr("loom_cli.cluster_cmd.wait_for_ready", lambda *_args, **_kwargs: _ready_status())
    monkeypatch.setattr("loom_cli.cluster_cmd.rendered_image_checks", lambda *_args, **_kwargs: [])


def _empty_environment_profile() -> EnvironmentStateProfile:
    return EnvironmentStateProfile(
        environment="staging",
        control_plane_environment="production",
        autoscaler_policies=[],
        gb10_desired_states=[],
        catalog_provisioning={},
        external_slurm_runner_prerequisites={},
        external_slurm_autoscaler_supervisors=[],
    )


def test_cluster_up_protected_conflict_fails_before_loading_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    RolloutLeaseManager(tmp_path).acquire(
        environment="staging",
        owner_id="owner-a",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    def _load_clients(_context: str | None) -> tuple[object, object, object, object]:
        raise AssertionError("cluster clients should not load when lock is held")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _load_clients)

    rc = main([
        "cluster",
        "up",
        "--environment",
        "staging",
        "--namespace",
        "loom-staging",
        "--rollout-lock-dir",
        str(tmp_path),
    ])

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
    evidence_path = tmp_path / "lock-evidence.json"

    rc = main([
        "cluster",
        "up",
        "--environment",
        "staging",
        "--namespace",
        "loom-staging",
        "--rollout-lock-dir",
        str(tmp_path),
        "--rollout-id",
        "staging-d46a16c",
        "--rollout-lock-evidence",
        str(evidence_path),
    ])

    assert rc == 0
    active_record = json.loads((tmp_path / "staging.lock").read_text(encoding="utf-8"))
    assert active_record["release_status"] == "released"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert [event["event"] for event in evidence["events"]] == [
        "acquired",
        "released",
    ]
    assert evidence["events"][0]["owner_id"] == "staging-d46a16c"


def test_environment_state_apply_protected_conflict_reports_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    RolloutLeaseManager(tmp_path).acquire(
        environment="staging",
        owner_id="cluster-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )
    monkeypatch.setattr(
        "loom_cli.admin_cmd._load_environment_state_profile_from_args",
        lambda _args: _empty_environment_profile(),
    )
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main([
        "admin",
        "environment-state",
        "apply",
        "--file",
        "deploy/environment-state/staging.toml",
        "--environment",
        "staging",
        "--rollout-lock-dir",
        str(tmp_path),
    ])

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
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main([
        "admin",
        "environment-state",
        "check",
        "--file",
        "deploy/environment-state/staging.toml",
        "--environment",
        "staging",
        "--rollout-id",
        "env-state-check-staging-d46a16c",
        "--rollout-lock-dir",
        str(tmp_path),
        "--rollout-lock-evidence",
        str(evidence_path),
        "--format",
        "json",
    ])

    assert rc == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    events = json.loads(evidence_path.read_text(encoding="utf-8"))["events"]
    assert events[0]["event"] == "acquired"
    assert events[0]["owner_id"] == "env-state-check-staging-d46a16c"
    assert events[1]["event"] == "released"


def test_development_cluster_up_does_not_create_rollout_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_cluster_up_happy_path(monkeypatch)

    rc = main([
        "cluster",
        "up",
        "--environment",
        "development",
        "--namespace",
        "loom-dev",
        "--rollout-lock-dir",
        str(tmp_path),
    ])

    assert rc == 0
    assert not list(tmp_path.glob("*.lock"))
