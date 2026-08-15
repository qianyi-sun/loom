from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.admin_cmd import dispatch

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _REPO_ROOT / "deploy/dev-fleet/capacity-control-plane.toml"
_EXECUTOR_PROFILE = _REPO_ROOT / "deploy/dev-fleet/capacity-pool-executor.toml.example"
_MANAGER_IMAGE = "ghcr.io/qianyi-sun/loom-capacity-manager@sha256:" + "a" * 64
_AUTHORITY = "00000000-0000-4000-8000-000000000901"


def test_admin_render_writes_only_the_exact_manifest(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = dispatch(
        [
            "capacity-control-plane",
            "render",
            "--file",
            str(_PROFILE),
            "--manager-image",
            _MANAGER_IMAGE,
            "--authority-incarnation",
            _AUTHORITY,
        ]
    )

    captured = capsys.readouterr()
    documents = [document for document in yaml.safe_load_all(captured.out) if document]
    assert result == 0
    assert captured.err == ""
    assert documents[0]["kind"] == "Namespace"
    assert documents[0]["metadata"]["name"] == "loom-dev"
    assert documents[5]["kind"] == "Deployment"
    assert documents[5]["spec"]["template"]["spec"]["containers"][0]["image"] == (_MANAGER_IMAGE)


def test_admin_render_rejects_invalid_release_without_partial_yaml(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = dispatch(
        [
            "capacity-control-plane",
            "render",
            "--file",
            str(_PROFILE),
            "--manager-image",
            "ghcr.io/qianyi-sun/loom-capacity-manager:latest",
            "--authority-incarnation",
            _AUTHORITY,
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert "immutable OCI reference" in captured.err


def test_admin_render_does_not_echo_rejected_profile_values(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_value = "do-not-log-this-accidental-password"
    profile = tmp_path / "capacity-control-plane.toml"
    profile.write_text(
        _PROFILE.read_text(encoding="utf-8") + f'\naccidental_password = "{secret_value}"\n',
        encoding="utf-8",
    )

    result = dispatch(
        [
            "capacity-control-plane",
            "render",
            "--file",
            str(profile),
            "--manager-image",
            _MANAGER_IMAGE,
            "--authority-incarnation",
            _AUTHORITY,
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: capacity control-plane render inputs are invalid\n"
    assert secret_value not in captured.err


def test_admin_render_does_not_echo_invalid_authority_input(
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_value = "do-not-log-this-invalid-authority"

    with pytest.raises(SystemExit) as stopped:
        dispatch(
            [
                "capacity-control-plane",
                "render",
                "--file",
                str(_PROFILE),
                "--manager-image",
                _MANAGER_IMAGE,
                "--authority-incarnation",
                secret_value,
            ]
        )

    captured = capsys.readouterr()
    assert stopped.value.code == 2
    assert captured.out == ""
    assert "must be a non-nil UUID" in captured.err
    assert secret_value not in captured.err


def test_admin_render_executor_writes_only_one_complete_inert_pool_config(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = dispatch(
        [
            "capacity-control-plane",
            "render-executor",
            "--file",
            str(_EXECUTOR_PROFILE),
            "--pool",
            "gb10",
            "--output",
            "config",
        ]
    )

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert result == 0
    assert captured.err == ""
    assert payload["pool_id"] == "gb10"
    assert payload["executor_id"].startswith("gb10-")
    assert payload["manager_origin"].endswith(".loom-dev.svc.cluster.local:8443")


def test_admin_render_executor_service_environment_is_nonsecret_and_zero_ceiling(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = dispatch(
        [
            "capacity-control-plane",
            "render-executor",
            "--file",
            str(_EXECUTOR_PROFILE),
            "--pool",
            "oldlab",
            "--output",
            "service-environment",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.err == ""
    assert "LOOM_CAPACITY_EXECUTOR_POOL=oldlab\n" in captured.out
    assert "LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING=0\n" in captured.out
    assert "token" not in captured.out.lower()
    assert "private" not in captured.out.lower()


def test_admin_render_executor_writes_only_the_selected_inventory_policy(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = dispatch(
        [
            "capacity-control-plane",
            "render-executor",
            "--file",
            str(_EXECUTOR_PROFILE),
            "--pool",
            "gb10",
            "--output",
            "inventory-policy",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert result == 0
    assert captured.err == ""
    assert payload["pool_id"] == "gb10"
    assert payload["controller_cluster"] == "gb10"
    assert payload["relevant_partitions"] == ["gb10-workers"]
    assert {node["pool_id"] for node in payload["nodes"]} == {"gb10"}
    assert "oldlab-node" not in captured.out


def test_admin_render_executor_rejects_invalid_inventory_without_partial_or_leaked_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rejected_value = "do-not-echo-this-inventory-policy-value"
    profile = tmp_path / "capacity-pool-executor.toml"
    profile.write_text(
        _EXECUTOR_PROFILE.read_text(encoding="utf-8")
        + f'\nunexpected_inventory_secret = "{rejected_value}"\n',
        encoding="utf-8",
    )

    result = dispatch(
        [
            "capacity-control-plane",
            "render-executor",
            "--file",
            str(profile),
            "--pool",
            "oldlab",
            "--output",
            "inventory-policy",
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: capacity pool-executor render inputs are invalid\n"
    assert rejected_value not in captured.err


def test_admin_status_executes_the_fixed_in_pod_zero_ceiling_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("not-a-live-config", encoding="utf-8")
    observed: list[list[str]] = []
    observed_timeouts: list[float | None] = []

    def run(
        command: list[str],
        *,
        check: bool,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        observed.append(command)
        observed_timeouts.append(timeout)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(subprocess, "run", run)

    result = dispatch(
        [
            "capacity-control-plane",
            "status",
            "--namespace",
            "loom-dev",
            "--kubeconfig",
            str(kubeconfig),
        ]
    )

    assert result == 0
    assert observed_timeouts == [15.0]
    assert observed == [
        [
            "kubectl",
            "--kubeconfig",
            str(kubeconfig.resolve()),
            "--request-timeout=10s",
            "--namespace",
            "loom-dev",
            "exec",
            "deployment/loom-capacity-manager",
            "-c",
            "manager",
            "--",
            "python",
            "-m",
            "loom_capacity_manager.health_probe",
            "--url",
            "https://127.0.0.1:8443/healthz",
            "--ca-file",
            "/var/run/loom-capacity-manager/runtime/credentials/server-ca.pem",
            "--certificate-file",
            "/var/run/loom-capacity-manager/runtime/credentials/health-certificate.pem",
            "--private-key-file",
            "/var/run/loom-capacity-manager/runtime/credentials/health-private-key.pem",
            "--server-certificate-file",
            "/var/run/loom-capacity-manager/runtime/credentials/server-certificate.pem",
        ]
    ]


def test_admin_status_timeout_is_a_bounded_failure(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(
        command: list[str],
        *,
        check: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(command, timeout)

    monkeypatch.setattr(subprocess, "run", run)

    result = dispatch(["capacity-control-plane", "status"])

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert "timed out" in captured.err


def test_admin_capacity_control_plane_has_no_mutating_subcommand(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as stopped:
        dispatch(["capacity-control-plane", "--help"])

    output = capsys.readouterr().out
    assert stopped.value.code == 0
    assert "{render,render-executor,status}" in output
    for forbidden in ("apply", "activate", "enable", "start", "set-ceiling"):
        with pytest.raises(SystemExit) as rejected:
            dispatch(["capacity-control-plane", forbidden])
        assert rejected.value.code == 2
