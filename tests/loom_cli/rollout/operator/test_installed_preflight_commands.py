from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.installed_preflight_commands import InstalledPreflightCommands
from loom_cli.rollout.operator.manifest_apply_contract import (
    MANIFEST_FIELD_MANAGER,
    server_side_apply_argv,
    server_side_schema_validation_argv,
)
from loom_cli.rollout.operator.readonly_preflight_authority import READONLY_KUBECONFIG_PATH
from loom_cli.rollout.systemd_readiness import probe_user_manager_readonly
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config


def _environment() -> dict[str, str]:
    return {
        "HOME": "/var/lib/loom-staging-rollout",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
        "USER": "loom-rollout",
    }


def test_adapters_preserve_exact_cwd_payload_and_readonly_kubeconfig(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def run(argv, **kwargs):
        calls.append({"argv": tuple(argv), **kwargs})
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    config = _config(tmp_path)
    commands = InstalledPreflightCommands(config, _environment(), run_subprocess=run)

    commands.git(["git", "status", "--porcelain=v1"])
    commands.readonly_json(("kubectl", "create", "--raw", "/review"), b'{"kind":"x"}')
    commands.manifest_server_dry_run("apiVersion: v1\nkind: ConfigMap\n")
    commands.manifest_schema_dry_run("apiVersion: v1\nkind: ConfigMap\n")
    commands.manifest_server_apply("apiVersion: v1\nkind: ConfigMap\n")
    commands.lifecycle_capacity_wait("loom-staging-capacity-aaaaaaaa-bbbbbbbb")
    commands.final_gate_helper(
        ("/usr/local/libexec/loom-staging-rollout-final-gate", "execute"),
        {
            "HOME": "/var/lib/loom-staging-rollout",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "XDG_RUNTIME_DIR": "/run/user/501",
        },
        3600,
    )

    assert calls[0]["cwd"] == config.runner_repo
    assert calls[0]["env"] == {
        **_environment(),
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
    }
    assert calls[1]["input"] == '{"kind":"x"}'
    assert calls[1]["env"] == {
        **_environment(),
        "KUBECONFIG": str(READONLY_KUBECONFIG_PATH),
    }
    assert calls[2]["argv"][-2:] == ("-f", "-")
    assert calls[2]["argv"] == server_side_apply_argv(
        config.namespace,
        kubeconfig=config.kubeconfig_path,
        dry_run=True,
    )
    assert f"--field-manager={MANIFEST_FIELD_MANAGER}" in calls[2]["argv"]
    assert "--server-side=true" in calls[2]["argv"]
    assert "--dry-run=server" in calls[2]["argv"]
    assert "--force-conflicts" not in calls[2]["argv"]
    assert calls[2]["input"] == "apiVersion: v1\nkind: ConfigMap\n"
    assert calls[2]["timeout"] == 120
    assert calls[3]["argv"] == server_side_schema_validation_argv(
        config.namespace,
        kubeconfig=config.kubeconfig_path,
    )
    assert "--force-conflicts" in calls[3]["argv"]
    assert "--dry-run=server" in calls[3]["argv"]
    assert calls[3]["input"] == "apiVersion: v1\nkind: ConfigMap\n"
    assert calls[3]["timeout"] == 120
    assert calls[4]["argv"] == server_side_apply_argv(
        config.namespace,
        kubeconfig=config.kubeconfig_path,
        output_json=True,
    )
    assert "--dry-run=server" not in calls[4]["argv"]
    assert calls[4]["input"] == "apiVersion: v1\nkind: ConfigMap\n"
    assert calls[4]["timeout"] == 120
    assert calls[5]["argv"][-4:] == (
        "wait",
        "--for=condition=complete",
        "job/loom-staging-capacity-aaaaaaaa-bbbbbbbb",
        "--timeout=1200s",
    )
    assert calls[5]["timeout"] == 1260
    assert calls[6]["timeout"] == 3600


def test_image_and_rehearsal_adapters_reject_authority_drift(tmp_path: Path) -> None:
    config = _config(tmp_path)
    commands = InstalledPreflightCommands(
        config,
        _environment(),
        run_subprocess=lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0, stdout="", stderr=""
        ),
    )

    with pytest.raises(ValueError, match="escaped exact candidate root"):
        commands.image(("docker", "build"), tmp_path / "other")
    with pytest.raises(ValueError, match="rehearsal helper execution authority"):
        commands.rehearsal_helper(("helper",), {"PATH": "/usr/bin"}, 10)
    with pytest.raises(ValueError, match="rehearsal helper execution authority"):
        commands.rehearsal_helper(
            ("helper",),
            {
                "HOME": "/var/lib/loom-staging-rollout",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/bin:/bin",
                "XDG_RUNTIME_DIR": "/run/user/501",
            },
            2401,
        )
    with pytest.raises(ValueError, match="command environment is invalid"):
        InstalledPreflightCommands(config, {**_environment(), "TOKEN": "forbidden"})
    with pytest.raises(ValueError, match="Job name"):
        commands.lifecycle_capacity_wait("../unsafe")


def test_systemd_preflight_is_allowlisted_and_uses_short_timeout(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    outputs = {
        ("systemctl", "--user", "show", "--property=Version", "--value"): "255.4\n",
        ("loginctl", "show-user", "995", "--property=Linger", "--value"): "yes\n",
        ("cat", "/proc/sys/kernel/random/boot_id"): ("7e88249c-33bf-4660-a8bd-d27fe375ee51\n"),
    }

    def run(argv, **kwargs):
        calls.append({"argv": tuple(argv), **kwargs})
        return SimpleNamespace(returncode=0, stdout=outputs.get(tuple(argv), ""), stderr="")

    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=run,
    )

    commands.systemd_preflight(("systemd-run", "--user", "/usr/bin/true"))
    evidence = probe_user_manager_readonly(commands.systemd_preflight, uid=995)

    assert calls[0]["timeout"] == 10
    assert calls[0]["argv"][0] == "systemd-run"
    assert evidence is not None
    assert evidence.boot_id == "7e88249c-33bf-4660-a8bd-d27fe375ee51"
    assert all(call["timeout"] == 10 for call in calls)
    with pytest.raises(ValueError, match="outside authority"):
        commands.systemd_preflight(("journalctl", "--user"))
    with pytest.raises(ValueError, match="outside authority"):
        commands.systemd_preflight(("cat", "/etc/passwd"))


def test_candidate_source_ssh_has_sub_dag_cancellation_timeout(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def run(argv, **kwargs):
        calls.append({"argv": tuple(argv), **kwargs})
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=run,
    )

    commands.candidate_source(("ssh", "trt-gb10-1", "probe"))

    assert calls[0]["argv"] == ("ssh", "trt-gb10-1", "probe")
    assert calls[0]["timeout"] == 12


def test_gb10_fleet_ssh_is_bounded_below_dag_timeout(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def run(argv, **kwargs):
        calls.append({"argv": tuple(argv), **kwargs})
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")

    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=run,
    )

    commands.gb10_fleet(("ssh", "trt-gb10-1", "probe"))

    assert calls[0]["argv"] == ("ssh", "trt-gb10-1", "probe")
    assert calls[0]["timeout"] == 5


def test_gb10_supervisor_controller_ssh_forwards_only_bounded_typed_stdin(
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []

    def run(argv, **kwargs):
        calls.append({"argv": tuple(argv), **kwargs})
        return SimpleNamespace(returncode=0, stdout="{}\n", stderr="")

    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=run,
    )

    commands.gb10_supervisor_controller(("ssh", "fixed-controller"), '{"schema_version":1}\n')

    assert calls == [
        {
            "argv": ("ssh", "fixed-controller"),
            "cwd": None,
            "env": _environment(),
            "input": '{"schema_version":1}\n',
            "timeout": 30,
        }
    ]
    with pytest.raises(ValueError, match="payload is invalid"):
        commands.gb10_supervisor_controller(
            ("ssh", "fixed-controller"), "x" * (4 * 1024 * 1024 + 1)
        )
