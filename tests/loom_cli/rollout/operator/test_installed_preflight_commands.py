from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.installed_preflight_commands import InstalledPreflightCommands
from loom_cli.rollout.operator.readonly_preflight_authority import READONLY_KUBECONFIG_PATH
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

    assert calls[0]["cwd"] == config.runner_repo
    assert calls[1]["input"] == '{"kind":"x"}'
    assert calls[1]["env"] == {
        **_environment(),
        "KUBECONFIG": str(READONLY_KUBECONFIG_PATH),
    }
    assert calls[2]["argv"][-2:] == ("-f", "-")
    assert calls[2]["input"] == "apiVersion: v1\nkind: ConfigMap\n"


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
    with pytest.raises(ValueError, match="command environment is invalid"):
        InstalledPreflightCommands(config, {**_environment(), "TOKEN": "forbidden"})
