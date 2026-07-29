from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.installed_preflight_commands import InstalledPreflightCommands
from loom_cli.rollout.operator.manifest_apply_contract import (
    MANIFEST_FIELD_MANAGER,
    server_side_apply_argv,
    server_side_schema_validation_argv,
)
from loom_cli.rollout.operator.model import APPROVED_REMOTE_URL, CandidateBinding
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


@pytest.mark.parametrize("result_age", (timedelta(0), timedelta(minutes=59)))
def test_prepare_admission_uses_only_fixed_exact_candidate_root_command(
    tmp_path: Path,
    result_age: timedelta,
) -> None:
    calls: list[dict[str, object]] = []
    payload = {
        "bootstrap_status": "converged",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "convergence_id": "c" * 64,
        "generation": 1,
        "kind": "staging_external_slurm_infrastructure_convergence",
        "node_count": 15,
        "receipt_path": (
            "/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure/"
            + "a" * 40
            + ".json"
        ),
        "receipt_sha256": "d" * 64,
        "requested_at": (datetime.now(UTC) - result_age).isoformat().replace("+00:00", "Z"),
        "result": "pass",
        "schema_version": 1,
        "source_controller": "oldlab-2",
        "source_controller_host": "trt-eai-oldlab-2",
    }

    def run(argv, **kwargs):
        calls.append({"argv": tuple(argv), **kwargs})
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            stderr="",
        )

    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=run,
    )
    candidate = CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )

    commands.prepare_admission(candidate)

    assert calls == [
        {
            "argv": (
                "/usr/bin/sudo",
                "-n",
                "/usr/local/libexec/loom-staging-external-slurm-authority",
                "converge-infrastructure",
                "--candidate-sha",
                "a" * 40,
                "--candidate-tree",
                "b" * 40,
            ),
            "cwd": None,
            "env": _environment(),
            "input": None,
            "timeout": 3720,
        }
    ]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: {**payload, "unexpected": True},
        lambda payload: {key: value for key, value in payload.items() if key != "convergence_id"},
        lambda payload: {**payload, "receipt_sha256": "not-a-digest"},
        lambda payload: {**payload, "requested_at": "2020-01-01T00:00:00Z"},
    ],
)
def test_prepare_admission_rejects_untrusted_producer_output(
    tmp_path: Path,
    mutate,
) -> None:
    payload = mutate(
        {
            "bootstrap_status": "converged",
            "candidate_sha": "a" * 40,
            "candidate_tree": "b" * 40,
            "convergence_id": "c" * 64,
            "generation": 1,
            "kind": "staging_external_slurm_infrastructure_convergence",
            "node_count": 15,
            "receipt_path": (
                "/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure/"
                + "a" * 40
                + ".json"
            ),
            "receipt_sha256": "d" * 64,
            "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "result": "pass",
            "schema_version": 1,
            "source_controller": "oldlab-2",
            "source_controller_host": "trt-eai-oldlab-2",
        }
    )
    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            stderr="",
        ),
    )
    candidate = CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )

    with pytest.raises(RuntimeError, match="preparation result"):
        commands.prepare_admission(candidate)


def test_prepare_admission_rejects_noncanonical_producer_output(tmp_path: Path) -> None:
    payload = {
        "bootstrap_status": "converged",
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "convergence_id": "c" * 64,
        "generation": 1,
        "kind": "staging_external_slurm_infrastructure_convergence",
        "node_count": 15,
        "receipt_path": (
            "/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure/"
            + "a" * 40
            + ".json"
        ),
        "receipt_sha256": "d" * 64,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "result": "pass",
        "schema_version": 1,
        "source_controller": "oldlab-2",
        "source_controller_host": "trt-eai-oldlab-2",
    }
    commands = InstalledPreflightCommands(
        _config(tmp_path),
        _environment(),
        run_subprocess=lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(payload, sort_keys=True) + "\n",
            stderr="",
        ),
    )
    candidate = CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha="a" * 40,
        image_tag="staging-aaaaaaa",
        fetched_at="2026-07-19T12:00:00Z",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )

    with pytest.raises(RuntimeError, match="noncanonical"):
        commands.prepare_admission(candidate)
