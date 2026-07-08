from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import pytest
from scripts.ops import staging_validation_capacity_runner as runner


def test_active_payload_sets_host_intents_slots_and_idle_ttl() -> None:
    current = {
        "image_tag": "staging-abc1234",
        "max_concurrent": 10,
        "env_config_version": "staging-abc1234",
        "source_git_commit": "a" * 40,
        "rollout_policy": {"mode": "all"},
        "env": {"LOOM_WORKER_BLOCKING_IO_MAX_WORKERS": "40"},
    }

    payload = runner.desired_state_payload(
        current,
        hosts=("trt-gb10-1", "trt-gb10-2"),
        intent="active",
        ttl_seconds=14400,
        adjust_idle_exit=True,
    )

    assert payload["target_slots"] == 20
    assert payload["host_intents"] == {
        "trt-gb10-1": "active",
        "trt-gb10-2": "active",
    }
    assert payload["env"]["LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS"] == "14400"
    assert payload["env"]["LOOM_WORKER_BLOCKING_IO_MAX_WORKERS"] == "40"


def test_release_payload_zeroes_slots_and_does_not_extend_idle_ttl() -> None:
    current = {
        "image_tag": "staging-abc1234",
        "max_concurrent": 10,
        "env_config_version": "staging-abc1234",
        "env": {"LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS": "14400"},
    }

    payload = runner.desired_state_payload(
        current,
        hosts=("trt-gb10-1",),
        intent="stopped",
        ttl_seconds=14400,
        adjust_idle_exit=False,
    )

    assert payload["target_slots"] == 0
    assert payload["host_intents"] == {"trt-gb10-1": "stopped"}
    assert payload["env"]["LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS"] == "14400"


@pytest.mark.parametrize(
    ("exit_code", "configured", "expected"),
    [
        (0, "auto", "stopped"),
        (1, "auto", "draining"),
        (1, "stopped", "stopped"),
        (0, "draining", "draining"),
    ],
)
def test_release_intent_for_result(exit_code: int, configured: str, expected: str) -> None:
    assert runner.release_intent_for_result(exit_code, configured) == expected


def test_status_mismatches_require_fresh_active_docker_worker() -> None:
    status = {
        "nodes": [
            {
                "hostname": "trt-gb10-1",
                "desired_intent": "active",
                "current_intent": "active",
                "apply_state": "applied",
                "current_image_tag": "staging-abc1234",
                "current_env_config_version": "staging-abc1234",
                "worker_fresh": False,
                "worker_backend_names": ["docker"],
            },
        ],
    }

    errors = runner.status_mismatches(
        status,
        hosts=("trt-gb10-1",),
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
    )

    assert errors == ["trt-gb10-1: worker_fresh=False"]


def test_status_mismatches_accept_stopped_nonfresh_worker() -> None:
    status = {
        "nodes": [
            {
                "hostname": "trt-gb10-1",
                "desired_intent": "stopped",
                "current_intent": "stopped",
                "apply_state": "stopped",
                "current_image_tag": "staging-abc1234",
                "current_env_config_version": "staging-abc1234",
                "worker_fresh": False,
                "worker_backend_names": [],
            },
        ],
    }

    assert (
        runner.status_mismatches(
            status,
            hosts=("trt-gb10-1",),
            intent="stopped",
            image_tag="staging-abc1234",
            env_config_version="staging-abc1234",
        )
        == []
    )


def test_parse_args_rejects_literal_admin_token() -> None:
    with pytest.raises(SystemExit):
        runner._parse_args(
            [
                "--cp-url",
                "http://127.0.0.1:18081",
                "--admin-token",
                "loom_admin_raw",
                "--ssh-config",
                "deploy/worker-pools/gb10/ssh_config",
                "--evidence-dir",
                "/tmp/evidence",
            ],
        )


def test_parse_args_rejects_stdin_admin_token_source() -> None:
    with pytest.raises(SystemExit):
        runner._parse_args(
            [
                "--cp-url",
                "http://127.0.0.1:18081",
                "--admin-token",
                "-",
                "--ssh-config",
                "deploy/worker-pools/gb10/ssh_config",
                "--evidence-dir",
                "/tmp/evidence",
            ],
        )


def test_parse_ttl_suffixes() -> None:
    assert runner._parse_ttl("2h") == 7200
    assert runner._parse_ttl("45m") == 2700
    with pytest.raises(argparse.ArgumentTypeError):
        runner._parse_ttl("forever")


def _node_agent_args(tmp_path: Path, *, dry_run: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        dry_run=dry_run,
        node_agent_command_timeout=0.01,
        node_agent_service="loom-gb10-node-agent.service",
        ssh_config=tmp_path / "ssh_config",
        ssh_connect_timeout=10,
        ssh_identity=None,
    )


def test_node_agent_start_uses_nonblocking_systemd_command(tmp_path: Path) -> None:
    result = runner.start_node_agents(
        _node_agent_args(tmp_path, dry_run=True),
        hosts=("trt-gb10-1",),
        phase="activate",
        evidence_dir=tmp_path,
    )

    assert result.ok is True
    log = next((Path(result.artifact or "")).glob("trt-gb10-1.log")).read_text()
    assert "systemctl --user start --no-block loom-gb10-node-agent.service" in log


def test_node_agent_start_times_out_per_host(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    created: list[HangingPopen] = []

    class HangingPopen:
        def __init__(self, cmd: list[str], **_: object) -> None:
            self.cmd = cmd
            self.killed = False
            created.append(self)

        def wait(self, timeout: float | None = None) -> int:
            if timeout is None:
                raise AssertionError("node-agent start must use a bounded wait timeout")
            raise subprocess.TimeoutExpired(cmd=self.cmd, timeout=timeout)

        def kill(self) -> None:
            self.killed = True

    monkeypatch.setattr(runner.subprocess, "Popen", HangingPopen)

    result = runner.start_node_agents(
        _node_agent_args(tmp_path),
        hosts=("trt-gb10-8",),
        phase="draining",
        evidence_dir=tmp_path,
    )

    assert result.ok is False
    assert result.detail == "node-agent failed on trt-gb10-8"
    assert created and created[0].killed is True
    log = next((Path(result.artifact or "")).glob("trt-gb10-8.log")).read_text()
    assert "timed_out_after_seconds=0.01" in log
    assert "exit_code=timeout" in log
