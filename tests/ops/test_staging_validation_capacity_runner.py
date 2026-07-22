from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pytest
from scripts.ops import staging_validation_capacity_runner as runner

ENVIRONMENT = "staging"
POOL_NAME = "gb10"
SOURCE_SHA = "abc1234" + "a" * 33


def _active_node(hostname: str, worker_id: str) -> dict[str, object]:
    return {
        "hostname": hostname,
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "active",
        "current_intent": "active",
        "desired_max_concurrent": 10,
        "current_max_concurrent": 10,
        "apply_state": "applied",
        "current_image_tag": "staging-abc1234",
        "current_env_config_version": "staging-abc1234",
        "source_git_commit": SOURCE_SHA,
        "source_git_dirty": False,
        "worker_id": worker_id,
        "worker_status": "active",
        "worker_fresh": True,
        "worker_backend_names": ["docker"],
    }


def test_active_payload_sets_host_intents_slots_and_idle_ttl() -> None:
    current = {
        "image_tag": "staging-abc1234",
        "max_concurrent": 10,
        "env_config_version": "staging-abc1234",
        "source_git_commit": "a" * 40,
        "host_intents": {
            "trt-gb10-7": "stopped",
            "trt-gb10-8": "active",
        },
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
        "trt-gb10-7": "stopped",
        "trt-gb10-8": "active",
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
    assert payload["host_intents"] == {
        "trt-gb10-1": "stopped",
        "trt-gb10-7": "stopped",
    }
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
        "unlinked_workers": [],
        "nodes": [
            {
                "hostname": "trt-gb10-1",
                "environment": ENVIRONMENT,
                "pool_name": POOL_NAME,
                "desired_intent": "active",
                "current_intent": "active",
                "desired_max_concurrent": 10,
                "current_max_concurrent": 10,
                "apply_state": "applied",
                "current_image_tag": "staging-abc1234",
                "current_env_config_version": "staging-abc1234",
                "source_git_commit": SOURCE_SHA,
                "source_git_dirty": False,
                "worker_id": "worker-1",
                "worker_status": "active",
                "worker_fresh": False,
                "worker_backend_names": ["docker"],
            },
        ],
    }

    errors = runner.status_mismatches(
        status,
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == ["trt-gb10-1: worker_fresh=False"]


def test_status_mismatches_accept_stopped_nonfresh_worker() -> None:
    status = {
        "unlinked_workers": [],
        "nodes": [
            {
                "hostname": "trt-gb10-1",
                "environment": ENVIRONMENT,
                "pool_name": POOL_NAME,
                "desired_intent": "stopped",
                "current_intent": "stopped",
                "desired_max_concurrent": 10,
                "current_max_concurrent": 10,
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
            environment=ENVIRONMENT,
            pool_name=POOL_NAME,
            intent="stopped",
            image_tag="staging-abc1234",
            env_config_version="staging-abc1234",
            source_git_commit=None,
        )
        == []
    )


def test_status_mismatches_accept_draining_nonfresh_worker() -> None:
    status = {
        "unlinked_workers": [],
        "nodes": [
            {
                "hostname": "trt-gb10-1",
                "environment": ENVIRONMENT,
                "pool_name": POOL_NAME,
                "desired_intent": "draining",
                "current_intent": "draining",
                "desired_max_concurrent": 10,
                "current_max_concurrent": 10,
                "apply_state": "draining",
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
            environment=ENVIRONMENT,
            pool_name=POOL_NAME,
            intent="draining",
            image_tag="staging-abc1234",
            env_config_version="staging-abc1234",
            source_git_commit=None,
        )
        == []
    )


def test_status_mismatches_require_nonfresh_draining_worker() -> None:
    status = {
        "unlinked_workers": [],
        "nodes": [
            {
                "hostname": "trt-gb10-1",
                "environment": ENVIRONMENT,
                "pool_name": POOL_NAME,
                "desired_intent": "draining",
                "current_intent": "draining",
                "desired_max_concurrent": 10,
                "current_max_concurrent": 10,
                "apply_state": "draining",
                "current_image_tag": "staging-abc1234",
                "current_env_config_version": "staging-abc1234",
                "worker_fresh": True,
                "worker_backend_names": ["docker"],
            },
        ],
    }

    assert runner.status_mismatches(
        status,
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="draining",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=None,
    ) == ["trt-gb10-1: worker still fresh after draining intent"]


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


def test_default_hosts_exclude_only_registered_blocker() -> None:
    assert len(runner.FULL_GB10_HOSTS) == 15
    assert len(runner.DEFAULT_HOSTS) == 14
    assert "trt-gb10-7" not in runner.DEFAULT_HOSTS
    assert set(runner.FULL_GB10_HOSTS) - set(runner.DEFAULT_HOSTS) == {"trt-gb10-7"}
    assert runner.EXPECTED_MAX_CONCURRENT == 10


@pytest.mark.parametrize("max_concurrent", [8, 11, 0, None, "invalid"])
def test_payload_rejects_max_concurrent_drift(max_concurrent: object) -> None:
    current = {
        "image_tag": "staging-abc1234",
        "max_concurrent": max_concurrent,
        "env_config_version": "staging-abc1234",
    }

    with pytest.raises(ValueError, match="max_concurrent"):
        runner.desired_state_payload(
            current,
            hosts=runner.DEFAULT_HOSTS,
            intent="active",
            ttl_seconds=14400,
            adjust_idle_exit=False,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("desired_max_concurrent", 11), ("current_max_concurrent", 8)],
)
def test_status_mismatches_reject_concurrency_drift(field: str, value: int) -> None:
    node = {
        "hostname": "trt-gb10-1",
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "active",
        "current_intent": "active",
        "desired_max_concurrent": 10,
        "current_max_concurrent": 10,
        "apply_state": "applied",
        "current_image_tag": "staging-abc1234",
        "current_env_config_version": "staging-abc1234",
        "source_git_commit": SOURCE_SHA,
        "source_git_dirty": False,
        "worker_id": "worker-1",
        "worker_status": "active",
        "worker_fresh": True,
        "worker_backend_names": ["docker"],
    }
    node[field] = value

    errors = runner.status_mismatches(
        {"nodes": [node], "unlinked_workers": []},
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == [f"trt-gb10-1: {field}={value!r}"]


@pytest.mark.parametrize(
    ("excluded_worker_fresh", "expected"),
    [
        (False, []),
        (True, ["trt-gb10-7: temporarily excluded host still has a fresh worker"]),
    ],
)
def test_status_mismatches_fail_closed_on_fresh_excluded_worker(
    excluded_worker_fresh: bool,
    expected: list[str],
) -> None:
    active = {
        "hostname": "trt-gb10-1",
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "active",
        "current_intent": "active",
        "desired_max_concurrent": 10,
        "current_max_concurrent": 10,
        "apply_state": "applied",
        "current_image_tag": "staging-abc1234",
        "current_env_config_version": "staging-abc1234",
        "source_git_commit": SOURCE_SHA,
        "source_git_dirty": False,
        "worker_id": "worker-1",
        "worker_status": "active",
        "worker_fresh": True,
        "worker_backend_names": ["docker"],
    }
    excluded = {
        "hostname": "trt-gb10-7",
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "stopped",
        "current_intent": "stopped",
        "apply_state": "stopped",
        "worker_fresh": excluded_worker_fresh,
        "worker_backend_names": ["docker"],
    }

    errors = runner.status_mismatches(
        {"nodes": [active, excluded], "unlinked_workers": []},
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == expected


def test_status_mismatches_rejects_fresh_undeclared_host() -> None:
    active = {
        "hostname": "trt-gb10-1",
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "active",
        "current_intent": "active",
        "desired_max_concurrent": 10,
        "current_max_concurrent": 10,
        "apply_state": "applied",
        "current_image_tag": "staging-abc1234",
        "current_env_config_version": "staging-abc1234",
        "source_git_commit": SOURCE_SHA,
        "source_git_dirty": False,
        "worker_id": "worker-1",
        "worker_status": "active",
        "worker_fresh": True,
        "worker_backend_names": ["docker"],
    }
    rogue = {
        "hostname": "trt-gb10-16",
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "active",
        "current_intent": "active",
        "apply_state": "applied",
        "worker_status": "active",
        "worker_fresh": True,
        "worker_backend_names": ["docker"],
    }

    errors = runner.status_mismatches(
        {"nodes": [active, rogue], "unlinked_workers": []},
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == ["trt-gb10-16: undeclared host reports active worker state"]


def test_status_mismatches_requires_unlinked_worker_inventory() -> None:
    errors = runner.status_mismatches(
        {"nodes": []},
        hosts=(),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == ["unlinked_workers: missing or invalid worker inventory"]


@pytest.mark.parametrize("hostname", ["trt-gb10-7", "trt-gb10-1"])
def test_status_mismatches_rejects_fresh_unlinked_worker(hostname: str) -> None:
    errors = runner.status_mismatches(
        {
            "nodes": [],
            "unlinked_workers": [
                {
                    "worker_id": f"duplicate-{hostname}",
                    "hostname": hostname,
                    "pool_name": "gb10",
                    "worker_fresh": True,
                }
            ],
        },
        hosts=(),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == [f"{hostname}: unlinked fresh worker duplicate-{hostname}"]


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("source_git_commit", "b" * 40, "source_git_commit="),
        ("source_git_dirty", True, "source_git_dirty=True"),
        ("worker_id", "", "worker_id=''"),
        ("worker_status", "draining", "worker_status='draining'"),
        (
            "worker_backend_names",
            "docker",
            "worker_backend_names must be a list of non-empty strings",
        ),
        ("environment", "production", "environment='production'"),
        ("pool_name", "other-pool", "pool_name='other-pool'"),
    ],
)
def test_status_mismatches_rejects_active_candidate_or_identity_drift(
    field: str,
    value: object,
    expected: str,
) -> None:
    node = _active_node("trt-gb10-1", "worker-1")
    node[field] = value

    errors = runner.status_mismatches(
        {"nodes": [node], "unlinked_workers": []},
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert any(expected in error for error in errors)


def test_status_mismatches_rejects_reused_or_unlinked_active_worker_ids() -> None:
    first = _active_node("trt-gb10-1", "worker-shared")
    second = _active_node("trt-gb10-2", "worker-shared")
    status = {
        "nodes": [first, second],
        "unlinked_workers": [
            {
                "worker_id": "worker-shared",
                "hostname": "trt-gb10-7",
                "pool_name": POOL_NAME,
                "worker_fresh": False,
            }
        ],
    }

    errors = runner.status_mismatches(
        status,
        hosts=("trt-gb10-1", "trt-gb10-2"),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert "trt-gb10-1: worker_id='worker-shared' also appears unlinked" in errors
    assert (
        "trt-gb10-2: worker_id='worker-shared' is already linked to trt-gb10-1" in errors
    )
    assert "trt-gb10-2: worker_id='worker-shared' also appears unlinked" in errors


@pytest.mark.parametrize(
    ("image_tag", "env_config_version", "source_git_commit", "expected"),
    [
        (
            "staging-abc1234",
            "staging-deadbee",
            SOURCE_SHA,
            "env_config_version must exactly match",
        ),
        ("latest", "latest", SOURCE_SHA, "image_tag must be staging-"),
        (
            "staging-abc1234",
            "staging-abc1234",
            "b" * 40,
            "image_tag SHA must match",
        ),
        (
            "staging-abc1234",
            "staging-abc1234",
            "abc1234",
            "full lowercase 40-character SHA",
        ),
    ],
)
def test_candidate_identity_rejects_self_inconsistent_desired_state(
    image_tag: str,
    env_config_version: str,
    source_git_commit: str,
    expected: str,
) -> None:
    errors = runner._candidate_identity_mismatches(
        image_tag=image_tag,
        env_config_version=env_config_version,
        source_git_commit=source_git_commit,
    )

    assert any(expected in error for error in errors)


def test_runner_rejects_invalid_candidate_before_any_desired_state_mutation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("RUNNER_ADMIN_TOKEN", "loom_admin_test_only")
    args = runner._parse_args(
        [
            "--cp-url",
            "http://127.0.0.1:18081",
            "--admin-token",
            "env:RUNNER_ADMIN_TOKEN",
            "--ssh-config",
            str(tmp_path / "ssh_config"),
            "--evidence-dir",
            str(tmp_path / "evidence"),
        ]
    )
    current = {
        "image_tag": "staging-abc1234",
        "env_config_version": "staging-abc1234",
        "source_git_commit": "b" * 40,
        "max_concurrent": 10,
        "host_intents": {},
    }
    put_payloads: list[dict[str, object]] = []
    monkeypatch.setattr(runner, "fetch_desired_state", lambda *_args: current)
    monkeypatch.setattr(
        runner,
        "put_desired_state",
        lambda _args, _token, payload: put_payloads.append(payload) or payload,
    )

    assert runner.run(args) == 1
    assert put_payloads == []
    summary = json.loads(
        (args.evidence_dir / "staging-validation-capacity-runner-summary.json").read_text()
    )
    assert [phase["phase"] for phase in summary["phases"]] == [
        "runner-error",
        "desired-state-release-skipped",
    ]


def test_status_mismatches_rejects_excluded_host_not_converged_to_stopped() -> None:
    active = _active_node("trt-gb10-1", "worker-1")
    excluded = {
        "hostname": "trt-gb10-7",
        "environment": ENVIRONMENT,
        "pool_name": POOL_NAME,
        "desired_intent": "active",
        "current_intent": "draining",
        "apply_state": "draining",
        "worker_fresh": False,
    }

    errors = runner.status_mismatches(
        {"nodes": [active, excluded], "unlinked_workers": []},
        hosts=("trt-gb10-1",),
        environment=ENVIRONMENT,
        pool_name=POOL_NAME,
        intent="active",
        image_tag="staging-abc1234",
        env_config_version="staging-abc1234",
        source_git_commit=SOURCE_SHA,
    )

    assert errors == [
        "trt-gb10-7: excluded desired_intent='active'",
        "trt-gb10-7: excluded current_intent='draining'",
        "trt-gb10-7: excluded apply_state='draining'",
    ]


@pytest.mark.parametrize(
    "value",
    [
        "trt-gb10-7",
        "trt-gb10-1,trt-gb10-7",
        "trt-gb10-1,trt-gb10-1",
        "unregistered-host",
    ],
)
def test_host_list_rejects_excluded_duplicate_or_unknown_hosts(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        runner._host_list(value)


def test_host_list_accepts_only_exact_merged_active_set() -> None:
    rendered = ",".join(runner.DEFAULT_HOSTS)

    assert runner._host_list(rendered) == runner.DEFAULT_HOSTS
    with pytest.raises(argparse.ArgumentTypeError, match="exact merged active"):
        runner._host_list(",".join(reversed(runner.DEFAULT_HOSTS)))
    with pytest.raises(argparse.ArgumentTypeError, match="exact merged active"):
        runner._host_list(",".join(runner.DEFAULT_HOSTS[:-1]))


def test_payload_forces_excluded_host_stopped_and_preserves_other_intents() -> None:
    current = {
        "image_tag": "staging-abc1234",
        "max_concurrent": 10,
        "env_config_version": "staging-abc1234",
        "host_intents": {
            "trt-gb10-7": "active",
            "trt-gb10-8": "draining",
        },
    }

    payload = runner.desired_state_payload(
        current,
        hosts=("trt-gb10-1",),
        intent="active",
        ttl_seconds=14400,
        adjust_idle_exit=False,
    )

    assert payload["target_slots"] == 10
    assert payload["host_intents"] == {
        "trt-gb10-1": "active",
        "trt-gb10-7": "stopped",
        "trt-gb10-8": "draining",
    }


def test_payload_rejects_runtime_readdition_of_excluded_host() -> None:
    current = {
        "image_tag": "staging-abc1234",
        "max_concurrent": 10,
        "env_config_version": "staging-abc1234",
    }

    with pytest.raises(ValueError, match="merged re-admission"):
        runner.desired_state_payload(
            current,
            hosts=("trt-gb10-7",),
            intent="active",
            ttl_seconds=14400,
            adjust_idle_exit=False,
        )


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
