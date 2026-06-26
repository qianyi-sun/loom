from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

from loom_cli import gb10_agent
from loom_cli.gb10_agent import (
    DesiredState,
    LocalWorkerState,
    build_plan,
    render_env_updates,
)


def test_build_plan_blocks_non_canary_host_until_policy_expands() -> None:
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="new-image",
        max_concurrent=10,
        env_config_version="gb10-env-v2",
        rollout_policy={"mode": "canary", "canary_hosts": ["trt-gb10-1"]},
        env={},
    )
    local = LocalWorkerState(
        hostname="trt-gb10-2",
        image_tag="old-image",
        pool_name="gb10-arm64",
        max_concurrent=5,
        env_config_version="gb10-env-v1",
    )

    plan = build_plan(desired, local)

    assert plan.needs_apply is True
    assert plan.blocked_reason == "waiting_for_canary"
    assert "image_tag" in plan.changes
    assert "max_concurrent" in plan.changes
    assert "env_config_version" in plan.changes


def test_build_plan_allows_canary_host_and_force_override() -> None:
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="new-image",
        max_concurrent=10,
        env_config_version="gb10-env-v2",
        rollout_policy={"mode": "canary", "canary_hosts": ["trt-gb10-1"]},
        env={},
    )
    blocked_local = LocalWorkerState(
        hostname="trt-gb10-2",
        image_tag="old-image",
        pool_name="gb10-arm64",
        max_concurrent=5,
        env_config_version="gb10-env-v1",
    )
    canary_local = LocalWorkerState(
        hostname="trt-gb10-1",
        image_tag="old-image",
        pool_name="gb10-arm64",
        max_concurrent=5,
        env_config_version="gb10-env-v1",
    )

    assert build_plan(desired, canary_local).blocked_reason is None
    assert build_plan(desired, blocked_local, force=True).blocked_reason is None


def test_render_env_updates_preserves_comments_and_appends_missing_keys(tmp_path: Path) -> None:
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text(
        "# operator note\n"
        "LOOM_IMAGE_TAG=old-image\n"
        "LOOM_WORKER_MAX_CONCURRENT=5\n",
        encoding="utf-8",
    )

    rendered = render_env_updates(
        env_file,
        {
            "LOOM_IMAGE_TAG": "new-image",
            "LOOM_WORKER_MAX_CONCURRENT": "10",
            "LOOM_WORKER_POOL_NAME": "gb10-arm64",
            "LOOM_WORKER_ENV_CONFIG_VERSION": "gb10-env-v2",
        },
    )

    assert rendered.splitlines() == [
        "# operator note",
        "LOOM_IMAGE_TAG=new-image",
        "LOOM_WORKER_MAX_CONCURRENT=10",
        "LOOM_WORKER_POOL_NAME=gb10-arm64",
        "LOOM_WORKER_ENV_CONFIG_VERSION=gb10-env-v2",
    ]


def test_apply_dry_run_uses_every_compose_file(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=old-image\n"
        "LOOM_WORKER_TOKEN=loom_w_existing_secret\n"
        "LOOM_WORKER_MINIO_SECRET_KEY=minio-secret\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=5\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=old-env\n",
        encoding="utf-8",
    )
    base = tmp_path / "docker-compose.remote-worker.yml"
    hostnet = tmp_path / "docker-compose.gb10-hostnet.yml"
    base.write_text("services: {}\n", encoding="utf-8")
    hostnet.write_text("services: {}\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="new-image",
        max_concurrent=10,
        env_config_version="new-env",
        rollout_policy={"mode": "all"},
        env={},
    )
    commands: list[list[str]] = []

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(gb10_agent, "_report_node", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gb10_agent,
        "_run",
        lambda argv, *, dry_run: commands.append(list(argv)),
    )

    rc = gb10_agent._apply(SimpleNamespace(
        cp_url="http://cp:8080",
        admin_token="env:LOOM_ADMIN_TOKEN",
        environment="production",
        pool_name="gb10-arm64",
        hostname="trt-gb10-1",
        env_file=env_file,
        compose_file=[base, hostnet],
        service="worker",
        drain_timeout_sec=600,
        dry_run=True,
        rollback=False,
        force=False,
        format="text",
    ))

    assert rc == 0
    assert len(commands) == 3
    for command in commands:
        assert command.count("-f") == 2
        assert str(base) in command
        assert str(hostnet) in command
    out = capsys.readouterr().out
    assert "LOOM_IMAGE_TAG=new-image" in out
    assert "loom_w_existing_secret" not in out
    assert "minio-secret" not in out


def test_apply_failure_leaves_env_file_retryable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=old-image\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=5\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=old-env\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "docker-compose.remote-worker.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="new-image",
        max_concurrent=10,
        env_config_version="new-env",
        rollout_policy={"mode": "all"},
        env={},
    )

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(gb10_agent, "_report_node", lambda *args, **kwargs: None)

    def _raise(argv, *, dry_run):  # type: ignore[no-untyped-def]
        raise subprocess.CalledProcessError(1, argv)

    monkeypatch.setattr(gb10_agent, "_run", _raise)

    rc = gb10_agent._apply(SimpleNamespace(
        cp_url="http://cp:8080",
        admin_token="env:LOOM_ADMIN_TOKEN",
        environment="production",
        pool_name="gb10-arm64",
        hostname="trt-gb10-1",
        env_file=env_file,
        compose_file=[compose_file],
        service="worker",
        drain_timeout_sec=600,
        dry_run=False,
        rollback=False,
        force=False,
        format="text",
    ))

    assert rc == 1
    assert "LOOM_IMAGE_TAG=old-image" in env_file.read_text(encoding="utf-8")
    assert "LOOM_WORKER_ENV_CONFIG_VERSION=old-env" in env_file.read_text(
        encoding="utf-8",
    )


def test_apply_builds_local_worker_image_when_registry_pull_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=old-image\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=5\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=old-env\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "docker-compose.remote-worker.yml"
    compose_file.write_text(
        "services:\n"
        "  worker:\n"
        "    image: loom-worker:${LOOM_IMAGE_TAG:-dev}\n"
        "    build:\n"
        "      context: ..\n"
        "      dockerfile: deploy/Dockerfile.worker\n",
        encoding="utf-8",
    )
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="new-image",
        max_concurrent=10,
        env_config_version="new-env",
        rollout_policy={"mode": "all"},
        env={},
    )
    commands: list[list[str]] = []

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(gb10_agent, "_report_node", lambda *args, **kwargs: None)

    def _run(argv, *, dry_run):  # type: ignore[no-untyped-def]
        commands.append(list(argv))
        if argv[-2:] == ["pull", "worker"]:
            raise subprocess.CalledProcessError(1, argv)

    monkeypatch.setattr(gb10_agent, "_run", _run)

    rc = gb10_agent._apply(SimpleNamespace(
        cp_url="http://cp:8080",
        admin_token="env:LOOM_ADMIN_TOKEN",
        environment="production",
        pool_name="gb10-arm64",
        hostname="trt-gb10-1",
        env_file=env_file,
        compose_file=[compose_file],
        service="worker",
        drain_timeout_sec=600,
        dry_run=False,
        rollback=False,
        force=False,
        format="text",
    ))

    assert rc == 0
    assert [command[-2:] for command in commands] == [
        ["pull", "worker"],
        ["build", "worker"],
        ["600", "worker"],
        ["-d", "worker"],
    ]
    rendered = env_file.read_text(encoding="utf-8")
    assert "LOOM_IMAGE_TAG=new-image" in rendered
    assert "LOOM_WORKER_ENV_CONFIG_VERSION=new-env" in rendered


def test_rollback_apply_publishes_previous_state_to_control_plane(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=bad-image\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=10\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=bad-env\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "docker-compose.remote-worker.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="bad-image",
        max_concurrent=10,
        env_config_version="bad-env",
        rollout_policy={"mode": "all"},
        env={},
        previous_image_tag="good-image",
        previous_max_concurrent=5,
        previous_env_config_version="good-env",
        previous_env={},
    )
    captured: dict[str, object] = {}

    class _Response:
        status_code = 200
        text = "{}"

        def json(self) -> dict[str, object]:
            return {}

    def _fake_put(url, *, headers, json, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        return _Response()

    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")
    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(gb10_agent.httpx, "put", _fake_put)
    monkeypatch.setattr(gb10_agent, "_report_node", lambda *args, **kwargs: None)
    monkeypatch.setattr(gb10_agent, "_run", lambda argv, *, dry_run: None)

    rc = gb10_agent._apply(SimpleNamespace(
        cp_url="http://cp:8080",
        admin_token="env:LOOM_ADMIN_TOKEN",
        environment="production",
        pool_name="gb10-arm64",
        hostname="trt-gb10-1",
        env_file=env_file,
        compose_file=[compose_file],
        service="worker",
        drain_timeout_sec=600,
        dry_run=False,
        rollback=True,
        force=True,
        format="text",
    ))

    assert rc == 0
    assert captured["url"] == (
        "http://cp:8080/admin/gb10-worker-pools/production/"
        "gb10-arm64/desired-state"
    )
    assert captured["json"] == {
        "image_tag": "good-image",
        "max_concurrent": 5,
        "env_config_version": "good-env",
        "rollout_policy": {"mode": "all"},
        "env": {},
        "force": True,
    }
