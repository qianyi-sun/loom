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


def test_build_plan_detects_capacity_intent_change() -> None:
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="current-image",
        max_concurrent=10,
        env_config_version="gb10-env-v2",
        target_slots=0,
        host_intents={"trt-gb10-1": "draining"},
        rollout_policy={"mode": "all"},
        env={},
    )
    local = LocalWorkerState(
        hostname="trt-gb10-1",
        image_tag="current-image",
        pool_name="gb10-arm64",
        max_concurrent=10,
        env_config_version="gb10-env-v2",
        capacity_intent="active",
    )

    plan = build_plan(desired, local)

    assert plan.needs_apply is True
    assert plan.blocked_reason is None
    assert plan.changes == ["capacity_intent"]
    assert plan.desired["capacity_intent"] == "draining"
    assert plan.current["capacity_intent"] == "active"


def test_build_plan_detects_source_git_commit_drift() -> None:
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="staging-53897aa",
        max_concurrent=10,
        env_config_version="staging-53897aa",
        source_git_commit="53897aa3d6917dfe0800b6291012ab512bbfc6df",
        rollout_policy={"mode": "all"},
        env={},
    )
    local = LocalWorkerState(
        hostname="trt-gb10-1",
        image_tag="staging-53897aa",
        pool_name="gb10-arm64",
        max_concurrent=10,
        env_config_version="staging-53897aa",
        source_git_commit="7b61049ffffffffff00000000000000000000000",
        source_git_dirty=False,
    )

    plan = build_plan(desired, local)

    assert plan.needs_apply is True
    assert plan.blocked_reason is None
    assert plan.changes == ["source_git_commit"]
    assert plan.desired["source_git_commit"] == ("53897aa3d6917dfe0800b6291012ab512bbfc6df")
    assert plan.current["source_git_commit"] == ("7b61049ffffffffff00000000000000000000000")


def test_build_plan_detects_dirty_source_checkout() -> None:
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="staging-53897aa",
        max_concurrent=10,
        env_config_version="staging-53897aa",
        source_git_commit="53897aa3d6917dfe0800b6291012ab512bbfc6df",
        rollout_policy={"mode": "all"},
        env={},
    )
    local = LocalWorkerState(
        hostname="trt-gb10-1",
        image_tag="staging-53897aa",
        pool_name="gb10-arm64",
        max_concurrent=10,
        env_config_version="staging-53897aa",
        source_git_commit="53897aa3d6917dfe0800b6291012ab512bbfc6df",
        source_git_dirty=True,
    )

    plan = build_plan(desired, local)

    assert plan.needs_apply is True
    assert plan.changes == ["source_git_dirty"]


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


def test_report_node_includes_compose_source_git_provenance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo = tmp_path / "loom-staging-76875ac"
    deploy_dir = repo / "deploy"
    deploy_dir.mkdir(parents=True)
    compose_file = deploy_dir / "docker-compose.remote-worker.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(
        ["git", "config", "user.email", "codex@example.invalid"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Codex"],
        cwd=repo,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=repo,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="staging-76875ac",
        max_concurrent=10,
        env_config_version="staging-76875ac",
        rollout_policy={"mode": "all"},
        env={},
    )
    local = LocalWorkerState(
        hostname="trt-gb10-1",
        image_tag="staging-76875ac",
        pool_name="gb10-arm64",
        max_concurrent=10,
        env_config_version="staging-76875ac",
    )
    captured: dict[str, object] = {}

    def _fake_post(url, *, headers, json, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json

        class _Response:
            status_code = 200

        return _Response()

    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")
    monkeypatch.setattr(gb10_agent.httpx, "post", _fake_post)

    gb10_agent._report_node(
        SimpleNamespace(
            cp_url="http://cp:8080",
            admin_token="env:LOOM_ADMIN_TOKEN",
            compose_file=[compose_file],
        ),
        desired=desired,
        local=local,
        apply_state="applied",
        last_apply_result="already current",
    )

    assert captured["url"] == (
        "http://cp:8080/admin/gb10-worker-pools/production/gb10-arm64/nodes/trt-gb10-1/report"
    )
    body = captured["json"]
    assert isinstance(body, dict)
    assert body["compose_project_dir"] == str(deploy_dir)
    assert body["source_git_commit"] == commit
    assert body["source_git_dirty"] is False


def test_render_env_updates_preserves_comments_and_appends_missing_keys(tmp_path: Path) -> None:
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text(
        "# operator note\nLOOM_IMAGE_TAG=old-image\nLOOM_WORKER_MAX_CONCURRENT=5\n",
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

    rc = gb10_agent._apply(
        SimpleNamespace(
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
        )
    )

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


def test_apply_dry_run_detects_worker_token_drift(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=current-image\n"
        "LOOM_WORKER_TOKEN=loom_w_old_secret\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=10\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=current-env\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "docker-compose.remote-worker.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    worker_token_file = tmp_path / "worker-token"
    worker_token_file.write_text("loom_w_new_secret\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="current-image",
        max_concurrent=10,
        env_config_version="current-env",
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

    rc = gb10_agent._apply(
        SimpleNamespace(
            cp_url="http://cp:8080",
            admin_token="env:LOOM_ADMIN_TOKEN",
            worker_token=f"file:{worker_token_file}",
            environment="production",
            pool_name="gb10-arm64",
            hostname="trt-gb10-1",
            env_file=env_file,
            compose_file=[compose_file],
            service="worker",
            drain_timeout_sec=600,
            dry_run=True,
            rollback=False,
            force=False,
            format="text",
        )
    )

    assert rc == 0
    assert len(commands) == 3
    out = capsys.readouterr().out
    assert "LOOM_WORKER_TOKEN=<redacted>" in out
    assert "loom_w_old_secret" not in out
    assert "loom_w_new_secret" not in out


def test_apply_restarts_missing_active_worker_when_release_metadata_is_current(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=current-image\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=10\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=current-env\n"
        "LOOM_GB10_CAPACITY_INTENT=active\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "docker-compose.remote-worker.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="current-image",
        max_concurrent=10,
        env_config_version="current-env",
        rollout_policy={"mode": "all"},
        env={},
    )
    commands: list[list[str]] = []
    reports: list[dict[str, object]] = []

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(
        gb10_agent,
        "_report_node",
        lambda _args, **kwargs: reports.append(kwargs),
    )
    monkeypatch.setattr(
        gb10_agent,
        "_compose_service_is_running",
        lambda _compose_base, _service: False,
        raising=False,
    )
    monkeypatch.setattr(
        gb10_agent,
        "_run",
        lambda argv, *, dry_run: commands.append(list(argv)),
    )

    rc = gb10_agent._apply(
        SimpleNamespace(
            cp_url="http://cp:8080",
            admin_token="env:LOOM_ADMIN_TOKEN",
            worker_token=None,
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
        )
    )

    assert rc == 0
    assert [command[-2:] for command in commands] == [
        ["pull", "worker"],
        ["-d", "worker"],
    ]
    assert reports[-1]["apply_state"] == "applied"
    assert reports[-1]["last_apply_result"] == "docker compose worker started"


def test_compose_service_running_accepts_project_scoped_container_name(
    monkeypatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def _fake_run(argv, **_kwargs):  # type: ignore[no-untyped-def]
        captured["argv"] = list(argv)
        return SimpleNamespace(
            returncode=0,
            stdout=(
                '{"Name":"loom-worker-build-staging-worker-1",'
                '"State":"running","Status":"Up 2 minutes"}\n'
            ),
        )

    monkeypatch.setattr(gb10_agent.subprocess, "run", _fake_run)

    assert gb10_agent._compose_service_is_running(
        ["docker", "compose", "--env-file", ".env"],
        "worker",
    )
    assert captured["argv"][-4:] == ["ps", "--format", "json", "worker"]


def test_apply_updates_source_checkout_before_compose(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=staging-old\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=10\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=staging-old\n",
        encoding="utf-8",
    )
    source_dir = tmp_path / "loom"
    deploy_dir = source_dir / "deploy"
    deploy_dir.mkdir(parents=True)
    compose_file = deploy_dir / "docker-compose.gb10-hostnet.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="staging-53897aa",
        max_concurrent=10,
        env_config_version="staging-53897aa",
        source_git_commit="53897aa3d6917dfe0800b6291012ab512bbfc6df",
        rollout_policy={"mode": "all"},
        env={},
    )
    commands: list[list[str]] = []

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(gb10_agent, "_report_node", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gb10_agent,
        "_source_git_provenance",
        lambda _source_dir: ("7b61049ffffffffff00000000000000000000000", False),
    )
    monkeypatch.setattr(
        gb10_agent,
        "_run",
        lambda argv, *, dry_run: commands.append(list(argv)),
    )

    rc = gb10_agent._apply(
        SimpleNamespace(
            cp_url="http://cp:8080",
            admin_token="env:LOOM_ADMIN_TOKEN",
            environment="production",
            pool_name="gb10-arm64",
            hostname="trt-gb10-1",
            env_file=env_file,
            compose_file=[compose_file],
            source_dir=source_dir,
            worker_token=None,
            service="worker",
            drain_timeout_sec=600,
            dry_run=False,
            rollback=False,
            force=False,
            format="text",
        )
    )

    assert rc == 0
    assert commands[:2] == [
        ["git", "-C", str(source_dir), "fetch", "--quiet", "origin"],
        [
            "git",
            "-C",
            str(source_dir),
            "checkout",
            "--detach",
            "53897aa3d6917dfe0800b6291012ab512bbfc6df",
        ],
    ]
    assert commands[2][:3] == ["docker", "compose", "--env-file"]
    temp_env_path = Path(commands[2][3])
    assert temp_env_path.parent != env_file.parent
    assert not temp_env_path.exists()


def test_apply_cleans_legacy_repo_temp_env_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_dir = tmp_path / "loom"
    source_dir.mkdir()
    env_file = source_dir / ".env"
    env_file.write_text(
        "LOOM_IMAGE_TAG=old-image\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=5\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=old-env\n",
        encoding="utf-8",
    )
    stale_temp = source_dir / "..env.stale.tmp"
    stale_temp.write_text("LOOM_WORKER_TOKEN=placeholder\n", encoding="utf-8")
    unrelated = source_dir / "..env.stale.txt"
    unrelated.write_text("keep\n", encoding="utf-8")
    compose_file = source_dir / "docker-compose.remote-worker.yml"
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
    commands: list[list[str]] = []

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(gb10_agent, "_report_node", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gb10_agent,
        "_run",
        lambda argv, *, dry_run: commands.append(list(argv)),
    )

    rc = gb10_agent._apply(
        SimpleNamespace(
            cp_url="http://cp:8080",
            admin_token="env:LOOM_ADMIN_TOKEN",
            environment="production",
            pool_name="gb10-arm64",
            hostname="trt-gb10-1",
            env_file=env_file,
            compose_file=[compose_file],
            source_dir=source_dir,
            worker_token=None,
            service="worker",
            drain_timeout_sec=600,
            dry_run=False,
            rollback=False,
            force=False,
            format="text",
        )
    )

    assert rc == 0
    assert not stale_temp.exists()
    assert unrelated.exists()
    temp_env_path = Path(commands[0][3])
    assert temp_env_path.parent != source_dir
    assert not temp_env_path.exists()


def test_apply_stopped_intent_stops_without_restart(
    tmp_path: Path,
    monkeypatch,
) -> None:
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(
        "LOOM_IMAGE_TAG=current-image\n"
        "LOOM_WORKER_POOL_NAME=gb10-arm64\n"
        "LOOM_WORKER_MAX_CONCURRENT=10\n"
        "LOOM_WORKER_ENV_CONFIG_VERSION=current-env\n"
        "LOOM_GB10_CAPACITY_INTENT=active\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "docker-compose.remote-worker.yml"
    compose_file.write_text("services: {}\n", encoding="utf-8")
    desired = DesiredState(
        environment="production",
        pool_name="gb10-arm64",
        image_tag="current-image",
        max_concurrent=10,
        env_config_version="current-env",
        rollout_policy={"mode": "all"},
        env={},
        target_slots=0,
        host_intents={"trt-gb10-1": "stopped"},
    )
    commands: list[list[str]] = []
    reports: list[dict[str, object]] = []

    monkeypatch.setattr(gb10_agent, "_fetch_desired_state", lambda _args: desired)
    monkeypatch.setattr(
        gb10_agent,
        "_report_node",
        lambda _args, **kwargs: reports.append(kwargs),
    )
    monkeypatch.setattr(
        gb10_agent,
        "_run",
        lambda argv, *, dry_run: commands.append(list(argv)),
    )

    rc = gb10_agent._apply(
        SimpleNamespace(
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
        )
    )

    assert rc == 0
    assert [command[-2:] for command in commands] == [["600", "worker"]]
    assert all("up" not in command for command in commands)
    assert reports[-1]["apply_state"] == "stopped"
    rendered = env_file.read_text(encoding="utf-8")
    assert "LOOM_GB10_CAPACITY_INTENT=stopped" in rendered


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

    rc = gb10_agent._apply(
        SimpleNamespace(
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
        )
    )

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

    rc = gb10_agent._apply(
        SimpleNamespace(
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
        )
    )

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

    rc = gb10_agent._apply(
        SimpleNamespace(
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
        )
    )

    assert rc == 0
    assert captured["url"] == (
        "http://cp:8080/admin/gb10-worker-pools/production/gb10-arm64/desired-state"
    )
    assert captured["json"] == {
        "image_tag": "good-image",
        "max_concurrent": 5,
        "env_config_version": "good-env",
        "source_git_commit": None,
        "rollout_policy": {"mode": "all"},
        "env": {},
        "force": True,
    }
