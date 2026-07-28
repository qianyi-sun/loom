from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import render_shared_capacity_adapter_service as renderer
from scripts.ops import shared_capacity_adapter as adapter

ROOT = Path(__file__).resolve().parents[2]
SHA = "a" * 40
OTHER_SHA = "b" * 40
REQUEST_ID = "11111111-1111-4111-8111-111111111111"


class FakeControlPlane:
    def __init__(self, policy: dict[str, Any]) -> None:
        self.policy = policy
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if kwargs["method"] == "PUT":
            self.policy.update(kwargs["body"])
        return dict(self.policy)


class EmptyControlPlane:
    def __init__(self) -> None:
        self.policy: dict[str, Any] | None = None
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if kwargs["method"] == "GET" and self.policy is None:
            raise adapter.PolicyMissingError("not found")
        if kwargs["method"] == "PUT":
            self.policy = {
                **kwargs["body"],
                "environment": "sandbox-qianyi",
                "pool_name": "gb10",
                "last_pending_slots": None,
                "last_actual_slots": None,
                "last_draining_slots": None,
            }
        assert self.policy is not None
        return dict(self.policy)


def _write(path: Path, text: str, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    path.chmod(mode)


def _fixture(
    tmp_path: Path,
    *,
    epoch: int = 3,
    max_slots: int = 12,
    candidate_sha: str = SHA,
    expires_at: datetime | None = None,
) -> tuple[adapter.AdapterConfig, Path]:
    root = tmp_path / "authority"
    root.mkdir(mode=0o700)
    config_path = tmp_path / "qianyi-gb10.toml"
    secret_path = tmp_path / "qianyi-admin.toml"
    handoff_path = root / "handoff.json"
    observation_path = root / "observations" / "qianyi-gb10.json"
    state_path = root / "adapters" / "qianyi-gb10.json"
    sandbox_state_path = tmp_path / "sandbox-state.json"
    _write(secret_path, '[admin]\ntoken = "loom_admin_test_secret"\n', 0o600)
    _write(
        sandbox_state_path,
        json.dumps(
            {
                "schema_version": 1,
                "sandbox": "qianyi",
                "compose_project": "loom-sandbox-qianyi",
                "candidate_sha": candidate_sha,
            },
        )
        + "\n",
    )
    expiry = expires_at or datetime(2026, 7, 28, 15, 0, tzinfo=UTC)
    _write(
        handoff_path,
        json.dumps(
            {
                "schema_version": 1,
                "request_id": REQUEST_ID,
                "lease_epoch": epoch,
                "sandbox": "qianyi",
                "environment": "sandbox-qianyi",
                "candidate_sha": candidate_sha,
                "pool_name": "gb10",
                "enabled": True,
                "min_slots": 0,
                "max_slots": max_slots,
                "expires_at": expiry.isoformat().replace("+00:00", "Z"),
                "preemptible": True,
            },
        )
        + "\n",
    )
    _write(
        config_path,
        "\n".join(
            (
                "schema_version = 1",
                'sandbox = "qianyi"',
                'environment = "sandbox-qianyi"',
                'pool_name = "gb10"',
                'control_plane_url = "http://127.0.0.1:20080"',
                f'admin_secret_file = "{secret_path}"',
                f'handoff_path = "{handoff_path}"',
                f'observation_path = "{observation_path}"',
                f'adapter_state_path = "{state_path}"',
                f'sandbox_state_path = "{sandbox_state_path}"',
                "max_slots_bound = 140",
                "timeout_seconds = 10",
                "",
            ),
        ),
    )
    return adapter.load_config(config_path), handoff_path


def _policy(
    *,
    candidate_sha: str = SHA,
    max_slots: int = 0,
    enabled: bool = False,
    pending: int | None = 0,
    active: int | None = 0,
    draining: int | None = 0,
) -> dict[str, Any]:
    return {
        "environment": "sandbox-qianyi",
        "pool_name": "gb10",
        "actuator": "slurm",
        "enabled": enabled,
        "min_slots": 0,
        "max_slots": max_slots,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
        "force": False,
        "disabled_reason": "awaiting broker handoff",
        "actuator_config": {
            "candidate_sha": candidate_sha,
            "external_runner": True,
        },
        "last_pending_slots": pending,
        "last_actual_slots": active,
        "last_draining_slots": draining,
    }


def test_apply_is_candidate_bound_persistent_and_idempotent(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)
    control_plane = FakeControlPlane(_policy())
    now = datetime(2026, 7, 28, 14, 0, tzinfo=UTC)

    first = adapter.run_once(config, now=now, http_json=control_plane)

    assert first["status"] == "applied"
    assert first["max_slots"] == 12
    assert [call["method"] for call in control_plane.calls] == ["GET", "PUT"]
    put = control_plane.calls[-1]["body"]
    assert put["enabled"] is True
    assert put["min_slots"] == 0
    assert put["max_slots"] == 12
    assert put["actuator_config"]["candidate_sha"] == SHA
    observation = json.loads(config.observation_path.read_text())
    assert observation == [
        {
            "request_id": REQUEST_ID,
            "lease_epoch": 3,
            "pending_slots": 0,
            "active_slots": 0,
            "draining_slots": 0,
            "terminal_slots": 0,
        },
    ]
    persisted = config.adapter_state_path.read_text()
    assert "loom_admin_test_secret" not in persisted

    second = adapter.run_once(
        config,
        now=now + timedelta(seconds=15),
        http_json=control_plane,
    )

    assert second["status"] == "unchanged"
    assert [call["method"] for call in control_plane.calls] == ["GET", "PUT", "GET"]


def test_adapter_rejects_overlapping_invocations(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)
    control_plane = FakeControlPlane(_policy())

    with adapter._exclusive_adapter_lock(config):
        with pytest.raises(adapter.AdapterError, match="already active"):
            adapter.run_once(
                config,
                now=datetime(2026, 7, 28, 14, 0, tzinfo=UTC),
                http_json=control_plane,
            )

    assert control_plane.calls == []


def test_bootstrap_creates_missing_candidate_bound_nonexclusive_policy_once(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path)
    control_plane = EmptyControlPlane()

    first = adapter.bootstrap_policy(config, http_json=control_plane)
    second = adapter.bootstrap_policy(config, http_json=control_plane)

    assert first["status"] == "created"
    assert second["status"] == "unchanged"
    assert [call["method"] for call in control_plane.calls] == ["GET", "PUT", "GET"]
    assert control_plane.policy is not None
    policy = control_plane.policy
    assert policy["enabled"] is False
    assert policy["max_slots"] == 0
    actuator = policy["actuator_config"]
    assert actuator["candidate_sha"] == SHA
    assert actuator["exclusive"] is False
    assert actuator["external_runner"] is True
    assert actuator["shared_capacity_managed"] is True
    assert actuator["container_cpus"] > 0
    assert actuator["container_memory_mib"] > 0
    assert actuator["container_pids"] == 4096
    assert actuator["job_pids_max"] == 65536
    assert (
        actuator["job_pids_max"]
        >= actuator["container_pids"] * actuator["requested_concurrency"]
    )
    assert actuator["slurm_account"] == "loom-dev-qianyi"
    assert actuator["qos_normal"] == "loom-dev"
    assert "loom_admin_test_secret" not in json.dumps(policy)


def test_bootstrap_rejects_existing_policy_authority_drift(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)
    expected = adapter._bootstrap_policy_body(config, candidate_sha=SHA)
    policy = {
        **expected,
        "environment": "sandbox-qianyi",
        "pool_name": "gb10",
        "last_pending_slots": 0,
        "last_actual_slots": 0,
        "last_draining_slots": 0,
    }
    policy["actuator_config"] = {**policy["actuator_config"], "exclusive": True}
    control_plane = FakeControlPlane(policy)

    with pytest.raises(adapter.AdapterError, match="immutable bootstrap"):
        adapter.bootstrap_policy(config, http_json=control_plane)

    assert [call["method"] for call in control_plane.calls] == ["GET"]


def test_bootstrap_rejects_job_pid_budget_below_concurrency_bound(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, _ = _fixture(tmp_path)
    templates = tmp_path / "policy-templates"
    templates.mkdir()
    source = (
        ROOT / "deploy/developer-sandboxes/shared-capacity-policies/gb10.toml"
    ).read_text()
    _write(
        templates / "gb10.toml",
        source.replace("job_pids_max = 65536", "job_pids_max = 32768"),
    )
    monkeypatch.setattr(adapter, "_POLICY_TEMPLATE_DIR", templates)

    with pytest.raises(adapter.AdapterError, match="below concurrency bound"):
        adapter._bootstrap_policy_body(config, candidate_sha=SHA)


def test_handoff_epoch_regression_and_same_epoch_rewrite_fail_closed(
    tmp_path: Path,
) -> None:
    config, handoff_path = _fixture(tmp_path, epoch=3)
    control_plane = FakeControlPlane(_policy())
    now = datetime(2026, 7, 28, 14, 0, tzinfo=UTC)
    adapter.run_once(config, now=now, http_json=control_plane)
    original = json.loads(handoff_path.read_text())

    original["lease_epoch"] = 2
    _write(handoff_path, json.dumps(original) + "\n")
    with pytest.raises(adapter.AdapterError, match="regressed"):
        adapter.run_once(config, now=now, http_json=control_plane)

    original["lease_epoch"] = 3
    original["max_slots"] = 13
    _write(handoff_path, json.dumps(original) + "\n")
    with pytest.raises(adapter.AdapterError, match="without an epoch"):
        adapter.run_once(config, now=now, http_json=control_plane)


def test_candidate_mismatch_never_calls_control_plane(tmp_path: Path) -> None:
    config, handoff_path = _fixture(tmp_path)
    payload = json.loads(handoff_path.read_text())
    payload["candidate_sha"] = OTHER_SHA
    _write(handoff_path, json.dumps(payload) + "\n")
    control_plane = FakeControlPlane(_policy())

    with pytest.raises(adapter.AdapterError, match="sandbox candidate"):
        adapter.run_once(
            config,
            now=datetime(2026, 7, 28, 14, 0, tzinfo=UTC),
            http_json=control_plane,
        )

    assert control_plane.calls == []


def test_policy_candidate_mismatch_never_mutates_policy(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path)
    control_plane = FakeControlPlane(_policy(candidate_sha=OTHER_SHA))

    with pytest.raises(adapter.AdapterError, match="policy candidate_sha"):
        adapter.run_once(
            config,
            now=datetime(2026, 7, 28, 14, 0, tzinfo=UTC),
            http_json=control_plane,
        )

    assert [call["method"] for call in control_plane.calls] == ["GET"]


def test_handoff_above_reviewed_pool_bound_fails_before_api(
    tmp_path: Path,
) -> None:
    config, handoff_path = _fixture(tmp_path, max_slots=141)
    control_plane = FakeControlPlane(_policy())

    with pytest.raises(adapter.AdapterError, match="reviewed pool bound"):
        adapter.run_once(
            config,
            now=datetime(2026, 7, 28, 14, 0, tzinfo=UTC),
            http_json=control_plane,
        )

    assert json.loads(handoff_path.read_text())["max_slots"] == 141
    assert control_plane.calls == []


def test_expired_handoff_only_drains_to_zero(tmp_path: Path) -> None:
    expiry = datetime(2026, 7, 28, 13, 0, tzinfo=UTC)
    config, _ = _fixture(tmp_path, max_slots=12, expires_at=expiry)
    control_plane = FakeControlPlane(
        _policy(max_slots=12, enabled=True, pending=2, active=8, draining=2),
    )

    report = adapter.run_once(
        config,
        now=expiry + timedelta(seconds=1),
        http_json=control_plane,
    )

    assert report["expired"] is True
    assert report["enabled"] is False
    assert report["max_slots"] == 0
    assert control_plane.calls[-1]["body"]["max_slots"] == 0
    assert report["observation"]["pending_slots"] == 2
    assert report["observation"]["active_slots"] == 8
    assert report["observation"]["draining_slots"] == 2


def test_missing_handoff_after_restart_drains_using_durable_epoch(
    tmp_path: Path,
) -> None:
    config, handoff_path = _fixture(tmp_path, max_slots=12)
    control_plane = FakeControlPlane(_policy())
    now = datetime(2026, 7, 28, 14, 0, tzinfo=UTC)
    adapter.run_once(config, now=now, http_json=control_plane)
    handoff_path.unlink()
    control_plane.policy.update(
        {
            "last_pending_slots": 0,
            "last_actual_slots": 8,
            "last_draining_slots": 4,
        },
    )

    report = adapter.run_once(
        config,
        now=now + timedelta(seconds=15),
        http_json=control_plane,
    )

    assert report["handoff_missing"] is True
    assert report["enabled"] is False
    assert report["max_slots"] == 0
    assert report["lease_epoch"] == 3
    assert control_plane.calls[-1]["body"]["max_slots"] == 0
    assert report["observation"]["active_slots"] == 8
    assert report["observation"]["draining_slots"] == 4


def test_unknown_status_preserves_committed_slots_after_restart(tmp_path: Path) -> None:
    config, _ = _fixture(tmp_path, max_slots=12)
    control_plane = FakeControlPlane(_policy())
    now = datetime(2026, 7, 28, 14, 0, tzinfo=UTC)
    adapter.run_once(config, now=now, http_json=control_plane)
    state = json.loads(config.adapter_state_path.read_text())
    state.update({"pending_slots": 3, "active_slots": 7, "draining_slots": 2})
    _write(config.adapter_state_path, json.dumps(state) + "\n", 0o600)
    control_plane.policy.update(
        {
            "last_pending_slots": None,
            "last_actual_slots": None,
            "last_draining_slots": None,
        },
    )

    report = adapter.run_once(
        config,
        now=now + timedelta(seconds=15),
        http_json=control_plane,
    )

    assert report["observation"]["pending_slots"] == 3
    assert report["observation"]["active_slots"] == 7
    assert report["observation"]["draining_slots"] == 2


def test_terminal_counter_is_monotonic_when_nonterminal_slots_finish(
    tmp_path: Path,
) -> None:
    config, _ = _fixture(tmp_path, max_slots=12)
    control_plane = FakeControlPlane(
        _policy(max_slots=12, enabled=True, pending=2, active=8, draining=2),
    )
    now = datetime(2026, 7, 28, 14, 0, tzinfo=UTC)
    adapter.run_once(config, now=now, http_json=control_plane)
    control_plane.policy.update(
        {
            "last_pending_slots": 0,
            "last_actual_slots": 7,
            "last_draining_slots": 0,
        },
    )

    report = adapter.run_once(
        config,
        now=now + timedelta(seconds=15),
        http_json=control_plane,
    )

    assert report["observation"]["terminal_slots"] == 5


def test_main_prints_only_generic_error(tmp_path: Path, capsys) -> None:
    _, handoff_path = _fixture(tmp_path)
    handoff_path.unlink()

    rc = adapter.main(["--config", str(tmp_path / "qianyi-gb10.toml"), "run"])

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"error":"shared-capacity-adapter-failed-safely"}\n'
    assert "loom_admin_test_secret" not in captured.err


def test_checked_in_configs_cover_three_sandboxes_and_two_pools() -> None:
    config_dir = ROOT / "deploy/developer-sandboxes/shared-capacity-adapters"
    configs = [adapter.load_config(path) for path in sorted(config_dir.glob("*.toml"))]

    assert {(item.sandbox, item.pool_name) for item in configs} == {
        (sandbox, pool)
        for sandbox in ("qianyi", "hongjian", "devansh")
        for pool in ("gb10", "oldlab")
    }
    assert len({item.handoff_path for item in configs}) == 6
    assert len({item.observation_path for item in configs}) == 6
    assert len({item.adapter_state_path for item in configs}) == 6
    assert {(item.pool_name, item.max_slots_bound) for item in configs} == {
        ("gb10", 140),
        ("oldlab", 20),
    }
    for config in configs:
        policy = adapter._bootstrap_policy_body(config, candidate_sha=SHA)
        actuator = policy["actuator_config"]
        assert policy["enabled"] is False
        assert policy["max_slots"] == 0
        assert actuator["candidate_sha"] == SHA
        assert actuator["exclusive"] is False
        assert actuator["external_runner"] is True
        assert actuator["shared_capacity_managed"] is True
        assert actuator["slurm_account"] == f"loom-dev-{config.sandbox}"
        assert actuator["qos_normal"] == "loom-dev"
        assert actuator["env_file"] == (
            f"/shared_work/loom/runtime/sandboxes/{config.sandbox}/"
            f"{SHA}/worker-{config.pool_name}.env"
        )
        assert "/candidates/" not in actuator["env_file"]
        assert actuator["container_cpus"] > 0
        assert actuator["container_memory_mib"] > 0
        assert actuator["container_pids"] > 0
        assert isinstance(actuator["job_pids_max"], int)
        assert actuator["job_pids_max"] > 0
        assert (
            actuator["job_pids_max"]
            >= actuator["container_pids"] * actuator["requested_concurrency"]
        )
        assert actuator["allowed_nodes"]
        assert config.admin_secret_file == Path(
            f"/srv/loom/developer-sandboxes/{config.sandbox}/secrets/admin.toml",
        )


def test_service_renderer_binds_exact_candidate_and_rejects_drift() -> None:
    template = (
        ROOT / "deploy/developer-sandboxes/loom-shared-capacity-adapter@.service"
    ).read_text()

    rendered = renderer.render_service_unit(template, git_sha=SHA)

    assert "${GIT_SHA}" not in rendered
    assert rendered.count(SHA) == 6
    assert "ExecStartPre=" in rendered
    assert " bootstrap" in rendered
    assert "ProtectSystem=strict" in rendered
    assert "ReadWritePaths=/var/lib/loom-shared-capacity" in rendered
    with pytest.raises(ValueError, match="40-character"):
        renderer.render_service_unit(template, git_sha="abc")
    with pytest.raises(ValueError, match="placeholder"):
        renderer.render_service_unit(template.replace("${GIT_SHA}", "mutable", 1), git_sha=SHA)
    with pytest.raises(ValueError, match="mutable runtime"):
        renderer.render_service_unit(
            template + "\n# /opt/loom-shared-capacity/current\n",
            git_sha=SHA,
        )
