from __future__ import annotations

import os
import urllib.request
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_cli.rollout.operator.installed_final_gate_executor as installed_module
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.installed_final_gate_executor import (
    BoundedStagingSmokeTransport,
    InstalledFinalGateExecutor,
)
from loom_cli.rollout.operator.protected_apply_executor import PROTECTED_KUBECONFIG_PATH
from loom_cli.rollout.operator.protected_environment_state_component import (
    HttpxProtectedEnvironmentStateTransport,
)
from loom_cli.rollout.operator.staging_smoke_authority import staging_smoke_authority
from loom_cli.rollout.preflight_contract import CheckOperation
from tests.loom_cli.rollout.operator.test_checkpoint_inventory_provider import _config
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


class _Response:
    status = 201

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        assert limit == 1024 * 1024 + 1
        return b'{"ok":true}'


class _Opener:
    def __init__(self) -> None:
        self.request: urllib.request.Request | None = None
        self.timeout: float | None = None

    def open(self, request: urllib.request.Request, *, timeout: float) -> _Response:
        self.request = request
        self.timeout = timeout
        return _Response()


def _bound_plan(tmp_path: Path) -> FinalGatePlan:
    plan = _plan(tmp_path)
    return replace(
        plan,
        runner_source_sha=plan.candidate_sha,
        runner_source_tree=plan.candidate_tree,
    )


def _executor(tmp_path: Path) -> InstalledFinalGateExecutor:
    plan = _bound_plan(tmp_path)
    config = replace(
        _config(tmp_path),
        config_path=Path("/etc/loom/staging-rollout.toml"),
        config_sha256=plan.runner_config_hash,
        kubeconfig_path=PROTECTED_KUBECONFIG_PATH,
        source_mode=plan.source_mode,  # type: ignore[arg-type]
        source_commit_sha=(
            plan.runner_source_sha if plan.source_mode == "sealed-cumulative" else None
        ),
        source_tree_sha=(
            plan.runner_source_tree if plan.source_mode == "sealed-cumulative" else None
        ),
        source_base_sha=(
            plan.approved_base_sha if plan.source_mode == "sealed-cumulative" else None
        ),
    )
    return InstalledFinalGateExecutor(
        config=config,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        verify_install=lambda **_kwargs: SimpleNamespace(
            ready=True,
            attestation=SimpleNamespace(
                asset_sha256={
                    "config": plan.runner_config_hash,
                    "worker-env-template": "a" * 64,
                },
                payload_digest=plan.runner_install_hash,
                source_base_sha=(
                    plan.approved_base_sha if plan.source_mode == "sealed-cumulative" else "none"
                ),
                source_mode=plan.source_mode,
                source_sha=plan.candidate_sha,
                source_tree_sha=(
                    plan.candidate_tree if plan.source_mode == "sealed-cumulative" else "none"
                ),
            ),
        ),  # type: ignore[arg-type]
    )


def test_smoke_transport_binds_https_route_headers_and_canonical_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opener = _Opener()
    monkeypatch.setattr(urllib.request, "build_opener", lambda *_handlers: opener)
    transport = BoundedStagingSmokeTransport("https://yylx.world/dev")

    status, body = transport(
        "POST",
        "/api/v1/admin/batches/on-behalf",
        "token-value",
        {"z": 2, "a": 1},
        {"X-Loom-Admin-Actor": "codex-v1-release-gate"},
    )

    assert status == 201
    assert body == b'{"ok":true}'
    assert opener.timeout == 30.0
    assert opener.request is not None
    assert opener.request.full_url == "https://yylx.world/dev/api/v1/admin/batches/on-behalf"
    assert opener.request.method == "POST"
    assert opener.request.data == b'{"a":1,"z":2}'
    assert opener.request.get_header("Authorization") == "Bearer token-value"
    assert opener.request.get_header("X-loom-admin-actor") == "codex-v1-release-gate"


@pytest.mark.parametrize(
    ("method", "path", "payload", "headers"),
    [
        ("DELETE", "/api/v1/batches/a", None, None),
        ("GET", "https://other.invalid/api/v1/health", None, None),
        ("GET", "/api/v1/health", {}, None),
        ("POST", "/api/v1/admin/batches/on-behalf", None, None),
        ("POST", "/api/v1/admin/batches/on-behalf", {}, {"Other": "value"}),
    ],
)
def test_smoke_transport_rejects_requests_outside_fixed_authority(
    method: str,
    path: str,
    payload: dict[str, object] | None,
    headers: dict[str, str] | None,
) -> None:
    transport = BoundedStagingSmokeTransport("https://yylx.world/dev")

    with pytest.raises(ValueError, match="request authority"):
        transport(method, path, "token", payload, headers)


def test_installed_executor_rejects_plan_config_drift_before_dispatch(tmp_path: Path) -> None:
    executor = _executor(tmp_path)
    plan = replace(_bound_plan(tmp_path), runner_config_hash="f" * 64)

    with pytest.raises(ValueError, match="plan drifted"):
        executor("final.summary", CheckOperation.VERIFY, plan)


def test_installed_executor_rejects_unowned_drift_action(tmp_path: Path) -> None:
    executor = _executor(tmp_path)

    with pytest.raises(ValueError, match="no fixed executor"):
        executor("final.drift", CheckOperation.VERIFY, _bound_plan(tmp_path))


def test_installed_executor_rechecks_live_runner_install(tmp_path: Path) -> None:
    executor = replace(
        _executor(tmp_path),
        verify_install=lambda **_kwargs: SimpleNamespace(
            ready=False,
            attestation=SimpleNamespace(
                asset_sha256={"config": "f" * 64},
                payload_digest="f" * 64,
                source_base_sha="none",
                source_mode="merged-dev",
                source_sha="f" * 40,
                source_tree_sha="none",
            ),
        ),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="plan drifted"):
        executor("final.drift", CheckOperation.VERIFY, _bound_plan(tmp_path))


def test_staging_smoke_authority_is_shared_and_fixed(tmp_path: Path) -> None:
    authority = staging_smoke_authority(_config(tmp_path))

    assert authority.to_record() == {
        "admin_actor": "codex-v1-release-gate",
        "agent": "oracle",
        "represented_username": "devansh",
        "required_worker_pool": "gb10",
        "task_id": "loom-smoke/gb10-oracle-hello-world",
        "team_id": "11111111-1111-4111-8111-111111111111",
    }


@pytest.mark.parametrize(
    ("check_id", "executor_name", "operation"),
    [
        ("final.protected-apply", "MigrationEpochProtectedApplyExecutor", CheckOperation.APPLY),
        ("final.convergence", "KubernetesProtectedConvergenceExecutor", CheckOperation.VERIFY),
    ],
)
def test_installed_protected_dispatch_binds_fixed_candidate_and_supervisor_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    check_id: str,
    executor_name: str,
    operation: CheckOperation,
) -> None:
    executor = _executor(tmp_path)
    plan = _bound_plan(tmp_path)
    sentinel_supervisor = object()
    sentinel_gb10 = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        installed_module,
        "build_fixed_external_supervisor_transport",
        lambda *, service_uid: (
            captured.setdefault("service_uid", service_uid),
            sentinel_supervisor,
        )[1],
    )
    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_ssh_transport",
        lambda *_args, **_kwargs: sentinel_gb10,
    )

    class _ProtectedExecutor:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def __call__(self, actual_check, actual_operation, actual_plan):
            assert (actual_check, actual_operation, actual_plan) == (check_id, operation, plan)
            return "dispatched"

    monkeypatch.setattr(installed_module, executor_name, _ProtectedExecutor)

    assert executor(check_id, operation, plan) == "dispatched"  # type: ignore[comparison-overlap]
    assert captured["candidate_root"] == executor.config.runner_repo
    assert captured["external_supervisor_transport"] is sentinel_supervisor
    assert captured["gb10_transport"] is sentinel_gb10
    environment_state = captured["environment_state_transport"]
    assert isinstance(environment_state, HttpxProtectedEnvironmentStateTransport)
    assert environment_state.candidate_root == executor.config.runner_repo
    assert environment_state.cp_url == executor.config.cp_url
    assert environment_state.expected_env_template_sha256 == "a" * 64
    assert captured["service_uid"] == os.geteuid()
