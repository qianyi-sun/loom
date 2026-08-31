from __future__ import annotations

import hashlib
import json
import os
import subprocess
import urllib.request
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_cli.rollout.operator.installed_final_gate_executor as installed_module
from loom_cli.rollout.gb10_readiness import FULL_GB10_HOSTS
from loom_cli.rollout.operator.config import environment_authority
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan, FinalGatePlanStore
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

NORMAL_GB10_WORKER_HOSTS = (
    "trt-gb10-1",
    "trt-gb10-3",
    "trt-gb10-4",
    "trt-gb10-5",
    "trt-gb10-6",
    "trt-gb10-7",
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)


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


def test_installed_executor_rejects_effective_group_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _executor(tmp_path)
    actual_gid = os.getegid()
    monkeypatch.setattr(installed_module.os, "getegid", lambda: actual_gid + 1)

    with pytest.raises(ValueError, match="authority is invalid"):
        replace(executor)


def test_installed_executor_uses_current_helper_with_historical_rollout_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical_executor = _executor(tmp_path)
    plan = _bound_plan(tmp_path)
    historical_repo = (
        environment_authority("staging").candidate_runtime_root / plan.candidate_sha / "repo"
    )
    historical_config = replace(
        historical_executor.config,
        runner_repo=historical_repo,
        cluster_config_path=(
            historical_repo / environment_authority("staging").candidate_cluster_config
        ),
    )
    current_sha = "c" * 40
    current_repo = environment_authority("staging").candidate_runtime_root / current_sha / "repo"
    current_config = replace(
        historical_config,
        runner_repo=current_repo,
        cluster_config_path=current_repo / environment_authority("staging").candidate_cluster_config,
        config_sha256="f" * 64,
    )
    current_install = SimpleNamespace(
        ready=True,
        attestation=SimpleNamespace(
            asset_sha256={
                "config": current_config.config_sha256,
                "worker-env-template": "e" * 64,
            },
            payload_digest="d" * 64,
            source_base_sha="none",
            source_mode="merged-dev",
            source_sha=current_sha,
            source_tree_sha="none",
        ),
    )
    resolved: list[dict[str, object]] = []

    def resolve_runtime(config: object, **bindings: object) -> object:
        resolved.append({"config": config, **bindings})
        return historical_config

    executor = replace(
        historical_executor,
        config=current_config,
        verify_install=lambda **_kwargs: current_install,
        resume_runtime_upgrade=SimpleNamespace(resolve=resolve_runtime),
    )
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        installed_module,
        "load_cluster_config",
        lambda _path: SimpleNamespace(container_registry="registry.invalid"),
    )
    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_external_supervisor_transport",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        installed_module,
        "build_fixed_external_supervisor_transport",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        installed_module,
        "FixedLocalExternalSupervisorCredentialTransport",
        lambda **_kwargs: object(),
        raising=False,
    )
    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_ssh_transport",
        lambda *_args, **_kwargs: object(),
    )

    class ProtectedExecutor:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def __call__(self, *_args: object) -> str:
            return "dispatched"

    monkeypatch.setattr(installed_module, "MigrationEpochProtectedApplyExecutor", ProtectedExecutor)

    assert (
        executor("final.protected-apply", CheckOperation.APPLY, plan) == "dispatched"
    )  # type: ignore[comparison-overlap]
    expected_cluster_path = (
        environment_authority("staging").candidate_runtime_root
        / plan.candidate_sha
        / "repo"
        / environment_authority("staging").candidate_cluster_config
    )
    assert resolved == [
        {
            "config": current_config,
            "candidate_sha": plan.candidate_sha,
            "candidate_tree": plan.candidate_tree,
            "runner_config_sha256": plan.runner_config_hash,
            "cluster_config_path": str(expected_cluster_path),
        }
    ]
    assert captured["candidate_root"] == historical_config.runner_repo
    environment_state = captured["environment_state_transport"]
    assert isinstance(environment_state, HttpxProtectedEnvironmentStateTransport)
    assert environment_state.candidate_root == historical_config.runner_repo
    assert environment_state.expected_env_template_sha256 == "e" * 64


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
    sentinel_oldlab_supervisor = object()
    sentinel_oldlab_credential = object()
    sentinel_gb10_supervisor = object()
    sentinel_gb10_credential = object()
    sentinel_gb10 = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        installed_module,
        "build_fixed_external_supervisor_transport",
        lambda *, service_uid: (
            captured.setdefault("service_uid", service_uid),
            sentinel_oldlab_supervisor,
        )[1],
    )

    def build_oldlab_credential(**kwargs):
        captured["oldlab_credential_builder"] = kwargs
        return sentinel_oldlab_credential

    monkeypatch.setattr(
        installed_module,
        "FixedLocalExternalSupervisorCredentialTransport",
        build_oldlab_credential,
        raising=False,
    )

    def build_gb10_credential(transport):
        captured["gb10_credential_builder"] = transport
        return sentinel_gb10_credential

    monkeypatch.setattr(
        installed_module,
        "GB10ExternalSupervisorCredentialTransport",
        build_gb10_credential,
        raising=False,
    )

    def build_gb10_supervisor(**kwargs):
        captured["gb10_supervisor_builder"] = kwargs
        return sentinel_gb10_supervisor

    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_external_supervisor_transport",
        build_gb10_supervisor,
    )

    def build_gb10_fleet(*args, **kwargs):
        captured["gb10_fleet_builder"] = (args, kwargs)
        return sentinel_gb10

    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_ssh_transport",
        build_gb10_fleet,
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
    assert captured["external_supervisor_transports"] == {
        "gx10-01c7": sentinel_gb10_supervisor,
        "TRT-EAI-OLDLAB-1": sentinel_oldlab_supervisor,
    }
    assert captured["external_supervisor_credential_transports"] == {
        "gx10-01c7": sentinel_gb10_credential,
        "TRT-EAI-OLDLAB-1": sentinel_oldlab_credential,
    }
    assert captured["gb10_credential_builder"] is sentinel_gb10_supervisor
    assert captured["external_supervisor_credential_identities"] == {
        "gx10-01c7": (995, 2007),
        "TRT-EAI-OLDLAB-1": (os.geteuid(), os.getegid()),
    }
    assert captured["oldlab_credential_builder"] == {
        "candidate_root": executor.config.runner_repo,
        "execution_host": "TRT-EAI-OLDLAB-1",
        "service_uid": os.geteuid(),
        "service_gid": os.getegid(),
    }
    assert captured["gb10_supervisor_builder"] == {
        "candidate_sha": plan.candidate_sha,
        "candidate_tree": plan.candidate_tree,
        "run": executor._supervisor_ssh_run,
    }
    gb10_fleet_args, gb10_fleet_kwargs = captured["gb10_fleet_builder"]
    assert gb10_fleet_args == (executor.config.cluster_config_path,)
    assert gb10_fleet_kwargs["run"] == executor._ssh_run
    assert captured["gb10_transport"] is sentinel_gb10
    environment_state = captured["environment_state_transport"]
    assert isinstance(environment_state, HttpxProtectedEnvironmentStateTransport)
    assert environment_state.candidate_root == executor.config.runner_repo
    assert environment_state.cp_url == executor.config.cp_url
    assert environment_state.expected_env_template_sha256 == "a" * 64
    assert captured["service_uid"] == os.geteuid()


def test_capacity_executor_binds_exact_profile_nodes_and_epoch(tmp_path: Path) -> None:
    plan = replace(
        _bound_plan(tmp_path),
        gb10_boot_ids={node: f"boot-{index}" for index, node in enumerate(FULL_GB10_HOSTS)},
    )
    calls: list[dict[str, object]] = []

    class _Transport:
        def accept_capacity(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(evidence_digest="d" * 64)

    capacity = installed_module.FinalCapacityExecutor(transport=_Transport())

    result = capacity("final.capacity", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.observed_epoch == plan.starting_mutation_epoch + 1
    assert result.evidence_digest == "d" * 64
    assert result.protected_mutation is True
    assert calls == [
        {
            "profile_sha256": plan.supervisor_profile_sha256,
            "nodes": NORMAL_GB10_WORKER_HOSTS,
        }
    ]


def test_capacity_executor_accepts_published_plan_with_sorted_boot_id_keys(
    tmp_path: Path,
) -> None:
    payload = _plan(tmp_path).to_dict()
    payload["gb10_boot_ids"] = {node: f"boot-{index}" for index, node in enumerate(FULL_GB10_HOSTS)}
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(
            {key: value for key, value in payload.items() if key != "plan_digest"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    plan = FinalGatePlan.from_dict(payload)
    attempt = tmp_path / "requests" / plan.request_id / "attempts" / str(plan.attempt_number)
    attempt.mkdir(parents=True, mode=0o700)
    store = FinalGatePlanStore(
        tmp_path,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
    )
    store.publish(plan)
    reloaded = store.read()
    calls: list[dict[str, object]] = []

    class _Transport:
        def accept_capacity(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(evidence_digest="d" * 64)

    result = installed_module.FinalCapacityExecutor(transport=_Transport())(
        "final.capacity",
        CheckOperation.APPLY,
        reloaded,
    )

    assert tuple(reloaded.gb10_boot_ids) != FULL_GB10_HOSTS
    assert result.ready
    assert calls == [
        {
            "profile_sha256": plan.supervisor_profile_sha256,
            "nodes": NORMAL_GB10_WORKER_HOSTS,
        }
    ]


def test_capacity_executor_normalizes_broker_failure_before_smoke(tmp_path: Path) -> None:
    plan = replace(
        _bound_plan(tmp_path),
        gb10_boot_ids={node: f"boot-{index}" for index, node in enumerate(FULL_GB10_HOSTS)},
    )

    class _Transport:
        def accept_capacity(self, **_kwargs):
            raise RuntimeError("secret remote detail")

    result = installed_module.FinalCapacityExecutor(transport=_Transport())(
        "final.capacity",
        CheckOperation.APPLY,
        plan,
    )

    assert result.blockers == {"capacity": "slurm-acceptance-unavailable"}
    assert result.protected_mutation is True
    assert result.observed_epoch == plan.starting_mutation_epoch + 1


def test_capacity_executor_normalizes_ssh_timeout_before_smoke(tmp_path: Path) -> None:
    plan = replace(
        _bound_plan(tmp_path),
        gb10_boot_ids={node: f"boot-{index}" for index, node in enumerate(FULL_GB10_HOSTS)},
    )

    class _Transport:
        def accept_capacity(self, **_kwargs):
            raise subprocess.TimeoutExpired(("ssh", "fixed-controller"), 1500)

    result = installed_module.FinalCapacityExecutor(transport=_Transport())(
        "final.capacity",
        CheckOperation.APPLY,
        plan,
    )

    assert result.blockers == {"capacity": "slurm-acceptance-unavailable"}
    assert result.protected_mutation is True
    assert result.observed_epoch == plan.starting_mutation_epoch + 1


@pytest.mark.parametrize("error_type", (OSError, ValueError))
def test_installed_capacity_normalizes_transport_builder_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
) -> None:
    executor = _executor(tmp_path)
    plan = replace(
        _bound_plan(tmp_path),
        gb10_boot_ids={node: f"boot-{index}" for index, node in enumerate(FULL_GB10_HOSTS)},
    )

    def build_transport(**_kwargs):
        raise error_type("secret identity detail")

    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_external_supervisor_transport",
        build_transport,
    )

    result = executor("final.capacity", CheckOperation.APPLY, plan)

    assert result.blockers == {"capacity": "slurm-acceptance-unavailable"}
    assert result.protected_mutation is True
    assert result.observed_epoch == plan.starting_mutation_epoch + 1


def test_capacity_executor_rejects_noncanonical_node_plan_before_ssh(tmp_path: Path) -> None:
    plan = replace(
        _bound_plan(tmp_path),
        gb10_boot_ids={
            **{node: f"boot-{index}" for index, node in enumerate(FULL_GB10_HOSTS[:-1])},
            "trt-gb10-16": "boot-outside-authority",
        },
    )

    class _Transport:
        def accept_capacity(self, **_kwargs):
            pytest.fail("noncanonical capacity plan reached SSH")

    with pytest.raises(ValueError, match="final capacity operation"):
        installed_module.FinalCapacityExecutor(transport=_Transport())(
            "final.capacity",
            CheckOperation.APPLY,
            plan,
        )


def test_installed_capacity_dispatch_reuses_candidate_bound_controller_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _executor(tmp_path)
    plan = _bound_plan(tmp_path)
    sentinel_transport = object()
    captured: dict[str, object] = {}

    def build_transport(**kwargs):
        captured["builder"] = kwargs
        return sentinel_transport

    class _CapacityExecutor:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def __call__(self, actual_check, actual_operation, actual_plan):
            assert (actual_check, actual_operation, actual_plan) == (
                "final.capacity",
                CheckOperation.APPLY,
                plan,
            )
            captured["transport"] = captured["transport_factory"]()
            return "dispatched"

    monkeypatch.setattr(
        installed_module,
        "build_fixed_gb10_external_supervisor_transport",
        build_transport,
    )
    monkeypatch.setattr(installed_module, "FinalCapacityExecutor", _CapacityExecutor, raising=False)

    assert executor("final.capacity", CheckOperation.APPLY, plan) == "dispatched"
    assert captured["transport"] is sentinel_transport
    assert captured["builder"] == {
        "candidate_sha": plan.candidate_sha,
        "candidate_tree": plan.candidate_tree,
        "run": executor._capacity_ssh_run,
    }


def test_installed_supervisor_ssh_runner_forwards_bounded_stdin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _executor(tmp_path)
    captured: dict[str, object] = {}

    def run(argv, **kwargs):
        captured["argv"] = tuple(argv)
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="{}\n", stderr="")

    monkeypatch.setattr(installed_module.subprocess, "run", run)

    result = executor._supervisor_ssh_run(("ssh", "fixed-host"), '{"operation":"observe"}\n')

    assert result.returncode == 0
    assert captured["argv"] == ("ssh", "fixed-host")
    assert captured["input"] == '{"operation":"observe"}\n'
    assert captured["timeout"] == 180
    assert captured["shell"] is False


def test_installed_supervisor_ssh_runner_rejects_oversized_stdin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = _executor(tmp_path)
    monkeypatch.setattr(
        installed_module.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("oversized stdin reached subprocess"),
    )

    with pytest.raises(ValueError, match="input is too large"):
        executor._supervisor_ssh_run(("ssh", "fixed-host"), "x" * (4 * 1024 * 1024 + 1))
