from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_deploy as deploy
from scripts.ops import developer_environment_registry as registry
from scripts.ops import shared_capacity_runtime_host as runtime_host
from scripts.ops import shared_capacity_supervisor as supervisor
from tests.ops.test_developer_environment_deploy import (
    FakeCapacityAuthority,
    FakeRunner,
    _candidate,
    _converge,
    _deployer,
    _register,
)

from loom_control_plane.shared_capacity_broker import SandboxId, SharedCapacityBroker

NOW = datetime(2026, 7, 30, 16, 0, tzinfo=UTC)
REPO_ROOT = Path(__file__).resolve().parents[2]


class RootMetadataPath(type(Path())):
    """Expose production-required root metadata without replacing file logic."""

    def lstat(self) -> Any:
        metadata = super().lstat()

        class RootOwnedMetadata:
            st_uid = 0
            st_gid = 0

            def __getattr__(self, name: str) -> Any:
                return getattr(metadata, name)

        return RootOwnedMetadata()


class StatefulOuterRunner(FakeRunner):
    """Stateful fake only for host systemd/Slurm boundaries."""

    def __init__(self) -> None:
        super().__init__()
        self.units: dict[str, dict[str, bool]] = {}
        self.slurm_jobs: dict[str, tuple[str, str, str, str]] = {}

    def _unit(self, name: str) -> dict[str, bool]:
        return self.units.setdefault(name, {"enabled": False, "active": False})

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> deploy.CommandResult:
        command = tuple(argv)
        if command[0] == "systemctl":
            with self.lock:
                self.calls.append(command)
            action = command[1]
            if action == "show":
                unit = command[2]
                property_name = next(
                    item.removeprefix("--property=")
                    for item in command
                    if item.startswith("--property=")
                )
                state = self._unit(unit)
                environment_instance = unit.startswith(
                    "loom-developer-environment@"
                ) and unit.endswith(".service")
                value = {
                    "LoadState": (
                        "loaded" if environment_instance or state["enabled"] else "not-found"
                    ),
                    "FragmentPath": (
                        "/etc/systemd/system/loom-developer-environment@.service"
                        if environment_instance or state["enabled"]
                        else ""
                    ),
                    "UnitFileState": "enabled" if state["enabled"] else "disabled",
                }[property_name]
                return deploy.CommandResult(0, value + "\n")
            unit = command[-1]
            state = self._unit(unit)
            if action == "enable":
                state["enabled"] = True
                state["active"] = "--now" in command or state["active"]
            elif action == "disable":
                state["enabled"] = False
                state["active"] = False if "--now" in command else state["active"]
            elif action == "start":
                state["active"] = True
            elif action == "stop":
                state["active"] = False
            return deploy.CommandResult(0, "")
        if command[0] == "squeue":
            with self.lock:
                self.calls.append(command)
            selector = command[command.index("--account") + 1] if "--account" in command else None
            user = command[command.index("--user") + 1] if "--user" in command else None
            rows = (
                "|".join((job_id, *binding))
                for job_id, binding in self.slurm_jobs.items()
                if (selector is None or binding[1] == selector)
                and (user is None or binding[0] == user)
            )
            return deploy.CommandResult(0, "".join(f"{row}\n" for row in rows))
        if command[0] == "scancel":
            with self.lock:
                self.calls.append(command)
            return deploy.CommandResult(0, "")
        return super().run(argv, cwd=cwd, env=env, expected=expected)


class ProductionGraphRuntimeAuthority:
    """Drive the real registry/runtime-host graph from the deployer."""

    @staticmethod
    def _refresh() -> None:
        runtime_host._COHORT_CACHE = runtime_host._load_registry_cohort(
            runtime_host.REGISTRY_SNAPSHOT_PATH
        )

    def reconcile(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self._refresh()
        return runtime_host.reconcile_registry_environment(str(context.environment["runtime_id"]))

    def check(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self._refresh()
        return runtime_host.check_registry_environment(str(context.environment["runtime_id"]))

    def acceptance_probe(self, _context: deploy.DeploymentContext) -> dict[str, Any]:
        return {"status": "passed", "payload_sha256": "9" * 64}

    def activate(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self._refresh()
        return runtime_host.reopen_registry_environment_admission(
            str(context.environment["runtime_id"])
        )

    def fence(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self._refresh()
        return runtime_host.fence_registry_environment(str(context.environment["runtime_id"]))

    def rollback(self, _context: deploy.DeploymentContext) -> dict[str, Any]:
        return {"status": "ready"}

    def retire(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self._refresh()
        return runtime_host.retire_registry_environment(str(context.environment["runtime_id"]))


def _commit_registry_baseline(
    authority: registry.DeveloperEnvironmentRegistry,
    environment: registry.EnvironmentRecord,
    candidate: registry.CandidateRecord,
    number: int,
) -> None:
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": f"baseline-deployment-{number:04d}",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    for current, following in zip(
        registry.DEPLOY_PHASES[:-1],
        registry.DEPLOY_PHASES[1:],
        strict=True,
    ):
        if following == "committed":
            authority.prepare_deployment_finalization(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_resource_generation=environment.resource_generation,
            )
            authority.record_deployment_finalization(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_resource_generation=environment.resource_generation,
                evidence={
                    "capacity_finalize_receipt_sha256": f"{number}" * 64,
                    "capacity_finalize_check_receipt_sha256": f"{number + 1}" * 64,
                    "runtime_reconcile_receipt_sha256": f"{number + 2}" * 64,
                    "runtime_prepare_check_receipt_sha256": f"{number + 3}" * 64,
                    "acceptance_probe_receipt_sha256": f"{number + 4}" * 64,
                },
            )
        authority.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=current,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )


def _write_supervisor_base(path: Path, authority_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            (
                "schema_version = 1",
                f'state_db = "{authority_root / "broker.sqlite3"}"',
                f'handoff_dir = "{authority_root / "handoffs"}"',
                f'observation_dir = "{authority_root / "observations"}"',
                f'supervisor_state_path = "{authority_root / "supervisor-state.json"}"',
                f'audit_path = "{authority_root / "supervisor-audit.jsonl"}"',
                f'evidence_path = "{authority_root / "evidence/latest.json"}"',
                "global_slot_budget = 140",
                "global_pending_slot_budget = 34",
                'instances = ["placeholder-gb10", "placeholder-oldlab"]',
                "[pool_slot_budgets]",
                "gb10 = 120",
                "oldlab = 20",
                "[pool_pending_slot_budgets]",
                "gb10 = 24",
                "oldlab = 10",
                "",
            )
        ),
        encoding="ascii",
    )
    path.chmod(0o644)


def _install_runtime_host_fixture(
    tmp_path: Path,
    authority: registry.DeveloperEnvironmentRegistry,
    runner: StatefulOuterRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    host_root = tmp_path / "runtime-host"
    state_root = RootMetadataPath(host_root / "state")
    config_root = RootMetadataPath(host_root / "config")
    authority_root = host_root / "authority"
    base_config = RootMetadataPath(config_root / "shared-capacity-supervisor.base.toml")
    rendered_config = RootMetadataPath(config_root / "shared-capacity-supervisor.toml")
    state_path = RootMetadataPath(state_root / "installer/state.json")
    node_binding = {
        "node_authority_source_sha": "a" * 40,
        "node_authority_source_tree": "b" * 40,
        "node_capacity_contract_sha256": "c" * 64,
    }

    _write_supervisor_base(base_config, authority_root)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "activation_status": "activated",
                "installation_mode": "fixed-registry-runtime",
                "admission_state": "open",
                **node_binding,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="ascii",
    )
    state_path.chmod(0o600)

    monkeypatch.setattr(runtime_host, "REGISTRY_SNAPSHOT_PATH", authority.snapshot_path)
    monkeypatch.setattr(runtime_host, "STATE_ROOT", state_root)
    monkeypatch.setattr(runtime_host, "ADAPTER_CONFIG_ROOT", config_root / "adapters")
    monkeypatch.setattr(runtime_host, "SUPERVISOR_BASE_CONFIG_PATH", base_config)
    monkeypatch.setattr(runtime_host, "SUPERVISOR_CONFIG_PATH", rendered_config)
    monkeypatch.setattr(runtime_host, "BROKER_STATE_DB_PATH", authority_root / "broker.sqlite3")
    monkeypatch.setattr(runtime_host, "STATE_PATH", state_path)
    monkeypatch.setattr(
        runtime_host,
        "ENVIRONMENT_RECONCILE_ROOT",
        state_root / "installer/environment-reconcile",
    )
    monkeypatch.setattr(
        runtime_host,
        "ENVIRONMENT_ADMISSION_INTENT_ROOT",
        tmp_path / "host/var/lib/loom-developer-environment-runtime/lifecycle/admission",
    )
    monkeypatch.setattr(runtime_host, "LOOM_RUNTIME_PYTHON_ROOT", REPO_ROOT / "src")
    monkeypatch.setattr(runtime_host, "_require_live_host", lambda: None)
    monkeypatch.setattr(runtime_host, "_lock", nullcontext)
    monkeypatch.setattr(runtime_host.os, "chown", lambda *_args: None)
    monkeypatch.setattr(
        runtime_host,
        "_validate_fixed_registry_node_capacity_prerequisite",
        lambda: dict(node_binding),
    )

    def run_host(
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        expected: set[int] | frozenset[int] = frozenset({0}),
    ) -> subprocess.CompletedProcess[str]:
        command = tuple(argv)
        runner.calls.append(command)
        if command[0] != "systemctl":
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=dict(env) if env is not None else None,
            )
            if completed.returncode not in expected:
                raise runtime_host.RuntimeHostError("outer host command failed safely")
            return completed
        action = command[1]
        if action == "daemon-reload":
            return subprocess.CompletedProcess(command, 0, "", "")
        unit = command[-1]
        state = runner._unit(unit)
        if action == "is-enabled":
            value = "enabled" if state["enabled"] else "disabled"
            return subprocess.CompletedProcess(command, 0 if state["enabled"] else 1, value, "")
        if action == "is-active":
            value = "active" if state["active"] else "inactive"
            return subprocess.CompletedProcess(command, 0 if state["active"] else 3, value, "")
        if action == "show":
            value = (
                "success"
                if "--property=Result" in command
                else "0"
                if "--property=ExecMainStatus" in command
                else ""
            )
            return subprocess.CompletedProcess(command, 0, value + "\n", "")
        if action == "stop":
            state["active"] = False
        elif action == "start":
            state["active"] = True
            if unit == runtime_host.SUPERVISOR_SERVICE:
                supervisor.run_once(supervisor.load_config(rendered_config), now=NOW)
        elif action == "enable":
            state["enabled"] = True
            state["active"] = "--now" in command or state["active"]
        elif action == "disable":
            state["enabled"] = False
            state["active"] = False if "--now" in command else state["active"]
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(runtime_host, "_run", run_host)
    runtime_host._COHORT_CACHE = None


def test_real_deployer_expands_committed_dynamic_cohort_without_touching_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_root = tmp_path / "registry/candidates"
    monkeypatch.setattr(registry, "SYSTEM_CANDIDATE_ROOT", candidate_root)
    authority = registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry/registry.sqlite3",
        candidate_root=candidate_root,
    )
    environments = [
        _register(authority, f"oidc:example:developer-{number}", number) for number in range(1, 6)
    ]
    candidates = [
        _candidate(authority, environment, tmp_path, f"developer-{number}")
        for number, environment in enumerate(environments, start=1)
    ]
    for number in range(1, 4):
        _commit_registry_baseline(
            authority,
            environments[number - 1],
            candidates[number - 1],
            number,
        )

    runner = StatefulOuterRunner()
    _install_runtime_host_fixture(tmp_path, authority, runner, monkeypatch)
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    instance.capacity_authority = FakeCapacityAuthority()
    instance.distributed_runtime_authority = ProductionGraphRuntimeAuthority()
    instance.environment_admission_fence = runtime_host.fence_registry_environment_intent

    fourth = environments[3]
    fifth = environments[4]
    fourth_result = _converge(instance, fourth, candidates[3], suffix="fourth")
    assert fourth_result["status"] == "committed"

    config = supervisor.load_config(runtime_host.SUPERVISOR_CONFIG_PATH)
    broker = SharedCapacityBroker(config.state_db, clock=lambda: NOW)
    for pool, target in (("oldlab", 4), ("gb10", 12)):
        broker.request_capacity(
            sandbox=SandboxId(fourth.runtime_id),
            candidate_sha=candidates[3].candidate_sha,
            pool=pool,
            min_slots=1,
            target_slots=target,
            ttl_seconds=7200,
            purpose="dynamic-cohort-production-graph",
            preemptible=True,
            idempotency_key=f"{fourth.runtime_id}-{pool}-committed",
        )
    supervisor.run_once(config, now=NOW)

    fourth_instances = tuple(f"{fourth.runtime_id}-{pool}" for pool in runtime_host.POOLS)
    fourth_adapter_bytes = {
        path.name: path.read_bytes()
        for path in runtime_host._environment_configs(fourth.runtime_id)
    }
    fourth_unit_states = {
        unit: dict(runner._unit(unit))
        for pair in runtime_host._environment_units(fourth.runtime_id)
        for unit in pair
    }
    fourth_handoff_bytes = {
        instance_name: (config.handoff_dir / "current" / f"{instance_name}.json").read_bytes()
        for instance_name in fourth_instances
    }
    fourth_job_ids = ("810041", "810042")
    runner.slurm_jobs = {
        fourth_job_ids[0]: (
            fourth.slurm_user,
            fourth.slurm_account,
            "RUNNING",
            f"loom-env-{fourth.runtime_id}-oldlab",
        ),
        fourth_job_ids[1]: (
            fourth.slurm_user,
            fourth.slurm_account,
            "RUNNING",
            f"loom-env-{fourth.runtime_id}-gb10",
        ),
    }
    before_fifth_calls = len(runner.calls)

    fifth_result = _converge(instance, fifth, candidates[4], suffix="fifth")
    assert fifth_result["status"] == "committed"

    snapshot = authority.snapshot()
    active = {row["env_id"]: row for row in snapshot["environments"] if row["state"] == "active"}
    committed = {
        row["env_id"]: row for row in snapshot["deployments"] if row["phase"] == "committed"
    }
    assert fourth.env_id in active and fifth.env_id in active
    assert fourth.env_id in committed and fifth.env_id in committed

    assert {
        path.name: path.read_bytes()
        for path in runtime_host._environment_configs(fourth.runtime_id)
    } == fourth_adapter_bytes
    assert {unit: dict(runner._unit(unit)) for unit in fourth_unit_states} == fourth_unit_states
    assert {
        instance_name: (config.handoff_dir / "current" / f"{instance_name}.json").read_bytes()
        for instance_name in fourth_instances
    } == fourth_handoff_bytes
    assert tuple(runner.slurm_jobs) == fourth_job_ids

    fifth_configs = runtime_host._environment_configs(fifth.runtime_id)
    assert all(path.is_file() and path.stat().st_mode & 0o777 == 0o600 for path in fifth_configs)
    for service, timer in runtime_host._environment_units(fifth.runtime_id):
        assert runner._unit(service)["active"] is True
        assert runner._unit(timer) == {"enabled": True, "active": True}
    manifest = json.loads(runtime_host.STATE_PATH.read_text(encoding="ascii"))["runtime_manifest"]
    manifest_runtime_ids = {row["runtime_id"] for row in manifest["environments"]}
    assert {fourth.runtime_id, fifth.runtime_id} <= manifest_runtime_ids
    current_config = supervisor.load_config(runtime_host.SUPERVISOR_CONFIG_PATH)
    current_manifest = json.loads(
        (current_config.handoff_dir / "current/manifest.json").read_text(encoding="utf-8")
    )
    assert set(fourth_instances) <= set(current_manifest["instances"])
    assert {
        f"{fifth.runtime_id}-gb10",
        f"{fifth.runtime_id}-oldlab",
    } <= set(current_manifest["instances"])

    fifth_calls = runner.calls[before_fifth_calls:]
    fourth_units = set(fourth_unit_states)
    assert not any(
        command[0] == "systemctl"
        and command[1] in {"stop", "disable", "restart"}
        and any(unit in command for unit in fourth_units)
        for command in fifth_calls
    )
    assert not any(command[0] == "scancel" for command in runner.calls)
