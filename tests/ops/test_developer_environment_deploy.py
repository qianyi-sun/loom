from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import threading
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_deploy as deploy
from scripts.ops import developer_environment_registry as registry
from scripts.ops import developer_sandbox_slurm_policy as slurm_policy


def _git(path: Path, *args: str) -> str:
    result = subprocess.run(
        ("git", "-C", str(path), *args),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.lock = threading.Lock()
        self.jobs = ""
        self.fail_up_once = False
        self.accounts: dict[str, str] = {}
        self.qos: set[str] = set()
        self.users: dict[str, tuple[str, str]] = {}
        self.resource_labels: dict[tuple[str, str], dict[str, str]] = {}
        self.systemd_load_state = "loaded"
        self.systemd_fragment = "/etc/systemd/system/loom-developer-environment@.service"
        self.systemd_unit_file_state = "disabled"
        self.compose_service_states: dict[str, str] = {}
        self.postgres_checkpoints = 0

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> deploy.CommandResult:
        del env, expected
        command = tuple(argv)
        with self.lock:
            self.calls.append(command)
        if command[0] == "git":
            result = subprocess.run(
                command,
                cwd=cwd,
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode:
                raise deploy.DeploymentError("host command failed safely")
            return deploy.CommandResult(0, result.stdout)
        if command[:2] == ("docker", "compose") and "up" in command:
            override = Path(
                command[max(index for index, value in enumerate(command) if value == "--file") + 1]
            )
            payload = json.loads(override.read_text(encoding="ascii"))
            network = payload["networks"]["default"]
            self.resource_labels[("network", network["name"])] = network["labels"]
            for volume in payload["volumes"].values():
                self.resource_labels[("volume", volume["name"])] = volume["labels"]
            self.compose_service_states = {service: "running" for service in deploy.ALL_SERVICES}
            if self.fail_up_once:
                self.fail_up_once = False
                raise deploy.DeploymentError("host command failed safely")
        if command[:2] == ("docker", "compose") and "exec" in command:
            if command[command.index("exec") + 2] == "postgres":
                self.postgres_checkpoints += 1
        if command[:2] == ("docker", "compose") and "stop" in command:
            service = command[-1]
            if service in self.compose_service_states:
                self.compose_service_states[service] = "exited"
        if command[:2] == ("docker", "compose") and "down" in command:
            self.compose_service_states.clear()
        if command[:2] == ("docker", "compose") and "ps" in command:
            rows = [
                {"Service": service, "State": state}
                for service, state in self.compose_service_states.items()
            ]
            return deploy.CommandResult(0, json.dumps(rows))
        if command[:2] == ("docker", "ps"):
            return deploy.CommandResult(
                0,
                "".join(f"container-{index}\n" for index in range(len(deploy.ALL_SERVICES))),
            )
        if (
            command[0] == "docker"
            and command[1] in {"volume", "network"}
            and command[2] == "inspect"
        ):
            key = (command[1], command[-1])
            if key not in self.resource_labels:
                return deploy.CommandResult(1, "")
            labels = self.resource_labels[key]
            return deploy.CommandResult(0, json.dumps(labels))
        if command[0] == "docker" and command[1] in {"volume", "network"} and command[2] == "rm":
            key = (command[1], command[-1])
            existed = self.resource_labels.pop(key, None) is not None
            return deploy.CommandResult(0 if existed else 1, "")
        if command[:2] == ("systemctl", "show"):
            value = {
                "--property=LoadState": self.systemd_load_state,
                "--property=FragmentPath": self.systemd_fragment,
                "--property=UnitFileState": self.systemd_unit_file_state,
            }[next(value for value in command if value.startswith("--property="))]
            return deploy.CommandResult(0, value + "\n")
        if command[:2] == ("systemctl", "enable"):
            self.systemd_load_state = "loaded"
            self.systemd_fragment = "/etc/systemd/system/loom-developer-environment@.service"
            self.systemd_unit_file_state = "enabled"
            return deploy.CommandResult(0, "")
        if command[:3] == ("systemctl", "disable", "--now"):
            self.systemd_load_state = "loaded"
            self.systemd_fragment = "/etc/systemd/system/loom-developer-environment@.service"
            self.systemd_unit_file_state = "disabled"
            return deploy.CommandResult(0, "")
        if command[0] == "sacctmgr":
            if "add" in command and "account" in command:
                account = command[command.index("account") + 1]
                self.accounts.setdefault(account, "")
            elif "add" in command and "qos" in command:
                self.qos.add(command[command.index("qos") + 1])
            elif "modify" in command and "account" in command:
                account = command[command.index("account") + 1]
                default = next(
                    value.split("=", 1)[1] for value in command if value.startswith("defaultqos=")
                )
                self.accounts[account] = default
            elif "add" in command and "user" in command:
                user = command[command.index("user") + 1]
                account = next(
                    value.split("=", 1)[1] for value in command if value.startswith("account=")
                )
                qos = next(
                    value.split("=", 1)[1] for value in command if value.startswith("defaultqos=")
                )
                self.users[user] = (account, qos)
            elif "modify" in command and "user" in command:
                user = command[command.index("user") + 1]
                account = next(
                    value.split("=", 1)[1] for value in command if value.startswith("account=")
                )
                qos = next(
                    value.split("=", 1)[1] for value in command if value.startswith("defaultqos=")
                )
                self.users[user] = (account, qos)
            elif "show" in command and "account" in command:
                account = command[command.index("account") + 1]
                qos = self.accounts.get(account)
                if qos is None:
                    return deploy.CommandResult(0, "")
                if "format=Account,QOS,DefaultQOS" in command:
                    return deploy.CommandResult(0, f"{account}|{qos}|{qos}\n")
                return deploy.CommandResult(0, f"{account}|\n")
            elif "show" in command and "qos" in command:
                qos = command[command.index("qos") + 1]
                return deploy.CommandResult(0, f"{qos}|\n" if qos in self.qos else "")
            elif "show" in command and "user" in command:
                user = command[command.index("user") + 1]
                binding = self.users.get(user)
                return (
                    deploy.CommandResult(0, f"{user}|{binding[0]}|{binding[1]}\n")
                    if binding is not None
                    else deploy.CommandResult(0, "")
                )
            return deploy.CommandResult(0, "")
        if command[0] == "squeue":
            return deploy.CommandResult(0, self.jobs)
        return deploy.CommandResult(0, "")


class _Response:
    status = 200

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class FakeCapacityAuthority:
    def __init__(self) -> None:
        self.contexts: list[deploy.DeploymentContext] = []
        self.checked_contexts: list[deploy.DeploymentContext] = []
        self.rolled_back_contexts: list[deploy.DeploymentContext] = []
        self.aborted_contexts: list[deploy.DeploymentContext] = []

    def abort(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.aborted_contexts.append(context)
        return {"status": "retired"}

    def reconcile(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.contexts.append(context)
        return {
            "status": "ready",
            "domains": {
                "oldlab": {"status": "ready"},
                "gb10": {"status": "ready"},
            },
        }

    def finalize(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.contexts.append(context)
        return {"status": "ready"}

    def finalize_check(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.checked_contexts.append(context)
        return {"status": "acceptance-prepared"}

    def check(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.checked_contexts.append(context)
        return {"status": "ready"}

    def rollback(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.rolled_back_contexts.append(context)
        return {"status": "ready"}

    def retire(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.rolled_back_contexts.append(context)
        return {"status": "retired", "payload_sha256": "8" * 64}

    def reactivate(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.contexts.append(context)
        return {"status": "revive-prepared", "payload_sha256": "7" * 64}


class FakeDistributedRuntimeAuthority:
    def __init__(self) -> None:
        self.reconciled: list[deploy.DeploymentContext] = []
        self.checked: list[deploy.DeploymentContext] = []
        self.rolled_back: list[deploy.DeploymentContext] = []
        self.retired: list[deploy.DeploymentContext] = []
        self.actions: list[str] = []

    @staticmethod
    def _result(action: str) -> dict[str, Any]:
        return {"status": "ready", "action": action}

    def reconcile(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("reconcile")
        self.reconciled.append(context)
        return self._result("reconcile")

    def check(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("check")
        self.checked.append(context)
        return self._result("check")

    def acceptance_probe(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("acceptance-probe")
        self.checked.append(context)
        return {
            "status": "passed",
            "action": "acceptance-probe",
            "payload_sha256": "9" * 64,
        }

    def activate(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("activate")
        self.checked.append(context)
        return self._result("activate")

    def fence(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("fence")
        self.checked.append(context)
        return self._result("fence")

    def rollback(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("rollback")
        self.rolled_back.append(context)
        return self._result("rollback")

    def retire(self, context: deploy.DeploymentContext) -> dict[str, Any]:
        self.actions.append("retire")
        self.retired.append(context)
        return self._result("retire")


class DistributedRuntimeReceiptRunner:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[tuple[str, ...]] = []

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> deploy.CommandResult:
        del cwd, env, expected
        command = tuple(argv)
        self.calls.append(command)
        action = command[1]
        deployment_id = command[-1]
        request_path = self.root / "requests" / f"{deployment_id}-{action}.json"
        request = json.loads(request_path.read_text(encoding="ascii"))
        retired = action in {"activate", "fence", "rollback", "retire"}
        shared_capacity = (
            {
                "schema_version": 1,
                "status": "ready",
                "runtime_id": request["runtime_id"],
                "instances": [f"{request['runtime_id']}-{domain}" for domain in ("oldlab", "gb10")],
            }
            if retired
            else {
                "schema_version": 1,
                "status": "prepared",
                "runtime_id": request["runtime_id"],
                "candidate_id": request["candidate_id"],
                "candidate_sha": request["candidate_sha"],
                "candidate_tree": request["candidate_tree"],
                "resource_generation": request["resource_generation"],
                "registry_generation": request["registry_generation"],
                "registry_payload_sha256": request["registry_snapshot_sha256"],
                "instances": [f"{request['runtime_id']}-{domain}" for domain in ("oldlab", "gb10")],
                "activation_status": "installed",
            }
        )
        unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-environment.runtime-receipt",
            "status": shared_capacity["status"],
            "action": action,
            "deployment_id": deployment_id,
            "env_id": request["env_id"],
            "runtime_id": request["runtime_id"],
            "candidate_id": request["candidate_id"],
            "candidate_sha": request["candidate_sha"],
            "candidate_tree": request["candidate_tree"],
            "effective_candidate_id": (None if action == "rollback" else request["candidate_id"]),
            "effective_candidate_sha": (None if action == "rollback" else request["candidate_sha"]),
            "effective_candidate_tree": (
                None if action == "rollback" else request["candidate_tree"]
            ),
            "resource_generation": request["resource_generation"],
            "registry_generation": request["registry_generation"],
            "registry_snapshot_sha256": request["registry_snapshot_sha256"],
            "request_sha256": request["payload_sha256"],
            "domains": ["oldlab", "gb10"],
            "nodes": [
                node
                for domain in ("oldlab", "gb10")
                for node in deploy.DOMAIN_RUNTIME_NODES[domain]
            ],
            "remote_link": {"status": "ready"},
            "domain_runtime": {"status": "ready"},
            "shared_capacity": shared_capacity,
            "completed_at": "2026-07-29T12:00:00Z",
        }
        receipt = {**unsigned, "payload_sha256": deploy._digest(unsigned)}
        receipt_root = self.root / "receipts"
        receipt_root.mkdir(parents=True, exist_ok=True)
        receipt_path = receipt_root / f"{deployment_id}-{action}.json"
        receipt_path.write_bytes(deploy._canonical(receipt))
        receipt_path.chmod(0o600)
        return deploy.CommandResult(0, "")


class CapacityReceiptRunner:
    def __init__(
        self,
        root: Path,
        *,
        domains: tuple[str, ...] = ("oldlab", "gb10"),
        omitted_identity_node: str | None = None,
    ) -> None:
        self.root = root
        self.domains = domains
        self.omitted_identity_node = omitted_identity_node

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> deploy.CommandResult:
        del cwd, env, expected
        deployment_id = argv[-1]
        request = json.loads(
            (self.root / "requests" / f"{deployment_id}.json").read_text(encoding="ascii")
        )
        domain_rows = {}
        for domain in self.domains:
            identity_preflight = {
                node: {
                    "status": "available",
                    "receipt_sha256": "e" * 64,
                }
                for node in deploy.DOMAIN_IDENTITY_NODES[domain]
                if node != self.omitted_identity_node
            }
            identity_convergence = {
                node: {
                    "request_id": hashlib.sha256(f"request:{node}".encode()).hexdigest(),
                    "result_sha256": "1" * 64,
                    "authority_receipt_sha256": "2" * 64,
                    "completed_at": "2026-07-29T12:00:00Z",
                    "status": "exact-existing",
                    "readback_receipt_sha256": "3" * 64,
                }
                for node in deploy.DOMAIN_IDENTITY_NODES[domain]
                if node != self.omitted_identity_node
            }
            slurm_convergence = {
                node: {
                    "action": "slurm-identity-converge",
                    "request_id": hashlib.sha256(f"slurm:{node}".encode()).hexdigest(),
                    "result_sha256": "4" * 64,
                    "authority_receipt_sha256": "5" * 64,
                    "completed_at": "2026-07-29T12:00:00Z",
                }
                for node in deploy.DOMAIN_IDENTITY_NODES[domain]
                if node != self.omitted_identity_node
            }
            policy_sha256 = deploy._digest(
                {node: proof["result_sha256"] for node, proof in slurm_convergence.items()}
            )
            domain_rows[domain] = {
                "status": "ready",
                "env_id": request["env_id"],
                "slurm_user": request["slurm_user"],
                "service_group": request["service_group"],
                "uid": request["uid"],
                "gid": request["gid"],
                "slurm_account": request["slurm_account"],
                "slurm_qos": request["slurm_qos"],
                "candidate_sha": request["candidate_sha"],
                "candidate_tree": request["candidate_tree"],
                "registry_snapshot_sha256": request["registry_snapshot_sha256"],
                "policy_generation": request["registry_generation"],
                "policy_sha256": policy_sha256,
                "authority_receipt_sha256": deploy._digest(
                    {
                        "identity_convergence": identity_convergence,
                        "slurm_convergence": slurm_convergence,
                    }
                ),
                **{
                    "oldlab": {
                        "cluster": "trt-oldlab",
                        "controller": "TRT-EAI-OLDLAB-1",
                        "submit_host": "trt-EAI-OLDLAB-2",
                    },
                    "gb10": {
                        "cluster": "trt-gb10",
                        "controller": "trt-gb10-1",
                        "submit_host": "trt-gb10-1",
                    },
                }[domain],
                "identity_preflight": identity_preflight,
                "identity_preflight_sha256": deploy._digest(identity_preflight),
                "identity_convergence": identity_convergence,
                "identity_convergence_sha256": deploy._digest(identity_convergence),
                "slurm_convergence": slurm_convergence,
                "slurm_convergence_sha256": deploy._digest(slurm_convergence),
                "completed_at": "2026-07-29T12:00:00Z",
            }
        receipt = deploy._bound(
            {
                "schema_version": 1,
                "kind": "loom.developer-environment.capacity-receipt",
                "status": ("acceptance-prepared" if argv[1] == "finalize" else "prepared"),
                "request_sha256": request["payload_sha256"],
                **{
                    key: request[key]
                    for key in (
                        "env_id",
                        "principal_id",
                        "deployment_id",
                        "candidate_id",
                        "candidate_sha",
                        "candidate_tree",
                        "resource_generation",
                        "registry_generation",
                        "registry_snapshot_sha256",
                        "slurm_user",
                        "service_group",
                        "slurm_account",
                        "slurm_qos",
                        "uid",
                        "gid",
                    )
                },
                "domains": domain_rows,
            }
        )
        path = self.root / "receipts" / f"{deployment_id}.json"
        path.write_bytes(deploy._canonical(receipt))
        path.chmod(0o600)
        return deploy.CommandResult(0, "")


class StaticCapacityCheckRunner:
    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = dict(payload)
        self.calls: list[tuple[str, ...]] = []

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> deploy.CommandResult:
        del cwd, env, expected
        self.calls.append(tuple(argv))
        return deploy.CommandResult(
            0,
            deploy._canonical(self.payload).decode("ascii"),
        )


def _register(
    authority: registry.DeveloperEnvironmentRegistry,
    principal: str,
    number: int,
) -> registry.EnvironmentRecord:
    return authority.register(
        {
            "schema_version": 1,
            "kind": registry.REGISTER_KIND,
            "principal_id": principal,
            "idempotency_key": f"registration-key-{number:04d}",
            "display_name": f"Developer {number}",
        }
    )


def _source_candidate(tmp_path: Path, suffix: str) -> tuple[Path, str, str, bytes]:
    source = tmp_path / f"source-{suffix}"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.email", "test@example.invalid")
    _git(source, "config", "user.name", "Test")
    compose = source / "deploy" / "docker-compose.dev.yml"
    compose.parent.mkdir()
    compose.write_text(
        "services:\n"
        + "".join(f"  {service}:\n    image: busybox\n" for service in deploy.ALL_SERVICES),
        encoding="utf-8",
    )
    (source / "candidate.txt").write_text(suffix, encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-m", f"candidate {suffix}")
    sha = _git(source, "rev-parse", "HEAD")
    tree = _git(source, "rev-parse", "HEAD^{tree}")
    bundle = tmp_path / f"candidate-{suffix}.bundle"
    _git(source, "bundle", "create", str(bundle), "HEAD")
    return bundle, sha, tree, bundle.read_bytes()


def _candidate(
    authority: registry.DeveloperEnvironmentRegistry,
    environment: registry.EnvironmentRecord,
    tmp_path: Path,
    suffix: str,
) -> registry.CandidateRecord:
    bundle, sha, tree, raw = _source_candidate(tmp_path, suffix)
    record = authority.import_candidate(
        {
            "schema_version": 1,
            "kind": registry.CANDIDATE_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": f"candidate-import-{suffix}-0001",
            "env_id": environment.env_id,
            "candidate_sha": sha,
            "candidate_tree": tree,
            "bundle_sha256": hashlib.sha256(raw).hexdigest(),
            "bundle_size": len(raw),
            "image_digests": {
                "amd64": "sha256:" + "a" * 64,
                "arm64": "sha256:" + "b" * 64,
            },
        }
    )
    persisted = Path(record.bundle_path)
    persisted.parent.mkdir(mode=0o700, parents=True)
    persisted.write_bytes(bundle.read_bytes())
    persisted.chmod(0o600)
    return record


def _deployer(
    authority: registry.DeveloperEnvironmentRegistry,
    tmp_path: Path,
    runner: FakeRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> deploy.DeveloperEnvironmentDeployer:
    def runtime_retire_executor(
        deployment_id: str,
        env_id: str,
        operation_sha256: str,
    ) -> dict[str, Any]:
        snapshot = authority.snapshot()
        environment = next(row for row in snapshot["environments"] if row["env_id"] == env_id)
        deployment = next(
            row for row in snapshot["deployments"] if row["deployment_id"] == deployment_id
        )
        candidate = next(
            row
            for row in snapshot["candidates"]
            if row["candidate_id"] == deployment["candidate_id"]
        )
        return deploy._bound(
            {
                "schema_version": 1,
                "kind": deploy.runtime_retire.COMBINED_RECEIPT_KIND,
                "status": "cleaned",
                "action": deploy.runtime_retire.ACTION,
                "deployment_id": deployment_id,
                "env_id": env_id,
                "principal_id": environment["principal_id"],
                "runtime_id": environment["runtime_id"],
                "resource_generation": environment["resource_generation"],
                "registry_generation": snapshot["generation"],
                "registry_snapshot_sha256": snapshot["payload_sha256"],
                "retire_operation_sha256": operation_sha256,
                "candidate_bindings": [
                    {
                        "candidate_id": candidate["candidate_id"],
                        "candidate_sha": candidate["candidate_sha"],
                        "candidate_tree": candidate["candidate_tree"],
                    }
                ],
                "nodes": {node: "a" * 64 for node in deploy.runtime_retire.NODES},
                "completed_at": "2026-07-29T20:02:00Z",
            }
        )

    instance = deploy.DeveloperEnvironmentDeployer(
        authority,
        runner=runner,
        require_root_metadata=False,
        manage_ownership=False,
        expected_hostname="",
        host_root=tmp_path / "host",
        capacity_authority=FakeCapacityAuthority(),
        distributed_runtime_authority=FakeDistributedRuntimeAuthority(),
        runtime_retire_executor=runtime_retire_executor,
        environment_admission_fence=lambda runtime_id, intent_sha256: deploy._bound(
            {
                "schema_version": 1,
                "status": "ready",
                "runtime_id": runtime_id,
                "intent_sha256": intent_sha256,
                "admission_token": intent_sha256[:32],
            }
        ),
    )
    monkeypatch.setattr(instance, "_ensure_identity", lambda _context: None)
    monkeypatch.setattr(deploy.urllib.request, "urlopen", lambda *_args, **_kwargs: _Response())
    return instance


def _converge(
    instance: deploy.DeveloperEnvironmentDeployer,
    environment: registry.EnvironmentRecord,
    candidate: registry.CandidateRecord,
    *,
    operation: str = "create",
    suffix: str = "0001",
) -> dict[str, Any]:
    return instance.converge(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        candidate_id=candidate.candidate_id,
        idempotency_key=f"deployment-{suffix}-idempotency",
        operation=operation,
    )


def test_exact_environment_fence_and_drain_precede_registry_transitions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:admission-order", 1)
    first = _candidate(authority, environment, tmp_path, "admission-order-one")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    events: list[tuple[str, str]] = []

    def fence(runtime_id: str, intent_sha256: str) -> dict[str, Any]:
        current = authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        )
        events.append(("fence", current.state))
        return deploy._bound(
            {
                "status": "ready",
                "runtime_id": runtime_id,
                "intent_sha256": intent_sha256,
                "admission_token": intent_sha256[:32],
            }
        )

    instance.environment_admission_fence = fence
    original_drain = instance._assert_drained

    def drain(bound_environment: Mapping[str, Any]) -> None:
        current = authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        )
        events.append(("drain", current.state))
        original_drain(bound_environment)

    monkeypatch.setattr(instance, "_assert_drained", drain)
    original_begin_deployment = authority.begin_deployment

    def begin_deployment(payload: Mapping[str, Any]) -> registry.DeploymentRecord:
        current = authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        )
        events.append(("begin-deployment", current.state))
        return original_begin_deployment(payload)

    monkeypatch.setattr(authority, "begin_deployment", begin_deployment)
    _converge(instance, environment, first, suffix="admission-order-one")
    assert events[:3] == [
        ("fence", "ready"),
        ("drain", "ready"),
        ("begin-deployment", "ready"),
    ]

    active = authority.lookup(
        environment.env_id,
        principal_id=environment.principal_id,
    )
    second = _candidate(authority, active, tmp_path, "admission-order-two")
    events.clear()
    _converge(
        instance,
        active,
        second,
        operation="update",
        suffix="admission-order-two",
    )
    assert events[:3] == [
        ("fence", "active"),
        ("drain", "active"),
        ("begin-deployment", "active"),
    ]

    events.clear()
    original_begin_retirement = authority.begin_retirement

    def begin_retirement(
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> registry.EnvironmentRecord:
        current = authority.lookup(env_id, principal_id=principal_id)
        events.append(("begin-retirement", current.state))
        return original_begin_retirement(
            env_id,
            principal_id=principal_id,
            expected_resource_generation=expected_resource_generation,
        )

    monkeypatch.setattr(authority, "begin_retirement", begin_retirement)
    instance.retire(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="retire-admission-order-0001",
    )
    assert events[:3] == [
        ("fence", "active"),
        ("drain", "active"),
        ("begin-retirement", "active"),
    ]


def test_direct_deployer_reloads_reallocated_ports_before_fence_and_begin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    listeners: set[int] = set()
    authority = registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry.sqlite3",
        port_inventory_collector=lambda: frozenset(listeners),
    )
    environment = _register(authority, "oidc:example:direct-port-race", 1)
    candidate = _candidate(authority, environment, tmp_path, "direct-port-race")
    old_ports = dict(environment.ports)
    listeners.add(environment.ports["control_plane"])
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    observed: list[tuple[str, int]] = []

    def fence(runtime_id: str, intent_sha256: str) -> dict[str, Any]:
        current = authority.lookup(environment.env_id)
        observed.append(("fence", current.resource_generation))
        return deploy._bound(
            {
                "status": "ready",
                "runtime_id": runtime_id,
                "intent_sha256": intent_sha256,
                "admission_token": intent_sha256[:32],
            }
        )

    instance.environment_admission_fence = fence
    original_begin = authority.begin_deployment

    def begin(payload: Mapping[str, Any]) -> registry.DeploymentRecord:
        observed.append(("begin", int(payload["expected_resource_generation"])))
        return original_begin(payload)

    monkeypatch.setattr(authority, "begin_deployment", begin)

    result = _converge(instance, environment, candidate)
    current = authority.lookup(environment.env_id)
    compose_env = (
        instance._global_runtime_path(
            "lifecycle",
            "environments",
            environment.env_id,
        )
        / "compose.env"
    ).read_text(encoding="ascii")

    assert result["status"] == "committed"
    assert observed[:2] == [("fence", 2), ("begin", 2)]
    assert min(current.ports.values()) == 23_016
    assert str(current.ports["control_plane"]) in compose_env
    assert str(old_ports["control_plane"]) not in compose_env


def test_direct_deployer_inventory_failure_precedes_fence_and_begin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def collect() -> frozenset[int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return frozenset()
        raise registry.RegistryError("inventory failed")

    authority = registry.DeveloperEnvironmentRegistry(
        tmp_path / "registry.sqlite3",
        port_inventory_collector=collect,
    )
    environment = _register(authority, "oidc:example:inventory-failure", 1)
    candidate = _candidate(authority, environment, tmp_path, "inventory-failure")
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)
    fenced: list[str] = []

    def unexpected_fence(runtime_id: str, _intent: str) -> dict[str, Any]:
        fenced.append(runtime_id)
        return {}

    instance.environment_admission_fence = unexpected_fence
    monkeypatch.setattr(
        authority,
        "begin_deployment",
        lambda _payload: pytest.fail("deployment must remain unopened"),
    )

    with pytest.raises(deploy.DeploymentError, match="port recovery failed safely"):
        _converge(instance, environment, candidate)

    assert fenced == []
    assert authority.snapshot()["deployments"] == []


def test_fence_crash_replays_same_active_intent_before_registry_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:fence-crash", 1)
    candidate = _candidate(authority, environment, tmp_path, "fence-crash")
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)
    observed: list[str] = []

    def crash_after_close(runtime_id: str, intent_sha256: str) -> dict[str, Any]:
        del runtime_id
        observed.append(intent_sha256)
        if len(observed) == 1:
            raise deploy.DeploymentError("injected crash after exact fence close")
        return deploy._bound(
            {
                "status": "ready",
                "intent_sha256": intent_sha256,
                "admission_token": intent_sha256[:32],
            }
        )

    instance.environment_admission_fence = crash_after_close
    with pytest.raises(deploy.DeploymentError, match="after exact fence close"):
        _converge(instance, environment, candidate, suffix="fence-crash")
    assert (
        authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        ).state
        == "ready"
    )
    intent = deploy._load_bound_json(
        instance._admission_intent_path(environment.runtime_id),
        kind=deploy.ADMISSION_INTENT_KIND,
    )
    assert intent is not None and intent["phase"] == "recorded"
    peer = _register(authority, "oidc:example:fence-crash-peer", 2)

    result = _converge(instance, environment, candidate, suffix="fence-crash")
    assert result["status"] == "committed"
    assert observed[0] == observed[1]
    assert (
        authority.lookup(
            peer.env_id,
            principal_id=peer.principal_id,
        )
        == peer
    )


def test_commit_before_activate_crash_reopens_then_rebuilds_usable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:activate-crash", 1)
    candidate = _candidate(authority, environment, tmp_path, "activate-crash")
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)
    runtime = instance.distributed_runtime_authority
    assert isinstance(runtime, FakeDistributedRuntimeAuthority)
    original_activate = runtime.activate
    failed = False

    def fail_first_activate(context: deploy.DeploymentContext) -> dict[str, Any]:
        nonlocal failed
        if not failed:
            failed = True
            runtime.actions.append("activate-crash")
            raise deploy.DeploymentError("injected crash before admission reopen")
        return original_activate(context)

    monkeypatch.setattr(runtime, "activate", fail_first_activate)
    with pytest.raises(deploy.DeploymentError, match="before admission reopen"):
        _converge(instance, environment, candidate, suffix="activate-crash")
    assert (
        authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        ).state
        == "active"
    )

    action_count = len(runtime.actions)
    result = _converge(instance, environment, candidate, suffix="activate-crash")
    assert result["status"] == "committed"
    assert runtime.actions[action_count : action_count + 2] == ["activate", "check"]
    committed = max(
        (
            row
            for row in authority.snapshot()["deployments"]
            if row["env_id"] == environment.env_id and row["phase"] == "committed"
        ),
        key=lambda row: row["updated_at"],
    )
    assert instance._global_runtime_path(
        "usable",
        f"{committed['deployment_id']}.json",
    ).is_file()


def test_fourth_developer_deploys_without_fixed_profile_or_resource_choice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environments = [
        _register(authority, f"oidc:example:developer-{number}", number) for number in range(1, 5)
    ]
    fourth = environments[-1]
    candidate = _candidate(authority, fourth, tmp_path, "fourth")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)

    result = _converge(instance, fourth, candidate)
    runtime_authority = instance.distributed_runtime_authority
    assert isinstance(runtime_authority, FakeDistributedRuntimeAuthority)
    assert runtime_authority.actions[-2:] == ["activate", "check"]
    lifecycle_root = instance._global_runtime_path(
        "lifecycle",
        "environments",
        fourth.env_id,
    )
    committed_manifest = (lifecycle_root / "host-manifest.json").read_bytes()
    committed_override = (lifecycle_root / "compose.override.json").read_bytes()
    _register(authority, "oidc:example:developer-5", 5)
    replay = _converge(instance, fourth, candidate)
    resumed = instance.resume(runtime_id=fourth.systemd_instance)

    assert result["status"] == "committed"
    assert replay["deployment_id"] == result["deployment_id"]
    assert resumed["deployment_id"] == result["deployment_id"]
    current = authority.lookup(fourth.env_id, principal_id=fourth.principal_id)
    assert current.current_candidate_id == candidate.candidate_id
    assert current.state == "active"
    encoded = json.dumps(result) + "\n".join(" ".join(call) for call in runner.calls)
    assert all(name not in encoded for name in ("loom-sandbox-qianyi", "hongjian", "devansh"))
    compose_env = (lifecycle_root / "compose.env").read_text(encoding="ascii")
    assert str(fourth.ports["control_plane"]) in compose_env
    assert fourth.database_name in compose_env
    assert fourth.provider_namespace in compose_env
    assert f"SLURM_ACCOUNT={fourth.slurm_account}" in compose_env
    assert f"SLURM_QOS={fourth.slurm_qos}" in compose_env
    assert f"SLURM_USER={fourth.slurm_user}" in compose_env
    assert (lifecycle_root / "host-manifest.json").read_bytes() == committed_manifest
    assert (lifecycle_root / "compose.override.json").read_bytes() == committed_override
    manifest = json.loads((lifecycle_root / "host-manifest.json").read_text(encoding="ascii"))
    override = json.loads((lifecycle_root / "compose.override.json").read_text(encoding="ascii"))
    snapshot = authority.snapshot()
    deployment = next(
        item for item in snapshot["deployments"] if item["deployment_id"] == result["deployment_id"]
    )
    assert manifest["resource_generation"] == current.resource_generation
    assert (
        deployment["applied_resource_generation"] == deployment["expected_resource_generation"] + 1
    )
    assert manifest["applied_registry_generation"] == deployment["applied_registry_generation"]
    assert (
        manifest["applied_registry_payload_sha256"] == deployment["applied_registry_payload_sha256"]
    )
    labels = override["services"]["worker"]["labels"]
    assert labels == {
        "loom.developer-environment.env-id": current.env_id,
        "loom.developer-environment.runtime-id": current.runtime_id,
        "loom.developer-environment.compose-project": current.compose_project,
        "loom.developer-environment.candidate-id": candidate.candidate_id,
        "loom.developer-environment.candidate-sha": candidate.candidate_sha,
        "loom.developer-environment.candidate-tree": candidate.candidate_tree,
        "loom.developer-environment.image-digest": result["image_digest"],
        "loom.developer-environment.resource-generation": str(current.resource_generation),
        "loom.developer-environment.registry-generation": str(
            deployment["applied_registry_generation"]
        ),
        "loom.developer-environment.registry-payload-sha256": deployment[
            "applied_registry_payload_sha256"
        ],
    }
    assert (
        "systemctl",
        "enable",
        f"loom-developer-environment@{fourth.systemd_instance}.service",
    ) in runner.calls
    assert len(snapshot["deployments"]) == 1


def test_root_lifecycle_tree_owns_lock_journal_manifest_and_compose_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:root-lifecycle", 1)
    candidate = _candidate(authority, environment, tmp_path, "root-lifecycle")
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)

    _converge(instance, environment, candidate, suffix="root-lifecycle")

    state_root = tmp_path / "host" / Path(environment.state_root).relative_to("/")
    lifecycle_root = instance._global_runtime_path(
        "lifecycle",
        "environments",
        environment.env_id,
    )
    assert stat.S_IMODE(lifecycle_root.stat().st_mode) == 0o700
    assert not {
        "deployment.lock",
        "deployment-journal.json",
        "host-manifest.json",
        "compose.env",
        "compose.override.json",
    }.intersection(path.name for path in state_root.iterdir())
    for name in (
        "deployment.lock",
        "deployment-journal.json",
        "host-manifest.json",
        "compose.env",
        "compose.override.json",
    ):
        path = lifecycle_root / name
        assert path.is_file() and not path.is_symlink()
        assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_deployment_lock_rejects_unlink_and_split_inode_attack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:split-lock", 1)
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)
    bound = next(
        row for row in authority.snapshot()["environments"] if row["env_id"] == environment.env_id
    )
    lock_path = instance._global_runtime_path(
        "lifecycle",
        "environments",
        environment.env_id,
        "deployment.lock",
    )
    original_flock = deploy.fcntl.flock

    def split_inode(descriptor: int, operation: int) -> None:
        original_flock(descriptor, operation)
        if operation == deploy.fcntl.LOCK_EX:
            lock_path.unlink()
            replacement = os.open(
                lock_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            os.close(replacement)

    monkeypatch.setattr(deploy.fcntl, "flock", split_inode)
    with pytest.raises(deploy.DeploymentError, match="lock identity drifted"):
        with instance._lock(bound):
            pytest.fail("split lock inode must never enter the critical section")


def test_service_writable_checkout_and_resume_compose_swap_fail_before_docker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:resume-toctou", 1)
    candidate = _candidate(authority, environment, tmp_path, "resume-toctou")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate, suffix="resume-toctou")
    active = authority.lookup(
        environment.env_id,
        principal_id=environment.principal_id,
    )
    snapshot = authority.snapshot()
    context = instance._active_committed_context(
        next(row for row in snapshot["environments"] if row["env_id"] == environment.env_id)
    )
    assert context is not None

    context.checkout.chmod(0o770)
    victim = context.checkout / "candidate.txt"
    original = victim.read_bytes()
    victim.rename(context.checkout / "candidate.old")
    victim.write_bytes(original)
    with pytest.raises(deploy.DeploymentError, match="checkout metadata is unsafe"):
        instance._verify_checkout(context, context.checkout)

    context.checkout.chmod(0o700)
    (context.checkout / "candidate.old").unlink()
    before_docker = sum(call[:2] == ("docker", "compose") for call in runner.calls)
    context.compose_override_path.write_bytes(b"{}\n")
    with pytest.raises(deploy.DeploymentError, match="Compose input binding"):
        _converge(
            instance,
            active,
            candidate,
            suffix="resume-toctou",
        )
    after_docker = sum(call[:2] == ("docker", "compose") for call in runner.calls)
    assert after_docker == before_docker


def test_dynamic_systemd_template_uses_only_registry_runtime_identity() -> None:
    unit = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/loom-developer-environment@.service"
    ).read_text(encoding="utf-8")

    assert (
        "ExecStart=/usr/bin/python3 -I -B "
        "/usr/local/libexec/loom-developer-environment-deploy "
        "resume --runtime-id %i --execute"
    ) in unit
    assert (
        "ExecStartPost=/usr/bin/python3 -I -B "
        "/usr/local/libexec/loom-developer-environment-deploy check --runtime-id %i"
    ) in unit
    assert "--env-id" not in unit
    assert all(name not in unit for name in ("qianyi", "hongjian", "devansh"))


def test_capacity_identity_inventory_matches_fixed_transport_domains() -> None:
    assert deploy.DOMAIN_IDENTITY_NODES == {
        domain: (values["authority_node"],)
        for domain, values in slurm_policy._CAPACITY_DOMAINS.items()
    }


def test_renew_active_enumerates_complete_dynamic_registry_cohort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.ops import developer_sandbox_host as legacy_host

    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environments = [
        _register(authority, f"oidc:example:renew-{number}", number) for number in range(1, 3)
    ]
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    for number, environment in enumerate(environments, 1):
        _converge(
            instance,
            environment,
            _candidate(authority, environment, tmp_path, f"renew-{number}"),
            suffix=f"renew-{number}",
        )
    profiles: list[legacy_host.Profile] = []

    def collect(
        profile: legacy_host.Profile,
        sha: str,
        tree: str,
    ) -> dict[str, Any]:
        profiles.append(profile)
        return {
            "env_id": profile.env_id,
            "resource_generation": profile.resource_generation,
            "registry_generation": profile.registry_generation,
            "registry_payload_sha256": profile.registry_payload_sha256,
            "candidate_sha": sha,
            "candidate_tree": tree,
            "nodes": {
                node: {}
                for node in (
                    *legacy_host.DOMAIN_PEERS["oldlab"],
                    *legacy_host.DOMAIN_PEERS["gb10"],
                )
            },
        }

    monkeypatch.setattr(
        legacy_host,
        "_collect_and_persist_remote_link_fleet",
        collect,
    )
    result = instance.renew_active()

    assert result["environment_count"] == 2
    assert result["env_ids"] == sorted(environment.env_id for environment in environments)
    assert {profile.sandbox for profile in profiles} == {
        environment.runtime_id for environment in environments
    }
    assert all(
        profile.registry_payload_sha256 == result["registry_snapshot_sha256"]
        for profile in profiles
    )


def test_two_independent_dynamic_environments_converge_concurrently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environments = [
        _register(authority, f"oidc:example:parallel-{number}", number) for number in range(1, 3)
    ]
    candidates = [
        _candidate(authority, environment, tmp_path, f"parallel-{number}")
        for number, environment in enumerate(environments, 1)
    ]
    runner = FakeRunner()
    instances = [
        _deployer(authority, tmp_path, runner, monkeypatch) for _environment in environments
    ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda values: _converge(*values),
                zip(instances, environments, candidates, strict=True),
            )
        )

    assert {row["status"] for row in results} == {"committed"}
    assert environments[0].compose_project != environments[1].compose_project
    assert not set(environments[0].ports.values()).intersection(environments[1].ports.values())


def test_crash_replay_resumes_same_deployment_and_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:crash", 1)
    candidate = _candidate(authority, environment, tmp_path, "crash")
    runner = FakeRunner()
    runner.fail_up_once = True
    instance = _deployer(authority, tmp_path, runner, monkeypatch)

    with pytest.raises(deploy.DeploymentError, match="host command failed safely"):
        _converge(instance, environment, candidate)
    failed_snapshot = authority.snapshot()
    active = [
        row for row in failed_snapshot["deployments"] if row["phase"] not in {"committed", "failed"}
    ]
    assert len(active) == 1
    assert active[0]["phase"] == "candidate-materialized"

    result = _converge(instance, environment, candidate)
    assert result["deployment_id"] == active[0]["deployment_id"]
    assert result["candidate_id"] == candidate.candidate_id
    assert (
        authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        ).state
        == "active"
    )


@pytest.mark.parametrize(
    ("failed_phase", "failed_action"),
    (
        ("requested", "_ensure_resources"),
        ("resources-verified", "_materialize_candidate"),
        ("candidate-materialized", "_prepare_services"),
    ),
)
def test_boot_resume_unit_precedes_resource_work_and_replays_exact_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_phase: str,
    failed_action: str,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, f"oidc:example:boot-{failed_phase}", 1)
    candidate = _candidate(authority, environment, tmp_path, f"boot-{failed_phase}")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    original = getattr(instance, failed_action)
    failed = False

    def fail_once(context: deploy.DeploymentContext) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise deploy.DeploymentError("injected pre-commit crash")
        original(context)

    monkeypatch.setattr(instance, failed_action, fail_once)

    with pytest.raises(deploy.DeploymentError, match="injected pre-commit crash"):
        _converge(instance, environment, candidate, suffix=f"boot-{failed_phase}")

    active = [
        row
        for row in authority.snapshot()["deployments"]
        if row["env_id"] == environment.env_id and row["phase"] not in {"committed", "failed"}
    ]
    unit = f"loom-developer-environment@{environment.systemd_instance}.service"
    enable = ("systemctl", "enable", unit)
    assert len(active) == 1
    assert active[0]["phase"] == failed_phase
    assert active[0]["candidate_id"] == candidate.candidate_id
    assert runner.systemd_unit_file_state == "enabled"
    assert runner.calls.count(enable) == 1

    resumed = instance.resume(runtime_id=environment.systemd_instance)

    assert resumed["deployment_id"] == active[0]["deployment_id"]
    assert resumed["candidate_id"] == candidate.candidate_id
    assert resumed["status"] == "committed"
    assert runner.calls.count(enable) == 1


def test_recorded_finalization_resume_performs_zero_mutations_or_new_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:finalization-resume", 1)
    candidate = _candidate(authority, environment, tmp_path, "finalization-resume")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "finalization-resume-deploy",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    for expected, following in zip(
        registry.DEPLOY_PHASES[:-2],
        registry.DEPLOY_PHASES[1:-1],
        strict=True,
    ):
        deployment = authority.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )
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
            "capacity_finalize_receipt_sha256": "1" * 64,
            "capacity_finalize_check_receipt_sha256": "2" * 64,
            "runtime_reconcile_receipt_sha256": "3" * 64,
            "runtime_prepare_check_receipt_sha256": "4" * 64,
            "acceptance_probe_receipt_sha256": "5" * 64,
        },
    )
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)
    snapshot = authority.snapshot()
    context = deploy._context(
        snapshot,
        next(row for row in snapshot["environments"] if row["env_id"] == environment.env_id),
        deployment_id=deployment.deployment_id,
        host_root=tmp_path / "host",
    )

    resumed = instance._prepare_finalization(context)

    assert resumed == {
        "capacity_finalize": "1" * 64,
        "capacity_finalize_check": "2" * 64,
        "runtime_reconcile": "3" * 64,
        "runtime_check": "4" * 64,
        "acceptance_probe": "5" * 64,
    }
    capacity = instance.capacity_authority
    runtime = instance.distributed_runtime_authority
    assert not capacity.contexts
    assert not capacity.checked_contexts
    assert not runtime.reconciled
    assert not runtime.checked


def test_committed_snapshot_publication_failure_rebuilds_derived_usable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:commit-publish-recovery", 1)
    candidate = _candidate(authority, environment, tmp_path, "commit-publish-recovery")
    instance = _deployer(authority, tmp_path, FakeRunner(), monkeypatch)
    original_publish = authority._publish_snapshot_bytes
    failed = False

    def fail_committed_once(raw: bytes) -> None:
        nonlocal failed
        payload = registry.DeveloperEnvironmentRegistry.verify_snapshot(raw)
        if not failed and any(row["phase"] == "committed" for row in payload["deployments"]):
            failed = True
            raise registry.RegistryError("injected snapshot publication failure")
        original_publish(raw)

    monkeypatch.setattr(authority, "_publish_snapshot_bytes", fail_committed_once)
    with pytest.raises(registry.RegistryError, match="injected"):
        _converge(instance, environment, candidate)
    committed = authority.snapshot()
    deployment = next(row for row in committed["deployments"] if row["phase"] == "committed")
    assert (
        next(row for row in committed["environments"] if row["env_id"] == environment.env_id)[
            "state"
        ]
        == "active"
    )
    assert not instance._global_runtime_path(
        "usable",
        f"{deployment['deployment_id']}.json",
    ).exists()
    capacity_mutations = len(instance.capacity_authority.contexts)
    runtime_mutations = len(instance.distributed_runtime_authority.reconciled)

    result = _converge(instance, environment, candidate)

    assert result["deployment_id"] == deployment["deployment_id"]
    assert instance._global_runtime_path(
        "usable",
        f"{deployment['deployment_id']}.json",
    ).is_file()
    assert len(instance.capacity_authority.contexts) == capacity_mutations
    assert len(instance.distributed_runtime_authority.reconciled) == runtime_mutations


def test_committed_update_can_rollback_to_exact_previous_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:rollback", 1)
    first = _candidate(authority, environment, tmp_path, "rollback-first")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, first)
    active = authority.lookup(environment.env_id, principal_id=environment.principal_id)
    second = _candidate(authority, active, tmp_path, "rollback-second")
    _converge(
        instance,
        active,
        second,
        operation="update",
        suffix="0002",
    )

    result = instance.rollback(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="rollback-operation-0001",
    )

    assert result["candidate_id"] == first.candidate_id
    restored = authority.lookup(environment.env_id, principal_id=environment.principal_id)
    assert restored.current_candidate_id == first.candidate_id
    assert runner.systemd_unit_file_state == "enabled"


def test_interrupted_update_rollback_replays_same_failed_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:rollback-replay", 1)
    first = _candidate(authority, environment, tmp_path, "rollback-replay-first")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, first)
    active = authority.lookup(environment.env_id, principal_id=environment.principal_id)
    second = _candidate(authority, active, tmp_path, "rollback-replay-second")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": active.principal_id,
            "idempotency_key": "interrupted-update-0001",
            "env_id": active.env_id,
            "candidate_id": second.candidate_id,
            "expected_resource_generation": active.resource_generation,
        }
    )

    class FailOnce(FakeDistributedRuntimeAuthority):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        def rollback(self, context: deploy.DeploymentContext) -> dict[str, Any]:
            self.rolled_back.append(context)
            if not self.failed:
                self.failed = True
                raise deploy.DeploymentError("injected runtime rollback failure")
            return self._result("rollback")

    runtime = FailOnce()
    instance.distributed_runtime_authority = runtime
    key = "rollback-replay-operation-0001"

    with pytest.raises(deploy.DeploymentError, match="injected"):
        instance.rollback(
            env_id=active.env_id,
            principal_id=active.principal_id,
            idempotency_key=key,
        )

    failed = next(
        row
        for row in authority.snapshot()["deployments"]
        if row["deployment_id"] == deployment.deployment_id
    )
    assert failed["phase"] == "failed"

    result = instance.rollback(
        env_id=active.env_id,
        principal_id=active.principal_id,
        idempotency_key=key,
    )

    assert result["candidate_id"] == first.candidate_id
    assert len(runtime.rolled_back) == 2
    assert {item.deployment_id for item in runtime.rolled_back} == {deployment.deployment_id}
    assert len(authority.snapshot()["deployments"]) == 2


def test_first_create_rollback_retires_capacity_before_releasing_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:create-abort", 1)
    candidate = _candidate(authority, environment, tmp_path, "create-abort")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    runner.systemd_unit_file_state = "enabled"
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "create-abort-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    for expected, following in zip(
        registry.DEPLOY_PHASES[:3],
        registry.DEPLOY_PHASES[1:4],
        strict=True,
    ):
        authority.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )
    capacity = instance.capacity_authority
    assert isinstance(capacity, FakeCapacityAuthority)

    result = instance.rollback(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="create-abort-rollback-0001",
    )

    assert result["status"] == "ready"
    assert [item.deployment_id for item in capacity.aborted_contexts] == [deployment.deployment_id]
    assert (
        "systemctl",
        "disable",
        "--now",
        f"loom-developer-environment@{environment.systemd_instance}.service",
    ) in runner.calls
    assert runner.systemd_unit_file_state == "disabled"
    snapshot = authority.snapshot()
    failed = next(
        item
        for item in snapshot["deployments"]
        if item["deployment_id"] == deployment.deployment_id
    )
    assert failed["phase"] == "failed"
    assert (
        next(item for item in snapshot["environments"] if item["env_id"] == environment.env_id)[
            "state"
        ]
        == "ready"
    )


def test_first_create_abort_failure_keeps_registry_deployment_in_flight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry.sqlite3")
    environment = _register(authority, "oidc:example:create-abort-failure", 1)
    candidate = _candidate(authority, environment, tmp_path, "create-abort-failure")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "create-abort-failure-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    for expected, following in zip(
        registry.DEPLOY_PHASES[:3],
        registry.DEPLOY_PHASES[1:4],
        strict=True,
    ):
        authority.advance_deployment(
            deployment.deployment_id,
            principal_id=environment.principal_id,
            expected_phase=expected,
            next_phase=following,
            expected_resource_generation=environment.resource_generation,
        )

    class AbortFails(FakeCapacityAuthority):
        def abort(self, context: deploy.DeploymentContext) -> dict[str, Any]:
            self.aborted_contexts.append(context)
            raise deploy.DeploymentError("injected capacity abort failure")

    instance.capacity_authority = AbortFails()
    with pytest.raises(deploy.DeploymentError, match="injected capacity abort"):
        instance.rollback(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="create-abort-failure-rollback-0001",
        )

    snapshot = authority.snapshot()
    still_active = next(
        item
        for item in snapshot["deployments"]
        if item["deployment_id"] == deployment.deployment_id
    )
    assert still_active["phase"] == "services-prepared"
    assert (
        next(item for item in snapshot["environments"] if item["env_id"] == environment.env_id)[
            "state"
        ]
        == "deploying"
    )


def test_snapshot_tamper_and_foreign_owner_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:owner", 1)
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    with pytest.raises(deploy.DeploymentError, match="ownership is invalid"):
        instance.check(env_id=environment.env_id, principal_id="oidc:example:other")

    original = authority.snapshot_bytes

    def tampered() -> bytes:
        payload = json.loads(original())
        payload["environments"][0]["ports"]["web"] += 1
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"

    monkeypatch.setattr(authority, "snapshot_bytes", tampered)
    with pytest.raises(deploy.DeploymentError, match="snapshot verification failed"):
        instance.check(env_id=environment.env_id, principal_id=environment.principal_id)


@pytest.mark.parametrize(
    ("domains", "error"),
    [
        (("oldlab", "gb10"), None),
        (("oldlab",), "domain set is invalid"),
    ],
)
def test_fixed_capacity_authority_requires_both_digest_bound_domains(
    tmp_path: Path,
    domains: tuple[str, ...],
    error: str | None,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:capacity", 1)
    candidate = _candidate(authority, environment, tmp_path, "capacity")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "capacity-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    snapshot = authority.snapshot()
    environment_row = next(
        row for row in snapshot["environments"] if row["env_id"] == environment.env_id
    )
    context = deploy._context(
        snapshot,
        environment_row,
        deployment_id=deployment.deployment_id,
        host_root=tmp_path / "host",
    )
    capacity_root = tmp_path / "capacity"
    producer = deploy.FixedCapacityAuthority(
        CapacityReceiptRunner(capacity_root, domains=domains),
        root=capacity_root,
        program=tmp_path / "fixed-producer",
        require_root_metadata=False,
    )

    if error is None:
        receipt = producer.reconcile(context)
        assert set(receipt["domains"]) == {"oldlab", "gb10"}
    else:
        with pytest.raises(deploy.DeploymentError, match=error):
            producer.reconcile(context)


def test_fixed_capacity_authority_rejects_incomplete_fleet_identity_preflight(
    tmp_path: Path,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:identity-preflight", 1)
    candidate = _candidate(authority, environment, tmp_path, "identity-preflight")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "identity-preflight-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    snapshot = authority.snapshot()
    environment_row = next(
        row for row in snapshot["environments"] if row["env_id"] == environment.env_id
    )
    context = deploy._context(
        snapshot,
        environment_row,
        deployment_id=deployment.deployment_id,
        host_root=tmp_path / "host",
    )
    capacity_root = tmp_path / "capacity"
    producer = deploy.FixedCapacityAuthority(
        CapacityReceiptRunner(
            capacity_root,
            omitted_identity_node="trt-gb10-1",
        ),
        root=capacity_root,
        program=tmp_path / "fixed-producer",
        require_root_metadata=False,
    )

    with pytest.raises(deploy.DeploymentError, match="domain binding is invalid"):
        producer.reconcile(context)


def test_fixed_capacity_check_is_current_registry_and_controller_bound(
    tmp_path: Path,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:capacity-check", 1)
    candidate = _candidate(authority, environment, tmp_path, "capacity-check")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "capacity-check-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    snapshot = authority.snapshot()
    context = deploy._context(
        snapshot,
        next(row for row in snapshot["environments"] if row["env_id"] == environment.env_id),
        deployment_id=deployment.deployment_id,
        host_root=tmp_path / "host",
    )
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.capacity-check",
        "status": "activated",
        "deployment_id": context.deployment_id,
        "env_id": context.env_id,
        "candidate_id": context.candidate_id,
        "candidate_sha": context.candidate_sha,
        "candidate_tree": context.candidate_tree,
        "resource_generation": context.resource_generation,
        "registry_generation": context.snapshot_generation,
        "registry_payload_sha256": context.snapshot_digest,
        "capacity_receipt_sha256": "a" * 64,
        "identity_node_count": 2,
        "domains": ["oldlab", "gb10"],
        "checked_at": "2026-07-29T12:00:00Z",
    }
    payload = {**unsigned, "payload_sha256": deploy._digest(unsigned)}
    runner = StaticCapacityCheckRunner(payload)
    producer = deploy.FixedCapacityAuthority(
        runner,
        root=tmp_path / "capacity",
        program=tmp_path / "fixed-producer",
        require_root_metadata=False,
    )

    assert producer.check(context)["status"] == "activated"
    assert runner.calls == [
        (
            str(tmp_path / "fixed-producer"),
            "check",
            "--deployment-id",
            context.deployment_id,
        )
    ]


def test_fixed_distributed_runtime_uses_only_registry_deployment_identity(
    tmp_path: Path,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:distributed", 1)
    candidate = _candidate(authority, environment, tmp_path, "distributed")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "distributed-deployment-0001",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    snapshot = authority.snapshot()
    context = deploy._context(
        snapshot,
        next(row for row in snapshot["environments"] if row["env_id"] == environment.env_id),
        deployment_id=deployment.deployment_id,
        host_root=tmp_path / "host",
    )
    runtime_root = tmp_path / "runtime-authority"
    runner = DistributedRuntimeReceiptRunner(runtime_root)
    runtime_authority = deploy.FixedDistributedRuntimeAuthority(
        runner,
        root=runtime_root,
        program=tmp_path / "fixed-runtime-authority",
        require_root_metadata=False,
    )

    for action in ("reconcile", "check", "activate", "fence", "rollback", "retire"):
        receipt = getattr(runtime_authority, action)(context)
        if action in {"reconcile", "check"}:
            assert receipt["status"] == "prepared"
            assert receipt["shared_capacity"]["activation_status"] == "installed"
        assert receipt["nodes"] == [
            node for domain in ("oldlab", "gb10") for node in deploy.DOMAIN_RUNTIME_NODES[domain]
        ]
        request = json.loads(
            (runtime_root / "requests" / f"{deployment.deployment_id}-{action}.json").read_text(
                encoding="ascii"
            )
        )
        assert set(request) == {
            "schema_version",
            "kind",
            "action",
            "deployment_id",
            "env_id",
            "principal_id",
            "runtime_id",
            "candidate_id",
            "candidate_sha",
            "candidate_tree",
            "resource_generation",
            "registry_generation",
            "registry_snapshot_sha256",
            "payload_sha256",
        }
        assert not any("path" in key or "node" in key or "pool" in key for key in request)

    assert runner.calls == [
        (
            str(tmp_path / "fixed-runtime-authority"),
            action,
            "--deployment-id",
            deployment.deployment_id,
        )
        for action in ("reconcile", "check", "activate", "fence", "rollback", "retire")
    ]


def test_fixed_runtime_rejects_generic_receipt_as_acceptance_probe(
    tmp_path: Path,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:acceptance-alias", 1)
    candidate = _candidate(authority, environment, tmp_path, "acceptance-alias")
    deployment = authority.begin_deployment(
        {
            "schema_version": 1,
            "kind": registry.DEPLOY_KIND,
            "principal_id": environment.principal_id,
            "idempotency_key": "acceptance-alias-deployment",
            "env_id": environment.env_id,
            "candidate_id": candidate.candidate_id,
            "expected_resource_generation": environment.resource_generation,
        }
    )
    snapshot = authority.snapshot()
    context = deploy._context(
        snapshot,
        next(row for row in snapshot["environments"] if row["env_id"] == environment.env_id),
        deployment_id=deployment.deployment_id,
        host_root=tmp_path / "host",
    )
    runtime_root = tmp_path / "runtime-authority"
    runtime_authority = deploy.FixedDistributedRuntimeAuthority(
        DistributedRuntimeReceiptRunner(runtime_root),
        root=runtime_root,
        program=tmp_path / "fixed-runtime-authority",
        require_root_metadata=False,
    )

    with pytest.raises(deploy.DeploymentError, match="input is unavailable"):
        runtime_authority.acceptance_probe(context)


def test_retire_rejects_foreign_or_nonterminal_job_without_cancelling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:foreign-job", 1)
    candidate = _candidate(authority, environment, tmp_path, "foreign-job")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)
    runner.jobs = "8123|someone-else|foreign|RUNNING|unrelated\n"

    with pytest.raises(deploy.DeploymentError, match="foreign or nonterminal"):
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-foreign-job-0001",
        )

    assert not any(call and call[0] == "scancel" for call in runner.calls)
    assert (
        authority.lookup(
            environment.env_id,
            principal_id=environment.principal_id,
        ).state
        == "active"
    )


def test_exact_owned_retirement_is_persistent_and_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:retire", 1)
    candidate = _candidate(authority, environment, tmp_path, "retire")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)

    first = instance.retire(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="retire-exact-owned-0001",
    )
    second = instance.retire(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="retire-exact-owned-0001",
    )

    assert first == second
    retired = authority.lookup(environment.env_id, principal_id=environment.principal_id)
    assert retired.state == "retired"
    receipt = deploy._load_bound_json(
        instance._cleanup_receipt_path(environment.env_id),
        kind=deploy.RETIRE_RECEIPT_KIND,
    )
    assert receipt is not None
    checkpoints = receipt["object_checkpoints"]
    assert checkpoints["postgres_checkpoint"]["status"] == "checkpointed"
    assert checkpoints["postgres_checkpoint"]["details"]["command"] == "CHECKPOINT"
    assert checkpoints["control_plane_stop"]["status"] == "stopped"
    assert checkpoints["minio_stop"]["status"] == "stopped"
    assert checkpoints["container_absence"]["details"]["container_count"] == 0
    assert checkpoints["privileged_compose_inputs"]["status"] == "removed"
    lifecycle_root = instance._global_runtime_path(
        "lifecycle",
        "environments",
        environment.env_id,
    )
    assert (lifecycle_root / "deployment-journal.json").is_file()
    assert (lifecycle_root / "host-manifest.json").is_file()
    assert not (lifecycle_root / "compose.env").exists()
    assert not (lifecycle_root / "compose.override.json").exists()
    assert runner.postgres_checkpoints == 1
    runtime_authority = instance.distributed_runtime_authority
    assert isinstance(runtime_authority, FakeDistributedRuntimeAuthority)
    assert runtime_authority.actions[-1:] == ["retire"]
    assert (
        "systemctl",
        "disable",
        "--now",
        f"loom-developer-environment@{environment.systemd_instance}.service",
    ) in runner.calls
    commands = [" ".join(call) for call in runner.calls]
    assert any(retired.postgres_volume in command for command in commands)
    assert any(retired.minio_volume in command for command in commands)
    assert not any(command.startswith("rm ") for command in commands)


def test_twenty_node_runtime_cleanup_is_required_before_local_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:retire-fleet-gate", 1)
    candidate = _candidate(authority, environment, tmp_path, "retire-fleet-gate")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)
    successful_executor = instance.runtime_retire_executor

    def unavailable_fleet(
        _deployment_id: str,
        _env_id: str,
        _operation_sha256: str,
    ) -> dict[str, Any]:
        raise deploy.DeploymentError("injected fleet retirement failure")

    instance.runtime_retire_executor = unavailable_fleet
    before = len(runner.calls)
    with pytest.raises(deploy.DeploymentError, match="fleet retirement failure"):
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-fleet-gate-0001",
        )

    operation = deploy._load_bound_json(
        instance._retire_operation_path(environment.env_id),
        kind=deploy.RETIRE_KIND,
    )
    assert operation is not None
    assert operation["phase"] == "capacity-retired"
    assert set(operation["evidence"]) == {"admission_fence", "capacity_retire"}
    assert operation["object_checkpoints"] == {}
    assert (
        authority.lookup(environment.env_id, principal_id=environment.principal_id).state
        == "quarantined"
    )
    blocked_calls = runner.calls[before:]
    assert not any(call[:3] == ("docker", "volume", "rm") for call in blocked_calls)
    assert not any(call[:3] == ("docker", "network", "rm") for call in blocked_calls)

    instance.runtime_retire_executor = successful_executor
    with pytest.raises(
        deploy.DeploymentError,
        match="retirement resumed; use a new create key",
    ):
        _converge(
            instance,
            environment,
            candidate,
            operation="update",
            suffix="quarantine-must-resume",
        )
    assert (
        authority.lookup(environment.env_id, principal_id=environment.principal_id).state
        == "retired"
    )


@pytest.mark.parametrize("failed_checkpoint", deploy.RETIRE_LOCAL_OBJECTS)
def test_retirement_resumes_after_each_object_action_checkpoint_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_checkpoint: str,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(
        authority,
        f"oidc:example:retire-checkpoint-{failed_checkpoint}",
        1,
    )
    candidate = _candidate(
        authority,
        environment,
        tmp_path,
        f"retire-checkpoint-{failed_checkpoint}",
    )
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)
    original_writer = instance._write_retire_operation
    injected = False

    def fail_after_exact_action(
        bound_environment: Mapping[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        nonlocal injected
        checkpoints = kwargs.get("object_checkpoints", {})
        if (
            not injected
            and kwargs.get("phase") == "runtime-retired"
            and failed_checkpoint in checkpoints
        ):
            injected = True
            raise deploy.DeploymentError("injected retire checkpoint publication failure")
        return original_writer(bound_environment, **kwargs)

    monkeypatch.setattr(instance, "_write_retire_operation", fail_after_exact_action)
    with pytest.raises(
        deploy.DeploymentError,
        match="injected retire checkpoint publication failure",
    ):
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-object-checkpoint-0001",
        )

    operation = deploy._load_bound_json(
        instance._retire_operation_path(environment.env_id),
        kind=deploy.RETIRE_KIND,
    )
    assert operation is not None
    assert failed_checkpoint not in operation["object_checkpoints"]

    result = instance.retire(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="retire-object-checkpoint-0001",
    )
    receipt = deploy._load_bound_json(
        instance._cleanup_receipt_path(environment.env_id),
        kind=deploy.RETIRE_RECEIPT_KIND,
    )
    assert result["status"] == "retired"
    assert receipt is not None
    assert set(receipt["object_checkpoints"]) == set(deploy.RETIRE_LOCAL_OBJECTS)
    if failed_checkpoint in {
        "control_plane_stop",
        "minio_stop",
        "systemd_unit",
        "postgres_volume",
        "minio_volume",
        "compose_network",
        "candidate_tree",
        "runtime_tree",
        "state_tree",
    }:
        assert (
            receipt["object_checkpoints"][failed_checkpoint]["status"]
            == "missing-after-authorized-retry"
        )
    assert instance.host_root is not None
    state_root = instance.host_root.joinpath(*Path(environment.state_root).parts[1:])
    assert not state_root.exists()
    assert (
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-object-checkpoint-0001",
        )
        == result
    )
    assert not state_root.exists()


def test_retirement_rejects_a_different_idempotency_key_after_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:retire-conflict", 1)
    candidate = _candidate(authority, environment, tmp_path, "retire-conflict")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)

    instance.retire(
        env_id=environment.env_id,
        principal_id=environment.principal_id,
        idempotency_key="retire-conflict-key-0001",
    )

    with pytest.raises(deploy.DeploymentError, match="idempotency key conflicts"):
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-conflict-key-0002",
        )


def test_ready_environment_cleanup_and_revive_preserve_allocated_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    original = _register(authority, "oidc:example:ready-revive", 1)
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)

    retired_result = instance.retire(
        env_id=original.env_id,
        principal_id=original.principal_id,
        idempotency_key="retire-ready-revive-0001",
    )
    retired = authority.lookup(original.env_id, principal_id=original.principal_id)
    revived_result = instance.revive(
        env_id=original.env_id,
        principal_id=original.principal_id,
        idempotency_key="revive-ready-revive-0001",
        registration_idempotency_key="registration-key-0001",
    )
    revived = authority.lookup(original.env_id, principal_id=original.principal_id)

    assert retired_result["cleanup_receipt_sha256"]
    assert retired.state == "retired"
    assert retired.resource_generation == original.resource_generation + 1
    assert revived_result["revive_journal_sha256"]
    assert revived.state == "ready"
    assert revived.resource_generation == original.resource_generation + 2
    cleanup = deploy._load_bound_json(
        instance._cleanup_receipt_path(original.env_id),
        kind=deploy.RETIRE_RECEIPT_KIND,
    )
    assert cleanup is not None
    assert cleanup["object_checkpoints"]["postgres_checkpoint"]["status"] == "not-present"
    assert cleanup["object_checkpoints"]["control_plane_stop"]["status"] == "not-present"
    assert cleanup["object_checkpoints"]["minio_stop"]["status"] == "not-present"
    assert cleanup["object_checkpoints"]["container_absence"]["status"] == "not-present"
    for field in (
        "env_id",
        "runtime_id",
        "uid",
        "gid",
        "service_user",
        "service_group",
        "candidate_root",
        "runtime_root",
        "state_root",
        "slurm_account",
        "slurm_qos",
    ):
        assert getattr(revived, field) == getattr(original, field)


def test_two_destroy_revive_cycles_keep_versioned_evidence_and_recover_commit_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    principal = "oidc:example:two-revive-cycles"
    original = _register(authority, principal, 1)
    first_candidate = _candidate(authority, original, tmp_path, "cycle-one")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, original, first_candidate, suffix="cycle-one")
    assert instance.host_root is not None

    def secret_hashes(environment: registry.EnvironmentRecord) -> dict[str, str]:
        state_root = instance.host_root.joinpath(
            *Path(environment.state_root).parts[1:],
        )
        return {
            name: hashlib.sha256(
                (state_root / "secrets" / name).read_bytes(),
            ).hexdigest()
            for name in ("environment.env", "sandbox.env", "admin.toml")
        }

    first_secret_hashes = secret_hashes(original)

    first_retire_key = "retire-cycle-one-0001"
    instance.retire(
        env_id=original.env_id,
        principal_id=principal,
        idempotency_key=first_retire_key,
    )
    first_retired = authority.lookup(original.env_id, principal_id=principal)
    old_create_key_replay = authority.register(
        {
            "schema_version": 1,
            "kind": registry.REGISTER_KIND,
            "principal_id": principal,
            "idempotency_key": "registration-key-0001",
            "display_name": "Developer 1",
        }
    )
    assert old_create_key_replay.state == "retired"
    assert old_create_key_replay.resource_generation == first_retired.resource_generation
    first_registration_key = "registration-cycle-one-new"
    authority.register(
        {
            "schema_version": 1,
            "kind": registry.REGISTER_KIND,
            "principal_id": principal,
            "idempotency_key": first_registration_key,
            "display_name": "Developer revived one",
        }
    )
    original_revive_writer = instance._write_revive_operation
    failed = False

    def fail_first_revive_publish(*args: Any, **kwargs: Any) -> dict[str, Any]:
        nonlocal failed
        if not failed:
            failed = True
            raise deploy.DeploymentError("injected revive journal publication failure")
        return original_revive_writer(*args, **kwargs)

    monkeypatch.setattr(instance, "_write_revive_operation", fail_first_revive_publish)
    with pytest.raises(deploy.DeploymentError, match="revive journal publication"):
        instance.revive(
            env_id=original.env_id,
            principal_id=principal,
            idempotency_key="revive-cycle-one-0001",
            registration_idempotency_key=first_registration_key,
        )
    assert authority.lookup(original.env_id, principal_id=principal).state == "ready"
    first_revive = instance.revive(
        env_id=original.env_id,
        principal_id=principal,
        idempotency_key="revive-cycle-one-0001",
        registration_idempotency_key=first_registration_key,
    )
    first_revive_path = instance._revive_operation_path(
        original.env_id,
        new_resource_generation=first_revive["resource_generation"],
        idempotency_key="revive-cycle-one-0001",
    )
    first_retire_path = instance._retire_operation_path(
        original.env_id,
        expected_resource_generation=first_retired.resource_generation - 1,
        idempotency_key=first_retire_key,
    )
    first_cleanup_path = instance._cleanup_receipt_path(
        original.env_id,
        expected_resource_generation=first_retired.resource_generation - 1,
        idempotency_key=first_retire_key,
    )
    first_cycle_bytes = {
        path: path.read_bytes() for path in (first_retire_path, first_cleanup_path)
    }

    ready = authority.lookup(original.env_id, principal_id=principal)
    second_candidate = _candidate(authority, ready, tmp_path, "cycle-two")
    _converge(instance, ready, second_candidate, suffix="cycle-two")
    second_secret_hashes = secret_hashes(ready)
    assert all(
        second_secret_hashes[name] != first_secret_hashes[name] for name in first_secret_hashes
    )
    first_cycle_bytes[first_revive_path] = first_revive_path.read_bytes()
    active_two = authority.lookup(original.env_id, principal_id=principal)
    second_retire_key = "retire-cycle-two-0001"
    instance.retire(
        env_id=original.env_id,
        principal_id=principal,
        idempotency_key=second_retire_key,
    )
    second_retired = authority.lookup(original.env_id, principal_id=principal)
    second_registration_key = "registration-cycle-two-new"
    authority.register(
        {
            "schema_version": 1,
            "kind": registry.REGISTER_KIND,
            "principal_id": principal,
            "idempotency_key": second_registration_key,
            "display_name": "Developer revived two",
        }
    )
    second_revive = instance.revive(
        env_id=original.env_id,
        principal_id=principal,
        idempotency_key="revive-cycle-two-0001",
        registration_idempotency_key=second_registration_key,
    )
    ready_three = authority.lookup(original.env_id, principal_id=principal)
    third_candidate = _candidate(authority, ready_three, tmp_path, "cycle-three")
    _converge(instance, ready_three, third_candidate, suffix="cycle-three")
    third_secret_hashes = secret_hashes(ready_three)

    assert active_two.resource_generation < second_retired.resource_generation
    assert second_revive["resource_generation"] == second_retired.resource_generation + 1
    assert all(
        third_secret_hashes[name] != second_secret_hashes[name] for name in second_secret_hashes
    )
    stable_identity_fields = (
        "env_id",
        "principal_id",
        "runtime_id",
        "uid",
        "gid",
        "ports",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
        "postgres_volume",
        "minio_volume",
    )
    assert all(
        getattr(ready_three, field) == getattr(original, field) for field in stable_identity_fields
    )
    for path, raw in first_cycle_bytes.items():
        assert path.read_bytes() == raw
    assert instance._retire_operation_path(
        original.env_id,
        expected_resource_generation=active_two.resource_generation,
        idempotency_key=second_retire_key,
    ).is_file()
    assert instance._cleanup_receipt_path(
        original.env_id,
        expected_resource_generation=active_two.resource_generation,
        idempotency_key=second_retire_key,
    ).is_file()
    assert instance._revive_operation_path(
        original.env_id,
        new_resource_generation=second_revive["resource_generation"],
        idempotency_key="revive-cycle-two-0001",
    ).is_file()


def test_retirement_rejects_unlabeled_volume_before_any_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:foreign-volume", 1)
    candidate = _candidate(authority, environment, tmp_path, "foreign-volume")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)
    runner.resource_labels[("volume", environment.postgres_volume)] = {}
    before = len(runner.calls)

    with pytest.raises(deploy.DeploymentError, match="foreign or unlabeled"):
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-foreign-volume-0001",
        )

    retirement_calls = runner.calls[before:]
    assert not any(call[:3] == ("docker", "volume", "rm") for call in retirement_calls)
    assert not any(call[:3] == ("docker", "network", "rm") for call in retirement_calls)


def test_retirement_rejects_foreign_systemd_fragment_before_disable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = registry.DeveloperEnvironmentRegistry(tmp_path / "registry" / "registry.sqlite3")
    environment = _register(authority, "oidc:example:foreign-systemd", 1)
    candidate = _candidate(authority, environment, tmp_path, "foreign-systemd")
    runner = FakeRunner()
    instance = _deployer(authority, tmp_path, runner, monkeypatch)
    _converge(instance, environment, candidate)
    runner.systemd_load_state = "loaded"
    runner.systemd_fragment = "/tmp/foreign.service"

    with pytest.raises(deploy.DeploymentError, match="systemd instance is foreign"):
        instance.retire(
            env_id=environment.env_id,
            principal_id=environment.principal_id,
            idempotency_key="retire-foreign-systemd-0001",
        )

    assert not any(call[:3] == ("systemctl", "disable", "--now") for call in runner.calls)


def test_legacy_seed_environment_uses_snapshot_resources_not_cohort_count(
    tmp_path: Path,
) -> None:
    payload = {
        "environments": [
            {
                "env_id": "denv-legacy-2f70d2a4d18b49ca",
                "principal_id": "unix-uid:1001",
                "layout_version": "legacy-v1",
            },
            {
                "env_id": "denv-dynamic-fourth",
                "principal_id": "oidc:example:fourth",
                "layout_version": "dynamic-v1",
            },
        ]
    }

    legacy = deploy.select_environment(
        payload,
        principal_id="unix-uid:1001",
        env_id=None,
        root=False,
    )
    fourth = deploy.select_environment(
        payload,
        principal_id="oidc:example:fourth",
        env_id=None,
        root=False,
    )

    assert legacy["layout_version"] == "legacy-v1"
    assert fourth["layout_version"] == "dynamic-v1"
    assert len(payload["environments"]) == 2
