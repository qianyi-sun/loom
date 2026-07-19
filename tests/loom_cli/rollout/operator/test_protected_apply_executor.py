from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_apply_executor import (
    MigrationEpochProtectedApplyExecutor,
    SubprocessProtectedApplyCommandRunner,
)
from loom_cli.rollout.preflight_contract import CheckOperation
from tests.loom_cli.rollout.operator.test_protected_migration_component import (
    _published_plan,
)


class Runner:
    def __init__(self, *, revision: str, epoch: int | None) -> None:
        self.revision = revision
        self.epoch = epoch
        self.calls: list[str] = []
        self.environment = {"KUBECONFIG": "/exact"}
        self.manifest_status = 1

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        command = " ".join(argv)
        if "SELECT version_num FROM alembic_version" in command:
            self.calls.append("migration-read")
            return (self.revision + "\n").encode()
        if "WITH bootstrapped AS" in command:
            self.calls.append("epoch-apply")
            variables = {
                argv[index + 1].split("=", 1)[0]: argv[index + 1].split("=", 1)[1]
                for index, value in enumerate(argv)
                if value == "-v"
            }
            assert variables["expected_epoch"] == str(self.epoch or 0)
            self.epoch = int(variables["expected_epoch"]) + 1
            return json.dumps(self._epoch_record(variables)).encode()
        if "FROM staging_mutation_epochs" in command:
            self.calls.append("epoch-read")
            return b"" if self.epoch is None else json.dumps(self._epoch_record()).encode()
        raise AssertionError(command)

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        if "--server-side=true" in argv:
            self.calls.append("manifest-apply")
            assert input_payload is not None
            self.manifest_status = 0
        elif "apply" in argv:
            self.calls.append("migration-apply")
            assert input_payload is not None
        else:
            self.calls.append("migration-wait")
            self.revision = "0067"

    def run_status(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert "diff" in argv
        assert input_payload is not None
        self.calls.append("manifest-diff")
        return self.manifest_status

    def _epoch_record(self, variables=None):
        exact = self.epoch in {1, 8}
        return {
            "environment": "staging",
            "namespace": "loom-staging",
            "epoch": self.epoch,
            "mutation_class": "rollout_apply" if exact else "lifecycle_gc",
            "request_id": "req-alpha" if exact else "req-prior0001",
            "evidence_sha256": variables["evidence_sha256"]
            if variables
            else (self.plan_digest if exact else "f" * 64),
        }

    plan_digest: str


def _attempt(state_root: Path) -> None:
    state_root.mkdir(mode=0o700, exist_ok=True)
    state_root.chmod(0o700)
    path = state_root / "requests/req-alpha/attempts/1"
    path.mkdir(parents=True, mode=0o700)


def test_executor_orders_epoch_before_nonlegacy_migration_and_recovers(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    _attempt(state)
    plan = _published_plan(tmp_path)
    runner = Runner(revision="0066", epoch=7)
    runner.plan_digest = plan.plan_digest
    executor = MigrationEpochProtectedApplyExecutor(
        state_root=state,
        service_uid=os.geteuid(),
        runner=runner,
    )

    result = executor("final.protected-apply", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.observed_epoch == 8
    assert result.protected_mutation
    assert runner.calls.index("epoch-apply") < runner.calls.index("migration-apply")
    assert runner.calls.index("migration-apply") < runner.calls.index("manifest-apply")
    before = tuple(runner.calls)
    assert executor("final.protected-apply", CheckOperation.APPLY, plan) == result
    assert "epoch-apply" not in runner.calls[len(before) :]
    assert "migration-apply" not in runner.calls[len(before) :]
    assert "manifest-apply" not in runner.calls[len(before) :]


def test_executor_orders_legacy_migration_before_epoch_bootstrap(tmp_path: Path) -> None:
    state = tmp_path / "state"
    _attempt(state)
    plan = replace(
        _published_plan(tmp_path),
        schema_revision="0065",
        starting_mutation_epoch=0,
    )
    runner = Runner(revision="0065", epoch=None)
    runner.plan_digest = plan.plan_digest
    executor = MigrationEpochProtectedApplyExecutor(
        state_root=state,
        service_uid=os.geteuid(),
        runner=runner,
    )

    result = executor("final.protected-apply", CheckOperation.APPLY, plan)

    assert result.observed_epoch == 1
    assert runner.calls.index("migration-apply") < runner.calls.index("epoch-apply")
    roots = sorted((state / "requests/req-alpha/attempts/1/protected-apply").iterdir())
    assert roots[0].name == "00-database-migration"
    assert roots[1].name == "01-mutation-epoch-claim"
    assert roots[2].name == "02-staging-manifests"


def test_executor_rejects_non_apply_operation(tmp_path: Path) -> None:
    executor = MigrationEpochProtectedApplyExecutor(
        state_root=tmp_path,
        service_uid=os.geteuid(),
        runner=Runner(revision="0066", epoch=7),
    )
    with pytest.raises(ValueError, match="operation is invalid"):
        executor("final.browser", CheckOperation.VERIFY, _published_plan(tmp_path))


def test_subprocess_runner_has_fixed_environment_and_redacted_failure(
    monkeypatch,
) -> None:
    runner = SubprocessProtectedApplyCommandRunner()
    calls = []

    def run(argv, **kwargs):
        calls.append((tuple(argv), kwargs))
        return SimpleNamespace(returncode=0, stdout=b"ok\n", stderr=b"")

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
        run,
    )

    assert (
        runner.capture_stdout(
            ("kubectl", "version", "--client"),
            env=runner.environment,
            timeout_seconds=5,
        )
        == b"ok\n"
    )
    assert calls[0][1]["env"] == runner.environment
    assert calls[0][1]["input"] is None

    assert (
        runner.run_status(
            ("kubectl", "diff", "-f", "-"),
            env=runner.environment,
            input_payload=b"manifest\n",
            timeout_seconds=5,
        )
        == 0
    )
    assert calls[1][1]["stdout"] is subprocess.DEVNULL
    assert calls[1][1]["stderr"] is subprocess.DEVNULL

    with pytest.raises(ValueError, match="invocation is invalid"):
        runner.capture_stdout(
            ("sh", "-c", "true"),
            env=runner.environment,
            timeout_seconds=5,
        )

    def fail(_argv, **_kwargs):
        return SimpleNamespace(returncode=1, stdout=b"", stderr=b"raw-secret-value")

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
        fail,
    )
    with pytest.raises(RuntimeError, match="failed safely") as exc:
        runner.capture_stdout(
            ("kubectl", "version", "--client"),
            env=runner.environment,
            timeout_seconds=5,
        )
    assert "raw-secret-value" not in str(exc.value)

    def fail_status(_argv, **_kwargs):
        return SimpleNamespace(returncode=2, stdout=b"", stderr=b"raw-secret-value")

    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_apply_executor.subprocess.run",
        fail_status,
    )
    with pytest.raises(RuntimeError, match="status subprocess failed safely"):
        runner.run_status(
            ("kubectl", "diff", "-f", "-"),
            env=runner.environment,
            input_payload=b"manifest\n",
            timeout_seconds=5,
        )
