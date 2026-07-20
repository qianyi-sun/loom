from __future__ import annotations

import json
import os
import subprocess
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.gb10_convergence import (
    GB10ConvergenceState,
    GB10FleetCandidateObservation,
    GB10HostCandidateObservation,
)
from loom_cli.rollout.operator.protected_apply_executor import (
    KubernetesProtectedConvergenceExecutor,
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
        if "FROM rate_cards" in command and "FROM provider_connections" not in command:
            self.calls.append("defaults-read")
            return b'{"rate_cards":[]}'
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


class GB10Fleet:
    def __init__(self, *, exact: bool = False) -> None:
        self.exact = exact
        self.calls: list[str] = []

    def observe(self, plan):
        self.calls.append("gb10-read")
        return GB10FleetCandidateObservation(
            hosts={
                host: GB10HostCandidateObservation(
                    host=host,
                    boot_id=boot_id,
                    baseline_ready=True,
                    candidate_source_exact=True,
                    checkout_exact=self.exact,
                    environment_exact=self.exact,
                    units_exact=self.exact,
                    legacy_absent=self.exact,
                    service_timer_exact=self.exact,
                    evidence_digest="b" * 64,
                )
                for host, boot_id in plan.gb10_boot_ids.items()
            },
            candidate_source_digest=plan.gb10_unit_digest,
        )

    def apply(self, _plan, convergence):
        assert convergence.state is GB10ConvergenceState.READY
        self.calls.append("gb10-apply")
        self.exact = True


def _attempt(state_root: Path) -> None:
    state_root.mkdir(mode=0o700, exist_ok=True)
    state_root.chmod(0o700)
    path = state_root / "requests/req-alpha/attempts/1"
    path.mkdir(parents=True, mode=0o700)


def _plan(tmp_path: Path):
    plan = _published_plan(tmp_path)
    token_path = tmp_path / "service-token"
    token_path.write_text("service-secret\n")
    token_path.chmod(0o600)
    trusted = read_trusted_file(
        token_path,
        service_uid=os.geteuid(),
        private=True,
        require_nonempty=True,
    )
    return replace(
        plan,
        service_token_source=f"file:{token_path}",
        secret_metadata_fingerprints={
            **plan.secret_metadata_fingerprints,
            "service": f"sha256:{trusted.metadata_fingerprint}",
        },
    )


def _defaults_request(**kwargs):
    assert kwargs["method"] == "GET"
    assert kwargs["path"] == "/api/v1/provider-connections"
    assert kwargs["token"] == "service-secret"
    assert kwargs["payload"] is None
    return 200, b'{"items":[]}'


def test_executor_orders_epoch_before_nonlegacy_migration_and_recovers(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    _attempt(state)
    plan = _plan(tmp_path)
    runner = Runner(revision="0066", epoch=7)
    runner.plan_digest = plan.plan_digest
    executor = MigrationEpochProtectedApplyExecutor(
        state_root=state,
        service_uid=os.geteuid(),
        runner=runner,
        gb10_transport=GB10Fleet(),
        production_defaults_request=_defaults_request,
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
        _plan(tmp_path),
        schema_revision="0065",
        starting_mutation_epoch=0,
    )
    runner = Runner(revision="0065", epoch=None)
    runner.plan_digest = plan.plan_digest
    executor = MigrationEpochProtectedApplyExecutor(
        state_root=state,
        service_uid=os.geteuid(),
        runner=runner,
        gb10_transport=GB10Fleet(),
        production_defaults_request=_defaults_request,
    )

    result = executor("final.protected-apply", CheckOperation.APPLY, plan)

    assert result.observed_epoch == 1
    assert runner.calls.index("migration-apply") < runner.calls.index("epoch-apply")
    roots = sorted((state / "requests/req-alpha/attempts/1/protected-apply").iterdir())
    assert roots[0].name == "00-database-migration"
    assert roots[1].name == "01-mutation-epoch-claim"
    assert roots[2].name == "02-staging-manifests"
    assert roots[3].name == "03-gb10-candidate"
    assert roots[4].name == "04-production-defaults"


def test_executor_rejects_non_apply_operation(tmp_path: Path) -> None:
    executor = MigrationEpochProtectedApplyExecutor(
        state_root=tmp_path,
        service_uid=os.geteuid(),
        runner=Runner(revision="0066", epoch=7),
        gb10_transport=GB10Fleet(),
    )
    with pytest.raises(ValueError, match="operation is invalid"):
        executor("final.browser", CheckOperation.VERIFY, _plan(tmp_path))


def test_convergence_reuses_exact_classifiers_without_mutating(tmp_path: Path) -> None:
    state = tmp_path / "state"
    _attempt(state)
    plan = _plan(tmp_path)
    runner = Runner(revision="0066", epoch=7)
    runner.plan_digest = plan.plan_digest
    gb10 = GB10Fleet()
    applied = MigrationEpochProtectedApplyExecutor(
        state_root=state,
        service_uid=os.geteuid(),
        runner=runner,
        gb10_transport=gb10,
        production_defaults_request=_defaults_request,
    )("final.protected-apply", CheckOperation.APPLY, plan)
    assert applied.ready
    before = tuple(runner.calls)

    result = KubernetesProtectedConvergenceExecutor(
        service_uid=os.geteuid(),
        runner=runner,
        gb10_transport=gb10,
        production_defaults_request=_defaults_request,
    )("final.convergence", CheckOperation.VERIFY, plan)

    assert result.ready
    assert not result.protected_mutation
    assert result.observed_epoch == 8
    convergence_calls = runner.calls[len(before) :]
    assert "migration-read" in convergence_calls
    assert "epoch-read" in convergence_calls
    assert "manifest-diff" in convergence_calls
    assert "defaults-read" not in convergence_calls
    assert all(not call.endswith("-apply") for call in convergence_calls)


def test_convergence_reports_drift_without_applying(tmp_path: Path) -> None:
    plan = _plan(tmp_path)
    runner = Runner(revision="0066", epoch=9)
    runner.plan_digest = plan.plan_digest
    runner.manifest_status = 1

    result = KubernetesProtectedConvergenceExecutor(
        service_uid=os.geteuid(),
        runner=runner,
        gb10_transport=GB10Fleet(exact=False),
        production_defaults_request=_defaults_request,
    )("final.convergence", CheckOperation.VERIFY, plan)

    assert not result.ready
    assert set(result.blockers) == {
        "database-migration",
        "mutation-epoch-claim",
        "staging-manifests",
        "gb10-candidate",
        "production-defaults",
    }
    assert all(not call.endswith("-apply") for call in runner.calls)


def test_convergence_rejects_mutating_or_wrong_check_operation(tmp_path: Path) -> None:
    executor = KubernetesProtectedConvergenceExecutor(
        service_uid=os.geteuid(),
        runner=Runner(revision="0067", epoch=8),
        gb10_transport=GB10Fleet(exact=True),
    )
    with pytest.raises(ValueError, match="operation is invalid"):
        executor("final.convergence", CheckOperation.APPLY, _plan(tmp_path))
    with pytest.raises(ValueError, match="operation is invalid"):
        executor("final.summary", CheckOperation.VERIFY, _plan(tmp_path))


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
        runner.capture_stdout_with_input(
            ("kubectl", "apply", "-f", "-"),
            env=runner.environment,
            input_payload=b"manifest\n",
            timeout_seconds=5,
        )
        == b"ok\n"
    )
    assert calls[1][1]["input"] == b"manifest\n"

    assert (
        runner.run_status(
            ("kubectl", "diff", "-f", "-"),
            env=runner.environment,
            input_payload=b"manifest\n",
            timeout_seconds=5,
        )
        == 0
    )
    assert calls[2][1]["stdout"] is subprocess.DEVNULL
    assert calls[2][1]["stderr"] is subprocess.DEVNULL

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
