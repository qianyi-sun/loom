from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan, RehearsalResources
from loom_cli.rollout.rehearsal_command_runner import InstalledRehearsalStepRunner


def _plan() -> RehearsalPlan:
    return RehearsalPlan(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        checkpoint_evidence_sha256="c" * 64,
        checkpoint_manifest_sha256="d" * 64,
        mutation_epoch=8,
        db_snapshot_identity="pgdump-sha256:" + "e" * 64,
        object_inventory_root="f" * 64,
        schema_revision="0066",
        image_digests={"loom-service": "sha256:" + "1" * 64},
        image_artifact_sha256="2" * 64,
        migration_plan_sha256="3" * 64,
        browser_report_schema_sha256="4" * 64,
        resources=RehearsalResources.derive(
            "rehearsal-" + "5" * 24,
            route_origin="https://staging.example.test/dev",
        ),
    )


def _authority(tmp_path: Path, run):
    executable = tmp_path / "helper"
    executable.write_text("#!/bin/sh\nexit 1\n")
    executable.chmod(0o755)
    state_root = tmp_path / "state"
    plan = _plan()
    directory = state_root / plan.resources.namespace
    directory.mkdir(parents=True, mode=0o700)
    plan_path = directory / "plan.json"
    plan_path.write_text(json.dumps(plan.to_record(), sort_keys=True, separators=(",", ":")))
    plan_path.chmod(0o600)
    return (
        InstalledRehearsalStepRunner(
            state_root=state_root,
            service_uid=os.getuid(),
            run=run,
            executable=executable,
            executable_owner_uid=os.getuid(),
        ),
        plan,
    )


def test_runner_uses_fixed_argv_environment_timeout_and_strict_json(tmp_path: Path) -> None:
    calls = []

    def run(argv, environment, timeout):
        calls.append((tuple(argv), dict(environment), timeout))
        payload = {
            "blockers": {},
            "check_id": "rehearsal.namespace",
            "cleanup_verified": False,
            "details": {"namespace": "created"},
            "passed": True,
            "plan_digest": plan.plan_digest,
            "schema_version": 1,
        }
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")

    authority, plan = _authority(tmp_path, run)
    outcome = authority("rehearsal.namespace", plan)

    assert outcome.passed
    argv, environment, timeout = calls[0]
    assert argv == (
        str(authority.executable),
        "execute",
        "--check-id",
        "rehearsal.namespace",
        "--plan",
        str(authority.state_root / plan.resources.namespace / "plan.json"),
        "--plan-sha256",
        plan.plan_digest,
    )
    assert set(environment) == {"HOME", "LANG", "LC_ALL", "PATH", "XDG_RUNTIME_DIR"}
    assert timeout == 300


def test_runner_accepts_normalized_failure_without_stderr(tmp_path: Path) -> None:
    def run(argv, environment, timeout):
        del environment, timeout
        payload = {
            "blockers": {"route": "candidate-mismatch"},
            "check_id": "rehearsal.browser",
            "cleanup_verified": False,
            "details": {"status": "blocked"},
            "passed": False,
            "plan_digest": plan.plan_digest,
            "schema_version": 1,
        }
        return subprocess.CompletedProcess(argv, 1, json.dumps(payload), "")

    authority, plan = _authority(tmp_path, run)

    assert authority("rehearsal.browser", plan).blockers == {
        "route": "candidate-mismatch"
    }


@pytest.mark.parametrize(
    "mutation",
    [
        {"schema_version": 2},
        {"plan_digest": "0" * 64},
        {"passed": True},
        {"cleanup_verified": True},
        {"unknown": "field"},
    ],
)
def test_runner_rejects_output_contract_drift(
    tmp_path: Path, mutation: dict[str, object]
) -> None:
    def run(argv, environment, timeout):
        del environment, timeout
        payload = {
            "blockers": {"route": "candidate-mismatch"},
            "check_id": "rehearsal.browser",
            "cleanup_verified": False,
            "details": {"status": "blocked"},
            "passed": False,
            "plan_digest": plan.plan_digest,
            "schema_version": 1,
        }
        payload.update(mutation)
        return subprocess.CompletedProcess(argv, 1, json.dumps(payload), "")

    authority, plan = _authority(tmp_path, run)

    with pytest.raises(ValueError, match="evidence drifted"):
        authority("rehearsal.browser", plan)


def test_runner_rejects_nonempty_stderr_or_plan_drift(tmp_path: Path) -> None:
    authority, plan = _authority(
        tmp_path,
        lambda argv, environment, timeout: subprocess.CompletedProcess(
            argv, 1, "{}", "diagnostic"
        ),
    )
    with pytest.raises(RuntimeError, match="output contract"):
        authority("rehearsal.browser", plan)

    plan_path = authority.state_root / plan.resources.namespace / "plan.json"
    plan_path.write_text("{}")
    with pytest.raises(ValueError, match="plan identity drifted"):
        authority("rehearsal.browser", plan)
