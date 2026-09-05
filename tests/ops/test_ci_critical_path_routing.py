from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from loom_control_plane import ci_runner_lease_broker as leases

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 4, tzinfo=UTC)
CRITICAL_JOBS = (
    "integration-1-of-2",
    "integration-2-of-2",
    "tests-root-1-of-2",
    "tests-root-2-of-2",
)


def _selected_keys(tmp_path: Path, **values: str) -> tuple[str, ...]:
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    script = next(
        step["run"] for step in workflow["jobs"]["ci-route"]["steps"] if step.get("id") == "keys"
    )
    output = tmp_path / "github-output"
    env = {
        **os.environ,
        "GATE_MODE": "full",
        "DOCS_ONLY": "false",
        "WEB_CHECKS": "true",
        "INTEGRATION": "true",
        "INTEGRATION_DOCKER": "true",
        **values,
        "GITHUB_OUTPUT": str(output),
    }
    subprocess.run(["bash", "-e", "-c", script], env=env, check=True, capture_output=True)
    value = output.read_text().splitlines()[-1].removeprefix("job_keys=")
    return tuple(json.loads(value))


def _request(keys: tuple[str, ...], run_id: int) -> leases.RouteRequest:
    return leases.RouteRequest(
        repository="qianyi-sun/loom",
        workflow_name="CI",
        workflow_id=leases.WORKFLOW_CLASS_CONTRACTS["CI"][0],
        workflow_run_id=run_id,
        run_attempt=1,
        head_sha="a" * 40,
        job_keys=keys,
    )


def _broker(tmp_path: Path) -> leases.CiRunnerLeaseBroker:
    config = leases.LeaseBrokerConfig.from_profile(ROOT / "deploy/ci-runners/oldlab5.toml")
    broker = leases.CiRunnerLeaseBroker(tmp_path / "leases.sqlite3", config)
    broker.record_trusted_workflow_generation(
        candidate_sha="b" * 40,
        candidate_tree="c" * 40,
        workflow_blobs={name: "d" * 40 for name in leases.WORKFLOW_CLASS_CONTRACTS},
        evidence={"kind": "installed_runtime", "runtime_sha": "b" * 40},
        predecessor_generation_id=None,
        now=NOW,
    )
    return broker


@pytest.mark.parametrize("occupied_slots", [0, 1, 3, 5])
def test_workflow_allocates_available_normal_slots_to_critical_tests_first(
    tmp_path: Path, occupied_slots: int
) -> None:
    keys = _selected_keys(tmp_path)
    broker = _broker(tmp_path)
    if occupied_slots:
        broker.allocate_route(
            _request(
                ("lint-and-static", "tests-packages", "go-checks", "runtime-payload", "web-checks")[
                    :occupied_slots
                ],
                100,
            ),
            now=NOW,
        )

    result = broker.allocate_route(_request(keys, 101), now=NOW)
    oldlab = {
        job.job_key for job in result.assignments if job.target is leases.PlacementTarget.OLDLAB
    }
    assert oldlab.intersection(CRITICAL_JOBS) == set(CRITICAL_JOBS[: 5 - occupied_slots])
    assert len(oldlab) == 5 - occupied_slots
    assert {job.job_key for job in result.assignments} == set(keys)
    assert len(result.assignments) == len(set(keys))


def test_priority_does_not_change_untrusted_workflow_eligibility(tmp_path: Path) -> None:
    keys = _selected_keys(tmp_path)
    result = _broker(tmp_path).decide_route(_request(keys, 102), now=NOW, allow_oldlab=False)

    assert not result.oldlab_eligible
    assert all(
        job.target is leases.PlacementTarget.GITHUB_HOSTED for job in result.document().assignments
    )


@pytest.mark.parametrize("integration", ["true", "false"])
@pytest.mark.parametrize("web", ["true", "false"])
@pytest.mark.parametrize("docker", ["true", "false"])
def test_priority_preserves_selected_job_set(
    tmp_path: Path, integration: str, web: str, docker: str
) -> None:
    keys = _selected_keys(
        tmp_path, INTEGRATION=integration, WEB_CHECKS=web, INTEGRATION_DOCKER=docker
    )
    expected = {
        "lint-and-static",
        "tests-root-1-of-2",
        "tests-root-2-of-2",
        "tests-packages",
        "go-checks",
        "runtime-payload",
    }
    if integration == "true":
        expected.update(CRITICAL_JOBS[:2])
    if web == "true":
        expected.add("web-checks")
    if docker == "true":
        expected.add("integration-docker")
    assert set(keys) == expected
    assert len(keys) == len(expected)
    if integration == "false":
        assert keys[:2] == CRITICAL_JOBS[2:]


@pytest.mark.parametrize("web", ["true", "false"])
def test_preflight_only_requests_its_original_runtime_jobs(tmp_path: Path, web: str) -> None:
    keys = _selected_keys(tmp_path, GATE_MODE="preflight", WEB_CHECKS=web)
    assert set(keys) == (
        {"runtime-payload", "web-checks"} if web == "true" else {"runtime-payload"}
    )
