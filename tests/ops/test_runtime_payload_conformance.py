from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from scripts import (
    component_ownership,
)
from scripts import (
    runtime_payload_conformance as conformance,
)
from scripts import (
    runtime_payload_dispatch as dispatch,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_PAYLOAD_PATHS = {
    "deploy/catalog/gb10-smoke/tasks/gb10-oracle-hello-world/tests/test_result.py",
    "packages/loom-benchmark-terminal-bench-2/tests/fixtures/"
    "tb2-task-chess-best-move/tests/test_best_move.py",
    "packages/loom-benchmark-terminal-bench-2/tests/fixtures/"
    "tb2-task-hello-world/tests/test_outputs.py",
    "tests/fixtures/tasks/healthcheck-flaky/tests/test_ok.py",
    "tests/fixtures/tasks/family-runs-dev/smoke/tests/test_result.py",
    "tests/fixtures/tasks/hello-world/tests/test_result.py",
    "tests/fixtures/tasks/in-box-cli/tests/test_out.py",
    "tests/fixtures/tasks/large-artifact/tests/test_payload.py",
    "tests/fixtures/tasks/multi-step-3/tests/test_phase1.py",
    "tests/fixtures/tasks/multi-step-3/tests/test_phase2.py",
    "tests/fixtures/tasks/multi-step-3/tests/test_phase3.py",
}

SAMPLE_PAYLOAD = "tests/fixtures/tasks/healthcheck-flaky/tests/test_ok.py"

EXPECTED_CASE_FIXTURES = {
    "deploy/catalog/gb10-smoke/tasks/gb10-oracle-hello-world/tests/test_result.py": {
        "result.txt"
    },
    "packages/loom-benchmark-terminal-bench-2/tests/fixtures/"
    "tb2-task-chess-best-move/tests/test_best_move.py": {"best_move.txt"},
    "packages/loom-benchmark-terminal-bench-2/tests/fixtures/"
    "tb2-task-hello-world/tests/test_outputs.py": {"hello.txt"},
    "tests/fixtures/tasks/healthcheck-flaky/tests/test_ok.py": {".ready", "ok.txt"},
    "tests/fixtures/tasks/family-runs-dev/smoke/tests/test_result.py": {"result.txt"},
    "tests/fixtures/tasks/hello-world/tests/test_result.py": {"result.txt"},
    "tests/fixtures/tasks/in-box-cli/tests/test_out.py": {"out.txt"},
    "tests/fixtures/tasks/large-artifact/tests/test_payload.py": {"payload.bin"},
    "tests/fixtures/tasks/multi-step-3/tests/test_phase1.py": {"step1.txt"},
    "tests/fixtures/tasks/multi-step-3/tests/test_phase2.py": {"step2.txt"},
    "tests/fixtures/tasks/multi-step-3/tests/test_phase3.py": {"step3.txt"},
}


def _manifest() -> component_ownership.Manifest:
    return component_ownership.load_manifest(
        REPO_ROOT / "config/component-ownership.toml"
    )


def test_runtime_payload_execution_plan_exactly_covers_declared_payloads() -> None:
    manifest = _manifest()
    tracked_paths = component_ownership._tracked_paths(REPO_ROOT)
    lane_paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=tracked_paths,
        lane="runtime-payload",
    )
    plan = component_ownership.lane_execution_plan(
        manifest,
        tracked_paths=tracked_paths,
        lane="runtime-payload",
    )
    planned_paths = [case["path"] for entry in plan for case in entry["cases"]]

    assert set(lane_paths) == EXPECTED_PAYLOAD_PATHS
    assert set(planned_paths) == EXPECTED_PAYLOAD_PATHS
    assert len(planned_paths) == len(set(planned_paths))
    assert {entry["policy"]["id"] for entry in plan} == {
        "virtual-app-v1",
        "virtual-workspace-v1",
    }
    assert {
        case.path: {fixture.path for fixture in case.fixture_files}
        for case in manifest.execution_cases
    } == EXPECTED_CASE_FIXTURES
    assert all(
        policy.container_image
        == "python@sha256:baf89808ec37adeaab83cec287adb4a2afa4a11c1d51e961c7ec737877e61af6"
        for policy in manifest.execution_policies
    )


def test_dispatcher_executes_every_declared_zero_argument_test(tmp_path: Path) -> None:
    payload = tmp_path / "test_payload.py"
    payload.write_text(
        "def test_second():\n    pass\n\ndef test_first():\n    pass\n",
        encoding="utf-8",
    )

    assert dispatch.execute(payload, logical_path="fixtures/test_payload.py") == (
        "test_first",
        "test_second",
    )


@pytest.mark.parametrize(
    ("source", "message"),
    [
        ("value = 1\n", "no executable tests"),
        ("def test_arg(value):\n    pass\n", "fixtures are unsupported"),
        ("async def test_async():\n    pass\n", "async payload tests"),
        ("@staticmethod\ndef test_decorated():\n    pass\n", "decorated payload tests"),
        ("class TestPayload:\n    def test_method(self):\n        pass\n", "class-based"),
        ("def test_repeat():\n    pass\ndef test_repeat():\n    pass\n", "repeats"),
    ],
)
def test_dispatcher_rejects_unsupported_test_shapes(
    tmp_path: Path,
    source: str,
    message: str,
) -> None:
    payload = tmp_path / "test_payload.py"
    payload.write_text(source, encoding="utf-8")

    with pytest.raises(RuntimeError, match=message):
        dispatch.execute(payload, logical_path="fixtures/test_payload.py")


def test_dispatcher_propagates_test_failure_and_rejects_return_values(
    tmp_path: Path,
) -> None:
    failing = tmp_path / "test_failing.py"
    failing.write_text("def test_failure():\n    assert False\n", encoding="utf-8")
    returning = tmp_path / "test_returning.py"
    returning.write_text("def test_returning():\n    return True\n", encoding="utf-8")

    with pytest.raises(AssertionError):
        dispatch.execute(failing, logical_path="fixtures/test_failing.py")
    with pytest.raises(RuntimeError, match="returned a value"):
        dispatch.execute(returning, logical_path="fixtures/test_returning.py")


def test_seed_case_materializes_only_declared_content_and_sparse_size(tmp_path: Path) -> None:
    manifest = _manifest()
    fixture_root = tmp_path / "workspace"
    case = manifest.execution_case(
        "tests/fixtures/tasks/large-artifact/tests/test_payload.py"
    )

    conformance._seed_case(case, fixture_root)

    assert (fixture_root / "payload.bin").stat().st_size == 100 * 1024 * 1024
    assert {path.name for path in fixture_root.iterdir()} == {"payload.bin"}
    assert all(not path.is_symlink() for path in fixture_root.rglob("*"))


def test_payload_container_is_restricted_and_cleanup_is_forced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _manifest().execution_policy("virtual-workspace-v1")
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        payload = REPO_ROOT / SAMPLE_PAYLOAD
        stdout = (
            json.dumps(
                {
                    "schema_version": 1,
                    "path": SAMPLE_PAYLOAD,
                    "payload_sha256": hashlib.sha256(payload.read_bytes()).hexdigest(),
                    "executed": ["test_ok_file", "test_ready_marker"],
                }
            )
            if command[:2] == ["docker", "run"]
            else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr(conformance.subprocess, "run", fake_run)
    evidence = conformance._run_payload(
        repo_root=REPO_ROOT,
        policy=policy,
        fixture_root=tmp_path,
        path=SAMPLE_PAYLOAD,
    )

    run_command = calls[0]
    assert evidence["executed"] == ["test_ok_file", "test_ready_marker"]
    assert ["--pull", "never"] == run_command[run_command.index("--pull") :][:2]
    assert ["--network", "none"] == run_command[run_command.index("--network") :][:2]
    assert "--read-only" in run_command
    assert ["--cap-drop", "ALL"] == run_command[run_command.index("--cap-drop") :][:2]
    assert ["--security-opt", "no-new-privileges"] == run_command[
        run_command.index("--security-opt") :
    ][:2]
    assert ["--pids-limit", "64"] == run_command[run_command.index("--pids-limit") :][:2]
    assert ["--memory", "256m"] == run_command[run_command.index("--memory") :][:2]
    assert ["--cpus", "1"] == run_command[run_command.index("--cpus") :][:2]
    assert ["--user", "65534:65534"] == run_command[run_command.index("--user") :][:2]
    assert ["--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=16m"] == run_command[
        run_command.index("--tmpfs") :
    ][:2]
    assert f"{REPO_ROOT / 'scripts/runtime_payload_dispatch.py'}:/runner/dispatch.py:ro" in run_command
    assert f"{REPO_ROOT / SAMPLE_PAYLOAD}:/payload/test.py:ro" in run_command
    assert f"{tmp_path}:/workspace:ro" in run_command
    assert calls[1][:3] == ["docker", "rm", "-f"]


def test_payload_timeout_still_forces_container_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = _manifest().execution_policy("virtual-workspace-v1")
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        if command[:2] == ["docker", "run"]:
            raise subprocess.TimeoutExpired(command, timeout=45)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(conformance.subprocess, "run", fake_run)
    with pytest.raises(subprocess.TimeoutExpired):
        conformance._run_payload(
            repo_root=REPO_ROOT,
            policy=policy,
            fixture_root=tmp_path,
            path=SAMPLE_PAYLOAD,
        )

    assert calls[-1][:3] == ["docker", "rm", "-f"]


def test_conformance_run_requires_exact_planned_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed_paths: list[str] = []

    def fake_run_payload(*, path: str, **_: Any) -> dict[str, Any]:
        executed_paths.append(path)
        return {"path": path, "executed": ["test_conformance"]}

    monkeypatch.setattr(conformance, "_run_payload", fake_run_payload)
    monkeypatch.setattr(conformance, "_pull_image", lambda **_: None)
    evidence = conformance.run(
        repo_root=REPO_ROOT,
        manifest_path=REPO_ROOT / "config/component-ownership.toml",
    )

    assert set(executed_paths) == EXPECTED_PAYLOAD_PATHS
    assert [item["path"] for item in evidence] == executed_paths
