"""Whole-input diagnostics must not confuse profile admission with equivalence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import tomli_w

from loom_cli import datasets_cmd


def _write_bundle(root: Path, name: str, **environment: Any) -> Path:
    bundle = root / "tasks" / name
    (bundle / "environment").mkdir(parents=True)
    (bundle / "tests").mkdir()
    config = {
        "task": {"id": name, "name": name},
        "environment": {
            "os": "linux",
            "dockerfile": "environment/Dockerfile",
            "docker_build_context": "environment",
            "workdir": "/app",
            "user": "agent",
            "network_policies_supported": ["gateway-only"],
            "baseline_network_policy": {"kind": "gateway-only"},
            **environment,
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "script", "args": {"script_path": "verifier/run.sh"}},
        "steps": [{"name": "main"}],
    }
    (bundle / "task.toml").write_text(tomli_w.dumps(config))
    (bundle / "instruction.md").write_text("Solve the task.\n")
    (bundle / "environment/Dockerfile").write_text("FROM ubuntu:24.04\nWORKDIR /app\n")
    (bundle / "tests/test.sh").write_text(
        "#!/bin/bash\n"
        "curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh\n"
        "source $HOME/.local/bin/env\n"
        "uvx -p 3.13 -w pytest==8.4.1 pytest /tests/test_outputs.py -rA\n"
        "if [ $? -eq 0 ]; then echo 1 > /logs/verifier/reward.txt; "
        "else echo 0 > /logs/verifier/reward.txt; fi\n"
    )
    (bundle / "tests/test_outputs.py").write_text("def test_result():\n    assert True\n")
    return bundle


def _report(root: Path, capsys: pytest.CaptureFixture[str], *, profile: bool = True) -> tuple[int, Any]:
    args = [
        "validate-local", str(root), "--id", "slice", "--display-name", "Slice",
        "--series", "custom", "--license-spdx", "MIT", "--source-subdir", "tasks",
        "--compatibility-report", "--json",
    ]
    if profile:
        args.extend(["--execution-profile", "nebius-terminus"])
    rc = datasets_cmd.dispatch(args)
    captured = capsys.readouterr()
    assert captured.err == ""
    return rc, json.loads(captured.out)


def test_report_covers_every_task_after_parse_and_adaptation_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    broken = _write_bundle(tmp_path, "a-malformed")
    (broken / "task.toml").write_text("not = valid = TOML\n")
    unsupported = _write_bundle(tmp_path, "b-bootstrap")
    (unsupported / "tests/test.sh").write_text("#!/bin/bash\nunknown-installer pytest\n")
    _write_bundle(tmp_path, "c-supported")
    original = {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    assert payload["task_count"] == 3
    reports = payload["compatibility_report"]["tasks"]
    assert [row["task_id"] for row in reports] == [
        "slice/a-malformed", "slice/b-bootstrap", "slice/c-supported",
    ]
    assert [row["status"] for row in reports] == ["blocked", "blocked", "converted"]
    assert reports[0]["diagnostics"][0]["category"] == "package_defect"
    assert reports[1]["diagnostics"][0]["category"] == "unsupported_conversion"
    assert reports[2]["admission_passed"] is True
    assert reports[2]["changes"]
    assert all(
        diagnostic["source_location"] and diagnostic["reason"] and diagnostic["suggested_action"]
        for report in reports for diagnostic in report["diagnostics"]
    )
    assert {str(p): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == original


def test_admission_pass_does_not_hide_changed_declared_requirements(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(
        tmp_path, "requirements", user="root", workdir="/data",
        network_policies_supported=["public"], baseline_network_policy={"kind": "public"},
    )
    config = {
        "task": {"name": "upstream-task"},
        "environment": {
            "user": "root", "workdir": "/data", "allow_internet": True,
            "mutable_paths": ["/data", "/home/root"],
            "services": [{"name": "database"}],
        },
        "verifier": {"user": "root"},
    }
    (bundle / "task.toml").write_text(tomli_w.dumps(config))

    rc, payload = _report(tmp_path, capsys)

    assert rc == 1
    report = payload["compatibility_report"]["tasks"][0]
    assert report["status"] == "blocked"
    assert report["original_requirements"]["environment"]["allow_internet"] is True
    assert report["original_requirements"]["environment"]["mutable_paths"] == ["/data", "/home/root"]
    codes = {diagnostic["code"] for diagnostic in report["diagnostics"]}
    assert {"task_identity", "runtime_egress", "mutable_paths", "services", "verifier_identity"} <= codes
    assert all(
        diagnostic["category"] == "runtime_capability"
        for diagnostic in report["diagnostics"] if diagnostic["code"] in codes - {"requirement_changed"}
    )


def test_report_without_profile_labels_schema_check_not_runtime_qualification(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    _write_bundle(tmp_path, "valid")

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 0
    report = payload["compatibility_report"]["tasks"][0]
    assert report["status"] == "schema_valid"
    assert report["admission_passed"] is None
    assert payload["compatibility_report"]["runtime_verified"] is False


def test_report_includes_raw_harbor_missing_identity_without_losing_next_task(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = _write_bundle(tmp_path, "anonymous")
    (bundle / "task.toml").write_text('[metadata]\nauthor_name = "A"\n[environment]\nos = "linux"\n')
    _write_bundle(tmp_path, "valid")

    rc, payload = _report(tmp_path, capsys, profile=False)

    assert rc == 1
    reports = payload["compatibility_report"]["tasks"]
    assert len(reports) == 2
    assert reports[0]["status"] == "blocked"
    assert "task.id" in reports[0]["diagnostics"][0]["reason"]
    assert reports[1]["status"] == "schema_valid"
