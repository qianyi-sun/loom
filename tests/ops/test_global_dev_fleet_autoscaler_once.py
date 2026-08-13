from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.ops import global_dev_fleet_autoscaler_once as executable

from loom_control_plane.global_execution_fence import GlobalExecutionFenceError


def test_parser_requires_independent_manager_trust_inputs() -> None:
    with pytest.raises(SystemExit):
        executable._parser().parse_args([])


def test_fenced_execution_writes_non_success_report_and_exits_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "output.json"
    input_path.write_text(
        json.dumps({"schema_version": 1, "demands": [], "observations": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(executable, "load_global_execution_witness", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "global_dev_fleet_autoscaler_once.py",
            "--state-db",
            str(tmp_path / "state.sqlite3"),
            "--input-json",
            str(input_path),
            "--output-json",
            str(output_path),
            "--global-budget",
            "1",
            "--pool-budget",
            "oldlab=1",
            "--global-execution-witness-json",
            str(tmp_path / "witness.json"),
            "--manager-public-key",
            str(tmp_path / "manager.pub"),
            "--expected-manager-public-key-sha256",
            "a" * 64,
        ],
    )

    assert executable.main() == 2
    report = json.loads(output_path.read_text(encoding="utf-8"))
    assert report["status"] == "fenced"
    assert report["aggregate"] == {"legacy_scale_up_fenced": True}
    assert report["grants"] == []


def test_unavailable_witness_exits_nonzero_without_an_unhandled_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = tmp_path / "input.json"
    input_path.write_text(
        json.dumps({"schema_version": 1, "demands": [], "observations": []}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        executable,
        "load_global_execution_witness",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            GlobalExecutionFenceError("global execution witness is unavailable")
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "global_dev_fleet_autoscaler_once.py",
            "--state-db",
            str(tmp_path / "state.sqlite3"),
            "--input-json",
            str(input_path),
            "--output-json",
            str(tmp_path / "output.json"),
            "--global-budget",
            "1",
            "--pool-budget",
            "oldlab=1",
            "--global-execution-witness-json",
            str(tmp_path / "witness.json"),
            "--manager-public-key",
            str(tmp_path / "manager.pub"),
            "--expected-manager-public-key-sha256",
            "a" * 64,
        ],
    )

    assert executable.main() == 2
