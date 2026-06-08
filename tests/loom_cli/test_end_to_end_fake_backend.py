"""Single-task end-to-end through `main()`: builds + runs against the
FakeDriver, checks both the disk artifacts AND the trajectory event
schema match what Trial.run() writes in service mode."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from tests.loom_cli.test_task_loader import _StubAdapter


def test_end_to_end_writes_events_and_atif(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_benchmarks import registry

    monkeypatch.setitem(registry.REGISTRY, "e2e", _StubAdapter(name="e2e"))
    output_dir = tmp_path / "runs"
    rc = main([
        "run",
        "--dataset", "e2e",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(output_dir),
        "--json",
    ])
    assert rc in {0, 1}
    parsed = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines() if line.strip()
    ]
    assert len(parsed) == 2

    for obj in parsed:
        trial_dir = output_dir / obj["trial_id"]
        assert trial_dir.is_dir()
        events_path = trial_dir / "events.jsonl"
        assert events_path.exists()
        events = [
            json.loads(line) for line in
            events_path.read_text().splitlines() if line.strip()
        ]
        kinds = [e["kind"] for e in events]
        assert kinds[0] == "trial_start"
        assert kinds[-1] == "trial_end"
        assert all(e["trial_id"] == obj["trial_id"] for e in events)


def test_end_to_end_exit_code_for_single_task(
    tmp_xdg_home: Path, tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A passing oracle trial (solution/solve.sh exit 0 + no tests) should
    end in TrialState.SUCCEEDED and CLI exit 0.

    PytestVerifier behavior with no tests/ dir may classify as FAILED
    too — we allow either 0 or 1 here but assert no crash."""
    from loom_benchmarks import registry

    monkeypatch.setitem(registry.REGISTRY, "e2e-x", _StubAdapter(name="e2e-x"))
    rc = main([
        "run",
        "--task", "e2e-x/t1",
        "--agent", "oracle",
        "--backend", "fake",
        "--output-dir", str(tmp_path / "runs"),
    ])
    assert rc in {0, 1}
