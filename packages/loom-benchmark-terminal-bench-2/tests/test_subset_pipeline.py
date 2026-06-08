"""End-to-end-shape integration: convert the 10-task subset, fabricate
TrialResults for three of them, and assert the emitted TB-2 report
matches the Harbor reference snapshot byte-for-byte (after canonical
sorting + identical UUIDs).

This is a snapshot test, not a live runner — TB-2 task execution
requires Docker and is exercised by the manual validation step in the
plan's self-review, not by pytest.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter
from loom_benchmark_terminal_bench_2.report import to_tb2_report

from loom.models.result import (
    AgentInfo,
    FailureReason,
    StepResult,
    TrialResult,
    TrialState,
)
from loom.models.trial import TrialConfig
from loom.models.verifier import CheckResult, VerifierResult

_TEAM = uuid4()
_START = datetime(2026, 6, 8, tzinfo=UTC)
_END = datetime(2026, 6, 8, 0, 1, tzinfo=UTC)


def _trial(
    uuid_hex: int, task: str, *, resolved: bool,
    fail: FailureReason | None = None,
) -> TrialResult:
    uuid_str = f"00000000-0000-0000-0000-{uuid_hex:012d}"
    return TrialResult(
        id=UUID(uuid_str),
        task_id=f"terminal-bench-2/{task}",
        task_checksum="sha256:abc",
        team_id=_TEAM,
        agent=AgentInfo(name="claude-code", version="1.0", mode="out-of-box"),
        config=TrialConfig(),
        state=TrialState.SUCCEEDED if resolved else TrialState.FAILED,
        failure_reason=fail,
        started_at=_START,
        finished_at=_END,
        reward={"resolved": 1.0 if resolved else 0.0},
        steps=[
            StepResult(
                step_name="main",
                verifier_result=VerifierResult(
                    rewards={"resolved": 1.0 if resolved else 0.0},
                    checks=[
                        CheckResult(
                            name="tb2_run_tests",
                            passed=resolved,
                            message=f"exit={0 if resolved else 1}",
                        ),
                    ],
                ),
            ),
        ],
    )


def test_subset_list_instances_orders_all_ten(fixtures_dir: Path) -> None:
    source = fixtures_dir / "tb2-subset-10"
    adapter = TerminalBench2Adapter()
    found = [i.instance_id for i in adapter.list_instances(
        source_dir=source, split="test",
    )]
    assert len(found) == 10
    assert found == sorted(found)


def test_report_matches_harbor_reference(fixtures_dir: Path) -> None:
    trials = [
        _trial(1, "hello-world", resolved=True),
        _trial(2, "chess-best-move", resolved=False,
               fail=FailureReason.AGENT_TIMEOUT),
        _trial(3, "blind-maze-explorer-5x5", resolved=True),
    ]
    report = to_tb2_report(trials)
    reference = json.loads(
        (fixtures_dir / "harbor-reference-results.json").read_text(),
    )

    report["results"] = sorted(report["results"], key=lambda r: r["task_id"])
    reference["results"] = sorted(
        reference["results"], key=lambda r: r["task_id"],
    )
    report["resolved_ids"] = sorted(report["resolved_ids"])
    report["unresolved_ids"] = sorted(report["unresolved_ids"])
    reference["resolved_ids"] = sorted(reference["resolved_ids"])
    reference["unresolved_ids"] = sorted(reference["unresolved_ids"])

    assert report == reference
