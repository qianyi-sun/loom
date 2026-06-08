"""to_tb2_report emits the canonical TB-2 BenchmarkResults shape."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from loom_benchmark_terminal_bench_2.report import to_tb2_report

from loom.models.result import AgentInfo, StepResult, TrialResult, TrialState
from loom.models.trial import TrialConfig
from loom.models.verifier import CheckResult, VerifierResult


def _make_trial(
    task_id: str, *, resolved: bool,
    in_tok: int = 100, out_tok: int = 50,
) -> TrialResult:
    return TrialResult(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        task_id=task_id,
        task_checksum="sha256:abc",
        team_id=uuid4(),
        agent=AgentInfo(name="claude-code", version="1.0", mode="out-of-box"),
        config=TrialConfig(),
        state=TrialState.SUCCEEDED if resolved else TrialState.FAILED,
        started_at=datetime(2026, 6, 8, tzinfo=UTC),
        finished_at=datetime(2026, 6, 8, 0, 1, tzinfo=UTC),
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


def test_report_top_level_fields() -> None:
    trials = [
        _make_trial("terminal-bench-2/hello-world", resolved=True),
        _make_trial("terminal-bench-2/crack-7z-hash", resolved=False),
    ]
    report = to_tb2_report(trials)

    assert report["accuracy"] == 0.5
    assert report["n_resolved"] == 1
    assert report["n_unresolved"] == 1
    assert report["resolved_ids"] == ["hello-world"]
    assert report["unresolved_ids"] == ["crack-7z-hash"]
    assert report["pass_at_k"] == {"1": 0.5}
    assert len(report["results"]) == 2


def test_report_trial_fields() -> None:
    [trial] = [_make_trial("terminal-bench-2/hello-world", resolved=True)]
    report = to_tb2_report([trial])

    (entry,) = report["results"]
    assert entry["task_id"] == "hello-world"
    assert entry["is_resolved"] is True
    assert entry["failure_mode"] == "none"
    assert entry["parser_results"] == {"tb2_run_tests": "passed"}
    assert entry["total_input_tokens"] == 0
    assert entry["total_output_tokens"] == 0
    assert entry["uuid"] == "00000000-0000-0000-0000-000000000001"


def test_report_failure_mode_inference() -> None:
    from loom.models.result import FailureReason

    trial = _make_trial("terminal-bench-2/x", resolved=False)
    trial = trial.model_copy(
        update={"failure_reason": FailureReason.AGENT_TIMEOUT},
    )
    report = to_tb2_report([trial])
    (entry,) = report["results"]
    assert entry["failure_mode"] == "agent_timeout"


def test_report_empty_list() -> None:
    report = to_tb2_report([])
    assert report == {
        "results": [],
        "accuracy": 0.0,
        "n_resolved": 0,
        "n_unresolved": 0,
        "resolved_ids": [],
        "unresolved_ids": [],
        "pass_at_k": {"1": 0.0},
    }
