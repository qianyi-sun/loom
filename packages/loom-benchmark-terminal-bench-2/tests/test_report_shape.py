"""to_tb2_report emits the canonical TB-2 BenchmarkResults shape."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from loom_benchmark_terminal_bench_2.report import (
    parse_tb21_verifier_output,
    to_tb2_report,
)

from loom.models.result import AgentInfo, StepResult, TrialResult, TrialState
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.models.verifier import CheckResult, VerifierResult

# Plan 23: TrialConfig requires agent_name + agent_model. Sibling
# packages don't import from the main repo's tests/, so we inline a
# stub here rather than share the test helper.
_STUB_CONFIG = TrialConfig(
    agent_name="claude-code",
    agent_model=ModelSpec(provider="anthropic", name="claude-opus-4-7"),
)


def _make_trial(
    task_id: str,
    *,
    resolved: bool,
    in_tok: int = 100,
    out_tok: int = 50,
) -> TrialResult:
    return TrialResult(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        task_id=task_id,
        task_checksum="sha256:abc",
        team_id=uuid4(),
        agent=AgentInfo(name="claude-code", version="1.0", mode="out-of-box"),
        config=_STUB_CONFIG,
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


def test_native_report_records_execution_and_runtime_provenance_separately() -> None:
    trial = _make_trial(
        "terminal-bench-2@tb2.1-r6/adaptive-rejection-sampler",
        resolved=True,
    )

    report = to_tb2_report(
        [trial],
        runtime_provenance={"runner": "staging", "agent_image": "terminus:2"},
    )

    assert report["tb21_provenance"] == {
        "physical_profile": "terminal-bench-2@tb2.1-r6",
        "hub_dataset": "terminal-bench/terminal-bench-2-1",
        "hub_revision": "6",
        "hub_metadata_version": (
            "sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a"
        ),
        "source_reference_snapshot": "dde3cd95b80ff25af5abd99a80b6513a018ad3b4",
        "source_reference_divergences": [
            {
                "task": "terminal-bench/sanitize-git-repo",
                "source_digest": (
                    "sha256:73c94a21ebe370bae843adbeeaaa9e991374867b18483aaf56c7cd470dcddea7"
                ),
                "hub_digest": (
                    "sha256:6e86297715fae62cd499fbdd27013e11a38d05d7e05b7f661cb50b4ecead128f"
                ),
            }
        ],
        "verifier_identity": "tb21-native-reward-file-v1",
    }
    assert report["runtime_provenance"] == {
        "runner": "staging",
        "agent_image": "terminus:2",
    }
    (entry,) = report["results"]
    assert entry["loom_provenance"] == {
        "physical_profile": "terminal-bench-2@tb2.1-r6",
        "hub_package_digest": (
            "sha256:bcaa2399985cd57666018025846289ab25e193ae0dd8fb7f0ffab2410c24d4de"
        ),
        "hub_metadata_version": (
            "sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a"
        ),
        "source_reference_snapshot": "dde3cd95b80ff25af5abd99a80b6513a018ad3b4",
        "source_reference_divergence": None,
        "bundle_checksum": "sha256:abc",
        "verifier_identity": "tb21-native-reward-file-v1",
    }


def test_zero_reward_is_a_valid_verifier_result(tmp_path) -> None:
    assert parse_tb21_verifier_output(tmp_path, reward_text="0\n").reward == 0.0


@pytest.mark.parametrize(
    ("reward_text", "failure_kind"),
    [
        (None, "missing_reward"),
        ("\n", "empty_reward"),
        ("not-a-number\n", "malformed_reward"),
        ("NaN\n", "malformed_reward"),
    ],
)
def test_invalid_reward_is_platform_failure_not_zero(
    tmp_path,
    reward_text: str | None,
    failure_kind: str,
) -> None:
    parsed = parse_tb21_verifier_output(tmp_path, reward_text=reward_text)
    assert parsed.reward is None
    assert parsed.failure_kind == failure_kind
