import pytest

from loom.terminal_result_semantics import (
    aggregate_reward_scalar,
    projected_result_conflicts,
    terminal_result_conflicts,
)


@pytest.mark.parametrize(
    ("rewards", "expected"),
    [
        (None, None),
        ({}, None),
        ({"artifact_complete": 1.0}, 1.0),
        ({"passed": 0.0}, 0.0),
        ({"a": 0.0, "b": 1.0}, 0.5),
    ],
)
def test_named_rewards_have_a_shared_scalar_projection(
    rewards: dict[str, float] | None,
    expected: float | None,
) -> None:
    original = None if rewards is None else dict(rewards)
    assert aggregate_reward_scalar(rewards) == expected
    assert rewards == original


def test_projection_rejects_success_with_terminal_failure_reason() -> None:
    conflicts = projected_result_conflicts(
        {
            "state": "succeeded",
            "failure_reason": "verifier_error",
            "reward": None,
        }
    )
    assert {conflict["field"] for conflict in conflicts} == {
        "result.reward",
        "result.failure_reason",
    }


def test_terminal_transition_rejects_result_state_mismatch() -> None:
    assert terminal_result_conflicts(
        state="succeeded",
        result={
            "state": "failed",
            "failure_reason": "verifier_error",
            "aggregate_reward": 0.0,
        },
        failure_reason=None,
        config={},
    ) == [
        {"field": "result.state", "expected": "succeeded", "actual": "failed"},
        {
            "field": "result.failure_reason",
            "expected": None,
            "actual": "verifier_error",
        },
    ]


def test_unscored_success_and_numeric_zero_are_both_valid() -> None:
    assert (
        terminal_result_conflicts(
            state="succeeded",
            result={"state": "succeeded", "reward": None},
            failure_reason=None,
            config={"skip_verifier": True},
        )
        == []
    )


def test_unscored_success_requires_explicit_skip_verifier() -> None:
    assert terminal_result_conflicts(
        state="succeeded",
        result={"state": "succeeded", "reward": None},
        failure_reason=None,
        config={},
    ) == [
        {
            "field": "result.reward",
            "expected": "numeric reward or config.skip_verifier=true",
            "actual": None,
        }
    ]
    assert (
        terminal_result_conflicts(
            state="succeeded",
            result={"state": "succeeded", "aggregate_reward": 0.0},
            failure_reason=None,
            config={},
        )
        == []
    )


def test_scored_success_keeps_nested_verifier_diagnostics() -> None:
    assert (
        terminal_result_conflicts(
            state="succeeded",
            result={
                "state": "succeeded",
                "aggregate_reward": 0.0,
                "steps": [
                    {
                        "verifier_result": {
                            "rewards": {"passed": 0.0},
                            "error": {"kind": "diagnostic", "message": "task failed"},
                        }
                    }
                ],
            },
            failure_reason=None,
            config={},
        )
        == []
    )


def test_legacy_failed_result_without_embedded_state_remains_valid() -> None:
    assert (
        terminal_result_conflicts(
            state="failed",
            result={"aggregate_reward": None},
            failure_reason="gateway_error",
            config={},
        )
        == []
    )
    assert (
        terminal_result_conflicts(
            state="failed",
            result=None,
            failure_reason="task_compatibility",
            config={},
        )
        == []
    )
