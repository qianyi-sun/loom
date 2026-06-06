from pathlib import PurePosixPath

import pytest

from loom.models.verifier import CheckResult, VerifierError, VerifierResult
from loom.verifier.composite import Aggregator, CompositeVerifier


class _StaticVerifier:
    def __init__(self, name: str, result: VerifierResult):
        self.name = name
        self._result = result

    async def verify(self, *, task, env, artifacts_dir, trajectory):  # type: ignore[no-untyped-def]
        return self._result


def _result(rewards: dict[str, float]) -> VerifierResult:
    return VerifierResult(rewards=rewards)


async def test_mean_aggregator():
    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", _result({"score": 0.8})),
            _StaticVerifier("b", _result({"score": 0.4})),
            _StaticVerifier("c", _result({"score": 0.6})),
        ],
        aggregator=Aggregator.MEAN,
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert r.rewards["score"] == pytest.approx(0.6)


async def test_min_aggregator():
    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", _result({"score": 0.8})),
            _StaticVerifier("b", _result({"score": 0.4})),
        ],
        aggregator=Aggregator.MIN,
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert r.rewards["score"] == 0.4


async def test_max_aggregator():
    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", _result({"score": 0.3})),
            _StaticVerifier("b", _result({"score": 0.7})),
        ],
        aggregator=Aggregator.MAX,
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert r.rewards["score"] == 0.7


async def test_weighted_aggregator():
    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", _result({"score": 1.0})),
            _StaticVerifier("b", _result({"score": 0.0})),
        ],
        aggregator=Aggregator.WEIGHTED,
        weights={"a": 0.9, "b": 0.1},
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert r.rewards["score"] == pytest.approx(0.9)


def test_weighted_requires_weights():
    with pytest.raises(ValueError):
        CompositeVerifier(
            verifiers=[_StaticVerifier("a", _result({"x": 0}))],
            aggregator=Aggregator.WEIGHTED,
        )


async def test_error_treated_as_zero():
    """Per spec §5.4: VerifierResult.error != None is 0-contribution in
    mean/weighted strategies."""
    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", _result({"x": 1.0})),
            _StaticVerifier("b", VerifierResult(
                rewards={},
                error=VerifierError(kind="missing_tests", message="x"),
            )),
        ],
        aggregator=Aggregator.MEAN,
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert r.rewards["x"] == pytest.approx(0.5)


async def test_concatenates_checks():
    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", VerifierResult(
                rewards={"x": 1.0},
                checks=[CheckResult(name="t1", passed=True)],
            )),
            _StaticVerifier("b", VerifierResult(
                rewards={"x": 0.5},
                checks=[CheckResult(name="t2", passed=False)],
            )),
        ],
        aggregator=Aggregator.MEAN,
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert len(r.checks) == 2
    assert {check.name for check in r.checks} == {"t1", "t2"}


async def test_custom_callable_aggregator():
    """Aggregator can be a custom function: AggregatorFn(results) → VerifierResult."""
    def pick_first(results):  # type: ignore[no-untyped-def]
        return results[0]

    c = CompositeVerifier(
        verifiers=[
            _StaticVerifier("a", _result({"x": 0.1})),
            _StaticVerifier("b", _result({"x": 0.9})),
        ],
        aggregator=pick_first,
    )
    r = await c.verify(task=None, env=None,  # type: ignore[arg-type]
                       artifacts_dir=PurePosixPath("/x"), trajectory=None)
    assert r.rewards["x"] == 0.1
