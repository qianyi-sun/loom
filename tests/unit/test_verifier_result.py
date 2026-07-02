import pytest
from pydantic import ValidationError

from loom.models.verifier import (
    CheckResult,
    VerifierError,
    VerifierResult,
)


def test_minimal_result():
    r = VerifierResult(rewards={"passed": 1.0})
    assert r.rewards["passed"] == 1.0
    assert r.checks == []
    assert r.confidence is None
    assert r.error is None


def test_confidence_in_range():
    VerifierResult(rewards={"x": 0.0}, confidence=0.0)
    VerifierResult(rewards={"x": 0.0}, confidence=1.0)
    with pytest.raises(ValidationError):
        VerifierResult(rewards={"x": 0.0}, confidence=-0.1)
    with pytest.raises(ValidationError):
        VerifierResult(rewards={"x": 0.0}, confidence=1.1)


def test_check_result():
    c = CheckResult(name="test_search", passed=True, score=1.0, duration_sec=0.42)
    assert c.passed is True


def test_check_result_preserves_detail():
    c = CheckResult(
        name="script_exit",
        passed=False,
        score=0.0,
        message="verifier command failed",
        detail={"exit_code": 1},
    )
    assert c.detail == {"exit_code": 1}


def test_verifier_error_kinds():
    for kind in ("missing_tests", "parse_failure", "exec_failure", "timeout", "internal"):
        VerifierError(kind=kind, message="x")  # type: ignore[arg-type]


def test_result_with_structured_extras():
    r = VerifierResult(
        rewards={"r": 0.5},
        checks=[CheckResult(name="t", passed=True)],
        structured={"junit_xml_path": "/tmp/report.xml"},
    )
    assert r.structured == {"junit_xml_path": "/tmp/report.xml"}
