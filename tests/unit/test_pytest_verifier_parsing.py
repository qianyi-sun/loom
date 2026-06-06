import pytest

from loom.verifier.pytest_verifier import parse_junit_xml


def test_parse_junit_xml_all_pass():
    xml = """<testsuites><testsuite name="t" tests="3" failures="0" errors="0"
        skipped="0"><testcase name="a" time="0.1"/><testcase name="b" time="0.2"/>
        <testcase name="c" time="0.0"/></testsuite></testsuites>"""
    result = parse_junit_xml(xml)
    assert result["total"] == 3
    assert result["passed"] == 3
    assert result["pass_rate"] == pytest.approx(1.0)
    assert len(result["checks"]) == 3
    assert all(c.passed for c in result["checks"])


def test_parse_junit_xml_with_failures():
    xml = """<testsuites><testsuite name="t" tests="3" failures="1" errors="0"
        skipped="0">
        <testcase name="a" time="0.1"/>
        <testcase name="b" time="0.2"><failure message="boom"/></testcase>
        <testcase name="c" time="0.0"/>
    </testsuite></testsuites>"""
    result = parse_junit_xml(xml)
    assert result["passed"] == 2
    assert result["pass_rate"] == pytest.approx(2 / 3)
    failing = [c for c in result["checks"] if not c.passed]
    assert len(failing) == 1
    assert failing[0].name == "b"
    assert "boom" in (failing[0].message or "")


def test_parse_junit_xml_with_error_node():
    """An <error> child (e.g., a collection error) also counts as a failed test."""
    xml = """<testsuites><testsuite name="t" tests="1" failures="0" errors="1"
        skipped="0">
        <testcase name="bad_import" time="0.0"><error message="ImportError"/></testcase>
    </testsuite></testsuites>"""
    result = parse_junit_xml(xml)
    assert result["passed"] == 0
    failing = [c for c in result["checks"] if not c.passed]
    assert len(failing) == 1
    assert "ImportError" in (failing[0].message or "")


def test_parse_junit_xml_handles_empty_suite():
    xml = """<testsuites><testsuite name="t" tests="0" failures="0" errors="0"
        skipped="0"/></testsuites>"""
    result = parse_junit_xml(xml)
    assert result["total"] == 0
    assert result["pass_rate"] == 0.0
    assert result["checks"] == []
