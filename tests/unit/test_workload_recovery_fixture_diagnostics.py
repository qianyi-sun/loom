"""Disposable recovery failures retain safe diagnostics without retrying writes."""

import subprocess

import pytest

from tests.integration.test_application_workload_recovery import _run_disposable_kubectl


@pytest.mark.parametrize("stderr,category", [
    (b'Error from server (Conflict): private-name: the object has been modified', "resource-conflict"),
    (b'Get "https://private.example": EOF', "unexpected-eof"),
    (b'Error from server (Forbidden): private-token', "forbidden"),
    (b'private-unclassified' * 1000, "unclassified"),
    (b'', "empty-stderr"),
], ids=["conflict", "eof", "forbidden", "bounded-unknown", "empty"])
def test_failed_workload_fixture_write_has_safe_diagnostics_without_retry(stderr, category):
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 1, b'private-output', stderr)

    with pytest.raises(RuntimeError) as raised:
        _run_disposable_kubectl(run, ("kubectl", "patch"), input=b'private-input')
    assert len(calls) == 1
    assert calls[0][1]["input"] == b'private-input'
    assert f"category={category}" in str(raised.value)
    assert "exit=1" in str(raised.value)
    assert "private" not in str(raised.value)
    assert len(str(raised.value)) < 200


def test_successful_workload_fixture_command_preserves_exact_result():
    expected = subprocess.CompletedProcess(("kubectl", "get"), 0, b'exact-output', b'')
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return expected

    assert _run_disposable_kubectl(run, expected.args, timeout=30) is expected
    assert calls == [(expected.args, {"timeout": 30})]
