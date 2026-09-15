"""Disposable-cluster diagnostics never retry writes or expose their payload."""

import pytest

from loom.dev_instance_runtime import (
    CommandResult,
    DevInstanceRuntimeError,
    KubernetesResourceVersionConflictError,
)
from tests.integration import test_personal_dev_storage_namespace as fixture


@pytest.mark.parametrize("conflict", [False, True])
@pytest.mark.parametrize("inner_status", ["1", "0", "255", "-1", "256", "secret", "", "1\nprivate"])
async def test_failed_fixture_write_preserves_exception_and_safe_diagnostics(monkeypatch, conflict, inner_status):
    error = (KubernetesResourceVersionConflictError if conflict else DevInstanceRuntimeError)("original")
    calls = []

    class Runner:
        async def run(self, argv, *, stdin=None, **kwargs):
            calls.append((argv, stdin))
            if "sh" in argv:
                raise error
            if "head" in argv:
                if argv[-1].endswith(".status"):
                    return CommandResult(inner_status, "")
                return CommandResult('Error from server (NotFound): namespaces "private-fixture-name" not found', "")
            if "inspect" in argv:
                return CommandResult('{"Running":true,"OOMKilled":false,"ExitCode":0}', "")
            return CommandResult("", "")

    monkeypatch.setattr(fixture, "AsyncCommandRunner", Runner)
    with pytest.raises(type(error)) as raised:
        await fixture._ContainerKubectl("a" * 64).run(["kubectl", "create", "-f", "-"],
            stdin='{"stringData":{"secret":"do-not-log"}}')
    assert raised.value is error
    assert len([argv for argv, _ in calls if "sh" in argv]) == 1
    assert "namespace-not-found" in " ".join(error.__notes__)
    assert "OOMKilled" in " ".join(error.__notes__)
    expected_status = inner_status if inner_status in {"0", "1", "255"} else "unavailable"
    assert "disposable kubectl exit status: " + expected_status in error.__notes__
    assert any(note.startswith("disposable kubectl stderr: bytes=") for note in error.__notes__)
    assert all(secret not in " ".join(error.__notes__) for secret in ("do-not-log", "private-fixture-name"))


@pytest.mark.parametrize("stderr,label", [
    ("", "empty-stderr"),
    ("error: EOF", "unexpected-eof"),
    ('Get "https://private.example": EOF', "unexpected-eof"),
    ("error: http2: client connection lost", "http2-error"),
    ("error: net/http: TLS handshake timeout", "timeout"),
    ("error: dial tcp: i/o timeout", "timeout"),
    ("error: context canceled", "request-cancelled"),
    ("error: the server doesn't have a resource type private", "discovery-error"),
    ("error: resource temporarily unavailable", "resource-unavailable"),
    ("error: too many open files", "file-descriptor-limit"),
    ("unknown do-not-log", "unclassified"),
])
def test_fixture_classifies_transport_failures_without_echoing_server_content(stderr, label):
    assert fixture._failure_category(stderr) == label


async def test_successful_fixture_command_returns_exact_production_result(monkeypatch):
    expected = CommandResult("exact success", "")
    calls = []

    class Runner:
        async def run(self, argv, **kwargs):
            calls.append(argv)
            return expected

    monkeypatch.setattr(fixture, "AsyncCommandRunner", Runner)
    assert await fixture._ContainerKubectl("a" * 64).run(["kubectl", "get", "namespace"]) is expected
    assert len(calls) == 1
