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
    assert any(note.startswith("disposable kubectl stderr: chars=") for note in error.__notes__)
    assert all(secret not in " ".join(error.__notes__) for secret in ("do-not-log", "private-fixture-name"))


@pytest.mark.parametrize("stderr", ["", "do-not-log" * 2000])
async def test_fixture_stderr_metadata_is_bounded_and_never_echoes_content(monkeypatch, stderr):
    error = DevInstanceRuntimeError("original")

    class Runner:
        async def run(self, argv, **kwargs):
            if "sh" in argv:
                raise error
            if "head" in argv and not argv[-1].endswith(".status"):
                assert argv[-2] == "8193"
                return CommandResult(stderr[:8193], "")
            raise DevInstanceRuntimeError("diagnostic unavailable")

    monkeypatch.setattr(fixture, "AsyncCommandRunner", Runner)
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await fixture._ContainerKubectl("a" * 64).run(["kubectl", "get", "namespace"])
    assert raised.value is error
    assert "do-not-log" not in " ".join(error.__notes__)
    assert f"disposable kubectl stderr: chars={min(len(stderr), 8192)}; at-read-limit={len(stderr) >= 8192}" in error.__notes__
    assert "disposable kubectl exit status: unavailable" in error.__notes__


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


def _retirement_reader_test(monkeypatch, values):
    from types import SimpleNamespace

    from tests.integration import test_legacy_writer_retirement_fence as retirement

    clock = [0.0]
    calls = []
    monkeypatch.setattr(retirement.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(retirement.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))

    class Reader:
        def list_namespaced_pod(self, namespace, **kwargs):
            calls.append((namespace, kwargs))
            value = values.pop(0) if len(values) > 1 else values[0]
            if isinstance(value, Exception):
                raise value
            return SimpleNamespace(items=value)

    return retirement, Reader(), calls


def _tls_eof():
    import ssl

    from urllib3.exceptions import MaxRetryError, SSLError

    return MaxRetryError(None, "/pods", SSLError(ssl.SSLEOFError(8, "unexpected EOF")))


def test_retirement_startup_read_waits_for_pods_after_transient_tls_eof(monkeypatch):
    from types import SimpleNamespace

    running = [SimpleNamespace(status=SimpleNamespace(phase="Running")) for _ in range(7)]
    retirement, reader, calls = _retirement_reader_test(monkeypatch, [_tls_eof(), [], running])
    assert retirement._wait_for_old_writer_pods(reader) is running
    assert len(calls) == 3
    assert all(namespace == "loom-staging" and 0 < kwargs["_request_timeout"] <= 5
               for namespace, kwargs in calls)


def test_retirement_startup_tls_eof_cannot_extend_deadline(monkeypatch):
    retirement, reader, calls = _retirement_reader_test(monkeypatch, [_tls_eof()])
    with pytest.raises(AssertionError, match="old writers did not start"):
        retirement._wait_for_old_writer_pods(reader, timeout_seconds=1)
    assert 1 < len(calls) <= 6


@pytest.mark.parametrize("kind", ["certificate", "authorization", "other-ssl", "other-transport"])
def test_retirement_startup_does_not_retry_other_failures(monkeypatch, kind):
    import ssl

    from kubernetes.client.exceptions import ApiException
    from urllib3.exceptions import MaxRetryError, SSLError

    error = {
        "certificate": MaxRetryError(None, "/pods", SSLError(ssl.SSLCertVerificationError("bad certificate"))),
        "authorization": ApiException(status=403),
        "other-ssl": MaxRetryError(None, "/pods", SSLError("other TLS error")),
        "other-transport": MaxRetryError(None, "/pods", RuntimeError("other transport error")),
    }[kind]
    retirement, reader, calls = _retirement_reader_test(monkeypatch, [error])
    with pytest.raises(type(error)) as raised:
        retirement._wait_for_old_writer_pods(reader)
    assert raised.value is error
    assert len(calls) == 1
