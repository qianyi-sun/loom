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
    runner = fixture._ContainerKubectl("a" * 64)
    runner.last_failure_notes = ["obsolete failure"]
    assert await runner.run(["kubectl", "get", "namespace"]) is expected
    assert runner.last_failure_notes == []
    assert len(calls) == 1


_CONNECTION_REFUSED = (
    "The connection to the server 127.0.0.1:6443 was refused - "
    "did you specify the right host or port?\n"
)


def _refusing_fixture_runner(monkeypatch, *, stderr=_CONNECTION_REFUSED, persistent=False):
    calls = []
    failures = []
    expected = CommandResult("exact recovered read", "")

    class Runner:
        async def run(self, argv, **kwargs):
            if "sh" in argv:
                calls.append((argv, kwargs))
                if persistent or len(calls) == 1:
                    error = DevInstanceRuntimeError("original refusal")
                    failures.append(error)
                    raise error
                return expected
            if "head" in argv:
                return CommandResult("1" if argv[-1].endswith(".status") else stderr, "")
            if "inspect" in argv:
                return CommandResult('{"Running":true,"OOMKilled":false,"ExitCode":0}', "")
            raise AssertionError("unexpected fixture diagnostic")

    monkeypatch.setattr(fixture, "AsyncCommandRunner", Runner)
    return calls, failures, expected


def test_fixture_classifies_kubectl_connection_refusal():
    assert fixture._failure_category(_CONNECTION_REFUSED) == "connection-refused"


@pytest.mark.parametrize("verb", ["get", "create", "replace", "patch", "delete"])
async def test_fixture_retries_connection_refusal_only_for_reads(monkeypatch, verb):
    calls, failures, expected = _refusing_fixture_runner(monkeypatch)
    runner = fixture._ContainerKubectl("a" * 64)
    if verb == "get":
        assert await runner.run(["kubectl", verb, "namespace"]) is expected
        assert len(calls) == 2
        assert runner.last_failure_notes == []
    else:
        with pytest.raises(DevInstanceRuntimeError) as raised:
            await runner.run(["kubectl", verb, "namespace"], stdin="private-write")
        assert raised.value is failures[0]
        assert len(calls) == 1


@pytest.mark.parametrize("stderr", [
    "forbidden", "x509: unknown authority", "unclassified-private",
    "x509: certificate error; connection refused",
    "forbidden: connection refused",
    "warning: private\n" + _CONNECTION_REFUSED,
])
async def test_fixture_does_not_retry_other_read_failures(monkeypatch, stderr):
    calls, failures, _ = _refusing_fixture_runner(monkeypatch, stderr=stderr)
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await fixture._ContainerKubectl("a" * 64).run(["kubectl", "get", "namespace"])
    assert raised.value is failures[0]
    assert len(calls) == 1


@pytest.mark.parametrize("timeout", [0.25, 120])
async def test_fixture_read_refusal_stops_within_original_and_retry_budgets(monkeypatch, timeout):
    from types import SimpleNamespace

    calls, failures, _ = _refusing_fixture_runner(monkeypatch, persistent=True)
    elapsed = [0.0]

    async def pause(seconds):
        elapsed[0] += seconds

    monkeypatch.setattr(fixture, "time", SimpleNamespace(monotonic=lambda: elapsed[0]), raising=False)
    monkeypatch.setattr(fixture.asyncio, "sleep", pause)
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await fixture._ContainerKubectl("a" * 64).run(
            ["kubectl", "get", "namespace"], timeout_seconds=timeout,
        )
    assert raised.value is failures[-1]
    assert elapsed[0] == pytest.approx(min(timeout, 5))
    assert 1 < len(calls) <= 60
    assert all(0 < options["timeout_seconds"] <= timeout for _, options in calls)


async def test_stopped_fixture_retains_safe_server_and_lifecycle_diagnostics(monkeypatch):
    error = DevInstanceRuntimeError("original failure")
    commands = []

    class Runner:
        async def run(self, argv, **kwargs):
            commands.append(argv)
            if "loom-k3s-diagnostics" in argv:
                return CommandResult('level=fatal msg="kube-apiserver exited: private-endpoint"\n'
                                     'error: no space left on device /private-path\n', "")
            if argv[:2] == ["docker", "events"]:
                return CommandResult('kill 15\ndie \nexec_create: private-command secret\n', "")
            if "inspect" in argv:
                return CommandResult('{"Running":false,"OOMKilled":false,"ExitCode":0}', "")
            raise error

    monkeypatch.setattr(fixture, "AsyncCommandRunner", Runner)
    runner = fixture._ContainerKubectl("a" * 64)
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await runner.run(["kubectl", "get", "namespace"])
    assert raised.value is error
    notes = "\n".join(error.__notes__)
    assert "disposable k3s log categories: api-server-exit,disk-full,fatal" in notes
    assert "disposable container lifecycle: die,kill-15" in notes
    assert all(value not in notes for value in ("private-endpoint", "private-path", "private-command", "secret"))
    assert len([argv for argv in commands if "loom-test-kubectl" in argv]) == 1


async def test_stopped_fixture_diagnostic_failure_preserves_original_error(monkeypatch):
    original = DevInstanceRuntimeError("original failure")

    class Runner:
        async def run(self, argv, **kwargs):
            if "inspect" in argv:
                return CommandResult('{"Running":false,"OOMKilled":false,"ExitCode":1}', "")
            if "loom-test-kubectl" in argv:
                raise original
            raise DevInstanceRuntimeError("private diagnostic failure")

    monkeypatch.setattr(fixture, "AsyncCommandRunner", Runner)
    with pytest.raises(DevInstanceRuntimeError) as raised:
        await fixture._ContainerKubectl("a" * 64).run(["kubectl", "create", "-f", "-"], stdin="secret")
    assert raised.value is original
    assert "disposable k3s log categories unavailable" in original.__notes__
    assert "disposable container lifecycle unavailable" in original.__notes__
    assert "private" not in "\n".join(original.__notes__)
