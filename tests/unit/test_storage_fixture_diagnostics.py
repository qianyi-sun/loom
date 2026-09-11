"""Disposable-cluster diagnostics never retry writes or expose their payload."""

import pytest

from loom.dev_instance_runtime import (
    CommandResult,
    DevInstanceRuntimeError,
    KubernetesResourceVersionConflictError,
)
from tests.integration import test_personal_dev_storage_namespace as fixture


@pytest.mark.parametrize("conflict", [False, True])
async def test_failed_fixture_write_preserves_exception_and_safe_diagnostics(monkeypatch, conflict):
    error = (KubernetesResourceVersionConflictError if conflict else DevInstanceRuntimeError)("original")
    calls = []

    class Runner:
        async def run(self, argv, *, stdin=None, **kwargs):
            calls.append((argv, stdin))
            if "sh" in argv:
                raise error
            if "head" in argv:
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
    assert all(secret not in " ".join(error.__notes__) for secret in ("do-not-log", "private-fixture-name"))


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
