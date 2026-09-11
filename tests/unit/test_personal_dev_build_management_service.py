"""Service recovery ownership must not become an intake/readiness shortcut."""

import asyncio
from importlib import import_module
from types import SimpleNamespace

import pytest


async def test_unconfigured_build_management_has_no_runtime():
    module = import_module("loom_service.personal_dev_build_management")
    settings = SimpleNamespace(personal_dev_build_management_config_file=None,
        personal_dev_build_management_config_sha256="")
    assert await module.build_personal_build_management_runtime(settings, admission=None) is None


async def test_service_runtime_owns_all_scope_tasks_and_closes_clients_after_cancellation():
    module = import_module("loom_service.personal_dev_build_management")
    events, started = [], [asyncio.Event(), asyncio.Event()]

    class Scope:
        def __init__(self, index):
            self.index = index

        async def run_forever(self, *, admission_enabled, poll_interval_seconds):
            assert admission_enabled() is False
            events.append(("start", self.index))
            started[self.index].set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append(("stop", self.index))

    class Client:
        def __init__(self, index):
            self.index = index

        async def aclose(self):
            events.append(("close", self.index))

    runtime = module.PersonalBuildManagementServiceRuntime(
        managers=(Scope(0), Scope(1)), clients=(Client(0), Client(1)))
    runtime.start()
    await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), timeout=1)
    with pytest.raises(RuntimeError, match="started"):
        runtime.start()
    await runtime.aclose()
    await runtime.aclose()
    assert events.count(("close", 0)) == events.count(("close", 1)) == 1
    assert max(i for i, event in enumerate(events) if event[0] == "stop") < min(i for i, event in enumerate(events) if event[0] == "close")


async def test_service_runtime_failure_is_observable_and_does_not_leak_other_scopes():
    module = import_module("loom_service.personal_dev_build_management")
    stopped, closed = asyncio.Event(), []

    class Scope:
        async def run_forever(self, **kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.set()

    class Broken:
        async def run_forever(self, **kwargs):
            raise RuntimeError("unexpected programming failure")

    class Client:
        async def aclose(self):
            closed.append(True)

    runtime = module.PersonalBuildManagementServiceRuntime(managers=(Scope(), Broken()), clients=(Client(), Client()))
    runtime.start()
    with pytest.raises(ExceptionGroup, match="TaskGroup"):
        await asyncio.wait_for(runtime.wait(), timeout=1)
    assert stopped.is_set()
    await runtime.aclose()
    assert len(closed) == 2
