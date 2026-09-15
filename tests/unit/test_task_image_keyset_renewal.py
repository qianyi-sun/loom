"""Native-only readiness and cancellation-resistant renewal lifecycle."""

import asyncio
from types import SimpleNamespace

import pytest

from loom_control_plane import task_image_execution as admission
from loom_control_plane.app import _cancel_and_drain_tasks
from loom_control_plane.task_image_keyset_renewal import TaskImageKeysetPublisher
from loom_task_image_authority.execution_config import load_execution_admission_settings
from tests.unit.test_task_image_execution_config import document, save


@pytest.mark.parametrize("swallow", [False, True])
async def test_renewal_shutdown_joins_cancellation_resistant_operation_before_clients_close(tmp_path, monkeypatch, swallow):
    settings = load_execution_admission_settings(save(tmp_path, document(tmp_path, admission=True)))
    engine = SimpleNamespace()
    publisher = TaskImageKeysetPublisher(engine, trust_root=settings.root.trust_root(), signer=None)
    entered, stopped = asyncio.Event(), asyncio.Event()
    cancelled = 0
    closed = []

    async def refresh():
        nonlocal cancelled
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled += 1
            if swallow:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled += 1
                    raise
            raise
        finally:
            stopped.set()

    publisher.refresh_if_needed = refresh

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            assert stopped.is_set()
            closed.append(True)

    async def drain(tasks):
        await _cancel_and_drain_tasks(tasks, grace_seconds=0.01)

    monkeypatch.setattr(admission, "HTTPSExecutionSigner", Client)
    monkeypatch.setattr(admission, "HTTPSKeysetSigner", Client)
    monkeypatch.setattr(admission, "TaskImageKeysetPublisher", lambda *args, **kwargs: publisher)
    monkeypatch.setattr(admission, "_cancel_and_drain_tasks", drain, raising=False)
    context = admission.configured_execution_service(engine, settings)
    service = await context.__aenter__()
    await asyncio.wait_for(entered.wait(), 1)
    assert not service.native_ready_enabled  # Renewal outage does not stop the app context.
    async with asyncio.timeout(0.5):
        await context.__aexit__(None, None, None)
    assert cancelled == (2 if swallow else 1)
    assert closed == [True, True]
