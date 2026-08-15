from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import pytest

from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.unit.test_capacity_agent_admission_contracts import publishable_release_fixture
from tests.unit.test_capacity_agent_runtime import _configuration


@dataclass
class _Receipt:
    intent_id: UUID
    protected_release_sha256: str
    receipt_digest: str
    replayed: bool = False
    executable: bool = True


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    def begin(self):
        return self


class _Factory:
    def __call__(self):
        return _Session()


class _Publisher:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.fail_once = fail_once
        self.calls: list[tuple[object, UUID]] = []

    async def publish_executable_protected_release(self, publication, *, idempotency_key: UUID):
        self.calls.append((publication, idempotency_key))
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("unavailable")
        return _Receipt(
            intent_id=publication.release.binding.intent_id,
            protected_release_sha256=publication.release.protected_release_sha256,
            receipt_digest="7" * 64,
        )


@pytest.mark.asyncio
async def test_release_runtime_without_pending_event_becomes_ready_without_http() -> None:
    from loom_capacity_agent.executable_release_reporter import (
        ExecutableProtectedReleaseReporterRuntime,
    )

    publisher = _Publisher()

    async def read_next(*_args: object, **_kwargs: object):
        return None

    async def acknowledge(*_args: object, **_kwargs: object):
        raise AssertionError("no pending publication must not acknowledge")

    runtime = ExecutableProtectedReleaseReporterRuntime(
        configuration=_configuration(),
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        read_next=read_next,
        acknowledge=acknowledge,
    )

    await runtime.initialize()
    await runtime.run_once()

    assert runtime.ready is True
    assert publisher.calls == []


@pytest.mark.asyncio
async def test_release_runtime_publishes_then_acknowledges_one_event() -> None:
    from loom_capacity_agent.executable_release_reporter import (
        ExecutableProtectedReleaseReporterRuntime,
    )

    publication = publishable_release_fixture()
    publisher = _Publisher()
    acknowledgements: list[tuple[object, str]] = []

    async def read_next(*_args: object, **_kwargs: object):
        return publication

    async def acknowledge(*_args: object, **kwargs: object):
        acknowledgements.append((kwargs["publication"], kwargs["manager_acknowledgement_digest"]))
        return object()

    runtime = ExecutableProtectedReleaseReporterRuntime(
        configuration=_configuration(),
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        read_next=read_next,
        acknowledge=acknowledge,
    )

    await runtime.initialize()
    await runtime.run_once()

    assert runtime.ready is True
    assert [item[0] for item in publisher.calls] == [publication]
    assert acknowledgements == [(publication, "7" * 64)]


@pytest.mark.asyncio
async def test_release_runtime_http_failure_leaves_cursor_unacknowledged() -> None:
    from loom_capacity_agent.executable_release_reporter import (
        ExecutableProtectedReleaseReporterRuntime,
    )

    publication = publishable_release_fixture()
    publisher = _Publisher(fail_once=True)
    acknowledgements: list[object] = []

    async def read_next(*_args: object, **_kwargs: object):
        return publication

    async def acknowledge(*_args: object, **kwargs: object):
        acknowledgements.append(kwargs["publication"])
        return object()

    runtime = ExecutableProtectedReleaseReporterRuntime(
        configuration=_configuration(),
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        read_next=read_next,
        acknowledge=acknowledge,
    )

    await runtime.initialize()
    with pytest.raises(RuntimeError, match="unavailable"):
        await runtime.run_once()
    assert runtime.ready is False
    assert acknowledgements == []

    await runtime.run_once()
    assert runtime.ready is True
    assert [item[0] for item in publisher.calls] == [publication, publication]
    assert acknowledgements == [publication]


@pytest.mark.asyncio
async def test_release_runtime_replays_same_idempotency_key_after_local_ack_failure() -> None:
    from loom_capacity_agent.executable_release_reporter import (
        ExecutableProtectedReleaseReporterRuntime,
    )

    publication = publishable_release_fixture()
    publisher = _Publisher()
    acknowledgements: list[str] = []
    fail_once = True

    async def read_next(*_args: object, **_kwargs: object):
        return publication

    async def acknowledge(*_args: object, **kwargs: object):
        nonlocal fail_once
        acknowledgements.append(kwargs["manager_acknowledgement_digest"])
        if fail_once:
            fail_once = False
            raise RuntimeError("local crash")
        return object()

    runtime = ExecutableProtectedReleaseReporterRuntime(
        configuration=_configuration(),
        session_factory=_Factory(),  # type: ignore[arg-type]
        publisher=publisher,
        read_next=read_next,
        acknowledge=acknowledge,
    )

    await runtime.initialize()
    with pytest.raises(RuntimeError, match="local crash"):
        await runtime.run_once()
    assert runtime.ready is False

    await runtime.run_once()
    assert runtime.ready is True
    assert len(publisher.calls) == 2
    assert publisher.calls[0][1] == publisher.calls[1][1]
    assert acknowledgements == ["7" * 64, "7" * 64]


@pytest.mark.asyncio
async def test_stable_release_publication_key_changes_only_with_publication_identity() -> None:
    from loom_capacity_agent.executable_release_reporter import stable_release_publication_key

    publication = publishable_release_fixture()
    changed_event = publication.model_copy(update={"event_id": publication.event_id + 1})
    changed_release = publication.release.model_copy(
        update={
            "protected_registration_epoch": publication.release.protected_registration_epoch + 1
        }
    )
    changed_digest = publication.model_copy(
        update={
            "release": changed_release,
            "publication_digest": "0" * 64,
        }
    )
    changed_publication = publication.model_copy(
        update={
            "release": changed_release,
            "publication_digest": canonical_executable_digest(changed_release),
        }
    )

    assert stable_release_publication_key(publication) == stable_release_publication_key(
        publication
    )
    assert stable_release_publication_key(publication) != stable_release_publication_key(
        changed_event
    )
    assert stable_release_publication_key(publication) != stable_release_publication_key(
        changed_publication
    )
    with pytest.raises(ValueError, match="digest changed"):
        stable_release_publication_key(changed_digest)
