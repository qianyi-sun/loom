from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from loom.personal_dev_builder_runtime import CompositePersonalDevBuildExecutor
from loom.personal_dev_native_builder_executor import (
    NativeAgentPersonalDevPlatformBuildExecutor,
)
from loom.personal_dev_native_builder_store import NativeBuilderGrantPolicy
from tests.unit.test_personal_dev_builder import _registration

_AGENT_ID = UUID("10000000-0000-0000-0000-000000000001")
_CONTRACT = '{"platform":"linux/arm64","schema_version":1}'
_CONTRACT_SHA256 = "6a70281ff4f91c00db4f7c1d0c0dadaead31b7a425fc1fb40dfb2c8a3b4bb714"


def _policy(_registration) -> NativeBuilderGrantPolicy:
    return NativeBuilderGrantPolicy(
        agent_instance_id=_AGENT_ID,
        agent_key_id="gb10-native-builder-v1",
        agent_image=(
            "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
            + "a" * 64
        ),
        builder_image=(
            "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
        ),
        runtime_profile_sha256="c" * 64,
        contract_json=_CONTRACT,
        contract_sha256=_CONTRACT_SHA256,
        artifact_max_bytes=8 * 1024 * 1024 * 1024,
        active_deadline_seconds=300,
    )


def _running_registration():
    registration = _registration()
    assert registration.build_attempt is not None
    return replace(
        registration,
        build_attempt=replace(registration.build_attempt, state="running"),
    )


class _Sessions:
    def __init__(self) -> None:
        self.entries: list[int] = []

    @asynccontextmanager
    async def __call__(self):
        identity = len(self.entries) + 1
        self.entries.append(identity)
        yield SimpleNamespace(identity=identity)


class _Authority:
    def __init__(self, registration) -> None:
        attempt = registration.build_attempt
        assert attempt is not None
        self.grant = SimpleNamespace(
            id=UUID("90000000-0000-0000-0000-000000000001"),
            candidate_id=registration.candidate.id,
            attempt_id=attempt.id,
            attempt_lease_epoch=attempt.lease_epoch,
            platform="linux/arm64",
            state="queued",
            failure_reason=None,
        )
        self.states: list[str] = []
        self.issue_sessions: list[int] = []
        self.get_sessions: list[int] = []
        self.cancel_sessions: list[int] = []
        self.cancel_calls: list[tuple[UUID, int, str]] = []
        self.observed = asyncio.Event()
        self.block_get: asyncio.Event | None = None

    def bind(self, session):
        authority = self

        class _Bound:
            async def issue(self, registration, policy, now):
                del registration, policy, now
                authority.issue_sessions.append(session.identity)
                return authority.grant

            async def get(self, attempt_id, attempt_lease_epoch, platform):
                authority.get_sessions.append(session.identity)
                assert attempt_id == authority.grant.attempt_id
                assert attempt_lease_epoch == authority.grant.attempt_lease_epoch
                assert platform == "linux/arm64"
                if authority.states:
                    authority.grant.state = authority.states.pop(0)
                authority.observed.set()
                if authority.block_get is not None:
                    await authority.block_get.wait()
                return authority.grant

            async def cancel(self, attempt_id, attempt_lease_epoch, platform, now):
                del now
                authority.cancel_sessions.append(session.identity)
                authority.cancel_calls.append(
                    (attempt_id, attempt_lease_epoch, platform)
                )
                authority.grant.state = "cancelled"
                return True

        return _Bound()


def _executor(
    registration,
    authority: _Authority,
    sessions: _Sessions,
    *,
    wait_timeout_seconds: float = 0.2,
    poll_interval_seconds: float = 0.001,
) -> NativeAgentPersonalDevPlatformBuildExecutor:
    return NativeAgentPersonalDevPlatformBuildExecutor(
        session_factory=sessions,  # type: ignore[arg-type]
        authority_factory=authority.bind,
        policy_factory=_policy,
        wait_timeout_seconds=wait_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )


async def test_native_executor_issues_once_and_observes_success_in_new_sessions(
    tmp_path: Path,
) -> None:
    registration = _running_registration()
    authority = _Authority(registration)
    authority.states = ["running", "succeeded"]
    sessions = _Sessions()
    executor = _executor(registration, authority, sessions)

    await executor.build_platform(registration, source_archive=tmp_path / "source.tar")

    assert len(authority.issue_sessions) == 1
    assert len(authority.get_sessions) == 2
    assert sessions.entries == [1, 2, 3]
    assert authority.issue_sessions[0] not in authority.get_sessions
    assert len(set(authority.get_sessions)) == 2
    assert authority.cancel_calls == []


async def test_native_executor_reports_bounded_terminal_failure(tmp_path: Path) -> None:
    registration = _running_registration()
    authority = _Authority(registration)
    authority.states = ["failed"]
    authority.grant.failure_reason = "candidate_supplied_secret_must_not_escape"
    sessions = _Sessions()
    executor = _executor(registration, authority, sessions)

    with pytest.raises(RuntimeError, match="native builder grant failed") as exc:
        await executor.build_platform(
            registration,
            source_archive=tmp_path / "source.tar",
        )

    assert "candidate_supplied_secret" not in str(exc.value)
    assert authority.cancel_calls == []


async def test_native_executor_timeout_cancels_only_exact_whole_attempt(
    tmp_path: Path,
) -> None:
    registration = _running_registration()
    authority = _Authority(registration)
    authority.states = ["running"]
    sessions = _Sessions()
    executor = _executor(
        registration,
        authority,
        sessions,
        wait_timeout_seconds=0.01,
    )

    with pytest.raises(TimeoutError, match="native builder grant deadline expired"):
        await executor.build_platform(
            registration,
            source_archive=tmp_path / "source.tar",
        )

    attempt = registration.build_attempt
    assert attempt is not None
    assert authority.cancel_calls == [
        (attempt.id, attempt.lease_epoch, "linux/arm64")
    ]
    assert authority.cancel_sessions[0] not in authority.get_sessions


async def test_native_executor_deadline_bounds_a_hung_store_observation(
    tmp_path: Path,
) -> None:
    registration = _running_registration()
    authority = _Authority(registration)
    authority.block_get = asyncio.Event()
    sessions = _Sessions()
    executor = _executor(
        registration,
        authority,
        sessions,
        wait_timeout_seconds=0.01,
    )

    with pytest.raises(TimeoutError, match="native builder grant deadline expired"):
        await asyncio.wait_for(
            executor.build_platform(
                registration,
                source_archive=tmp_path / "source.tar",
            ),
            timeout=0.2,
        )

    assert len(authority.cancel_calls) == 1


async def test_native_executor_coroutine_cancellation_cancels_exact_grant(
    tmp_path: Path,
) -> None:
    registration = _running_registration()
    authority = _Authority(registration)
    authority.states = ["running"]
    sessions = _Sessions()
    executor = _executor(
        registration,
        authority,
        sessions,
        poll_interval_seconds=30.0,
    )
    task = asyncio.create_task(
        executor.build_platform(
            registration,
            source_archive=tmp_path / "source.tar",
        )
    )
    await authority.observed.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    attempt = registration.build_attempt
    assert attempt is not None
    assert authority.cancel_calls == [
        (attempt.id, attempt.lease_epoch, "linux/arm64")
    ]


async def test_native_executor_cleanup_is_exact_and_idempotent() -> None:
    registration = _running_registration()
    authority = _Authority(registration)
    sessions = _Sessions()
    executor = _executor(registration, authority, sessions)

    await executor.cleanup_platform(registration)
    await executor.cleanup_platform(registration)

    attempt = registration.build_attempt
    assert attempt is not None
    assert authority.cancel_calls == [
        (attempt.id, attempt.lease_epoch, "linux/arm64"),
        (attempt.id, attempt.lease_epoch, "linux/arm64"),
    ]
    assert authority.cancel_sessions == [1, 2]


class _PlatformExecutor:
    def __init__(self, *, failure: str | None = None) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False
        self.cleaned = 0
        self.failure = failure

    async def build_platform(self, registration, *, source_archive):
        del registration, source_archive
        self.started.set()
        try:
            await self.release.wait()
            if self.failure is not None:
                raise RuntimeError(self.failure)
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    async def cleanup_platform(self, registration):
        del registration
        self.cleaned += 1


class _PublicationExporter:
    def __init__(self, publication, *, failure: str | None = None) -> None:
        self.publication = publication
        self.failure = failure
        self.calls = 0

    async def publish(self, registration):
        del registration
        self.calls += 1
        if self.failure is not None:
            raise RuntimeError(self.failure)
        return self.publication


async def test_composite_starts_both_platforms_before_exporting(tmp_path: Path) -> None:
    registration = _running_registration()
    amd64 = _PlatformExecutor()
    arm64 = _PlatformExecutor()
    publication = {"exact": ["publication", 1]}
    exporter = _PublicationExporter(publication)
    executor = CompositePersonalDevBuildExecutor(
        platform_executors={
            "linux/amd64": amd64,
            "linux/arm64": arm64,
        },
        exporter=exporter,  # type: ignore[arg-type]
    )
    task = asyncio.create_task(
        executor.build(registration, source_archive=tmp_path / "source.tar")
    )

    await asyncio.wait_for(
        asyncio.gather(amd64.started.wait(), arm64.started.wait()),
        timeout=0.2,
    )
    assert exporter.calls == 0
    amd64.release.set()
    await asyncio.sleep(0)
    assert exporter.calls == 0
    arm64.release.set()

    assert await task is publication
    assert exporter.calls == 1


async def test_composite_failure_cancels_sibling_and_cleanup_reaches_both(
    tmp_path: Path,
) -> None:
    registration = _running_registration()
    amd64 = _PlatformExecutor(failure="amd64_failed")
    arm64 = _PlatformExecutor()
    exporter = _PublicationExporter({"must": "not run"})
    executor = CompositePersonalDevBuildExecutor(
        platform_executors={
            "linux/amd64": amd64,
            "linux/arm64": arm64,
        },
        exporter=exporter,  # type: ignore[arg-type]
    )
    task = asyncio.create_task(
        executor.build(registration, source_archive=tmp_path / "source.tar")
    )
    await asyncio.wait_for(
        asyncio.gather(amd64.started.wait(), arm64.started.wait()),
        timeout=0.2,
    )
    amd64.release.set()

    with pytest.raises(RuntimeError, match="amd64_failed"):
        await task
    await executor.cleanup(registration)

    assert arm64.cancelled is True
    assert exporter.calls == 0
    assert amd64.cleaned == 1
    assert arm64.cleaned == 1


async def test_composite_exporter_failure_still_allows_exact_dual_cleanup(
    tmp_path: Path,
) -> None:
    registration = _running_registration()
    amd64 = _PlatformExecutor()
    arm64 = _PlatformExecutor()
    exporter = _PublicationExporter({}, failure="publish_failed")
    executor = CompositePersonalDevBuildExecutor(
        platform_executors={
            "linux/amd64": amd64,
            "linux/arm64": arm64,
        },
        exporter=exporter,  # type: ignore[arg-type]
    )
    amd64.release.set()
    arm64.release.set()

    try:
        with pytest.raises(RuntimeError, match="publish_failed"):
            await executor.build(
                registration,
                source_archive=tmp_path / "source.tar",
            )
    finally:
        await executor.cleanup(registration)

    assert amd64.cleaned == 1
    assert arm64.cleaned == 1
