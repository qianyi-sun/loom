from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import loom_personal_dev_native_builder_probe.__main__ as probe_main
from loom.db.schema import PersonalDevNativeBuilderAgent, PersonalDevNativeBuildGrant
from loom_personal_dev_native_builder_probe.__main__ import (
    NativeBuilderProbeError,
    NativeBuilderProbeInventoryDriftError,
    canonical_native_builder_agent_status,
    read_native_builder_agent_status,
)

_INSTANCE_ID = UUID("10000000-0000-0000-0000-000000000001")
_BOOT_ID = UUID("20000000-0000-0000-0000-000000000001")
_LAST_SEEN = datetime(2026, 8, 30, 17, 30, 0, tzinfo=UTC)


def _status(*, instance_id: UUID = _INSTANCE_ID) -> dict[str, object]:
    return {
        "active_grant_ids": [],
        "agent_image": (
            "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
            + "a" * 64
        ),
        "agent_instance_id": str(instance_id),
        "agent_key_id": "gb10-native-builder-v1",
        "available": True,
        "builder_image": (
            "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
        ),
        "host_architecture": "aarch64",
        "host_boot_id": str(_BOOT_ID),
        "host_name": "gx10-01c7",
        "managed_grant_ids": [],
        "max_concurrency": 2,
        "platform": "linux/arm64",
        "protocol_version": 1,
        "provider": "gb10-gvisor-docker-v1",
        "readiness_evidence_sha256": "c" * 64,
        "runtime_profile_sha256": "d" * 64,
        "unavailable_reason": None,
    }


def _agent(*, instance_id: UUID = _INSTANCE_ID) -> PersonalDevNativeBuilderAgent:
    status = _status(instance_id=instance_id)
    return PersonalDevNativeBuilderAgent(
        instance_id=instance_id,
        key_id=status["agent_key_id"],
        provider=status["provider"],
        platform=status["platform"],
        protocol_version=status["protocol_version"],
        host_name=status["host_name"],
        host_architecture=status["host_architecture"],
        host_boot_id=_BOOT_ID,
        agent_image=status["agent_image"],
        builder_image=status["builder_image"],
        runtime_profile_sha256=status["runtime_profile_sha256"],
        max_concurrency=status["max_concurrency"],
        managed_grant_ids_json=[],
        active_grant_ids_json=[],
        available=True,
        unavailable_reason=None,
        readiness_evidence_sha256=status["readiness_evidence_sha256"],
        status_json=status,
        status_sha256="e" * 64,
        last_poll_requested_at=_LAST_SEEN,
        last_poll_nonce=UUID("30000000-0000-0000-0000-000000000001"),
        first_seen_at=_LAST_SEEN,
        last_seen_at=_LAST_SEEN,
        updated_at=_LAST_SEEN,
    )


def _set_inventory(
    agent: PersonalDevNativeBuilderAgent,
    *,
    managed: list[str],
    active: list[str],
) -> None:
    agent.managed_grant_ids_json = managed
    agent.active_grant_ids_json = active
    agent.status_json = {
        **agent.status_json,
        "active_grant_ids": active,
        "managed_grant_ids": managed,
    }


@pytest.mark.asyncio
async def test_probe_reads_one_exact_agent_as_canonical_secret_free_json(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await session.execute(delete(PersonalDevNativeBuildGrant))
            await session.execute(delete(PersonalDevNativeBuilderAgent))
            session.add(_agent())
            await session.flush()

            observed = await read_native_builder_agent_status(session)
            payload = canonical_native_builder_agent_status(observed)

            assert json.loads(payload) == {
                "agent": {**_status(), "last_seen_at": "2026-08-30T17:30:00Z"},
                "schema": "loom-personal-dev-native-builder-agent-status-v1",
            }
            assert payload == json.dumps(
                json.loads(payload),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii") + b"\n"
            lowered = payload.lower()
            for forbidden in (
                b"nonce",
                b"signature",
                b"capability",
                b"url",
                b"key_file",
                b"database",
                b"postgres",
            ):
                assert forbidden not in lowered
            assert not session.dirty
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_probe_reports_absence_but_rejects_ambiguous_agent_identity(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await session.execute(delete(PersonalDevNativeBuildGrant))
            await session.execute(delete(PersonalDevNativeBuilderAgent))
            await session.flush()
            assert await read_native_builder_agent_status(session) == {
                "agent": None,
                "schema": "loom-personal-dev-native-builder-agent-status-v1",
            }

            session.add(_agent())
            second = _agent(
                instance_id=UUID("10000000-0000-0000-0000-000000000002")
            )
            second.key_id = "gb10-native-builder-v2"
            second.status_json = {
                **second.status_json,
                "agent_key_id": second.key_id,
            }
            session.add(second)
            await session.flush()

            with pytest.raises(NativeBuilderProbeError, match="ambiguous"):
                await read_native_builder_agent_status(session)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_probe_rejects_non_uuid_inventory_instead_of_omitting_it(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await session.execute(delete(PersonalDevNativeBuildGrant))
            await session.execute(delete(PersonalDevNativeBuilderAgent))
            agent = _agent()
            agent.managed_grant_ids_json = [7]  # type: ignore[list-item]
            agent.status_json = {
                **agent.status_json,
                "managed_grant_ids": [7],
            }
            session.add(agent)
            await session.flush()

            with pytest.raises(NativeBuilderProbeError, match="invalid"):
                await read_native_builder_agent_status(session)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_probe_rejects_reported_grant_without_exact_durable_running_row(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await session.execute(delete(PersonalDevNativeBuildGrant))
            await session.execute(delete(PersonalDevNativeBuilderAgent))
            agent = _agent()
            grant_id = "40000000-0000-0000-0000-000000000001"
            _set_inventory(agent, managed=[grant_id], active=[grant_id])
            session.add(agent)
            await session.flush()

            with pytest.raises(NativeBuilderProbeInventoryDriftError, match="inventory"):
                await read_native_builder_agent_status(session)
            await session.commit()

        completed = subprocess.run(
            [sys.executable, "-m", "loom_personal_dev_native_builder_probe"],
            cwd=Path(__file__).resolve().parents[2],
            env={**os.environ, "LOOM_SVC_DB_URL": postgres_url},
            check=False,
            capture_output=True,
        )
        assert completed.returncode == 3
        assert completed.stdout == b""
        assert completed.stderr == b"personal-dev native-builder probe failed\n"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_probe_module_uses_service_database_url_and_writes_exact_document(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await session.execute(delete(PersonalDevNativeBuildGrant))
            await session.execute(delete(PersonalDevNativeBuilderAgent))
            await session.commit()
    finally:
        await engine.dispose()

    completed = subprocess.run(
        [sys.executable, "-m", "loom_personal_dev_native_builder_probe"],
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, "LOOM_SVC_DB_URL": postgres_url},
        check=False,
        capture_output=True,
    )

    assert completed.returncode == 0
    assert completed.stdout == (
        b'{"agent":null,"schema":'
        b'"loom-personal-dev-native-builder-agent-status-v1"}\n'
    )
    assert completed.stderr == b""


def test_probe_module_fails_closed_without_service_database_url() -> None:
    environment = dict(os.environ)
    environment.pop("LOOM_SVC_DB_URL", None)

    completed = subprocess.run(
        [sys.executable, "-m", "loom_personal_dev_native_builder_probe"],
        cwd=Path(__file__).resolve().parents[2],
        env=environment,
        check=False,
        capture_output=True,
    )

    assert completed.returncode == 2
    assert completed.stdout == b""
    assert completed.stderr == b"personal-dev native-builder probe failed\n"


class _PublicStoreClient:
    def __init__(self, response: object = None, error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.buckets: list[str] = []
        self.close_calls = 0

    def head_bucket(self, **kwargs: str) -> object:
        if set(kwargs) != {"Bucket"}:
            raise AssertionError("unexpected public-store probe arguments")
        self.buckets.append(kwargs["Bucket"])
        if self.error is not None:
            raise self.error
        return self.response

    def close(self) -> None:
        self.close_calls += 1


def test_public_store_probe_accepts_only_authenticated_http_200_metadata() -> None:
    client = _PublicStoreClient(
        {
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            }
        }
    )

    probe_main._probe_public_store_client(client, bucket="artifacts")

    assert client.buckets == ["artifacts"]


@pytest.mark.parametrize(
    "response",
    [
        None,
        {},
        {"ResponseMetadata": None},
        {"ResponseMetadata": {}},
        {"ResponseMetadata": {"HTTPStatusCode": True}},
        {"ResponseMetadata": {"HTTPStatusCode": "200"}},
        {"ResponseMetadata": {"HTTPStatusCode": 199}},
        {"ResponseMetadata": {"HTTPStatusCode": 204}},
        {"ResponseMetadata": {"HTTPStatusCode": 403}},
    ],
)
def test_public_store_probe_rejects_malformed_or_non_200_metadata(
    response: object,
) -> None:
    client = _PublicStoreClient(response)

    with pytest.raises(
        probe_main.NativeBuilderPublicStoreUnavailableError,
        match=r"^personal-dev native-builder public store is unavailable$",
    ):
        probe_main._probe_public_store_client(client, bucket="artifacts")


def test_public_store_probe_replaces_provider_exception_text() -> None:
    client = _PublicStoreClient(error=RuntimeError("secret provider detail"))

    with pytest.raises(
        probe_main.NativeBuilderPublicStoreUnavailableError,
        match=r"^personal-dev native-builder public store is unavailable$",
    ) as raised:
        probe_main._probe_public_store_client(client, bucket="artifacts")

    assert str(raised.value) == "personal-dev native-builder public store is unavailable"


def test_enabled_public_store_probe_uses_exact_bounded_client_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _PublicStoreClient(
        {"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    observed: dict[str, object] = {}

    def client_factory(service_name: str, **kwargs: object) -> _PublicStoreClient:
        observed.update({"service_name": service_name, **kwargs})
        return client

    monkeypatch.setenv("LOOM_SVC_PERSONAL_DEV_NATIVE_BUILDER_ENABLED", "true")
    monkeypatch.setenv(
        "LOOM_SVC_MINIO_PUBLIC_ENDPOINT",
        "https://objects.dev.yylx.world:443",
    )
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "exact-access-key")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "exact-secret-key")
    monkeypatch.setenv("LOOM_SVC_MINIO_REGION", "us-east-1")
    monkeypatch.setenv("LOOM_SVC_ARTIFACTS_BUCKET", "artifacts")
    monkeypatch.setattr(probe_main.boto3, "client", client_factory)

    probe_main._probe_public_store_from_environment()

    assert client.buckets == ["artifacts"]
    assert client.close_calls == 1
    assert observed["service_name"] == "s3"
    assert observed["endpoint_url"] == "https://objects.dev.yylx.world:443"
    assert observed["aws_access_key_id"] == "exact-access-key"
    assert observed["aws_secret_access_key"] == "exact-secret-key"
    assert observed["region_name"] == "us-east-1"
    assert observed["verify"] is True
    config: Any = observed["config"]
    assert config.connect_timeout == 2
    assert config.read_timeout == 2
    assert config.retries == {
        "mode": "standard",
        "total_max_attempts": 1,
    }
    assert config.proxies == {}


def test_disabled_public_store_probe_is_inert(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOM_SVC_PERSONAL_DEV_NATIVE_BUILDER_ENABLED", raising=False)

    probe_main._probe_public_store_from_environment()


@pytest.mark.parametrize(
    "endpoint",
    [
        "",
        "http://objects.dev.yylx.world",
        "https://user@objects.dev.yylx.world",
        "https://objects.dev.yylx.world/path",
        "https://objects.dev.yylx.world?query=true",
        "https://objects.dev.yylx.world#fragment",
        "https://objects.dev.yylx.world:0",
        "https://objects.dev.yylx.world:65536",
    ],
)
def test_enabled_public_store_probe_rejects_relaxed_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
) -> None:
    monkeypatch.setenv("LOOM_SVC_PERSONAL_DEV_NATIVE_BUILDER_ENABLED", "true")
    monkeypatch.setenv("LOOM_SVC_MINIO_PUBLIC_ENDPOINT", endpoint)
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "exact-access-key")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "exact-secret-key")
    monkeypatch.setenv("LOOM_SVC_MINIO_REGION", "us-east-1")
    monkeypatch.setenv("LOOM_SVC_ARTIFACTS_BUCKET", "artifacts")

    with pytest.raises(
        probe_main.NativeBuilderPublicStoreUnavailableError,
        match=r"^personal-dev native-builder public store is unavailable$",
    ):
        probe_main._probe_public_store_from_environment()


@pytest.mark.asyncio
async def test_public_store_probe_precedes_database_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable() -> None:
        raise probe_main.NativeBuilderPublicStoreUnavailableError(
            "personal-dev native-builder public store is unavailable"
        )

    def unexpected_engine(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("database engine was created before public-store readiness")

    monkeypatch.setattr(
        probe_main,
        "_probe_public_store_from_environment",
        unavailable,
    )
    monkeypatch.setattr(probe_main, "create_async_engine", unexpected_engine)

    with pytest.raises(probe_main.NativeBuilderPublicStoreUnavailableError):
        await probe_main._run("postgresql://database-value")


def test_probe_main_maps_public_store_failure_to_fixed_exit_four(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def unavailable(_database_url: str) -> bytes:
        raise probe_main.NativeBuilderPublicStoreUnavailableError(
            "personal-dev native-builder public store is unavailable"
        )

    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://secret-provider-value")
    monkeypatch.setattr(probe_main, "_run", unavailable)

    assert probe_main.main() == 4
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "personal-dev native-builder probe failed\n"
