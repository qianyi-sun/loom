"""Environment-scoped database client for protected executable admission."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    DrainedExecutableWorkerV2,
    ExecutableDrainRequestV2,
    ExecutableReleaseReceiptV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
    ProtectedIntentObservationV2,
    RegisteredExecutableWorkerV2,
    WithdrawnExecutableWorkerV2,
)
from loom_capacity_agent.claim_guard import (
    ExecutableClaimProposalV2,
    ExecutableClaimReceiptV2,
)
from loom_capacity_agent.client import read_owner_only_bytes
from loom_capacity_agent.executable_admission import ExecutableAdmissionStore
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
)

_MAX_DATABASE_URL_BYTES = 16 * 1024


class ExecutableAdmissionClientError(RuntimeError):
    """Environment-scoped executable admission could not be validated."""


def _database_url(path: Path) -> str:
    try:
        payload = read_owner_only_bytes(path, max_bytes=_MAX_DATABASE_URL_BYTES)
    except (OSError, ValueError) as exc:
        raise ExecutableAdmissionClientError(str(exc)) from exc
    return _database_url_from_bytes(payload)


def _database_url_from_bytes(payload: bytes) -> str:
    if not payload or len(payload) > _MAX_DATABASE_URL_BYTES:
        raise ExecutableAdmissionClientError("database URL exceeds its byte bound")
    try:
        value = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExecutableAdmissionClientError("database URL is not UTF-8") from exc
    if value.endswith("\n"):
        value = value[:-1]
    if not value or value != value.strip() or any(item in value for item in ("\r", "\n", "\x00")):
        raise ExecutableAdmissionClientError("database URL must contain one exact line")
    try:
        parsed = make_url(value)
    except (ArgumentError, ValueError) as exc:
        raise ExecutableAdmissionClientError("database URL is invalid") from exc
    query = dict(parsed.query)
    if (
        parsed.drivername != "postgresql+psycopg"
        or not parsed.username
        or not parsed.password
        or not parsed.host
        or not parsed.database
        or query != {"sslmode": "verify-full"}
    ):
        raise ExecutableAdmissionClientError(
            "database URL must be credential-scoped PostgreSQL with verify-full TLS"
        )
    return value


def _connection_bounds(
    *,
    timeout_seconds: int,
    pool_timeout_seconds: int | None,
) -> tuple[int, int]:
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise ExecutableAdmissionClientError(
            "database connection timeout must be an integer between 1 and 60 seconds"
        )
    pool_timeout = timeout_seconds if pool_timeout_seconds is None else pool_timeout_seconds
    if type(pool_timeout) is not int or not 1 <= pool_timeout <= 60:
        raise ExecutableAdmissionClientError(
            "database pool timeout must be an integer between 1 and 60 seconds"
        )
    return timeout_seconds, pool_timeout


class DatabaseExecutableAdmissionClient:
    """Call only bounded executable procedures for one exact environment incarnation."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
        operation_timeout_seconds: float = 10.0,
        statement_timeout_ms: int = 10_000,
        lock_timeout_ms: int = 5_000,
    ) -> None:
        if not isinstance(subject_id, UUID) or not isinstance(subject_incarnation, UUID):
            raise ExecutableAdmissionClientError("database client subject scope is invalid")
        if subject_id == subject_incarnation:
            raise ExecutableAdmissionClientError("database client subject identities overlap")
        if (
            not isinstance(operation_timeout_seconds, (int, float))
            or not 0.05 <= float(operation_timeout_seconds) <= 60.0
        ):
            raise ExecutableAdmissionClientError(
                "database operation timeout must be between 0.05 and 60 seconds"
            )
        if type(statement_timeout_ms) is not int or not 1 <= statement_timeout_ms <= 60_000:
            raise ExecutableAdmissionClientError(
                "database statement timeout must be an integer between 1 and 60000 ms"
            )
        if type(lock_timeout_ms) is not int or not 1 <= lock_timeout_ms <= 60_000:
            raise ExecutableAdmissionClientError(
                "database lock timeout must be an integer between 1 and 60000 ms"
            )
        self._engine = engine
        self._factory = async_sessionmaker(engine, expire_on_commit=False)
        self.subject_id = subject_id
        self.subject_incarnation = subject_incarnation
        self._operation_timeout_seconds = float(operation_timeout_seconds)
        self._statement_timeout_ms = statement_timeout_ms
        self._lock_timeout_ms = lock_timeout_ms

    @classmethod
    def from_database_url_file(
        cls,
        path: Path,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
        timeout_seconds: int = 10,
        pool_timeout_seconds: int | None = None,
        statement_timeout_ms: int = 10_000,
        lock_timeout_ms: int = 5_000,
        operation_timeout_seconds: float = 10.0,
    ) -> DatabaseExecutableAdmissionClient:
        try:
            payload = read_owner_only_bytes(path, max_bytes=_MAX_DATABASE_URL_BYTES)
        except (OSError, ValueError) as exc:
            raise ExecutableAdmissionClientError(str(exc)) from exc
        return cls.from_database_url_bytes(
            payload,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            timeout_seconds=timeout_seconds,
            pool_timeout_seconds=pool_timeout_seconds,
            statement_timeout_ms=statement_timeout_ms,
            lock_timeout_ms=lock_timeout_ms,
            operation_timeout_seconds=operation_timeout_seconds,
        )

    @classmethod
    def from_database_url_bytes(
        cls,
        payload: bytes,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
        timeout_seconds: int = 10,
        pool_timeout_seconds: int | None = None,
        statement_timeout_ms: int = 10_000,
        lock_timeout_ms: int = 5_000,
        operation_timeout_seconds: float = 10.0,
    ) -> DatabaseExecutableAdmissionClient:
        timeout_seconds, pool_timeout = _connection_bounds(
            timeout_seconds=timeout_seconds,
            pool_timeout_seconds=pool_timeout_seconds,
        )
        url = _database_url_from_bytes(payload)
        engine = create_async_engine(
            url,
            connect_args={"connect_timeout": timeout_seconds},
            isolation_level="SERIALIZABLE",
            pool_timeout=pool_timeout,
            pool_pre_ping=True,
        )
        return cls(
            engine,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            operation_timeout_seconds=operation_timeout_seconds,
            statement_timeout_ms=statement_timeout_ms,
            lock_timeout_ms=lock_timeout_ms,
        )

    async def _store_call(self, method: str, *args: object, **kwargs: object) -> object:
        try:
            async with asyncio.timeout(self._operation_timeout_seconds):
                async with self._factory() as session, session.begin():
                    await session.execute(
                        text(f"SET LOCAL lock_timeout = '{self._lock_timeout_ms}ms'")
                    )
                    await session.execute(
                        text(f"SET LOCAL statement_timeout = '{self._statement_timeout_ms}ms'")
                    )
                    store = ExecutableAdmissionStore(
                        session,
                        subject_id=self.subject_id,
                        subject_incarnation=self.subject_incarnation,
                    )
                    return await getattr(store, method)(*args, **kwargs)
        except TimeoutError as exc:
            raise ExecutableAdmissionClientError(
                "protected admission transaction timed out"
            ) from exc

    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> PreparedExecutableAdmissionV2:
        result = await self._store_call(
            "prepare_worker",
            request,
            bootstrap_sha256=bootstrap_sha256,
        )
        assert isinstance(result, PreparedExecutableAdmissionV2)
        return result

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> BoundExecutableWorkerV2:
        result = await self._store_call("bind_slurm_job", request)
        assert isinstance(result, BoundExecutableWorkerV2)
        return result

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str | None = None,
        predecessor_worker_credential: str | None = None,
    ) -> RegisteredExecutableWorkerV2:
        result = await self._store_call(
            "register_worker",
            request,
            bootstrap_capability=bootstrap_capability,
            predecessor_worker_credential=predecessor_worker_credential,
        )
        assert isinstance(result, RegisteredExecutableWorkerV2)
        return result

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> DrainedExecutableWorkerV2:
        result = await self._store_call("begin_drain", request)
        assert isinstance(result, DrainedExecutableWorkerV2)
        return result

    async def withdraw_unregistered_worker(
        self,
        request: ExecutableWorkerWithdrawalRequestV2,
    ) -> WithdrawnExecutableWorkerV2:
        result = await self._store_call("withdraw_unregistered_worker", request)
        assert isinstance(result, WithdrawnExecutableWorkerV2)
        return result

    async def acknowledge_release(
        self,
        request: ExecutableReleaseRequestV2,
        *,
        current_worker_credential: str,
    ) -> ExecutableReleaseReceiptV2:
        result = await self._store_call(
            "acknowledge_release",
            request,
            current_worker_credential=current_worker_credential,
        )
        assert isinstance(result, ExecutableReleaseReceiptV2)
        return result

    async def admit_claim(
        self,
        proposal: ExecutableClaimProposalV2,
    ) -> ExecutableClaimReceiptV2 | None:
        result = await self._store_call("admit_claim", proposal)
        assert result is None or isinstance(result, ExecutableClaimReceiptV2)
        return result

    async def observe_intent(
        self,
        binding: ExecutableIntentBindingV2,
    ) -> ProtectedIntentObservationV2:
        result = await self._store_call("observe_intent", binding)
        assert isinstance(result, ProtectedIntentObservationV2)
        return result

    async def aclose(self) -> None:
        await self._engine.dispose()

    async def __aenter__(self) -> DatabaseExecutableAdmissionClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()


__all__ = ["DatabaseExecutableAdmissionClient", "ExecutableAdmissionClientError"]
