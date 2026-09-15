"""Bounded public-keyset renewal; no private keys, worker tokens or offline starts."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from loom_task_image_authority.keyset_signing_request import (
    KeysetSigningRequest,
    canonical_keyset_signing_request,
)
from loom_task_image_authority.publication_keyset import (
    MAX_KEYSET_ENVELOPE_BYTES,
    ExecutionGrantTrustRoot,
    _instant,
    verify_publication_keyset,
)
from loom_task_image_authority.publication_keyset_store import (
    finalize_keyset,
    prepare_keyset,
    read_keyset,
)

_RENEW_BEFORE = timedelta(seconds=150)
_READY_MARGIN = timedelta(seconds=30)
logger = logging.getLogger(__name__)


class KeysetSigner(Protocol):
    async def sign_keyset(self, canonical_request: bytes, *, maximum_reply_bytes: int) -> bytes: ...


def _clock() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class TaskImageKeysetPublisher:
    """Current signed authority, not cached runtime authorization.

    Each pass releases all database locks before fixed-operation signing. Other
    replicas may win publication; the next pass authenticates their retained
    snapshot. Failed signing never rewrites or extends the previous artifact.
    """

    def __init__(self, engine: AsyncEngine, *, trust_root: ExecutionGrantTrustRoot,
                 signer: KeysetSigner, clock: Callable[[], datetime] = _clock,
                 timeout_seconds: float = 10.0) -> None:
        trust_root.__post_init__()
        if type(timeout_seconds) is not float or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 10:
            raise ValueError("invalid keyset renewal deadline")
        self._engine, self._root, self._signer, self._clock, self._timeout = engine, trust_root, signer, clock, timeout_seconds
        self._expires_at: datetime | None = None
        self._lock = asyncio.Lock()
        self._stopping = asyncio.Event()

    @property
    def ready(self) -> bool:
        return not self._stopping.is_set() and self._expires_at is not None and self._root.activated_at <= self._clock() < min(self._expires_at, self._root.expires_at) - _READY_MARGIN

    def stop(self) -> None:
        self._stopping.set()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        async with self._engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with AsyncSession(connection, expire_on_commit=False) as session, session.begin():
                await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
                await session.execute(text(
                    "SELECT pg_catalog.set_config('statement_timeout', :bound, true), "
                    "pg_catalog.set_config('idle_in_transaction_session_timeout', :bound, true)"
                ), {"bound": f"{math.ceil(self._timeout * 1000)}ms"})
                yield session

    async def refresh_if_needed(self) -> bool:
        async with asyncio.timeout(self._timeout), self._lock:
            current = None
            async with self._transaction() as session:
                prepared = await prepare_keyset(session, trust_root=self._root)
                try:
                    current = await read_keyset(session, trust_root=self._root, expected_state=prepared.previous_state, clock=self._clock)
                except ValueError:
                    # Missing, expired or mismatched prior distribution is not
                    # authority. Independently sign the current retained public
                    # key snapshot; do not alter or trust the historical wire.
                    self._expires_at = None
            if current is not None:
                verify_publication_keyset(current.wire, trust_root=self._root, expected_state=current.state, now=self._clock())
                self._expires_at = current.expires_at
                if current.expires_at - self._clock() > _RENEW_BEFORE:
                    return False
            request = KeysetSigningRequest.model_validate(dict(
                schema="loom.task-image-keyset-signing-request/v1", environment=prepared.environment,
                previous_keyset_version=prepared.previous_state.keyset_version,
                proposed_keyset_version=prepared.proposed_state.keyset_version,
                revocation_epoch=prepared.proposed_state.revocation_epoch, keys=prepared.keys,
            ))
            wire = await self._signer.sign_keyset(canonical_keyset_signing_request(request), maximum_reply_bytes=MAX_KEYSET_ENVELOPE_BYTES)
            verified = verify_publication_keyset(wire, trust_root=self._root, expected_state=prepared.proposed_state, now=self._clock())
            if _instant(verified.keyset.expires_at) - self._clock() <= _RENEW_BEFORE:
                raise ValueError("keyset signer/root lifetime is insufficient for renewal cadence")
            async with self._transaction() as session:
                retained = await finalize_keyset(session, preparation=prepared, wire=wire, trust_root=self._root, clock=self._clock)
            verify_publication_keyset(retained.wire, trust_root=self._root, expected_state=retained.state, now=self._clock())
            self._expires_at = retained.expires_at
            return True

    async def run(self) -> None:
        while not self._stopping.is_set():
            try:
                await self.refresh_if_needed()
            except (ValueError, TimeoutError, ConnectionError, OSError, SQLAlchemyError) as exc:
                # Fixed low-frequency retry; do not log URLs, credentials or
                # raw signer/database replies. Existing validity still expires.
                logger.warning("task_image_keyset_renewal_unavailable error_type=%s", type(exc).__name__)
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=30)
            except TimeoutError:
                pass
