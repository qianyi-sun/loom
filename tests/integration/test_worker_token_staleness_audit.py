"""Integration tests for the worker-token staleness audit (issue #165).

The audit lives in `loom_control_plane.metrics_refresher.refresh_once`
and publishes `loom_worker_tokens_stale_count{reason="..."}`. It counts
LIVE worker-type tokens (not revoked, not naturally expired) that are
either:

- unused_30d: COALESCE(last_seen_at, issued_at) < NOW() - 30 days
- aged_90d:   issued_at < NOW() - 90 days

The two reasons can overlap (a token is both unused 30d AND aged 90d);
each is counted independently against its filter.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Token
from loom_control_plane.metrics import WORKER_TOKENS_STALE_COUNT
from loom_control_plane.metrics_refresher import refresh_once


@pytest.fixture(autouse=True)
async def _cleanup_tokens(postgres_url: str) -> None:  # type: ignore[return]
    """Remove all token rows after each test so cases are isolated."""
    yield  # type: ignore[misc]
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        await s.execute(delete(Token))
        await s.commit()
    await engine.dispose()
    # The gauge labels persist across tests because prometheus_client
    # keeps them. Reset to a known state so cross-test contamination
    # never gives false confidence.
    WORKER_TOKENS_STALE_COUNT.clear()


async def _make_factory(postgres_url: str):  # type: ignore[return]
    engine = create_async_engine(postgres_url)
    return async_sessionmaker(engine, expire_on_commit=False), engine


def _hash(n: int) -> bytes:
    return n.to_bytes(32, "big")


async def _insert_worker_token(
    factory,
    *,
    token_hash: bytes,
    issued_at: datetime,
    last_seen_at: datetime | None = None,
    revoked_at: datetime | None = None,
    expires_at: datetime | None = None,
    token_type: str = "worker",
) -> None:
    async with factory() as s:
        await s.execute(insert(Token).values(
            token_hash=token_hash,
            type=token_type,
            scopes=["worker:claim", "worker:report", "worker:index"],
            issued_at=issued_at,
            last_seen_at=last_seen_at,
            revoked_at=revoked_at,
            expires_at=expires_at,
        ))
        await s.commit()


async def test_fresh_token_is_not_flagged(postgres_url: str) -> None:
    """A token minted yesterday with no last_seen_at should NOT be
    flagged as unused — the never-used fallback uses issued_at."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        await _insert_worker_token(
            factory,
            token_hash=_hash(1),
            issued_at=now - timedelta(days=1),
            last_seen_at=None,
        )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        aged = WORKER_TOKENS_STALE_COUNT.labels(reason="aged_90d")
        assert unused._value.get() == 0
        assert aged._value.get() == 0
    finally:
        await engine.dispose()


async def test_unused_30d_token_is_flagged(postgres_url: str) -> None:
    """Two tokens: one unused 31 days, one unused 5 days. Only the
    first should count toward unused_30d."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        await _insert_worker_token(
            factory,
            token_hash=_hash(1),
            issued_at=now - timedelta(days=60),
            last_seen_at=now - timedelta(days=31),
        )
        await _insert_worker_token(
            factory,
            token_hash=_hash(2),
            issued_at=now - timedelta(days=10),
            last_seen_at=now - timedelta(days=5),
        )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        assert unused._value.get() == 1
    finally:
        await engine.dispose()


async def test_never_used_token_old_enough_is_flagged(
    postgres_url: str,
) -> None:
    """A token minted 35 days ago, never presented (last_seen_at IS
    NULL), should be flagged via the issued_at fallback."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        await _insert_worker_token(
            factory,
            token_hash=_hash(1),
            issued_at=now - timedelta(days=35),
            last_seen_at=None,
        )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        assert unused._value.get() == 1
    finally:
        await engine.dispose()


async def test_aged_90d_token_is_flagged(postgres_url: str) -> None:
    """A token minted 95 days ago should count toward aged_90d
    regardless of last_seen_at recency."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        await _insert_worker_token(
            factory,
            token_hash=_hash(1),
            issued_at=now - timedelta(days=95),
            last_seen_at=now - timedelta(minutes=5),  # actively used
        )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        aged = WORKER_TOKENS_STALE_COUNT.labels(reason="aged_90d")
        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        assert aged._value.get() == 1
        # Same token; actively-used so should NOT be unused_30d.
        assert unused._value.get() == 0
    finally:
        await engine.dispose()


async def test_revoked_and_expired_tokens_are_excluded(
    postgres_url: str,
) -> None:
    """Revoked or naturally-expired tokens can't authenticate anyway —
    they shouldn't appear in the live-staleness count."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        # Revoked but otherwise stale
        await _insert_worker_token(
            factory,
            token_hash=_hash(1),
            issued_at=now - timedelta(days=120),
            last_seen_at=now - timedelta(days=60),
            revoked_at=now - timedelta(days=1),
        )
        # Naturally expired
        await _insert_worker_token(
            factory,
            token_hash=_hash(2),
            issued_at=now - timedelta(days=120),
            last_seen_at=now - timedelta(days=60),
            expires_at=now - timedelta(days=1),
        )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        aged = WORKER_TOKENS_STALE_COUNT.labels(reason="aged_90d")
        assert unused._value.get() == 0
        assert aged._value.get() == 0
    finally:
        await engine.dispose()


async def test_non_worker_token_types_are_excluded(
    postgres_url: str,
) -> None:
    """The audit targets worker tokens only. Batch-runner and team
    tokens have their own rotation cadences (or none) and must not
    contaminate the count."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        for token_type, h in (("batch_runner", 1), ("team", 2)):
            await _insert_worker_token(
                factory,
                token_hash=_hash(h),
                issued_at=now - timedelta(days=200),
                last_seen_at=now - timedelta(days=100),
                token_type=token_type,
            )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        aged = WORKER_TOKENS_STALE_COUNT.labels(reason="aged_90d")
        assert unused._value.get() == 0
        assert aged._value.get() == 0
    finally:
        await engine.dispose()


async def test_overlapping_unused_and_aged_count_independently(
    postgres_url: str,
) -> None:
    """One token that's both unused 60d AND aged 120d should count
    1 in each label — the reasons are independent filters."""
    factory, engine = await _make_factory(postgres_url)
    try:
        now = datetime.now(UTC)
        await _insert_worker_token(
            factory,
            token_hash=_hash(1),
            issued_at=now - timedelta(days=120),
            last_seen_at=now - timedelta(days=60),
        )
        async with factory() as s:
            await refresh_once(s, expiry_sec=30)

        unused = WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d")
        aged = WORKER_TOKENS_STALE_COUNT.labels(reason="aged_90d")
        assert unused._value.get() == 1
        assert aged._value.get() == 1
    finally:
        await engine.dispose()
