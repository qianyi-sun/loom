"""Rate card models + lookup (spec §4.7 rate_cards table).

The DB table stores the payload as JSONB; this module parses it into Pydantic
models + provides lookup helpers. A small TTL-refresh cache wraps the DB lookup.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import RateCard
from loom.models.types import ModelSpec
from loom_llm_gateway.errors import RateCardNotFoundError


class RateCardEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    provider: str
    model: str
    tier: str | None = None
    region: str | None = None
    input_per_mtok: float = Field(ge=0)
    output_per_mtok: float = Field(ge=0)
    cache_read_per_mtok: float = Field(ge=0)
    cache_write_per_mtok: float = Field(ge=0)


class RateCardTable(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str
    captured_at: datetime
    entries: list[RateCardEntry]


def lookup_entry(table: RateCardTable, spec: ModelSpec) -> RateCardEntry:
    """Match by (provider, model, tier?, region?) — most-specific wins."""
    candidates = [
        e for e in table.entries
        if e.provider == spec.provider and e.model == spec.name
    ]
    if not candidates:
        raise RateCardNotFoundError(
            f"no entry for {spec.provider}/{spec.name}",
        )

    def _score(e: RateCardEntry) -> int:
        score = 0
        if e.tier == spec.tier:
            score += 2 if spec.tier else 1
        if e.region == spec.region:
            score += 2 if spec.region else 1
        return score

    return max(candidates, key=_score)


SessionFactory = Callable[[], AbstractAsyncContextManager[AsyncSession]]


@dataclass
class RateCardCache:
    """In-memory cache of the active rate-card table. TTL refresh.

    `session_factory` is a callable that returns an async-context-manager
    AsyncSession (typically `async_sessionmaker(engine)`).
    """

    session_factory: Any
    ttl_sec: int
    _table: RateCardTable | None = None
    _fetched_at: float = 0.0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def get(self) -> RateCardTable:
        now = time.monotonic()
        async with self._lock:
            if self._table is None or now - self._fetched_at > self.ttl_sec:
                self._table = await self._fetch_latest()
                self._fetched_at = now
            return self._table

    def invalidate(self) -> None:
        """Drop the cached table so the next get() refetches.

        Used by the admin upsert path so a freshly-uploaded card is visible
        without waiting for the TTL.
        """
        self._table = None
        self._fetched_at = 0.0

    async def _fetch_latest(self) -> RateCardTable:
        async with self.session_factory() as session:
            row = (await session.execute(
                select(RateCard).order_by(RateCard.captured_at.desc()).limit(1),
            )).scalar_one_or_none()
        if row is None:
            raise RateCardNotFoundError("no rate card registered")
        payload = dict(row.table or {})
        payload["id"] = row.id
        payload["captured_at"] = row.captured_at
        return RateCardTable(**payload)


def hash_table(table: RateCardTable) -> str:
    """Stable hash for trajectory event `rate_card_hash` field.

    Excludes `captured_at` so logically-identical tables hash to the same
    value across re-registrations.
    """
    serialised = table.model_dump_json(exclude={"captured_at"})
    return hashlib.sha256(serialised.encode()).hexdigest()


def compute_cost_usd(
    entry: RateCardEntry,
    *,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int,
    cache_write_tokens: int,
) -> float:
    """Spec §4.4.1: cost = sum of token category × per-Mtok rate."""

    def _at(tokens: int, per_mtok: float) -> float:
        return tokens * per_mtok / 1_000_000

    return (
        _at(input_tokens, entry.input_per_mtok)
        + _at(output_tokens, entry.output_per_mtok)
        + _at(cached_input_tokens, entry.cache_read_per_mtok)
        + _at(cache_write_tokens, entry.cache_write_per_mtok)
    )
