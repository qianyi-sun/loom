"""FastAPI app factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.rate_card import RateCardCache
from loom_llm_gateway.routes import admin, chat, health, messages


def create_app(settings: GatewaySettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(str(settings.db_url))
        app.state.settings = settings
        app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
        app.state.rate_card_cache = RateCardCache(
            session_factory=app.state.session_factory,
            ttl_sec=settings.rate_card_cache_ttl_sec,
        )
        # Long-lived httpx client for native dialect passthroughs
        # (anthropic, openai responses, gemini). Plan 9 A9.1 — we do NOT
        # round-trip through LiteLLM for these.
        app.state.upstream_client = httpx.AsyncClient(
            timeout=settings.upstream_timeout_sec,
        )
        try:
            yield
        finally:
            await app.state.upstream_client.aclose()
            await engine.dispose()

    app: FastAPI = FastAPI(
        title="Loom LLM Gateway", version="0.0.1", lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(chat.router)
    app.include_router(messages.router)
    app.include_router(admin.router)
    return app


__all__ = ["create_app"]
