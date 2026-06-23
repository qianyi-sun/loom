"""FastAPI app factory."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from prometheus_client import make_asgi_app
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier, load_optional_admin_secret_verifier
from loom.security.secret_store import assert_existing_secrets_decryptable
from loom_llm_gateway.config import GatewaySettings
from loom_llm_gateway.egress_client_pool import EgressClientPool
from loom_llm_gateway.rate_card import RateCardCache
from loom_llm_gateway.routes import (
    admin,
    chat,
    facade_anthropic,
    facade_google,
    facade_openai,
    gemini,
    health,
    messages,
    responses,
)


def _load_admin_secret_verifier(
    settings: GatewaySettings,
) -> AdminSecretVerifier | None:
    """Load singleton admin auth material for Gateway admin routes."""
    production = os.environ.get("LOOM_ENV", "").lower() == "production"
    return load_optional_admin_secret_verifier(
        settings.admin_secret_file,
        production=production,
    )


async def _assert_secret_store_startup(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return await assert_existing_secrets_decryptable(session)


def create_app(settings: GatewaySettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(str(settings.db_url))
        admin_secret_verifier = _load_admin_secret_verifier(settings)
        app.state.settings = settings
        app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
        await _assert_secret_store_startup(app.state.session_factory)
        app.state.admin_secret_verifier = admin_secret_verifier
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
        # #190 PR-C2: per-connection-id egress-proxy client pool. When
        # `egress_proxy_url` is empty (default), `pool.get(...)` falls
        # through to the shared upstream_client — egress mode opt-in,
        # rollback is one env-var flip + restart.
        app.state.egress_client_pool = EgressClientPool(
            upstream_client=app.state.upstream_client,
            proxy_url=settings.egress_proxy_url,
            upstream_timeout_sec=settings.upstream_timeout_sec,
        )
        try:
            yield
        finally:
            await app.state.egress_client_pool.aclose()
            await app.state.upstream_client.aclose()
            await engine.dispose()

    app: FastAPI = FastAPI(
        title="Loom LLM Gateway", version="0.0.1", lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(chat.router)
    app.include_router(messages.router)
    app.include_router(responses.router)
    app.include_router(gemini.router)
    app.include_router(facade_openai.router)
    app.include_router(facade_anthropic.router)
    app.include_router(facade_google.router)
    app.include_router(admin.router)
    # /metrics: prometheus_client ASGI app. Gateway is internal-only
    # per #77 boundary — scrapers reach it via cluster DNS.
    app.mount("/metrics", make_asgi_app())
    return app


__all__ = ["create_app"]
