"""Explicit dedicated service composition; no registration, provisioning or flags."""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from loom_task_image_authority.config import read_owner_only_bytes, read_owner_only_secret
from loom_task_image_signer.config import SignerSettings
from loom_task_image_signer.keys import load_signing_key
from loom_task_image_signer.policy import SignerPolicy
from loom_task_image_signer.preflight import verify_signer_database_role
from loom_task_image_signer.server import SignerServer


async def _shutdown(server: SignerServer | None, engine: AsyncEngine) -> None:
    try:
        if server is not None:
            await server.aclose()
    finally:
        await engine.dispose()


@asynccontextmanager
async def running_signer(settings: SignerSettings) -> AsyncIterator[SignerServer]:
    if any(name.startswith("PG") for name in os.environ):
        raise ValueError("dedicated signer refuses ambient libpq settings")
    database_url = make_url(read_owner_only_secret(settings.database_url_file))
    if (
        database_url.drivername != "postgresql+psycopg" or not database_url.username
        or not database_url.password or not database_url.database or not database_url.host
        or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", database_url.database)
        or not re.fullmatch(r"[A-Za-z0-9_.:-]+", database_url.host)
        or set(database_url.query) - {"sslmode", "sslrootcert"}
        or any(type(value) is not str for value in database_url.query.values())
    ):
        raise ValueError("signer requires explicit authenticated PostgreSQL")
    if database_url.host not in {"127.0.0.1", "::1"}:
        root = database_url.query.get("sslrootcert")
        if database_url.query.get("sslmode") != "verify-full" or type(root) is not str or not Path(root).is_absolute():
            raise ValueError("remote signer database requires verified TLS and explicit CA")
    engine = create_async_engine(
        database_url, pool_size=settings.limits.maximum_operations,
        max_overflow=0, pool_timeout=settings.policy_timeout_seconds,
        connect_args={"connect_timeout": 5},
    )
    server: SignerServer | None = None
    try:
        # Do not load private signing handles or open a listener until the real
        # login has passed exact effective authority/schema validation.
        await verify_signer_database_role(engine)
        publication = load_signing_key(settings.publication.seed_file, expected_public_key=settings.publication.public_bytes())
        execution = load_signing_key(settings.execution.seed_file, expected_public_key=settings.execution.public_bytes())
        # TLS private identity is also protected; SSL parses it without prompts.
        read_owner_only_bytes(settings.private_key_file, max_bytes=16 * 1024)
        policy = SignerPolicy(
            engine, trust_root=settings.trust_root(), publication_key_id=settings.publication.key_id,
            publication_provider=publication, execution_provider=execution,
            selections=settings.selections, timeout_seconds=settings.policy_timeout_seconds,
            keyset_lifetime_seconds=settings.keyset_lifetime_seconds,
        )
        server = SignerServer(
            policy, ca_file=settings.ca_file, certificate_file=settings.certificate_file,
            private_key_file=settings.private_key_file,
            peer_operations={pin: frozenset(operations) for pin, operations in settings.peer_operations.items()},
            limits=settings.limits,
        )
        await server.start(host=settings.host, port=settings.port)
        yield server
    finally:
        # A second caller cancellation must not advance pool disposal ahead of
        # the retained server-close owner. The cleanup task owns both lifetimes.
        closing = asyncio.create_task(_shutdown(server, engine))
        await asyncio.shield(closing)
