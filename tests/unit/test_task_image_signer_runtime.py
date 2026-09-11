"""Owned startup order: verify SQL authority before loading keys or opening TLS."""

import asyncio
import importlib

import pytest

from loom_task_image_signer.config import decode_signer_settings
from tests.unit.test_task_image_signer_config import document


def module():
    name = "loom_task_image_signer.runtime"
    assert importlib.util.find_spec(name) is not None, "dedicated signer runtime missing"
    return importlib.import_module(name)


def configured(tmp_path):
    import json
    return decode_signer_settings(json.dumps(document(tmp_path)).encode())


async def test_failed_database_preflight_never_loads_keys_or_opens_listener(tmp_path, monkeypatch):
    m = module()
    events = []
    class Engine:
        async def dispose(self):
            events.append("dispose")
    monkeypatch.setattr(m, "read_owner_only_secret", lambda path: "postgresql+psycopg://signer:password@127.0.0.1/loom")
    monkeypatch.setattr(m, "create_async_engine", lambda *args, **kwargs: Engine())
    async def refused(engine):
        events.append("preflight")
        raise ValueError("too-broad")
    monkeypatch.setattr(m, "verify_signer_database_role", refused)
    monkeypatch.setattr(m, "load_signing_key", lambda *args, **kwargs: pytest.fail("keys loaded before admission"))
    monkeypatch.setattr(m, "SignerServer", lambda *args, **kwargs: pytest.fail("listener constructed before admission"))
    with pytest.raises(ValueError):
        async with m.running_signer(configured(tmp_path)):
            pytest.fail("failed preflight yielded a server")
    assert events == ["preflight", "dispose"]


async def test_runtime_closes_listener_and_database_after_cancelled_service(tmp_path, monkeypatch):
    m = module()
    events = []
    class Engine:
        async def dispose(self):
            events.append("dispose")
    class Server:
        def __init__(self, *args, **kwargs):
            events.append("server")
        async def start(self, **kwargs):
            events.append("start")
        async def aclose(self):
            events.append("close")
    monkeypatch.setattr(m, "read_owner_only_secret", lambda path: "postgresql+psycopg://signer:password@127.0.0.1/loom")
    monkeypatch.setattr(m, "read_owner_only_bytes", lambda *args, **kwargs: b"validated-tls-key")
    monkeypatch.setattr(m, "create_async_engine", lambda *args, **kwargs: Engine())
    async def verified(engine):
        events.append("preflight")
    monkeypatch.setattr(m, "verify_signer_database_role", verified)
    monkeypatch.setattr(m, "load_signing_key", lambda *args, **kwargs: events.append("key"))
    monkeypatch.setattr(m, "SignerPolicy", lambda *args, **kwargs: events.append("policy"))
    monkeypatch.setattr(m, "SignerServer", Server)
    with pytest.raises(asyncio.CancelledError):
        async with m.running_signer(configured(tmp_path)):
            raise asyncio.CancelledError
    assert events == ["preflight", "key", "key", "policy", "server", "start", "close", "dispose"]


@pytest.mark.parametrize("url", [
    "sqlite:///tmp/file", "postgresql+psycopg://signer:password@db.example/loom",
    "postgresql+psycopg://signer:password@db.example/loom?sslmode=require",
    "postgresql+psycopg://signer:password@db.example/loom?sslmode=verify-full",
    "postgresql+psycopg://signer:password@127.0.0.1/loom?host=db.example&sslmode=disable",
    "postgresql+psycopg://signer:password@127.0.0.1/loom?hostaddr=192.0.2.1&sslmode=disable",
    "postgresql+psycopg://signer:password@127.0.0.1/loom?service=remote",
])
async def test_remote_database_requires_verified_tls_before_connection(tmp_path, monkeypatch, url):
    m = module()
    monkeypatch.setattr(m, "read_owner_only_secret", lambda path: url)
    monkeypatch.setattr(m, "create_async_engine", lambda *args, **kwargs: pytest.fail("unsafe database connection attempted"))
    with pytest.raises(ValueError):
        async with m.running_signer(configured(tmp_path)):
            pytest.fail("unsafe database opened")


@pytest.mark.parametrize("name", ["PGHOSTADDR", "PGSERVICE", "PGSSLMODE"])
async def test_ambient_libpq_destination_or_tls_overrides_refused(tmp_path, monkeypatch, name):
    m = module()
    monkeypatch.setenv(name, "untrusted-override")
    monkeypatch.setattr(m, "read_owner_only_secret", lambda path: "postgresql+psycopg://signer:password@127.0.0.1/loom")
    monkeypatch.setattr(m, "create_async_engine", lambda *args, **kwargs: pytest.fail("ambient override reached database connection"))
    with pytest.raises(ValueError):
        async with m.running_signer(configured(tmp_path)):
            pytest.fail("ambient override admitted")


async def test_interrupted_context_cleanup_keeps_database_until_server_joins(tmp_path, monkeypatch):
    m = module()
    closing, release, disposed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    class Engine:
        async def dispose(self):
            disposed.set()
    class Server:
        def __init__(self, *args, **kwargs):
            pass
        async def start(self, **kwargs):
            pass
        async def aclose(self):
            closing.set()
            await release.wait()
    monkeypatch.setattr(m, "read_owner_only_secret", lambda path: "postgresql+psycopg://signer:password@127.0.0.1/loom")
    monkeypatch.setattr(m, "read_owner_only_bytes", lambda *args, **kwargs: b"validated-tls-key")
    monkeypatch.setattr(m, "create_async_engine", lambda *args, **kwargs: Engine())
    async def verified(engine):
        pass
    monkeypatch.setattr(m, "verify_signer_database_role", verified)
    monkeypatch.setattr(m, "load_signing_key", lambda *args, **kwargs: None)
    monkeypatch.setattr(m, "SignerPolicy", lambda *args, **kwargs: None)
    monkeypatch.setattr(m, "SignerServer", Server)
    async def caller():
        async with m.running_signer(configured(tmp_path)):
            pass
    task = asyncio.create_task(caller())
    try:
        await asyncio.wait_for(closing.wait(), 1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert not disposed.is_set(), "database disposed while signer work still owns it"
    finally:
        release.set()
        await asyncio.wait_for(disposed.wait(), 1)
