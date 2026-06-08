"""registry.py — load default in-tree JSON, fetch over HTTPS, cache."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from loom_cli.registry import (
    RegistryFetchError,
    load_registry_entries,
    purge_registry_cache,
    resolve_registry_url,
)


def test_resolve_url_prefers_explicit_arg(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM_REGISTRY_URL", "https://env.example/r.json")
    assert resolve_registry_url("https://flag.example/r.json") == \
        "https://flag.example/r.json"


def test_resolve_url_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM_REGISTRY_URL", "https://env.example/r.json")
    assert resolve_registry_url(None) == "https://env.example/r.json"


def test_resolve_url_falls_back_to_default_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOM_REGISTRY_URL", raising=False)
    url = resolve_registry_url(None)
    assert url.startswith("file://"), url
    assert url.endswith("default-registry.json")


def test_load_default_returns_non_builtin_entries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.delenv("LOOM_REGISTRY_URL", raising=False)
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path))
    entries = load_registry_entries(url=None)
    slugs = {e.slug for e in entries}
    # Default registry only holds entries that aren't already shipped as
    # builtin entry-points (which union_entries would mask anyway).
    assert "terminal-bench-2" in slugs
    tb2 = next(e for e in entries if e.slug == "terminal-bench-2")
    assert tb2.source == "registry"
    assert tb2.available_pip_spec == "loom-benchmark-terminal-bench-2"


def test_remote_fetch_is_cached_to_disk(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path))

    payload = {
        "registry_version": 1,
        "entries": [{
            "slug": "demo", "display_name": "Demo", "license_spdx": "MIT",
            "license_url": "https://x.example", "task_count": 1,
            "available": "loom-benchmark-demo",
        }],
    }

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    from loom_cli import registry as reg_mod
    monkeypatch.setattr(reg_mod, "_build_client",
                        lambda: httpx.Client(transport=transport))

    e1 = load_registry_entries(url="https://r.example/registry.json")
    e2 = load_registry_entries(url="https://r.example/registry.json")
    assert calls["n"] == 1
    assert [e.slug for e in e1] == [e.slug for e in e2] == ["demo"]

    purge_registry_cache()
    load_registry_entries(url="https://r.example/registry.json")
    assert calls["n"] == 2


def test_fetch_error_raises_registry_fetch_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    transport = httpx.MockTransport(handler)
    from loom_cli import registry as reg_mod
    monkeypatch.setattr(reg_mod, "_build_client",
                        lambda: httpx.Client(transport=transport))

    with pytest.raises(RegistryFetchError):
        load_registry_entries(url="https://r.example/r.json")
