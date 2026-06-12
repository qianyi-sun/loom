"""catalog.py — load default in-tree JSON, fetch over HTTPS, cache."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from loom_cli.catalog import (
    CatalogFetchError,
    load_catalog_entries,
    purge_catalog_cache,
    resolve_catalog_url,
)


def test_resolve_url_prefers_explicit_arg(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM_CATALOG_URL", "https://env.example/r.json")
    assert resolve_catalog_url("https://flag.example/r.json") == \
        "https://flag.example/r.json"


def test_resolve_url_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOOM_CATALOG_URL", "https://env.example/r.json")
    assert resolve_catalog_url(None) == "https://env.example/r.json"


def test_resolve_url_falls_back_to_default_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOM_CATALOG_URL", raising=False)
    url = resolve_catalog_url(None)
    assert url.startswith("file://"), url
    assert url.endswith("default-catalog.json")


def test_load_default_returns_non_builtin_entries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.delenv("LOOM_CATALOG_URL", raising=False)
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path))
    entries = load_catalog_entries(url=None)
    slugs = {e.slug for e in entries}
    # Default catalog only holds entries that aren't already shipped as
    # builtin entry-points (which union_entries would mask anyway).
    assert "terminal-bench-2" in slugs
    tb2 = next(e for e in entries if e.slug == "terminal-bench-2")
    assert tb2.source == "catalog"
    assert tb2.available_pip_spec == "loom-benchmark-terminal-bench-2"


def test_file_catalog_url_decodes_escaped_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path / "cache"))
    catalog_dir = tmp_path / "dir with spaces"
    catalog_dir.mkdir()
    catalog_file = catalog_dir / "catalog.json"
    catalog_file.write_text(json.dumps({
        "catalog_version": 1,
        "entries": [{
            "slug": "demo", "display_name": "Demo", "license_spdx": "MIT",
            "license_url": "https://x.example", "task_count": 1,
            "available": "loom-benchmark-demo",
        }],
    }))

    entries = load_catalog_entries(url=catalog_file.as_uri())

    assert [entry.slug for entry in entries] == ["demo"]


def test_remote_fetch_is_cached_to_disk(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path))

    payload = {
        "catalog_version": 1,
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
    from loom_cli import catalog as cat_mod
    monkeypatch.setattr(cat_mod, "_build_client",
                        lambda: httpx.Client(transport=transport))

    e1 = load_catalog_entries(url="https://r.example/catalog.json")
    e2 = load_catalog_entries(url="https://r.example/catalog.json")
    assert calls["n"] == 1
    assert [e.slug for e in e1] == [e.slug for e in e2] == ["demo"]

    purge_catalog_cache()
    load_catalog_entries(url="https://r.example/catalog.json")
    assert calls["n"] == 2


def test_fetch_error_raises_catalog_fetch_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("LOOM_CACHE_DIR", str(tmp_path))

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    transport = httpx.MockTransport(handler)
    from loom_cli import catalog as cat_mod
    monkeypatch.setattr(cat_mod, "_build_client",
                        lambda: httpx.Client(transport=transport))

    with pytest.raises(CatalogFetchError):
        load_catalog_entries(url="https://r.example/r.json")
