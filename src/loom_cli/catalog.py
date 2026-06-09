"""Remote catalog loader — list of pip-installable benchmark/task
adapters Loom knows about. Defaults to the in-tree
`catalog_data/default-catalog.json`. URL overridable via
`--catalog-url` (passed as `url=`) or the `LOOM_CATALOG_URL` env var.

Distinct from the in-process `loom_benchmarks.registry` and
`loom_launcher.registry` dicts (which hold *already-loaded* adapter
instances); this module is about the *catalog of installable
packages* — what `loom datasets list` shows under "available."

Responses are cached on disk at `${LOOM_CACHE_DIR:-${XDG_CACHE_HOME:-
~/.cache}}/loom/catalog/<sha256(url)>.json` for 24h.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from loom_cli.discovery import DatasetEntry

_DEFAULT_TTL_SECONDS = 24 * 60 * 60


class CatalogFetchError(RuntimeError):
    """Raised when the catalog cannot be retrieved."""


def _default_catalog_path() -> Path:
    return (
        Path(__file__).parent / "catalog_data" / "default-catalog.json"
    )


def resolve_catalog_url(explicit: str | None) -> str:
    if explicit:
        return explicit
    env = os.environ.get("LOOM_CATALOG_URL")
    if env:
        return env
    return _default_catalog_path().as_uri()


def _cache_dir() -> Path:
    base = os.environ.get("LOOM_CACHE_DIR")
    if base:
        return Path(base) / "loom" / "catalog"
    xdg = os.environ.get("XDG_CACHE_HOME")
    root = Path(xdg) if xdg else Path.home() / ".cache"
    return root / "loom" / "catalog"


def _normalize_url(url: str) -> str:
    """Stable cache key — strip trailing slash + empty query/fragment so
    equivalent URLs (`https://x/r.json` and `https://x/r.json?`) share
    one cache entry."""
    p = urlparse(url)
    path = p.path.rstrip("/") or "/"
    return p._replace(path=path, query=p.query, fragment="").geturl()


def _cache_path(url: str) -> Path:
    digest = hashlib.sha256(_normalize_url(url).encode("utf-8")).hexdigest()
    return _cache_dir() / f"{digest}.json"


def purge_catalog_cache() -> None:
    d = _cache_dir()
    if not d.exists():
        return
    for child in d.iterdir():
        if child.is_file():
            child.unlink()


def _build_client() -> httpx.Client:
    return httpx.Client(timeout=10.0, follow_redirects=True)


def _fetch_payload(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme == "file":
        try:
            return json.loads(Path(parsed.path).read_text())  # type: ignore[no-any-return]
        except (OSError, json.JSONDecodeError) as exc:
            raise CatalogFetchError(f"failed to read {url}: {exc}") from exc
    cache = _cache_path(url)
    if cache.exists() and (time.time() - cache.stat().st_mtime) < _DEFAULT_TTL_SECONDS:
        try:
            return json.loads(cache.read_text())  # type: ignore[no-any-return]
        except json.JSONDecodeError:
            cache.unlink(missing_ok=True)  # corrupt cache; re-fetch
    try:
        with _build_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            payload: dict[str, Any] = resp.json()
    except httpx.HTTPError as exc:
        raise CatalogFetchError(f"failed to fetch {url}: {exc}") from exc
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(payload))
    return payload


@dataclass(frozen=True)
class _RawEntry:
    slug: str
    display_name: str
    license_spdx: str
    license_url: str
    task_count: int | None
    available: str


def _coerce(raw: dict[str, Any]) -> _RawEntry:
    return _RawEntry(
        slug=str(raw["slug"]),
        display_name=str(raw["display_name"]),
        license_spdx=str(raw["license_spdx"]),
        license_url=str(raw["license_url"]),
        task_count=(int(raw["task_count"]) if raw.get("task_count") is not None else None),
        available=str(raw["available"]),
    )


def load_catalog_entries(*, url: str | None) -> list[DatasetEntry]:
    resolved = resolve_catalog_url(url)
    payload = _fetch_payload(resolved)
    if payload.get("catalog_version") != 1:
        raise CatalogFetchError(
            f"unsupported catalog_version: {payload.get('catalog_version')!r}",
        )
    out: list[DatasetEntry] = []
    for raw in payload.get("entries", []):
        r = _coerce(raw)
        out.append(DatasetEntry(
            slug=r.slug,
            source="catalog",
            display_name=r.display_name,
            license_spdx=r.license_spdx,
            license_url=r.license_url,
            task_count=r.task_count,
            status="available",
            available_pip_spec=r.available,
            entry_point=None,
        ))
    out.sort(key=lambda e: e.slug)
    return out
