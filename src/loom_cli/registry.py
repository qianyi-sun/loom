"""Remote registry loader. Defaults to the in-tree
`registry_data/default-registry.json`. URL overridable via
`--registry-url` (passed as `url=`) or the `LOOM_REGISTRY_URL` env var.

Responses are cached on disk at `${LOOM_CACHE_DIR:-${XDG_CACHE_HOME:-
~/.cache}}/loom/registry/<sha256(url)>.json` for 24h.
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


class RegistryFetchError(RuntimeError):
    """Raised when the remote registry cannot be retrieved."""


def _default_registry_path() -> Path:
    return (
        Path(__file__).parent / "registry_data" / "default-registry.json"
    )


def resolve_registry_url(explicit: str | None) -> str:
    if explicit:
        return explicit
    env = os.environ.get("LOOM_REGISTRY_URL")
    if env:
        return env
    return _default_registry_path().as_uri()


def _cache_dir() -> Path:
    base = os.environ.get("LOOM_CACHE_DIR")
    if base:
        return Path(base) / "loom" / "registry"
    xdg = os.environ.get("XDG_CACHE_HOME")
    root = Path(xdg) if xdg else Path.home() / ".cache"
    return root / "loom" / "registry"


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


def purge_registry_cache() -> None:
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
            raise RegistryFetchError(f"failed to read {url}: {exc}") from exc
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
        raise RegistryFetchError(f"failed to fetch {url}: {exc}") from exc
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


def load_registry_entries(*, url: str | None) -> list[DatasetEntry]:
    resolved = resolve_registry_url(url)
    payload = _fetch_payload(resolved)
    if payload.get("registry_version") != 1:
        raise RegistryFetchError(
            f"unsupported registry_version: {payload.get('registry_version')!r}",
        )
    out: list[DatasetEntry] = []
    for raw in payload.get("entries", []):
        r = _coerce(raw)
        out.append(DatasetEntry(
            slug=r.slug,
            source="registry",
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
