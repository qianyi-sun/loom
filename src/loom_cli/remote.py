"""CP-connected discovery — calls `GET /api/v1/benchmarks` on the
service pointed to by `LOOM_SERVER_URL` (or the `--server-url` flag).
Failures degrade silently to an empty list so `loom datasets list`
remains usable in air-gapped + offline environments."""

from __future__ import annotations

import logging
import os

import httpx

from loom_cli.discovery import DatasetEntry

logger = logging.getLogger(__name__)


def _build_client() -> httpx.Client:
    return httpx.Client(timeout=5.0, follow_redirects=False)


def _resolve_server(explicit: str | None) -> str | None:
    return explicit or os.environ.get("LOOM_SERVER_URL")


def _resolve_token(explicit: str | None) -> str | None:
    return explicit or os.environ.get("LOOM_API_TOKEN")


def load_remote_entries(
    *, server_url: str | None, token: str | None,
) -> list[DatasetEntry]:
    url = _resolve_server(server_url)
    if not url:
        return []
    headers: dict[str, str] = {}
    tok = _resolve_token(token)
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    try:
        with _build_client() as client:
            resp = client.get(
                url.rstrip("/") + "/api/v1/benchmarks", headers=headers,
            )
            resp.raise_for_status()
            payload = resp.json()
    except httpx.HTTPError as exc:
        logger.warning("loom service /benchmarks fetch failed: %s", exc)
        return []

    out: list[DatasetEntry] = []
    for item in payload.get("items", []):
        upstream_kind = item.get("upstream_kind")
        out.append(DatasetEntry(
            slug=str(item["id"]),
            source="remote",
            display_name=str(item.get("display_name", item["id"])),
            license_spdx=str(item.get("license_spdx", "UNKNOWN")),
            license_url=str(item.get("license_url", "")),
            task_count=None,
            status="remote-only",
            available_pip_spec=None,
            entry_point=None,
            upstream_kind=str(upstream_kind) if upstream_kind else None,
        ))
    out.sort(key=lambda e: e.slug)
    return out
