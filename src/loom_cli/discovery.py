"""Discovery layer — dataclass + union(builtin, registry, remote)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

Source = Literal["builtin", "registry", "remote"]
Status = Literal["installed", "available", "remote-only"]


@dataclass(frozen=True)
class DatasetEntry:
    slug: str
    source: Source
    display_name: str
    license_spdx: str
    license_url: str
    task_count: int | None
    status: Status
    available_pip_spec: str | None
    entry_point: str | None


def union_entries(
    *,
    builtin: Iterable[DatasetEntry],
    registry: Iterable[DatasetEntry],
    remote: Iterable[DatasetEntry],
) -> list[DatasetEntry]:
    """Merge the three sources, deduping on slug.

    Precedence: builtin > remote > registry. Rationale:
    - If pip-installed locally, that's the source of truth.
    - Else, if a CP service has it, prefer the live service over a
      potentially-stale registry snapshot.
    - Else, surface as "available" from the registry.
    """
    seen: dict[str, DatasetEntry] = {}
    for e in registry:
        seen[e.slug] = e
    for e in remote:
        seen[e.slug] = e
    for e in builtin:
        seen[e.slug] = e
    return sorted(seen.values(), key=lambda x: x.slug)


def discover_all(
    *,
    registry_url: str | None,
    server_url: str | None,
    token: str | None,
) -> list[DatasetEntry]:
    from loom_cli.builtin import load_builtin_entries
    from loom_cli.registry import RegistryFetchError, load_registry_entries
    from loom_cli.remote import load_remote_entries

    builtin = load_builtin_entries()
    try:
        registry = load_registry_entries(url=registry_url)
    except RegistryFetchError:
        registry = []
    remote = load_remote_entries(server_url=server_url, token=token)
    return union_entries(builtin=builtin, registry=registry, remote=remote)
