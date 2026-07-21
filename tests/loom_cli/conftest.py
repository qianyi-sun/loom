"""Shared fixtures."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, cast

import pytest
from loom_benchmarks.base import UpstreamSource


class _FetchUpstream(Protocol):
    def __call__(
        self,
        source: UpstreamSource,
        *,
        cache_root: Path,
        refresh: bool = False,
    ) -> Path: ...


@pytest.fixture()
def tmp_xdg_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point XDG_CONFIG_HOME at a tmp dir so loom_cli.config doesn't
    touch the developer's real ~/.config/loom."""
    config_root = tmp_path / "xdg-config"
    config_root.mkdir()
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_root))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    return config_root


@pytest.fixture(autouse=True)
def _resolve_synthetic_benchmark_source_locally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the shared CLI benchmark stub deterministic and offline."""
    from loom_cli import task_loader

    original_fetch_upstream = cast(
        _FetchUpstream,
        task_loader.fetch_upstream,
    )
    synthetic_source = tmp_path / "synthetic-benchmark-source"
    synthetic_source.mkdir()

    def _fetch_upstream(
        source: UpstreamSource,
        *,
        cache_root: Path,
        refresh: bool = False,
    ) -> Path:
        if source.locator == "stub/dataset":
            return synthetic_source
        return original_fetch_upstream(
            source,
            cache_root=cache_root,
            refresh=refresh,
        )

    monkeypatch.setattr(task_loader, "fetch_upstream", _fetch_upstream)
