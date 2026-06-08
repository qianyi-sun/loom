"""TB-2 adapter declares Apache-2.0, which is in the default team_quotas
license_allowlist Plan 13 ships. This test guards against an accidental
license-tag regression (e.g., a refactor that silently changes the
attribute to None or to a restricted SPDX)."""

from __future__ import annotations

from loom_benchmark_terminal_bench_2 import adapter as tb2_adapter

# Default allowlist from Plan 13's team_quotas server_default — see
# src/loom/db/schema.py TeamQuota model.
_DEFAULT_ALLOWLIST = frozenset({
    "MIT", "Apache-2.0", "BSD-3-Clause", "CC-BY-4.0",
})


def test_license_is_apache_2() -> None:
    assert tb2_adapter.license_spdx == "Apache-2.0"


def test_license_clears_default_allowlist() -> None:
    assert tb2_adapter.license_spdx in _DEFAULT_ALLOWLIST


def test_license_url_points_to_upstream_repo() -> None:
    assert "laude-institute/terminal-bench" in tb2_adapter.license_url
    assert tb2_adapter.license_url.endswith("LICENSE")
