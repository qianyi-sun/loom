"""TB-2 adapter declares stable upstream license metadata.

This guards against an accidental license-tag regression, for example a
refactor that silently changes the attribute to None or the wrong SPDX value.
"""

from __future__ import annotations

from loom_benchmark_terminal_bench_2 import adapter as tb2_adapter


def test_license_is_apache_2() -> None:
    assert tb2_adapter.license_spdx == "Apache-2.0"


def test_license_url_points_to_upstream_repo() -> None:
    assert "harbor-framework/terminal-bench-2-1" in tb2_adapter.license_url
    assert tb2_adapter.license_url.endswith("LICENSE")
