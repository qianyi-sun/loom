"""SWE-Bench (full split). Spec §5.2 row 2.

Shares the entire conversion path with SWE-Bench Verified — only the
upstream locator + name differ. We subclass rather than duplicate.
"""

from __future__ import annotations

from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import UpstreamSource


class SWEBenchAdapter(SWEBenchVerifiedAdapter):
    name = "swe-bench"
    display_name = "SWE-Bench (full)"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="princeton-nlp/SWE-bench",
        revision=None,
    )
