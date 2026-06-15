"""SkillLearnBench — sibling of SkillFlow. Spec §5.2 row 13.

Same passthrough conversion as SkillFlow; only the upstream URL +
display name differ.
"""

from __future__ import annotations

from loom_benchmarks.adapters.skillflow import SkillFlowAdapter
from loom_benchmarks.base import UpstreamSource


class SkillLearnBenchAdapter(SkillFlowAdapter):
    name = "skilllearnbench"
    display_name = "SkillLearnBench"
    series = "skill"
    upstream_source = UpstreamSource(
        kind="git",
        locator="https://github.com/cxcscmu/SkillLearnBench.git",
        revision="main",
    )
    license_url = (
        "https://github.com/cxcscmu/SkillLearnBench/blob/main/LICENSE"
    )
