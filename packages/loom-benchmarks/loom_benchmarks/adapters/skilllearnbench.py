"""SkillLearnBench — sibling of SkillFlow. Spec §5.2 row 13.

Same passthrough conversion as SkillFlow; only the upstream URL +
display name differ. All metadata loads from catalog.json.
"""

from __future__ import annotations

from loom_benchmarks.adapters.skillflow import SkillFlowAdapter


class SkillLearnBenchAdapter(SkillFlowAdapter):
    name = "skilllearnbench"
