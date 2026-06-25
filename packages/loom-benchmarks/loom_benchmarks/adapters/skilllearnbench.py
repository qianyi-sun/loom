"""SkillLearnBench adapter.

Upstream `cxcscmu/SkillLearnBench` ships per-task bundles under
`tasks/<family>/<task>/` whose Dockerfiles do
`COPY skills /root/.<agent>/skills`. The `skills/` source lives at the
upstream repo **root**, not in the bundle — it holds one skill bundle
per (method, family) under `skills/<method>/<family>/`. The chosen
method IS the system under test for SkillLearnBench: agents read the
copied skills at runtime and the score reflects skill quality.

This adapter is otherwise a passthrough on top of `SkillFlowAdapter`;
it overrides `list_instances` to stash the upstream root + emit
`method` and `oracle_eligible` tags, and overrides
`_convert_bundle_instance` to overlay the
chosen method's per-family skill bundle on top of the empty `skills/`
placeholder that the parent class writes.

`skill_method` is read from the catalog entry's `params.skill_method`,
defaulting to `human_authored` (the canonical reference baseline).
Future method coverage adds catalog rows with the same upstream and a
different slug + `skill_method` value — no adapter code change needed.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

from loom_benchmarks.adapters.skillflow import SkillFlowAdapter
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask
from loom_benchmarks.util import sha256_of_dir


class SkillLearnBenchAdapter(SkillFlowAdapter):
    name = "skilllearnbench"

    @property
    def skill_method(self) -> str:
        return self._params.get("skill_method") or "human_authored"

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for inst in super().list_instances(source_dir=source_dir, split=split):
            raw = dict(inst.raw)
            raw["__upstream_root"] = str(source_dir)
            tags = {
                **inst.tags,
                "method": self.skill_method,
                "oracle_eligible": self._oracle_eligible_tag(inst),
            }
            yield BenchmarkInstance(
                instance_id=inst.instance_id,
                split=inst.split,
                raw=raw,
                tags=tags,
            )

    @staticmethod
    def _oracle_eligible_tag(instance: BenchmarkInstance) -> str:
        source_path = instance.raw.get("__source_path")
        if not source_path:
            return "false"
        solve = Path(str(source_path)) / "solution" / "solve.sh"
        return "true" if solve.is_file() else "false"

    def _convert_bundle_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
        task_id: str,
    ) -> ConvertedTask:
        result = super()._convert_bundle_instance(
            instance, out_dir=out_dir, task_id=task_id,
        )
        injected = self._inject_skills(instance, out_dir=out_dir)
        if not injected:
            return result
        # Skill injection mutated the bundle after super() hashed it.
        return ConvertedTask(
            task_id=result.task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=result.license_spdx,
            warnings=result.warnings,
        )

    def _inject_skills(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> bool:
        upstream_root = instance.raw.get("__upstream_root")
        if not upstream_root:
            return False
        family = instance.instance_id.split("/", 1)[0]
        for candidate in (
            Path(upstream_root) / "skills" / self.skill_method / family,
            Path(upstream_root) / "repo" / "skills" / self.skill_method / family,
        ):
            if candidate.is_dir():
                source = candidate
                break
        else:
            return False

        target = out_dir / "skills"
        target.mkdir(exist_ok=True)
        keep = target / ".keep"
        if keep.exists():
            keep.unlink()
        for child in source.iterdir():
            dest = target / child.name
            if child.is_symlink():
                continue
            if child.is_dir():
                shutil.copytree(child, dest, dirs_exist_ok=True)
            else:
                shutil.copy2(child, dest)
        return True
