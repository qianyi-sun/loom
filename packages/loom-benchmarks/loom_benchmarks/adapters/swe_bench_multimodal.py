"""SWE-Bench Multimodal. Spec §5.2 row 3.

Same conversion as SWE-Bench Verified, plus screenshot URLs from the
upstream `image_assets` field appended to `instruction.md`. We delegate
to the parent for solution.sh + tests + task.toml, then overwrite
instruction.md with the body that includes the image links.

Upstream stores `image_assets` as a JSON string mapping section names
to URL lists (e.g. `{"problem_statement": ["https://user-images..."]}`).
We render those as inline-markdown image links rather than downloading
+ embedding to keep the bundle size sane and avoid per-import network
chatter; the worker's HTTP fetcher can re-resolve them at trial time
if the agent needs the pixels.
"""

from __future__ import annotations

import json
from pathlib import Path

from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask
from loom_benchmarks.util import sha256_of_dir


class SWEBenchMultimodalAdapter(SWEBenchVerifiedAdapter):
    # All metadata loads from benchmarks.json. Inherits convert_instance
    # from the Verified parent + overrides for image-asset rendering.
    name = "swe-bench-multimodal"

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        # Delegate to parent so solution/, tests/, task.toml are
        # generated. The parent also writes instruction.md =
        # problem_statement.
        super().convert_instance(instance, out_dir=out_dir)

        body = str(instance.raw["problem_statement"])
        assets_raw = instance.raw.get("image_assets")
        assets: dict[str, list[str]] = {}
        if isinstance(assets_raw, str) and assets_raw:
            try:
                parsed = json.loads(assets_raw)
                if isinstance(parsed, dict):
                    assets = {
                        k: [u for u in v if isinstance(u, str)]
                        for k, v in parsed.items()
                        if isinstance(v, list)
                    }
            except json.JSONDecodeError:
                pass
        elif isinstance(assets_raw, dict):
            assets = {
                k: [u for u in v if isinstance(u, str)]
                for k, v in assets_raw.items()
                if isinstance(v, list)
            }
        for section, urls in assets.items():
            for i, url in enumerate(urls):
                body += f"\n\n![{section}-{i}]({url})"
        (out_dir / "instruction.md").write_text(body)

        return ConvertedTask(
            task_id=f"{self.name}/{instance.instance_id}",
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
