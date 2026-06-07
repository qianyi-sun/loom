"""SWE-Bench Multimodal. Spec §5.2 row 3.

Same conversion as SWE-Bench Verified, plus base64-encoded screenshot
attachments inlined into `instruction.md`. We delegate to the parent
for solution.sh + tests + task.toml, then overwrite instruction.md
with the enriched body that includes the markdown image embeds.
"""

from __future__ import annotations

import base64
from pathlib import Path

from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource
from loom_benchmarks.util import embed_base64_image, sha256_of_dir


class SWEBenchMultimodalAdapter(SWEBenchVerifiedAdapter):
    name = "swe-bench-multimodal"
    display_name = "SWE-Bench Multimodal"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="princeton-nlp/SWE-bench_Multimodal",
        revision=None,
    )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        # Delegate to parent so solution/, tests/, task.toml are
        # generated (task_id, image, FAIL_TO_PASS, etc. all share the
        # SWE-Bench Verified rules). The parent also writes
        # instruction.md = problem_statement.
        super().convert_instance(instance, out_dir=out_dir)

        # Re-render instruction.md with the screenshots inlined.
        body = str(instance.raw["problem_statement"])
        for i, b64 in enumerate(instance.raw.get("image_assets") or []):
            body += "\n\n" + embed_base64_image(
                base64.b64decode(b64), alt_text=f"screenshot-{i}",
            )
        (out_dir / "instruction.md").write_text(body)

        return ConvertedTask(
            task_id=f"{self.name}/{instance.instance_id}",
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
