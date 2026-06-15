"""SWE-Bench Multimodal adapter contract (Plan 15 Phase 3)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from loom_benchmarks.adapters.swe_bench_multimodal import (
    SWEBenchMultimodalAdapter,
)
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig

FIXTURE = (
    Path(__file__).parent
    / "fixtures" / "swe_bench_multimodal" / "sample.json"
)


def test_multimodal_renders_image_assets_as_markdown_links(tmp_path: Path) -> None:
    """Upstream stores image_assets as a JSON string mapping section
    names to URL lists. We append those as inline-markdown image links
    rather than downloading + base64-embedding (keeps bundle size sane;
    the worker can re-resolve the URLs at trial time)."""
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchMultimodalAdapter().convert_instance(inst, out_dir=tmp_path)
    md = (tmp_path / "instruction.md").read_text()
    assert "Tooltip is misaligned" in md
    assert "![problem_statement-0]" in md
    assert "user-images.githubusercontent.com" in md


def test_multimodal_uses_multimodal_task_id(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchMultimodalAdapter().convert_instance(inst, out_dir=tmp_path)
    cfg = TaskConfig.model_validate(
        tomllib.loads((tmp_path / "task.toml").read_text()),
    )
    assert cfg.task.id == "swe-bench-multimodal/vega__vega-lite-9001"


def test_multimodal_no_images_falls_back_to_plain(tmp_path: Path) -> None:
    rec = json.loads(FIXTURE.read_text())[0]
    rec["image_assets"] = ""
    inst = BenchmarkInstance(
        instance_id=rec["instance_id"], split="test", raw=rec,
    )
    SWEBenchMultimodalAdapter().convert_instance(inst, out_dir=tmp_path)
    md = (tmp_path / "instruction.md").read_text()
    assert "![" not in md
    assert "Tooltip is misaligned" in md
