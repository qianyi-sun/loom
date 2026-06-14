"""SWE-Bench (full split). Spec §5.2 row 2.

Shares the conversion path with SWE-Bench Verified — only the upstream
locator + name differ. We subclass to inherit `convert_instance`,
then override `list_instances` so each row is tagged with whether it's
also in the Verified subset.

PR-1 series/tags rework: the Verified subset is no longer a separate
benchmark. Operators who want to run only verified problems use the
SPA's tag filter (`verified=true`) on this benchmark. This eliminates
the replication footgun where group-selecting the SWE-Bench series
would run the same 500 instances twice.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import datasets  # type: ignore[import-untyped]

from loom_benchmarks.adapters.swe_bench_verified import SWEBenchVerifiedAdapter
from loom_benchmarks.base import BenchmarkInstance, UpstreamSource


class SWEBenchAdapter(SWEBenchVerifiedAdapter):
    name = "swe-bench"
    display_name = "SWE-Bench (full)"
    series = "swe-bench"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="princeton-nlp/SWE-bench",
        revision=None,
    )

    # Cache the Verified instance-id set across list_instances calls so
    # republishing different splits doesn't reload the same dataset.
    _VERIFIED_LOCATOR = "princeton-nlp/SWE-bench_Verified"

    def _load_verified_ids(self, source_dir: Path, split: str) -> frozenset[str]:
        """Return the set of instance_ids in the Verified curated
        subset for `split`. Used to stamp the `verified` tag on each
        full-split row."""
        ds = datasets.load_dataset(
            self._VERIFIED_LOCATOR, cache_dir=str(source_dir),
        )
        if split not in ds:
            return frozenset()
        return frozenset(
            str(cast(dict[str, Any], dict(rec))["instance_id"])
            for rec in ds[split]
        )

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        verified_ids = self._load_verified_ids(source_dir, split)
        ds = datasets.load_dataset(
            self.upstream_source.locator, cache_dir=str(source_dir),
        )[split]
        for record in ds:
            rec = cast(dict[str, Any], dict(record))
            instance_id = str(rec["instance_id"])
            repo = str(rec.get("repo") or "").strip()
            tags = {
                "verified": "true" if instance_id in verified_ids else "false",
            }
            if repo:
                tags["repo"] = repo
            yield BenchmarkInstance(
                instance_id=instance_id,
                split=split,
                raw=rec,
                tags=tags,
            )
