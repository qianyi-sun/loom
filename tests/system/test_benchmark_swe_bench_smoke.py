"""SWE-Bench Verified end-to-end smoke (Plan 16 Task 4 / spec §14.4).

Opt-in via LOOM_RUN_SWE_BENCH_SMOKE=1 — each per-instance Docker
image is ~5 GB and pulling 3 is a 15 GB+ disk + bandwidth cost.

Uses the same monkey-patch pattern as the HumanEval smoke: the
adapter's `list_instances` reads the local fixture instead of HF,
import → submit → wait for terminal state.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.import_cmd import run_import

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures/swe_bench_verified/sample.json"
)


@pytest.mark.swe_bench_smoke
@pytest.mark.skipif(
    os.environ.get("LOOM_RUN_SWE_BENCH_SMOKE") != "1",
    reason="set LOOM_RUN_SWE_BENCH_SMOKE=1 to pull ~5GB per-instance images",
)
async def test_swe_bench_verified_three_instances(
    compose_stack: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmarks.adapters import swe_bench_verified as sbv
    from loom_benchmarks.base import BenchmarkInstance

    base = json.loads(_FIXTURE.read_text())[0]
    instances: list[dict[str, object]] = []
    for i in range(3):
        rec = dict(base)
        rec["instance_id"] = f"smoke__smoke-{i}"
        instances.append(rec)

    def _fake_list(
        self: sbv.SWEBenchVerifiedAdapter, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in instances:
            yield BenchmarkInstance(
                instance_id=str(r["instance_id"]), split=split, raw=r,
            )

    monkeypatch.setattr(
        sbv.SWEBenchVerifiedAdapter, "list_instances", _fake_list,
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    minio_store = MinioObjectStore(
        endpoint_url=compose_stack["minio"],
        access_key=compose_stack["minio_access_key"],
        secret_key=compose_stack["minio_secret_key"],
    )
    minio_store._client.create_bucket(Bucket="loom-benchmarks")

    # PR-1 (series): the `swe-bench-verified` adapter slug is gone —
    # verified status is now a tag on the unified `swe-bench` benchmark.
    # The smoke imports the parent so the same code path exercises
    # the merged-tag list_instances + convert path. We don't filter by
    # tag here because run_import doesn't filter — it converts the
    # first `limit` rows regardless.
    stats = await run_import(
        benchmark="swe-bench",
        db_url=compose_stack["db_url"],
        object_store=minio_store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        limit=3,
        imported_by="smoke",
    )
    assert stats["converted"] == 3

    cp = compose_stack["control_plane"]
    token = compose_stack["team_token"]

    submitted: list[str] = []
    for i in range(3):
        r = httpx.post(
            f"{cp}/trials",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "task_id": f"swe-bench-verified/smoke__smoke-{i}",
                "config": {},
            },
            timeout=10,
        )
        assert r.status_code == 201, r.text
        submitted.append(r.json()["trial_id"])

    deadline = time.time() + 1800  # 30 min for image pulls + solves
    pending = set(submitted)
    final: dict[str, str] = {}
    while pending and time.time() < deadline:
        for trial_id in list(pending):
            r = httpx.get(
                f"{cp}/trials/{trial_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            assert r.status_code == 200, r.text
            state = r.json()["state"]
            if state in ("succeeded", "failed", "cancelled"):
                final[trial_id] = state
                pending.discard(trial_id)
        if pending:
            time.sleep(5.0)

    assert not pending, f"timed out waiting for: {pending}; final={final}"
    assert all(s == "succeeded" for s in final.values()), final
