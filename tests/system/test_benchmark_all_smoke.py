"""Unified all-benchmark opt-in end-to-end smoke (issue #806).

For every registered benchmark, submit 1-3 representative tasks
through the real Loom stack (docker-compose test rig), and assert
each trial reaches `succeeded`. Validates the adapter surface end
to end — not scores — so a single new adapter regression will fail
this smoke without needing per-benchmark tests.

Opt-in only. Set LOOM_RUN_ALL_BENCHMARKS_SMOKE=1 to run. Wall-clock
budget on a warm cache is ~45-60 min; some benchmarks pull multi-GB
container images on first run.

Pattern mirrors ``tests/system/test_benchmark_humaneval_smoke.py``
and ``tests/system/test_benchmark_swe_bench_smoke.py``: the adapter's
``list_instances`` is monkey-patched to yield from the local fixture
(no upstream fetch), ``run_import`` drops the rows into the DB, then
trials are posted through the control plane and polled until they
land in a terminal state.
"""

from __future__ import annotations

import importlib
import json
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import pytest

from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.import_cmd import run_import

_FIXTURE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures"
)

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_ALL_BENCHMARKS_SMOKE") != "1",
    reason="set LOOM_RUN_ALL_BENCHMARKS_SMOKE=1 to run the "
    "all-benchmark opt-in smoke",
)


@dataclass(frozen=True)
class BenchmarkCase:
    """One benchmark's slot in the all-benchmark smoke.

    ``benchmark_id`` is the name passed to ``run_import`` AND used as
    the trial task_id prefix (``f"{benchmark_id}/{instance_id}"``).
    ``adapter_import_path`` + ``adapter_class_name`` locate the class
    whose ``list_instances`` gets monkey-patched. ``fixture_path`` is
    relative to ``_FIXTURE_ROOT``. ``instance_ids`` names the records
    to submit — the fixture is filtered to match, so partial fixtures
    are fine as long as they contain these ids under ``id_field``.
    """

    benchmark_id: str
    adapter_import_path: str
    adapter_class_name: str
    fixture_path: str
    instance_ids: tuple[str, ...]
    id_field: str = "task_id"
    timeout_sec: int = 600
    marks: tuple[pytest.MarkDecorator, ...] = field(default_factory=tuple)


CASES: tuple[BenchmarkCase, ...] = (
    BenchmarkCase(
        benchmark_id="humaneval",
        adapter_import_path="loom_benchmarks.adapters.humaneval",
        adapter_class_name="HumanEvalAdapter",
        fixture_path="humaneval/sample.json",
        instance_ids=("HumanEval/0", "HumanEval/1"),
        id_field="task_id",
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="mbpp",
        adapter_import_path="loom_benchmarks.adapters.mbpp",
        adapter_class_name="MBPPAdapter",
        fixture_path="mbpp/sample.json",
        instance_ids=("11",),
        id_field="task_id",
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="bfcl",
        adapter_import_path="loom_benchmarks.adapters.bfcl",
        adapter_class_name="BFCLAdapter",
        fixture_path="bfcl/sample.json",
        instance_ids=("simple_0",),
        id_field="id",
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="livecodebench",
        adapter_import_path="loom_benchmarks.adapters.livecodebench",
        adapter_class_name="LiveCodeBenchAdapter",
        fixture_path="livecodebench/sample.json",
        instance_ids=("lcb-9001",),
        id_field="question_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="gaia",
        adapter_import_path="loom_benchmarks.adapters.gaia",
        adapter_class_name="GAIAAdapter",
        fixture_path="gaia/sample.json",
        instance_ids=("c61d22de-5f6c-4958-a7f6-5e9707bd3466",),
        id_field="task_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="osworld",
        adapter_import_path="loom_benchmarks.adapters.osworld",
        adapter_class_name="OSWorldAdapter",
        fixture_path="osworld/sample.json",
        instance_ids=("8849e9da-d935-46bb-9bef-d3204c1f1e3c",),
        id_field="id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="webarena",
        adapter_import_path="loom_benchmarks.adapters.webarena",
        adapter_class_name="WebArenaAdapter",
        fixture_path="webarena/sample.json",
        instance_ids=("42",),
        id_field="task_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="swe-bench",
        adapter_import_path="loom_benchmarks.adapters.swe_bench",
        adapter_class_name="SWEBenchAdapter",
        fixture_path="swe_bench/sample.json",
        instance_ids=("astropy__astropy-12907",),
        id_field="instance_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="swe-bench-verified",
        adapter_import_path="loom_benchmarks.adapters.swe_bench_verified",
        adapter_class_name="SWEBenchVerifiedAdapter",
        fixture_path="swe_bench_verified/sample.json",
        instance_ids=("django__django-12345",),
        id_field="instance_id",
        timeout_sec=1800,
    ),
    BenchmarkCase(
        benchmark_id="swe-bench-multimodal",
        adapter_import_path="loom_benchmarks.adapters.swe_bench_multimodal",
        adapter_class_name="SWEBenchMultimodalAdapter",
        fixture_path="swe_bench_multimodal/sample.json",
        instance_ids=("vega__vega-lite-9001",),
        id_field="instance_id",
        timeout_sec=1800,
    ),
)


def _load_adapter_class(case: BenchmarkCase) -> type:
    module = importlib.import_module(case.adapter_import_path)
    return getattr(module, case.adapter_class_name)


def _load_fixture_rows(case: BenchmarkCase) -> list[dict[str, object]]:
    path = _FIXTURE_ROOT / case.fixture_path
    rows = json.loads(path.read_text())
    wanted = set(case.instance_ids)
    # Some fixtures store the id as int (mbpp task_id, webarena task_id);
    # instance_ids in the case is str, so normalize both sides.
    filtered = [r for r in rows if str(r.get(case.id_field)) in wanted]
    missing = wanted - {str(r.get(case.id_field)) for r in filtered}
    assert not missing, (
        f"{case.benchmark_id}: fixture {path} missing instance ids: "
        f"{sorted(missing)}"
    )
    return filtered


@pytest.mark.parametrize(
    "case",
    [pytest.param(c, id=c.benchmark_id, marks=c.marks) for c in CASES],
)
async def test_benchmark_smoke(
    case: BenchmarkCase,
    compose_stack: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmarks.base import BenchmarkInstance

    adapter_cls = _load_adapter_class(case)
    rows = _load_fixture_rows(case)

    def _fake_list(
        self: object, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in rows:
            yield BenchmarkInstance(
                instance_id=str(r[case.id_field]),
                split=split,
                raw=r,
            )

    monkeypatch.setattr(adapter_cls, "list_instances", _fake_list)
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    minio_store = MinioObjectStore(
        endpoint_url=compose_stack["minio"],
        access_key=compose_stack["minio_access_key"],
        secret_key=compose_stack["minio_secret_key"],
    )
    # Idempotent: the humaneval/swe-bench smokes create the same
    # bucket in the same session — subsequent create_bucket calls
    # against MinIO succeed as no-op for the same-region owner.
    minio_store._client.create_bucket(Bucket="loom-benchmarks")

    stats = await run_import(
        benchmark=case.benchmark_id,
        db_url=compose_stack["db_url"],
        object_store=minio_store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        limit=len(rows),
        imported_by="all-benchmarks-smoke",
    )
    assert stats["converted"] == len(rows), (
        f"{case.benchmark_id}: expected {len(rows)} converted, "
        f"got {stats}"
    )

    cp = compose_stack["control_plane"]
    token = compose_stack["team_token"]

    submitted: list[str] = []
    for instance_id in case.instance_ids:
        r = httpx.post(
            f"{cp}/trials",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "task_id": f"{case.benchmark_id}/{instance_id}",
                "config": {},
            },
            timeout=10,
        )
        assert r.status_code == 201, (
            f"{case.benchmark_id}/{instance_id}: {r.status_code} {r.text}"
        )
        submitted.append(r.json()["trial_id"])

    deadline = time.time() + case.timeout_sec
    pending = set(submitted)
    final: dict[str, str] = {}
    poll_interval = 5.0 if case.timeout_sec >= 900 else 2.0
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
            time.sleep(poll_interval)

    assert not pending, (
        f"{case.benchmark_id}: timed out waiting for "
        f"{pending}; final={final}"
    )
    assert all(s == "succeeded" for s in final.values()), (
        f"{case.benchmark_id}: non-success terminal states: {final}"
    )
