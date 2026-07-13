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
    whose ``list_instances`` gets monkey-patched.

    Two record-sourcing modes:

    * ``fixture_path`` (relative to ``_FIXTURE_ROOT``): load JSON rows,
      filter by ``str(row[id_field]) in instance_ids``. Row order
      matches ``instance_ids`` after filtering. Used when the fixture
      row's raw id-field is exactly the adapter's ``instance_id``.
    * ``raw_records``: use these dicts directly, paired positionally
      with ``instance_ids``. Used when the adapter's ``list_instances``
      derives the instance_id from more than the raw id-field (e.g.
      AIME parses ``(year, exam, num)`` from a URL, then joins into
      ``"{year}-{exam}/{num}"``). Also used to synthesize test rows
      per year without maintaining a multi-year fixture file.

    Exactly one of ``fixture_path`` / ``raw_records`` is required.
    ``id_field`` is only consulted when ``fixture_path`` is set.
    """

    benchmark_id: str
    adapter_import_path: str
    adapter_class_name: str
    instance_ids: tuple[str, ...]
    fixture_path: str | None = None
    raw_records: tuple[dict[str, object], ...] | None = None
    id_field: str = "task_id"
    timeout_sec: int = 600
    marks: tuple[pytest.MarkDecorator, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if (self.fixture_path is None) == (self.raw_records is None):
            raise ValueError(
                f"{self.benchmark_id}: set exactly one of fixture_path "
                "or raw_records",
            )
        if self.raw_records is not None and len(self.raw_records) != len(
            self.instance_ids,
        ):
            raise ValueError(
                f"{self.benchmark_id}: raw_records length "
                f"({len(self.raw_records)}) must match instance_ids length "
                f"({len(self.instance_ids)}) — pairs are positional",
            )


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
    # AIME: the adapter derives instance_id from a URL parse (aime-22..24)
    # or a `(problem_idx, exam)` composite (aime-25), so the raw fixture
    # id-field doesn't map. Synthesize one row per year in
    # ``raw_records`` and let the fake list_instances yield with the
    # already-computed instance_id.
    BenchmarkCase(
        benchmark_id="aime-22",
        adapter_import_path="loom_benchmarks.adapters.aime",
        adapter_class_name="AIME22Adapter",
        instance_ids=("2022-I/1",),
        raw_records=(
            {
                "problem": (
                    "Find the number of ordered pairs of positive "
                    "integers (a, b) such that 1/a + 1/b = 1/2022."
                ),
                "answer": "45",
                "url": (
                    "https://artofproblemsolving.com/wiki/index.php/"
                    "2022_AIME_I_Problems/Problem_1"
                ),
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="aime-23",
        adapter_import_path="loom_benchmarks.adapters.aime",
        adapter_class_name="AIME23Adapter",
        instance_ids=("2023-I/1",),
        raw_records=(
            {
                "problem": (
                    "Five men and nine women stand equally spaced around "
                    "a circle in random order. Compute the probability "
                    "that every man stands diametrically opposite a "
                    "woman."
                ),
                "answer": "191",
                "url": (
                    "https://artofproblemsolving.com/wiki/index.php/"
                    "2023_AIME_I_Problems/Problem_1"
                ),
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="aime-24",
        adapter_import_path="loom_benchmarks.adapters.aime",
        adapter_class_name="AIME24Adapter",
        instance_ids=("2024-I/1",),
        raw_records=(
            {
                "problem": (
                    "Every morning Aya goes for a 9-kilometer walk. "
                    "Compute the total time she spends walking."
                ),
                "answer": "204",
                "url": (
                    "https://artofproblemsolving.com/wiki/index.php/"
                    "2024_AIME_I_Problems/Problem_1"
                ),
            },
        ),
        timeout_sec=600,
    ),
    BenchmarkCase(
        benchmark_id="aime-25",
        adapter_import_path="loom_benchmarks.adapters.aime_2025",
        adapter_class_name="AIME25Adapter",
        instance_ids=("2025-I/1",),
        raw_records=(
            {
                "problem": (
                    "Find the sum of all integer bases b > 9 for which "
                    "17_b is a divisor of 97_b."
                ),
                "answer": "70",
                "exam": "I",
                "problem_idx": "1",
            },
        ),
        timeout_sec=600,
    ),
)


def _load_adapter_class(case: BenchmarkCase) -> type:
    module = importlib.import_module(case.adapter_import_path)
    return getattr(module, case.adapter_class_name)


def _resolve_rows(
    case: BenchmarkCase,
) -> list[tuple[str, dict[str, object]]]:
    """Return ``[(instance_id, raw), ...]`` in the order the smoke
    should submit trials."""
    if case.raw_records is not None:
        return list(zip(case.instance_ids, case.raw_records, strict=True))

    assert case.fixture_path is not None
    path = _FIXTURE_ROOT / case.fixture_path
    rows = json.loads(path.read_text())
    wanted = set(case.instance_ids)
    # Some fixtures store the id as int (mbpp task_id, webarena task_id);
    # instance_ids in the case is str, so normalize both sides.
    by_id: dict[str, dict[str, object]] = {}
    for r in rows:
        key = str(r.get(case.id_field))
        if key in wanted:
            by_id[key] = r
    missing = wanted - set(by_id)
    assert not missing, (
        f"{case.benchmark_id}: fixture {path} missing instance ids: "
        f"{sorted(missing)}"
    )
    return [(iid, by_id[iid]) for iid in case.instance_ids]


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
    rows = _resolve_rows(case)

    def _fake_list(
        self: object, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for instance_id, raw in rows:
            yield BenchmarkInstance(
                instance_id=instance_id,
                split=split,
                raw=raw,
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
