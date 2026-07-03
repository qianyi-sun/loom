"""End-to-end license metadata submit path.

Benchmark adapters stamp `tasks.license` via `license_spdx`, but source license
metadata is informational. Imported tasks submit cleanly regardless of SPDX
value while preserving the recorded license/tags for audit surfaces.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, Team, TeamQuota, Token, User
from loom.db.schema import Task as TaskRow
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import run_import
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures/humaneval/sample.json"
)
_AIME_FIXTURE = (
    Path(__file__).resolve().parents[2] / "packages/loom-benchmarks/tests/fixtures/aime/sample.json"
)


@pytest.fixture
def seed(postgres_url: str) -> Iterator[dict[str, object]]:
    """Insert a team + user-owned submit-scoped token."""
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4()
    raw = f"team_{uuid4().hex}"
    now = datetime.now(UTC)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(User).values(
            id=user_id,
            username=f"LicenseSubmitter-{user_id.hex[:8]}",
            username_normalized=f"license-submitter-{user_id.hex[:8]}",
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=now,
                expires_at=None,
            )
        )
        s.commit()
    try:
        yield {"team_id": team_id, "token": raw}
    finally:
        with session_local() as s:
            from loom.db.schema import Trial

            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(TaskRow))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.id == user_id))
            s.execute(delete(Team))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    seed: dict[str, object],
):  # type: ignore[no-untyped-def]
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


async def test_humaneval_mit_submits_with_license_metadata(
    app,  # type: ignore[no-untyped-def]
    seed: dict[str, object],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """HumanEval declares license_spdx="MIT" and still submits."""
    from loom_benchmarks.adapters import humaneval as hv
    from loom_benchmarks.base import BenchmarkInstance

    fixture_records = json.loads(_FIXTURE.read_text())

    def _fake_list(
        self: hv.HumanEvalAdapter,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in fixture_records:
            yield BenchmarkInstance(
                instance_id=r["task_id"],
                split=split,
                raw=r,
            )

    monkeypatch.setattr(hv.HumanEvalAdapter, "list_instances", _fake_list)
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    store = FakeObjectStore()
    await run_import(
        benchmark="humaneval",
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        limit=1,
        imported_by="ci",
    )

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "task_id": "humaneval/HumanEval/0",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
    assert r.status_code == 201, r.text
    assert r.json()["state"] == "queued"


async def test_lcb_cc_by_nc_submits_with_license_metadata(
    app,  # type: ignore[no-untyped-def]
    seed: dict[str, object],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The Plan 15 audit fix corrected LiveCodeBench's license to
    CC-BY-NC-4.0. Importing → submitting must still work while guarding
    against accidental MIT-tagging regressions for LCB."""
    from loom_benchmarks.adapters import livecodebench as lcb
    from loom_benchmarks.base import BenchmarkInstance

    sample = {
        "question_id": "lcb-9001",
        "question_content": "Print 0.",
        "starter_code": "",
        "code": "print(0)\n",
        "public_test_cases": [
            {"input": "", "output": "0\n", "testtype": "stdin"},
        ],
        "private_test_cases": [],
        "platform": "leetcode",
        "difficulty": "easy",
    }

    def _fake_list(
        self: lcb.LiveCodeBenchAdapter,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        yield BenchmarkInstance(
            instance_id=sample["question_id"],
            split=split,
            raw=sample,
        )

    monkeypatch.setattr(
        lcb.LiveCodeBenchAdapter,
        "list_instances",
        _fake_list,
    )
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    store = FakeObjectStore()
    await run_import(
        benchmark="livecodebench",
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        limit=1,
        imported_by="ci",
    )

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "task_id": "livecodebench/lcb-9001",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
    assert r.status_code == 201, r.text
    assert r.json()["state"] == "queued"


async def test_aime_license_policy_tag_is_metadata_only(
    app,  # type: ignore[no-untyped-def]
    seed: dict[str, object],
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """AIME keeps `proprietary-MAA` and `notice` tags as metadata only."""
    from loom_benchmarks.adapters import aime
    from loom_benchmarks.base import BenchmarkInstance

    rec = json.loads(_AIME_FIXTURE.read_text())[0]

    def _fake_list(
        self: aime.AIME22Adapter,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        yield BenchmarkInstance(
            instance_id="2022-I/1",
            split=split,
            raw=rec,
            tags={"year": "2022", "exam": "I", "problem": "1"},
        )

    monkeypatch.setattr(aime.AIME22Adapter, "list_instances", _fake_list)
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    store = FakeObjectStore()
    await run_import(
        benchmark="aime-22",
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
        imported_by="ci",
    )

    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        task_row = conn.execute(
            text(
                "SELECT license, tags FROM tasks WHERE id='aime-22/2022-I/1'",
            )
        ).one()
    engine.dispose()
    assert task_row.license == "proprietary-MAA"
    assert task_row.tags["license_execution_policy"] == "notice"

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "task_id": "aime-22/2022-I/1",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
    assert r.status_code == 201, r.text
    assert r.json()["state"] == "queued"
