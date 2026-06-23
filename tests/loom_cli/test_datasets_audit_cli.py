from __future__ import annotations

from typing import Any

import pytest

from loom_cli import datasets_cmd
from loom_cli.benchmark_readiness import BenchmarkReadinessItem


def _item() -> BenchmarkReadinessItem:
    return BenchmarkReadinessItem(
        id="fake-bench",
        display_name="Fake Bench",
        series="fake",
        adapter_status="available",
        manifest_status="registered",
        raw_task_count=1,
        valid_task_config_count=1,
        invalid_task_config_count=0,
        license_allowed_task_count=1,
        license_blocked_task_count=0,
        blocked_licenses=[],
        source_schemes=["hf"],
        materializer_status="available",
        smoke_status="unknown",
        readiness_state="runnable",
        blocker_reason=None,
    )


def test_audit_requires_db_url(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_DB_URL", raising=False)
    rc = datasets_cmd.dispatch(["audit", "--all"])
    assert rc == 2
    assert "db-url" in capsys.readouterr().err.lower()


def test_audit_requires_all_or_benchmark(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = datasets_cmd.dispatch(["audit", "--db-url", "postgresql://x/y"])
    assert rc == 2
    assert "--all" in capsys.readouterr().err


def test_audit_prints_json(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_audit(**kwargs: Any) -> list[BenchmarkReadinessItem]:
        assert kwargs["benchmark"] == "fake-bench"
        assert kwargs["db_url"] == "postgresql://x/y"
        return [_item()]

    monkeypatch.setattr(datasets_cmd, "run_readiness_audit", fake_run_audit)
    rc = datasets_cmd.dispatch([
        "audit",
        "fake-bench",
        "--db-url",
        "postgresql://x/y",
        "--json",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert '"id": "fake-bench"' in out
    assert '"readiness_state": "runnable"' in out


def test_audit_prints_table(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_audit(**_kwargs: Any) -> list[BenchmarkReadinessItem]:
        return [_item()]

    monkeypatch.setattr(datasets_cmd, "run_readiness_audit", fake_run_audit)
    rc = datasets_cmd.dispatch([
        "audit",
        "--all",
        "--db-url",
        "postgresql://x/y",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "READINESS" in out
    assert "fake-bench" in out
