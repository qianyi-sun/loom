from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom_cli import benchmark_readiness, datasets_cmd
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


def test_activate_rejects_a_non_tb21_alias(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    rc = datasets_cmd.dispatch(
        [
            "activate",
            "other-benchmark",
            "--profile",
            "terminal-bench-2@tb2.1-r6",
            "--audit-json",
            str(tmp_path / "audit.json"),
            "--db-url",
            "postgresql://x/y",
        ]
    )

    assert rc == 2
    assert "terminal-bench-2" in capsys.readouterr().err


def test_tb21_audit_json_requires_the_physical_profile(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    rc = datasets_cmd.dispatch(
        [
            "audit",
            "other-benchmark",
            "--tb21-audit-json",
            str(tmp_path / "audit.json"),
            "--db-url",
            "postgresql://x/y",
        ]
    )

    assert rc == 2
    assert "terminal-bench-2@tb2.1-r6" in capsys.readouterr().err


def test_audit_prints_json(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_audit(**kwargs: Any) -> list[BenchmarkReadinessItem]:
        assert kwargs["benchmark"] == "fake-bench"
        assert kwargs["db_url"] == "postgresql://x/y"
        return [_item()]

    monkeypatch.setattr(datasets_cmd, "run_readiness_audit", fake_run_audit)
    rc = datasets_cmd.dispatch(
        [
            "audit",
            "fake-bench",
            "--db-url",
            "postgresql://x/y",
            "--json",
        ]
    )
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
    rc = datasets_cmd.dispatch(
        [
            "audit",
            "--all",
            "--db-url",
            "postgresql://x/y",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "READINESS" in out
    assert "fake-bench" in out


def test_audit_verify_bundles_requires_minio(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("LOOM_MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("LOOM_MINIO_SECRET_KEY", raising=False)
    monkeypatch.delenv("LOOM_SVC_MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("LOOM_SVC_MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("LOOM_SVC_MINIO_SECRET_KEY", raising=False)

    rc = datasets_cmd.dispatch(
        [
            "audit",
            "--all",
            "--db-url",
            "postgresql://x/y",
            "--verify-bundles",
        ]
    )

    assert rc == 2
    assert "minio" in capsys.readouterr().err.lower()


def test_audit_verify_bundles_prints_summary_and_fails_on_missing(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stores: list[dict[str, object]] = []

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_run_audit(**_kwargs: Any) -> list[BenchmarkReadinessItem]:
        return [_item()]

    async def fake_bundle_audit(**kwargs: Any) -> Any:
        assert kwargs["benchmark"] is None
        assert kwargs["db_url"] == "postgresql://x/y"
        assert kwargs["object_store"] is not None
        return type(
            "BundlePresenceReport",
            (),
            {
                "s3_tasks": 2,
                "verified": 1,
                "missing": 1,
                "missing_sources": ["s3://loom-benchmarks/fake/missing/"],
            },
        )()

    monkeypatch.setattr(datasets_cmd, "run_readiness_audit", fake_run_audit)
    monkeypatch.setattr(datasets_cmd, "run_bundle_presence_audit", fake_bundle_audit, raising=False)
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)

    rc = datasets_cmd.dispatch(
        [
            "audit",
            "--all",
            "--db-url",
            "postgresql://x/y",
            "--verify-bundles",
            "--minio-endpoint",
            "http://target-minio:9000",
            "--minio-access-key",
            "target-access",
            "--minio-secret-key",
            "target-secret",
        ]
    )

    assert rc == 1
    assert stores == [
        {
            "endpoint_url": "http://target-minio:9000",
            "access_key": "target-access",
            "secret_key": "target-secret",
        }
    ]
    out = capsys.readouterr().out
    assert "bundle_presence" in out
    assert "s3_tasks=2" in out
    assert "missing=1" in out


@pytest.mark.asyncio
async def test_bundle_presence_audit_checks_internal_task_toml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeEngine:
        async def dispose(self) -> None:
            return None

    class FakeResult:
        def all(self) -> list[tuple[str, str]]:
            return [
                ("fake-bench/present", "s3://loom-benchmarks/fake/present/"),
                ("fake-bench/missing", "s3://loom-benchmarks/fake/missing/"),
                ("fake-bench/hf", "hf://PRHW/loom-benchmark-fake@rev/task/"),
            ]

    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def execute(self, _statement: object) -> FakeResult:
            return FakeResult()

    class FakeObjectStore:
        async def get_object(self, *, bucket: str, key: str) -> bytes:
            if (bucket, key) == ("loom-benchmarks", "fake/present/task.toml"):
                return b"task"
            raise KeyError(f"s3://{bucket}/{key}")

    monkeypatch.setattr(
        benchmark_readiness,
        "create_async_engine",
        lambda _db_url: FakeEngine(),
    )
    monkeypatch.setattr(
        benchmark_readiness,
        "async_sessionmaker",
        lambda *_args, **_kwargs: lambda: FakeSession(),
    )

    report = await benchmark_readiness.run_bundle_presence_audit(
        db_url="postgresql://x/y",
        object_store=FakeObjectStore(),
    )

    assert report.s3_tasks == 2
    assert report.verified == 1
    assert report.missing == 1
    assert report.missing_sources == ["s3://loom-benchmarks/fake/missing/"]
