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


def test_tb21_audit_json_writes_activation_evidence(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from loom_benchmark_tool.audit_cmd import AuditResult

    class FakeObjectStore:
        def __init__(self, **_kwargs: object) -> None:
            pass

    class FakeEngine:
        async def dispose(self) -> None:
            pass

    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *_args: object) -> None:
            pass

    async def fake_audit(_session: object, *, object_store: object) -> AuditResult:
        assert isinstance(object_store, FakeObjectStore)
        return AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=89,
            private_workspace_isolation=True,
            snapshot_id="sha256:" + "a" * 64,
        )

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.audit_cmd.audit_tb21_profile", fake_audit)
    monkeypatch.setattr(
        "sqlalchemy.ext.asyncio.create_async_engine",
        lambda _db_url: FakeEngine(),
    )
    monkeypatch.setattr(
        "sqlalchemy.ext.asyncio.async_sessionmaker",
        lambda *_args, **_kwargs: lambda: FakeSession(),
    )
    evidence = tmp_path / "tb21-audit.json"

    rc = datasets_cmd.dispatch(
        [
            "audit",
            "terminal-bench-2@tb2.1-r6",
            "--tb21-audit-json",
            str(evidence),
            "--db-url",
            "postgresql://x/y",
            "--minio-endpoint",
            "http://minio:9000",
            "--minio-access-key",
            "access",
            "--minio-secret-key",
            "secret",
        ],
    )

    assert rc == 0
    assert '"verified_bundles": 89' in evidence.read_text()
    assert "verified_bundles=89" in capsys.readouterr().out


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
    from loom_cli.benchmark_readiness import (
        BundlePresenceReport,
        BundleVerificationFailure,
    )

    stores: list[dict[str, object]] = []

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_run_audit(**_kwargs: Any) -> list[BenchmarkReadinessItem]:
        return [_item()]

    async def fake_bundle_audit(**kwargs: Any) -> BundlePresenceReport:
        assert kwargs["benchmark"] is None
        assert kwargs["db_url"] == "postgresql://x/y"
        assert kwargs["object_store"] is not None
        return BundlePresenceReport(
            s3_tasks=2,
            verified=1,
            failures=(
                BundleVerificationFailure(
                    task_id="fake-bench/missing",
                    source="s3://loom-benchmarks/fake/missing/",
                    reason="download_error",
                ),
            ),
        )

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
    assert "bundle_verification" in out
    assert "s3_tasks=2" in out
    assert "failed=1" in out
    assert "verification_errors=1" in out


def test_audit_verify_bundles_fails_on_checksum_mismatch(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_benchmark_tool.audit_cmd import AuditResult
    from loom_cli.benchmark_readiness import (
        BundlePresenceReport,
        BundleVerificationFailure,
    )

    class FakeObjectStore:
        def __init__(self, **_kwargs: object) -> None:
            pass

    async def fake_run_audit(**_kwargs: Any) -> list[AuditResult]:
        return []

    async def fake_bundle_audit(**_kwargs: Any) -> BundlePresenceReport:
        return BundlePresenceReport(
            s3_tasks=2,
            verified=1,
            failures=(
                BundleVerificationFailure(
                    task_id="fake-bench/mismatch",
                    source="s3://loom-benchmarks/fake/mismatch/",
                    reason="checksum_mismatch",
                    expected_checksum="a" * 64,
                    actual_checksum="b" * 64,
                ),
            ),
        )

    monkeypatch.setattr(datasets_cmd, "run_readiness_audit", fake_run_audit)
    monkeypatch.setattr(datasets_cmd, "run_bundle_presence_audit", fake_bundle_audit)
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)

    rc = datasets_cmd.dispatch(
        [
            "audit",
            "fake-bench",
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
    out = capsys.readouterr().out
    assert "bundle_verification" in out
    assert "failed=1" in out
    assert "checksum_mismatches=1" in out
    assert "checksum_mismatch fake-bench/mismatch" in out


@pytest.mark.asyncio
async def test_bundle_audit_hashes_complete_bundles_and_includes_tasksets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom.trajectory.storage import FakeObjectStore

    class FakeEngine:
        async def dispose(self) -> None:
            return None

    class FakeResult:
        def all(self) -> list[tuple[str, str, str, str | None, str | None]]:
            return [
                (
                    "fake-bench/match",
                    "052bb32821a047eda3588caba3c91a4448dca7d338f219add0e91d98e71e975d",
                    "s3://loom-benchmarks/fake/match/",
                    "fake-bench",
                    None,
                ),
                (
                    "fake-bench/mismatch",
                    "052bb32821a047eda3588caba3c91a4448dca7d338f219add0e91d98e71e975d",
                    "s3://loom-benchmarks/fake/mismatch/",
                    "fake-bench",
                    None,
                ),
                (
                    "task-set/task",
                    "2e6df2a07b91ca4c82475a570ddfe571f97db243859d86f317f67320d204a532",
                    "s3://loom-benchmarks/task-set/task/",
                    None,
                    "task-set",
                ),
            ]

    class FakeSession:
        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def execute(self, _statement: object) -> FakeResult:
            return FakeResult()

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

    store = FakeObjectStore(
        objects={
            ("loom-benchmarks", "fake/match/instruction.md"): b"alpha\n",
            ("loom-benchmarks", "fake/match/task.toml"): b"task\n",
            ("loom-benchmarks", "fake/mismatch/instruction.md"): b"beta\n",
            ("loom-benchmarks", "fake/mismatch/task.toml"): b"task\n",
            ("loom-benchmarks", "task-set/task/task.toml"): b"taskset\n",
        },
    )
    report = await benchmark_readiness.run_bundle_presence_audit(
        db_url="postgresql://x/y",
        object_store=store,
    )

    assert report.s3_tasks == 3
    assert report.verified == 2
    assert report.failed == 1
    assert report.checksum_mismatches == 1
    assert report.verification_errors == 0
    assert [failure.to_dict() for failure in report.failures] == [
        {
            "actual_checksum": (
                "5b1601fabcc6a8facb84b16ae5a6c5fcdd1daa776f49eaaf7638a56bdc79c59c"
            ),
            "expected_checksum": (
                "052bb32821a047eda3588caba3c91a4448dca7d338f219add0e91d98e71e975d"
            ),
            "reason": "checksum_mismatch",
            "source": "s3://loom-benchmarks/fake/mismatch/",
            "task_id": "fake-bench/mismatch",
        }
    ]
