"""Full subcommand surface: list (filters + --json), show, install, refresh."""

from __future__ import annotations

import json

import pytest

from loom_cli.datasets_cmd import dispatch
from loom_cli.discovery import DatasetEntry


@pytest.fixture()
def patched_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_builtin() -> list[DatasetEntry]:
        return [DatasetEntry(
            slug="humaneval", source="builtin", display_name="HumanEval",
            license_spdx="MIT", license_url="", task_count=164,
            status="installed", available_pip_spec=None,
            entry_point="loom_benchmarks.adapters.humaneval:HumanEvalAdapter",
        )]

    def fake_catalog(*, url: str | None) -> list[DatasetEntry]:
        return [DatasetEntry(
            slug="terminal-bench-2", source="catalog",
            display_name="Terminal-Bench 2.0", license_spdx="Apache-2.0",
            license_url="", task_count=None, status="available",
            available_pip_spec="loom-benchmark-terminal-bench-2",
            entry_point=None,
        )]

    def fake_remote(
        *, server_url: str | None, token: str | None,
    ) -> list[DatasetEntry]:
        return [DatasetEntry(
            slug="custom-rl-bench", source="remote",
            display_name="Custom RL Bench", license_spdx="proprietary",
            license_url="", task_count=None, status="remote-only",
            available_pip_spec=None, entry_point=None,
        )]

    monkeypatch.setattr("loom_cli.builtin.load_builtin_entries", fake_builtin)
    monkeypatch.setattr("loom_cli.catalog.load_catalog_entries", fake_catalog)
    monkeypatch.setattr("loom_cli.remote.load_remote_entries", fake_remote)


def test_list_default_unions_all_three(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "terminal-bench-2" in out
    assert "custom-rl-bench" in out


def test_list_installed_filter(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--installed"])
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "terminal-bench-2" not in out
    assert "custom-rl-bench" not in out


def test_list_available_filter(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--available"])
    out = capsys.readouterr().out
    assert "terminal-bench-2" in out
    assert "humaneval" not in out


def test_list_remote_filter(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--remote"])
    out = capsys.readouterr().out
    assert "custom-rl-bench" in out
    assert "humaneval" not in out


def test_list_json_emits_machine_readable(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--json"])
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["count"] == 3
    assert {i["slug"] for i in parsed["items"]} == {
        "humaneval", "terminal-bench-2", "custom-rl-bench",
    }


def test_show_prints_full_details(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["show", "humaneval"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "MIT" in out
    assert "164" in out
    assert "loom_benchmarks.adapters.humaneval:HumanEvalAdapter" in out


def test_show_unknown_slug_exits_nonzero(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["show", "no-such-thing"])
    assert rc == 2
    assert "not found" in capsys.readouterr().err.lower()


def test_install_invokes_install_dataset(
    patched_discovery: None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: dict[str, str] = {}

    def fake_install(*, pip_spec: str) -> str:
        seen["spec"] = pip_spec
        return "Successfully installed ...\n"

    monkeypatch.setattr("loom_cli.install.install_dataset", fake_install)
    rc = dispatch(["install", "terminal-bench-2"])
    assert rc == 0
    assert seen["spec"] == "loom-benchmark-terminal-bench-2"


def test_install_unknown_slug(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["install", "no-such-thing"])
    assert rc == 2
    assert "not found" in capsys.readouterr().err.lower()


def test_refresh_catalog_purges_cache(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    called = {"n": 0}

    def fake_purge() -> None:
        called["n"] += 1

    monkeypatch.setattr("loom_cli.catalog.purge_catalog_cache", fake_purge)
    rc = dispatch(["refresh-catalog"])
    assert rc == 0
    assert called["n"] == 1
    assert "purged" in capsys.readouterr().out.lower()


def test_provision_catalog_provision_invokes_provisioner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    class FakeCatalog:
        def __init__(self, db_url: str) -> None:
            captured.setdefault("catalogs", []).append(db_url)

    class FakeObjects:
        def __init__(self, **kwargs: object) -> None:
            captured.setdefault("stores", []).append(kwargs)

    async def fake_provision(**kwargs: object):
        from loom_cli.catalog_provision import ProvisionStats

        captured.update(kwargs)
        return ProvisionStats(
            ready_agents=2,
            ready_benchmarks=2,
            ready_tasks=3,
            source_objects=5,
            target_objects_uploaded=1,
            target_objects_skipped=4,
            target_objects_missing=0,
            bytes_uploaded=11,
            bytes_skipped=44,
        )

    monkeypatch.setattr(
        "loom_cli.catalog_provision.PostgresCatalogStore",
        FakeCatalog,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.Boto3CatalogObjectStore",
        FakeObjects,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.provision_ready_benchmark_catalog",
        fake_provision,
    )

    rc = dispatch([
        "provision-catalog",
        "--source-db-url",
        "postgresql://source/db",
        "--target-db-url",
        "postgresql://target/db",
        "--source-minio-endpoint",
        "http://source-minio:9000",
        "--source-minio-access-key",
        "source-access",
        "--source-minio-secret-key",
        "source-secret",
        "--target-minio-endpoint",
        "http://target-minio:9000",
        "--target-minio-access-key",
        "target-access",
        "--target-minio-secret-key",
        "target-secret",
        "--target-bucket",
        "loom-benchmarks",
        "--imported-by",
        "release:staging",
    ])

    assert rc == 0
    assert captured["catalogs"] == [
        "postgresql://source/db",
        "postgresql://target/db",
    ]
    assert captured["target_bucket"] == "loom-benchmarks"
    assert captured["imported_by"] == "release:staging"
    out = capsys.readouterr().out
    assert "ready_agents=2" in out
    assert "ready_benchmarks=2" in out
    assert "ready_tasks=3" in out
    assert "source_objects=5" in out
    assert "uploaded=1" in out
    assert "skipped=4" in out
    assert "missing=0" in out


def test_provision_catalog_provision_uses_service_target_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setenv("LOOM_CATALOG_SOURCE_DB_URL", "postgresql://source/db")
    monkeypatch.setenv("LOOM_CATALOG_SOURCE_MINIO_ENDPOINT", "http://source-minio:9000")
    monkeypatch.setenv("LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY", "source-access")
    monkeypatch.setenv("LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY", "source-secret")
    monkeypatch.delenv("LOOM_DB_URL", raising=False)
    monkeypatch.delenv("LOOM_MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("LOOM_MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("LOOM_MINIO_SECRET_KEY", raising=False)
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://target/db")
    monkeypatch.setenv("LOOM_SVC_MINIO_ENDPOINT", "http://target-minio:9000")
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "target-access")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "target-secret")

    class FakeCatalog:
        def __init__(self, db_url: str) -> None:
            captured.setdefault("catalogs", []).append(db_url)

    class FakeObjects:
        def __init__(self, **kwargs: object) -> None:
            captured.setdefault("stores", []).append(kwargs)

    async def fake_provision(**kwargs: object):
        from loom_cli.catalog_provision import ProvisionStats

        captured.update(kwargs)
        return ProvisionStats(
            ready_agents=2,
            ready_benchmarks=1,
            ready_tasks=100,
            source_objects=200,
            target_objects_uploaded=200,
            target_objects_skipped=0,
            target_objects_missing=0,
            bytes_uploaded=123,
            bytes_skipped=0,
        )

    monkeypatch.setattr(
        "loom_cli.catalog_provision.PostgresCatalogStore",
        FakeCatalog,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.Boto3CatalogObjectStore",
        FakeObjects,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.provision_ready_benchmark_catalog",
        fake_provision,
    )

    rc = dispatch(["provision-catalog"])

    assert rc == 0
    assert captured["catalogs"] == [
        "postgresql://source/db",
        "postgresql://target/db",
    ]
    assert captured["stores"] == [
        {
            "endpoint_url": "http://source-minio:9000",
            "access_key": "source-access",
            "secret_key": "source-secret",
        },
        {
            "endpoint_url": "http://target-minio:9000",
            "access_key": "target-access",
            "secret_key": "target-secret",
        },
    ]
    assert "ready_tasks=100" in capsys.readouterr().out


def test_register_uses_service_db_url_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.delenv("LOOM_DB_URL", raising=False)
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://target/db")

    async def fake_register(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "registered": 100,
            "legacy_placeholders": 0,
            "skipped": 0,
            "repo_id": "PRHW/loom-benchmark-skilllearnbench",
            "revision": "main",
        }

    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    rc = dispatch(["register", "skilllearnbench"])

    assert rc == 0
    assert captured["db_url"] == "postgresql://target/db"
    assert "registered=100" in capsys.readouterr().out


def test_register_db_url_precedence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: list[str] = []

    monkeypatch.setenv("LOOM_DB_URL", "postgresql://legacy/db")
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://service/db")

    async def fake_register(**kwargs: object) -> dict[str, object]:
        seen.append(str(kwargs["db_url"]))
        return {
            "registered": 1,
            "legacy_placeholders": 0,
            "skipped": 0,
            "repo_id": "PRHW/loom-benchmark-skilllearnbench",
            "revision": "main",
        }

    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    assert dispatch(["register", "skilllearnbench"]) == 0
    assert dispatch([
        "register",
        "skilllearnbench",
        "--db-url",
        "postgresql://flag/db",
    ]) == 0

    assert seen == ["postgresql://legacy/db", "postgresql://flag/db"]
    assert capsys.readouterr().out.count("registered=1") == 2


def test_register_mirror_to_object_store_passes_minio_target(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}
    stores: list[dict[str, object]] = []

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_register(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "registered": 100,
            "legacy_placeholders": 0,
            "skipped": 0,
            "mirrored": 100,
            "mirror_uploaded": 200,
            "mirror_skipped": 0,
            "repo_id": "PRHW/loom-benchmark-skilllearnbench",
            "revision": "7908700",
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    rc = dispatch([
        "register",
        "skilllearnbench",
        "--db-url",
        "postgresql://target/db",
        "--revision",
        "7908700",
        "--mirror-to-object-store",
        "--minio-endpoint",
        "http://target-minio:9000",
        "--minio-access-key",
        "target-access",
        "--minio-secret-key",
        "target-secret",
        "--bucket",
        "loom-benchmarks",
    ])

    assert rc == 0
    assert stores == [
        {
            "endpoint_url": "http://target-minio:9000",
            "access_key": "target-access",
            "secret_key": "target-secret",
        }
    ]
    assert captured["mirror_to_object_store"] is True
    assert captured["object_store"] is not None
    assert captured["bucket"] == "loom-benchmarks"
    out = capsys.readouterr().out
    assert "mirrored=100" in out
    assert "mirror_uploaded=200" in out


def test_verify_minio_env_precedence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stores: list[dict[str, object]] = []

    monkeypatch.setenv("LOOM_MINIO_ENDPOINT", "http://legacy-minio:9000")
    monkeypatch.setenv("LOOM_MINIO_ACCESS_KEY", "legacy-access")
    monkeypatch.setenv("LOOM_MINIO_SECRET_KEY", "legacy-secret")
    monkeypatch.setenv("LOOM_SVC_MINIO_ENDPOINT", "http://service-minio:9000")
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "service-access")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "service-secret")

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_verify(**kwargs: object) -> dict[str, object]:
        return {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "results": [{"passed": True}],
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.verify_cmd.run_verify", fake_verify)

    assert dispatch(["verify", "skilllearnbench", "--limit", "1"]) == 0
    assert dispatch([
        "verify",
        "skilllearnbench",
        "--limit",
        "1",
        "--minio-endpoint",
        "http://flag-minio:9000",
        "--minio-access-key",
        "flag-access",
        "--minio-secret-key",
        "flag-secret",
    ]) == 0

    assert stores == [
        {
            "endpoint_url": "http://legacy-minio:9000",
            "access_key": "legacy-access",
            "secret_key": "legacy-secret",
        },
        {
            "endpoint_url": "http://flag-minio:9000",
            "access_key": "flag-access",
            "secret_key": "flag-secret",
        },
    ]
    assert capsys.readouterr().out.count("passed=1") == 2


def test_import_passes_instance_ids_to_benchmark_tool(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            captured["store_kwargs"] = kwargs

    async def fake_run_import(**kwargs: object) -> dict[str, int]:
        captured.update(kwargs)
        return {"converted": 2, "warnings": 0}

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.import_cmd.run_import", fake_run_import)

    rc = dispatch([
        "import",
        "humaneval",
        "--db-url",
        "postgresql://loom:loom@db/loom",
        "--minio-endpoint",
        "http://minio:9000",
        "--minio-access-key",
        "access",
        "--minio-secret-key",
        "secret",
        "--instance-id",
        "HumanEval/1",
        "--instance-id",
        "HumanEval/0",
    ])

    assert rc == 0
    assert captured["instance_ids"] == {"HumanEval/0", "HumanEval/1"}
    assert "converted=2" in capsys.readouterr().out


def test_publish_passes_instance_ids_to_benchmark_tool(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    async def fake_run_publish(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "published": 1,
            "warnings": 0,
            "target": "hf",
            "repo_id": "fake-org/loom-benchmark-humaneval",
            "revision": "fake-rev",
        }

    monkeypatch.setattr("loom_benchmark_tool.publish_cmd.run_publish", fake_run_publish)

    rc = dispatch([
        "publish",
        "humaneval",
        "--hf-org",
        "fake-org",
        "--hf-token",
        "fake-token",
        "--instance-id",
        "HumanEval/1",
    ])

    assert rc == 0
    assert captured["instance_ids"] == {"HumanEval/1"}
    assert captured["target"] == "hf"
    assert "published=1" in capsys.readouterr().out


def test_publish_target_object_store_requires_minio_flags(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--target=object-store bails cleanly when MinIO creds are missing."""
    for var in (
        "LOOM_MINIO_ENDPOINT",
        "LOOM_MINIO_ACCESS_KEY",
        "LOOM_MINIO_SECRET_KEY",
        "LOOM_SVC_MINIO_ENDPOINT",
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
    ):
        monkeypatch.delenv(var, raising=False)

    rc = dispatch([
        "publish",
        "humaneval",
        "--target",
        "object-store",
    ])

    captured = capsys.readouterr()
    assert rc == 2
    assert "requires:" in captured.err


def test_publish_target_object_store_builds_minio_client(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--target=object-store instantiates MinIO client + threads flags through."""
    captured: dict[str, object] = {}
    minio_ctor_kwargs: dict[str, object] = {}

    class FakeMinio:
        def __init__(self, **kwargs: object) -> None:
            minio_ctor_kwargs.update(kwargs)

    async def fake_run_publish(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "published": 5,
            "warnings": 0,
            "target": "object-store",
            "repo_id": "s3://loom-benchmarks/humaneval",
            "revision": "abc123",
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeMinio)
    monkeypatch.setattr("loom_benchmark_tool.publish_cmd.run_publish", fake_run_publish)

    rc = dispatch([
        "publish",
        "humaneval",
        "--target",
        "object-store",
        "--minio-endpoint",
        "http://minio.local:9000",
        "--minio-access-key",
        "minioadmin",
        "--minio-secret-key",
        "minioadmin123",
        "--bucket",
        "loom-benchmarks",
    ])

    assert rc == 0
    assert captured["target"] == "object-store"
    assert captured["object_store"] is not None
    assert captured["bucket"] == "loom-benchmarks"
    assert minio_ctor_kwargs == {
        "endpoint_url": "http://minio.local:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin123",
    }
    assert "target=object-store" in capsys.readouterr().out


def test_publish_failure_redacts_hf_token_from_cli_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    hf_token = "hf_1234567890abcdef1234"

    async def fake_run_publish(**kwargs: object) -> dict[str, object]:
        raise RuntimeError(f"403 Forbidden for token {kwargs['hf_token']}")

    monkeypatch.setattr("loom_benchmark_tool.publish_cmd.run_publish", fake_run_publish)

    rc = dispatch([
        "publish",
        "humaneval",
        "--hf-org",
        "PRHW",
        "--hf-token",
        hf_token,
    ])

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert rc == 1
    assert "publish failed" in captured.err
    assert "403 Forbidden" in captured.err
    assert "[REDACTED:hf-token]" in captured.err
    assert hf_token not in combined


def test_top_level_main_routes_to_datasets(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_cli.__main__ import main

    def fake_dispatch(argv: list[str]) -> int:
        print("ROUTED:" + " ".join(argv))
        return 0

    monkeypatch.setattr("loom_cli.datasets_cmd.dispatch", fake_dispatch)
    rc = main(["datasets", "list", "--json"])
    assert rc == 0
    assert "ROUTED:list --json" in capsys.readouterr().out
