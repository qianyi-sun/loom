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

    def fake_run_publish(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "published": 1,
            "warnings": 0,
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
    assert "published=1" in capsys.readouterr().out


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
