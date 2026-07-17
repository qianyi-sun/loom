from __future__ import annotations

from pathlib import Path

import pytest
from scripts.ops import staging_rollout_shared_work2_export as helper


class Result:
    def __init__(self, returncode: int, stdout: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def _active_export() -> str:
    return f"/shared_work2 192.168.50.103/32({','.join(sorted(helper.EXPECTED_OPTIONS))})\n"


def test_checked_in_export_is_exact_host_only_allowance() -> None:
    payload = helper._asset_payload().decode("ascii")

    assert payload.startswith("/shared_work2 192.168.50.103/32(")
    assert "192.168.50.0/24" not in payload
    assert "no_root_squash" in payload
    assert "sec=sys" in payload


def test_export_check_requires_exact_active_client_and_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper, "_file_is_exact", lambda _payload: True)

    assert helper.converge(install=False, run=lambda _argv: Result(0, _active_export())) is False

    with pytest.raises(helper.ExportError, match="not active"):
        helper.converge(
            install=False,
            run=lambda _argv: Result(0, _active_export().replace("/32", "/24")),
        )

    with pytest.raises(helper.ExportError, match="not active"):
        helper.converge(
            install=False,
            run=lambda _argv: Result(0, _active_export().replace("sec=sys", "sec=sys,insecure")),
        )


def test_export_check_requires_exact_installed_fragment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper, "_file_is_exact", lambda _payload: False)

    with pytest.raises(helper.ExportError, match="not installed"):
        helper.converge(install=False, run=lambda _argv: Result(0, _active_export()))


def test_export_install_is_idempotent_and_refreshes_before_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installs = iter(((True, False), (False, False)))
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    monkeypatch.setattr(helper, "_install_file", lambda _payload: next(installs))

    def run(argv):  # type: ignore[no-untyped-def]
        calls.append(tuple(argv))
        return Result(0, _active_export() if argv[-1] == "-v" else "")

    assert helper.converge(install=True, run=run) is True
    assert helper.converge(install=True, run=run) is False
    assert calls.count((str(helper.EXPORTFS), "-ra")) == 2
    assert calls.count((str(helper.EXPORTFS), "-v")) == 2


def test_new_fragment_rolls_back_if_export_refresh_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed: list[bytes] = []
    removed_directories: list[bool] = []
    results = iter((Result(1), Result(0)))
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    monkeypatch.setattr(helper, "_install_file", lambda _payload: (True, True))
    monkeypatch.setattr(helper, "_remove_exact_file", removed.append)
    monkeypatch.setattr(
        helper,
        "_remove_created_exports_directory",
        lambda: removed_directories.append(True),
    )

    with pytest.raises(helper.ExportError, match="refresh failed safely"):
        helper.converge(install=True, run=lambda _argv: next(results))

    assert removed == [helper._asset_payload()]
    assert removed_directories == [True]


def test_created_directory_rolls_back_even_if_export_refresh_rollback_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    removed_directories: list[bool] = []
    results = iter((Result(1), Result(1)))
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    monkeypatch.setattr(helper, "_install_file", lambda _payload: (True, True))
    monkeypatch.setattr(helper, "_remove_exact_file", lambda _payload: None)
    monkeypatch.setattr(
        helper,
        "_remove_created_exports_directory",
        lambda: removed_directories.append(True),
    )

    with pytest.raises(helper.ExportError, match="refresh and rollback failed safely"):
        helper.converge(install=True, run=lambda _argv: next(results))

    assert removed_directories == [True]


def test_missing_exports_directory_is_created_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exports_parent = tmp_path / "etc"
    exports_parent.mkdir()
    exports_directory = exports_parent / "exports.d"
    monkeypatch.setattr(helper, "EXPORTS_DIRECTORY", exports_directory)
    monkeypatch.setattr(helper, "EXPORTS_PATH", exports_directory / "allowance.exports")

    def validate(path: Path) -> int:
        if not path.exists():
            raise helper.ExportError("missing")
        return helper.os.open(path, helper._DIRECTORY_FLAGS)

    monkeypatch.setattr(helper, "_validate_directory", validate)
    monkeypatch.setattr(helper.os, "fchown", lambda *_args, **_kwargs: None)

    assert helper._ensure_exports_directory() is True
    assert exports_directory.is_dir()
    assert exports_directory.stat().st_mode & 0o777 == 0o755
    assert helper._ensure_exports_directory() is False


def test_export_asset_rejects_any_path_or_client_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asset = tmp_path / "allowance.exports"
    asset.write_text(
        helper._asset_payload().decode("ascii").replace("192.168.50.103/32", "192.168.50.0/24"),
        encoding="ascii",
    )
    monkeypatch.setattr(helper, "ASSET", asset)

    with pytest.raises(helper.ExportError, match="asset is invalid"):
        helper._asset_payload()


def test_export_install_requires_exact_fixed_sealed_source_before_converge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    converged: list[bool] = []
    validated: list[helper.SealedSource] = []
    monkeypatch.setattr(
        helper,
        "converge",
        lambda *, install: converged.append(install) or False,
    )
    monkeypatch.setattr(helper, "validate_sealed_source", validated.append)

    assert helper.main(["install"]) == 1
    assert converged == []

    assert (
        helper.main(
            [
                "install",
                "--sealed-source-sha",
                "a" * 40,
                "--sealed-source-tree",
                "b" * 40,
                "--sealed-approved-base-sha",
                "c" * 40,
            ]
        )
        == 0
    )
    assert validated == [
        helper.SealedSource(
            path=helper.REPO_ROOT,
            commit_sha="a" * 40,
            tree_sha="b" * 40,
            base_sha="c" * 40,
        )
    ]
    assert converged == [True]
