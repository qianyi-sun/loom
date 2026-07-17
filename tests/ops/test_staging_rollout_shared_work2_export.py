from __future__ import annotations

import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import staging_rollout_shared_work2_export as helper

_REAL_FSTAT = os.fstat


class Result:
    def __init__(self, returncode: int, stdout: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = ""


def _active_export() -> str:
    return (
        "/shared_work2 \t192.168.50.103/32("
        "sync,wdelay,hide,no_subtree_check,sec=sys,rw,no_root_squash,no_all_squash)\n"
    )


def _active_etab() -> str:
    options = ",".join(sorted(helper.EXPECTED_OPTIONS))
    return f"/shared_work2\t192.168.50.103/32({options},rw,secure,no_root_squash)\n"


def _root_fstat(fd: int, *, file_uid: int = 0) -> SimpleNamespace:
    metadata = _REAL_FSTAT(fd)
    is_directory = stat.S_ISDIR(metadata.st_mode)
    return SimpleNamespace(
        st_mode=(stat.S_IFDIR | 0o755) if is_directory else (stat.S_IFREG | 0o644),
        st_uid=0 if is_directory else file_uid,
        st_gid=0,
        st_dev=metadata.st_dev,
        st_ino=metadata.st_ino,
        st_nlink=1,
        st_size=metadata.st_size,
    )


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
    monkeypatch.setattr(helper, "_read_exact_etab", _active_etab)

    assert helper.converge(install=False, run=lambda _argv: Result(0, _active_export())) is False

    with pytest.raises(helper.ExportError, match="not active"):
        helper.converge(
            install=False,
            run=lambda _argv: Result(0, _active_export().replace("/32", "/24")),
        )

    monkeypatch.setattr(
        helper,
        "_read_exact_etab",
        lambda: _active_etab().replace("sec=sys", "sec=sys,insecure"),
    )
    with pytest.raises(helper.ExportError, match="not active"):
        helper.converge(install=False, run=lambda _argv: Result(0, _active_export()))


def test_active_export_uses_canonical_etab_not_exportfs_summary_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper, "_read_exact_etab", _active_etab)

    assert helper._export_is_active(lambda _argv: Result(0, _active_export())) is True


@pytest.mark.parametrize(
    "etab",
    [
        "",
        _active_etab().replace("/32", "/24"),
        _active_etab().replace("sec=sys", "sec=sys,insecure"),
        _active_etab() + _active_etab(),
    ],
)
def test_active_export_rejects_missing_drifted_or_duplicate_etab(etab: str) -> None:
    assert helper._etab_has_exact_active_export(etab) is False


def test_etab_reader_requires_bounded_root_owned_exact_state_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nfs_state = tmp_path / "nfs"
    nfs_state.mkdir(mode=0o755)
    etab = nfs_state / "etab"
    etab.write_text(_active_etab(), encoding="ascii")
    etab.chmod(0o644)
    monkeypatch.setattr(helper, "NFS_STATE_DIRECTORY", nfs_state)
    monkeypatch.setattr(helper, "ETAB", etab)
    monkeypatch.setattr(
        helper.os,
        "fstat",
        lambda fd: _root_fstat(fd),
    )

    assert helper._read_exact_etab() == _active_etab()

    monkeypatch.setattr(
        helper.os,
        "fstat",
        lambda fd: SimpleNamespace(
            **{
                **vars(_root_fstat(fd)),
                "st_uid": 1000 if stat.S_ISREG(_REAL_FSTAT(fd).st_mode) else 0,
            }
        ),
    )
    with pytest.raises(helper.ExportError, match="table is unsafe"):
        helper._read_exact_etab()


def test_export_check_requires_exact_installed_fragment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper, "_file_is_exact", lambda _payload: False)
    monkeypatch.setattr(helper, "_read_exact_etab", _active_etab)

    with pytest.raises(helper.ExportError, match="not installed"):
        helper.converge(install=False, run=lambda _argv: Result(0, _active_export()))


def test_export_install_is_idempotent_and_refreshes_before_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installs = iter(((True, False), (False, False)))
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(helper.os, "geteuid", lambda: 0)
    monkeypatch.setattr(helper, "_install_file", lambda _payload: next(installs))
    monkeypatch.setattr(helper, "_read_exact_etab", _active_etab)

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
