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


def test_export_check_requires_exact_installed_fragment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(helper, "_file_is_exact", lambda _payload: False)

    with pytest.raises(helper.ExportError, match="not installed"):
        helper.converge(install=False, run=lambda _argv: Result(0, _active_export()))


def test_export_install_is_idempotent_and_refreshes_before_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installs = iter((True, False))
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
