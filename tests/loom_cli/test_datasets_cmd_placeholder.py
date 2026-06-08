"""Plan 23 ships a placeholder that lists the built-in REGISTRY entries
so users immediately have a working `loom datasets list`. Plan 24
swaps this out for entry-point + remote-registry discovery."""

from __future__ import annotations

import pytest

from loom_cli.__main__ import main


def test_datasets_list_includes_humaneval(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["datasets", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "swe-bench-verified" in out


def test_datasets_list_emits_header_columns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(["datasets", "list"])
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("SLUG")
    assert "LICENSE" in out[0]
