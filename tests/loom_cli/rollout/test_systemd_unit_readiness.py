from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from loom_cli.rollout.systemd_unit_readiness import UNIT_PATHS, inspect_systemd_units

REPO_ROOT = Path(__file__).resolve().parents[3]


def _copy_units(tmp_path: Path) -> Path:
    for relative in UNIT_PATHS:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO_ROOT / relative, destination)
        destination.chmod(0o644)
    return tmp_path


def test_exact_units_pass_static_semantics_and_systemd_analyze(tmp_path: Path) -> None:
    root = _copy_units(tmp_path)
    calls: list[tuple[str, ...]] = []

    def run(argv):
        calls.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    result = inspect_systemd_units(root, run=run)

    assert result.ready
    assert set(result.unit_sha256) == set(UNIT_PATHS)
    assert result.failed_units == {}
    assert calls[0][:2] == ("systemd-analyze", "verify")
    assert len(result.unit_set_digest) == 64


def test_semantic_failure_does_not_run_systemd_analyze(tmp_path: Path) -> None:
    root = _copy_units(tmp_path)
    service = root / UNIT_PATHS[0]
    service.write_text(service.read_text().replace("Type=oneshot", "Type=simple"))
    calls: list[object] = []

    result = inspect_systemd_units(
        root,
        run=lambda _argv: calls.append(object()),  # type: ignore[arg-type,return-value]
    )

    assert not result.ready
    assert result.failed_units[UNIT_PATHS[0]] == "loom-contract"
    assert calls == []


def test_symlinked_unit_fails_closed(tmp_path: Path) -> None:
    root = _copy_units(tmp_path)
    service = root / UNIT_PATHS[2]
    target = tmp_path / "outside.service"
    target.write_text(service.read_text())
    service.unlink()
    service.symlink_to(target)

    result = inspect_systemd_units(
        root,
        run=lambda argv: subprocess.CompletedProcess(argv, 0, "", ""),
    )

    assert not result.ready
    assert result.failed_units[UNIT_PATHS[2]] == "source-authority"
