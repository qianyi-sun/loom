from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEXT_SUFFIXES = {
    ".json",
    ".md",
    ".py",
    ".sh",
    ".toml",
    ".tsx",
    ".yaml",
    ".yml",
}


def _tracked_text_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    ignored_prefixes = (
        "archive/",
        "web/coverage/",
    )
    ignored_files = {
        "migrations/versions/0008_cloud_compute_records.py",
        "migrations/versions/0106_" + "day" + "tona_service_worker.py",
        "migrations/versions/0111_retire_" + "day" + "tona_sandbox_ledger.py",
        "tests/ops/test_no_retired_" + "day" + "tona.py",
    }
    return [
        ROOT / relative
        for relative in result.stdout.splitlines()
        if Path(relative).suffix in TEXT_SUFFIXES
        and relative not in ignored_files
        and not relative.startswith(ignored_prefixes)
    ]


def test_retired_provider_surface_is_absent() -> None:
    provider = "day" + "tona"
    forbidden_paths = (
        "docs/architecture/" + provider + "-service-worker.md",
        "src/loom_control_plane/routes/" + provider + "_sandboxes.py",
        "src/loom_drivers/" + provider,
    )
    assert all(not (ROOT / path).exists() for path in forbidden_paths)

    findings: list[str] = []
    for path in _tracked_text_files():
        if not path.is_file():
            continue
        if provider in path.read_text(
            encoding="utf-8",
            errors="replace",
        ).lower():
            findings.append(str(path.relative_to(ROOT)))
    assert findings == []
