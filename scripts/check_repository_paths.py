from __future__ import annotations

import subprocess
import sys
from pathlib import Path

FORBIDDEN_TRACKED_PREFIXES = ("docs/superpowers/",)


def _repository_root(cwd: Path) -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or "git rev-parse failed"
        raise RuntimeError(detail)
    return Path(result.stdout.strip())


def _tracked_paths(repo: Path) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or "git ls-files failed"
        raise RuntimeError(detail)
    return tuple(path for path in result.stdout.split("\0") if path)


def main() -> int:
    try:
        tracked_paths = _tracked_paths(_repository_root(Path.cwd()))
    except RuntimeError as exc:
        print(f"Unable to inspect tracked repository paths: {exc}", file=sys.stderr)
        return 2

    forbidden = sorted(
        path
        for path in tracked_paths
        if path == "docs/superpowers"
        or path.startswith(FORBIDDEN_TRACKED_PREFIXES)
    )
    if not forbidden:
        return 0

    print("Forbidden tracked repository paths:", file=sys.stderr)
    for path in forbidden:
        print(f"  {path}", file=sys.stderr)
    print(
        "Move durable designs to docs/architecture/ and keep execution plans local.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
