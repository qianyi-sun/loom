from __future__ import annotations

import os
import subprocess
from pathlib import Path

from loom.verifier.pytest_verifier import build_pytest_install_command


def test_pytest_install_command_falls_back_for_old_pip(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "fallback-used"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"-c\" ]; then\n"
        "  exit 1\n"
        "fi\n"
        "if [ \"$1\" = \"-m\" ] && [ \"$2\" = \"pip\" ]; then\n"
        "  for arg in \"$@\"; do\n"
        "    if [ \"$arg\" = \"--root-user-action=ignore\" ]; then\n"
        "      exit 2\n"
        "    fi\n"
        "  done\n"
        f"  touch {marker}\n"
        "  exit 0\n"
        "fi\n"
        "exit 3\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    result = subprocess.run(
        ["sh", "-lc", build_pytest_install_command()],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert marker.exists()
