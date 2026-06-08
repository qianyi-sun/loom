"""`loom datasets install <slug>` — thin pip wrapper.

We invoke pip via `[sys.executable, "-m", "pip", "install", spec]` so
the install lands in the same venv the CLI runs in. Shell-metacharacter
specs are rejected up-front since this is user-supplied input that
becomes a process argv element.
"""

from __future__ import annotations

import re
import subprocess
import sys

# Policy filter, not a security boundary: subprocess.run is called with a
# list (not a shell string), so pip never sees an interpreted shell. The
# regex blocks specs that *look* shell-injecty in error messages and logs.
_FORBIDDEN = re.compile(r"[;&|`$<>\n\r]")


class InstallError(RuntimeError):
    """Raised when pip install fails or the spec is rejected."""


def install_dataset(*, pip_spec: str) -> str:
    if _FORBIDDEN.search(pip_spec):
        raise InstallError(
            f"pip spec contains forbidden character(s): {pip_spec!r}",
        )
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", pip_spec],
            check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise InstallError(
            f"pip install {pip_spec!r} failed: {exc.stderr.strip()}",
        ) from exc
    return str(result.stdout)
