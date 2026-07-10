"""Legacy Terminus-2 subprocess runner — removed (#744).

The Harbor-embedded worker runtime lives at ``loom.agent.terminus2.runtime``.
This module remains only so import paths fail with a clear migration message.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    del argv
    print(
        "terminus-2 no longer runs via loom_launcher.terminus_2_runner. "
        "Use the Harbor-embedded LoomTerminus2Runtime (worker routes agent.name "
        "'terminus-2' automatically). See #744.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
