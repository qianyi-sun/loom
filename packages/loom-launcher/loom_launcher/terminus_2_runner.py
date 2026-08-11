"""Compatibility entry point for the built-in Terminus2 runtime.

The Harbor-embedded worker runtime lives at ``loom.agent.terminus2.runtime``.
This module makes the unsupported launcher invocation fail with a clear usage
message.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    del argv
    print(
        "terminus-2 is not a loom-launcher subprocess runtime. "
        "Use the Harbor-embedded LoomTerminus2Runtime; the worker routes agent.name "
        "'terminus-2' automatically.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
