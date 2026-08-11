"""Adapter registry self-registration entry point.

Importing this package self-registers the production launcher adapters plus
`HelloAdapter`, the reference used by tests.
"""

from loom_launcher.adapters import (
    aider,  # noqa: F401 — self-registers
    claude_code,  # noqa: F401 — self-registers
    codex,  # noqa: F401 — self-registers
    gemini_cli,  # noqa: F401 — self-registers
    hello,  # noqa: F401 — self-registers
    kimi_cli,  # noqa: F401 — self-registers
    mini_swe_agent,  # noqa: F401 — self-registers
    opencode,  # noqa: F401 — self-registers
    openhands,  # noqa: F401 — self-registers
    openhands_sdk,  # noqa: F401 — self-registers
    qwen_cli,  # noqa: F401 — self-registers
    swe_agent,  # noqa: F401 — self-registers
    # terminus-2 is a native builtin runtime and does not register here.
)

__all__: list[str] = []
