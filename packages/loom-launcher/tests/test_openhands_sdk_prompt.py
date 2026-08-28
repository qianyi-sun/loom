from __future__ import annotations

from loom_launcher.openhands_sdk_prompt import (
    TERMINUS_STYLE_SYSTEM_SUFFIX,
    terminus_style_argv_suffix,
)


def test_terminus_style_system_suffix_is_non_empty() -> None:
    assert "Analysis:" in TERMINUS_STYLE_SYSTEM_SUFFIX
    assert "Plan:" in TERMINUS_STYLE_SYSTEM_SUFFIX
    assert "reasoning_content" in TERMINUS_STYLE_SYSTEM_SUFFIX


def test_terminus_style_argv_suffix() -> None:
    assert terminus_style_argv_suffix({}) == []
    assert terminus_style_argv_suffix({"LOOM_OPENHANDS_TERMINUS_STYLE": "0"}) == []
    assert terminus_style_argv_suffix({"LOOM_OPENHANDS_TERMINUS_STYLE": "1"}) == [
        "--terminus-style"
    ]
    assert terminus_style_argv_suffix({"LOOM_OPENHANDS_TERMINUS_STYLE": "true"}) == [
        "--terminus-style"
    ]
    assert terminus_style_argv_suffix({"LOOM_OPENHANDS_TERMINUS_STYLE": "yes"}) == [
        "--terminus-style"
    ]
