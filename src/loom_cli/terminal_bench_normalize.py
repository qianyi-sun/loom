"""Compatibility import for Terminal-Bench task.toml normalization."""

from __future__ import annotations

from loom.terminal_bench_normalize import (
    DEFAULT_AGENT_TIMEOUT_SEC,
    DEFAULT_VERIFIER_SCRIPT_PATH,
    DEFAULT_VERIFIER_TIMEOUT_SEC,
    is_terminal_bench_shape,
    normalize_terminal_bench_task_toml,
)

__all__ = [
    "DEFAULT_AGENT_TIMEOUT_SEC",
    "DEFAULT_VERIFIER_SCRIPT_PATH",
    "DEFAULT_VERIFIER_TIMEOUT_SEC",
    "is_terminal_bench_shape",
    "normalize_terminal_bench_task_toml",
]
