"""Import hygiene for `loom_cli.model_spec` (issue #94)."""

from __future__ import annotations

import sys


def test_model_spec_import_does_not_pull_benchmark_fetch_deps() -> None:
    """Parser helpers must not require optional benchmark/datasets deps."""
    blocked_prefixes = (
        "loom_cli.task_loader",
        "loom_benchmarks",
        "datasets",
    )
    saved = {
        name: module
        for name, module in sys.modules.items()
        if name == "loom_cli.model_spec"
        or name.startswith("loom_cli.")
        or name.startswith("loom_benchmarks")
        or name == "datasets"
    }
    for name in saved:
        del sys.modules[name]

    try:
        import loom_cli.model_spec as model_spec  # noqa: F401

        loaded = set(sys.modules)
        for prefix in blocked_prefixes:
            assert not any(
                name == prefix or name.startswith(f"{prefix}.")
                for name in loaded
            ), f"unexpected import while loading model_spec: {prefix}"
    finally:
        for name in list(sys.modules):
            if (
                name == "loom_cli.model_spec"
                or name.startswith("loom_cli.")
                or name.startswith("loom_benchmarks")
                or name == "datasets"
            ) and name not in saved:
                del sys.modules[name]
        sys.modules.update(saved)
