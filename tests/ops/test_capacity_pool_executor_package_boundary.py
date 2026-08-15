"""Fail-closed source boundary for the controller-local pool executor package."""

from __future__ import annotations

from pathlib import Path

PACKAGE_ROOT = Path("src/loom_capacity_pool_executor")


def test_pool_executor_observer_has_no_scheduler_mutation_surface() -> None:
    forbidden = (
        "sbatch",
        "scancel",
        "srun",
        "shell=True",
        "os.system",
        "import subprocess",
        "from subprocess",
        "pyslurm",
    )
    sources = tuple(PACKAGE_ROOT.rglob("*.py"))

    assert sources
    assert not any(
        token in source.read_text(encoding="utf-8") for source in sources for token in forbidden
    )


def test_pool_executor_observer_is_not_imported_by_the_dry_run_package() -> None:
    dry_run_sources = tuple(Path("src/loom_capacity_executor").glob("*.py"))

    assert dry_run_sources
    assert not any(
        "loom_capacity_pool_executor" in source.read_text(encoding="utf-8")
        for source in dry_run_sources
    )
