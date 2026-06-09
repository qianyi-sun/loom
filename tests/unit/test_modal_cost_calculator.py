"""calc_modal_cost — sanity bounds + SKU-table behavior."""

from __future__ import annotations


def test_calc_cost_no_gpu_cpu_only() -> None:
    from loom.cost.cloud import calc_modal_cost

    c = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu=None,
    )
    assert c > 0
    assert c < 0.01  # sanity ceiling for 1 minute CPU-only


def test_calc_cost_h100_dominates() -> None:
    """H100 charge should dwarf CPU+RAM at any typical config.

    Pinned to absolute USD floors instead of a ratio so the test
    survives RAM/CPU configuration drift. At 60 s on H100 the GPU
    portion alone is 60 * 0.001147 ≈ $0.0688, which is >50× any
    plausible 1-minute CPU+RAM cost (a 1 vCPU + 8 GiB instance costs
    ~$0.0042/min).
    """
    from loom.cost.cloud import calc_modal_cost

    h100 = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu="H100",
    )
    assert h100 > 0.05  # absolute USD floor for 60 s on H100
    assert h100 < 0.1   # absolute USD ceiling (sanity)


def test_calc_cost_multi_gpu_scales_gpu_portion() -> None:
    """Multi-GPU scales the GPU portion linearly; CPU+RAM is counted once."""
    from loom.cost.cloud import calc_modal_cost

    cpu_only = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu=None,
    )
    one = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu="H100",
    )
    eight = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu="H100:8",
    )
    gpu_only_one = one - cpu_only
    gpu_only_eight = eight - cpu_only
    assert abs(gpu_only_eight - 8 * gpu_only_one) < 1e-6


def test_calc_cost_unknown_gpu_zero_gpu_charge() -> None:
    """Unknown GPU types fall back to CPU+RAM only — better to
    under-bill than over-bill in the rollup."""
    from loom.cost.cloud import calc_modal_cost

    known = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu="A10",
    )
    unknown = calc_modal_cost(
        billed_seconds=60.0, cpu=1.0, memory_mb=1024, gpu="Z9000",
    )
    assert unknown < known
    assert unknown > 0  # CPU+RAM still counted


def test_calc_cost_zero_seconds_returns_zero() -> None:
    from loom.cost.cloud import calc_modal_cost

    assert calc_modal_cost(
        billed_seconds=0.0, cpu=1.0, memory_mb=1024, gpu="H100",
    ) == 0.0
    assert calc_modal_cost(
        billed_seconds=-1.0, cpu=1.0, memory_mb=1024, gpu="H100",
    ) == 0.0
