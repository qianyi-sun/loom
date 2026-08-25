"""Cloud-compute cost math.

Rate tables are USD per second per resource unit. Treat the result of
``calc_modal_cost()`` as an estimate — Modal's authoritative billing is
on their dashboard.

Source: Modal pricing snapshot from 2026-06-09.
"""

from __future__ import annotations

# Modal pricing (USD / second) at 2026-06-09. Reflects the public pricing
# page; revisit quarterly.
_MODAL_CPU_PER_CORE_SEC = 0.000038        # ~$0.135/hr per vCPU
_MODAL_MEM_PER_GIB_SEC = 0.0000028        # ~$0.01/hr per GiB
_MODAL_GPU_PER_SEC: dict[str, float] = {
    "T4":           0.000164,   # $0.59/hr
    "L4":           0.000218,   # $0.78/hr
    "A10":          0.000306,   # $1.10/hr
    "L40S":         0.000511,   # $1.84/hr
    "A100":         0.000639,   # $2.30/hr (40 GB default)
    "A100-40GB":    0.000639,
    "A100-80GB":    0.000817,   # $2.94/hr
    "RTX-PRO-6000": 0.000758,   # $2.73/hr
    "H100":         0.001147,   # $4.13/hr
    "H100!":        0.001147,
    "H200":         0.001381,   # $4.97/hr
    "B200":         0.001833,   # $6.60/hr
    "B200+":        0.001833,
}


def _gpu_rate(gpu: str | None) -> tuple[float, int]:
    """Returns (per-second rate per GPU, count). Unknown ⇒ (0.0, 1)."""
    if gpu is None:
        return (0.0, 0)
    base, _, count_str = gpu.partition(":")
    rate = _MODAL_GPU_PER_SEC.get(base, 0.0)
    try:
        count = int(count_str) if count_str else 1
    except ValueError:
        count = 1
    return (rate, count)


def calc_modal_cost(
    *,
    billed_seconds: float,
    cpu: float,
    memory_mb: int,
    gpu: str | None,
) -> float:
    """Estimate Modal-billed dollars for a sandbox run.

    Returns USD. Rate table is hard-coded; treat the result as an
    estimate.
    """
    if billed_seconds <= 0:
        return 0.0
    cpu_cost = (
        _MODAL_CPU_PER_CORE_SEC * max(cpu, 0.0) * billed_seconds
    )
    mem_cost = (
        _MODAL_MEM_PER_GIB_SEC * (memory_mb / 1024.0) * billed_seconds
    )
    gpu_rate, gpu_count = _gpu_rate(gpu)
    gpu_cost = gpu_rate * gpu_count * billed_seconds
    return cpu_cost + mem_cost + gpu_cost
