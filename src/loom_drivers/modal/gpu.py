"""Modal GPU type registry + validation.

Source: https://modal.com/docs/guide/gpu (snapshot 2026-06-09). Update this
constant when Modal adds new GPU SKUs.
"""

from __future__ import annotations

MODAL_GPU_TYPES: frozenset[str] = frozenset({
    "T4", "L4", "A10", "L40S",
    "A100", "A100-40GB", "A100-80GB",
    "RTX-PRO-6000",
    "H100", "H100!", "H200",
    "B200", "B200+",
})


class ModalGPUError(ValueError):
    """Raised on invalid --gpu values."""


def validate_gpu(value: str | None) -> str | None:
    """Validate a Modal GPU spec.

    Accepts:
      - ``None`` (no GPU)
      - One of ``MODAL_GPU_TYPES``
      - ``"<TYPE>:<N>"`` multi-GPU spec where ``TYPE`` is in
        ``MODAL_GPU_TYPES`` and ``N`` is a positive integer.
    """
    if value is None:
        return None
    if ":" in value:
        base, _, count_str = value.partition(":")
        if base not in MODAL_GPU_TYPES:
            raise ModalGPUError(
                f"Unknown Modal GPU type {base!r}. "
                f"Valid types: {sorted(MODAL_GPU_TYPES)}",
            )
        try:
            count = int(count_str)
        except ValueError as exc:
            raise ModalGPUError(
                f"Invalid multi-GPU count in {value!r}: "
                f"{count_str!r} is not an integer",
            ) from exc
        if count < 1:
            raise ModalGPUError(
                f"Multi-GPU count must be >= 1, got {count}",
            )
        return value
    if value not in MODAL_GPU_TYPES:
        raise ModalGPUError(
            f"Unknown Modal GPU type {value!r}. "
            f"Valid types: {sorted(MODAL_GPU_TYPES)}",
        )
    return value
