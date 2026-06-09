"""Modal GPU type table + validator."""

from __future__ import annotations

import pytest


def test_modal_gpu_types_constant_contains_known_ids() -> None:
    from loom_drivers.modal.gpu import MODAL_GPU_TYPES

    assert "A10" in MODAL_GPU_TYPES
    assert "H100" in MODAL_GPU_TYPES
    assert "A100-40GB" in MODAL_GPU_TYPES


def test_validate_gpu_accepts_known() -> None:
    from loom_drivers.modal.gpu import validate_gpu

    assert validate_gpu("A10") == "A10"
    assert validate_gpu("H100:2") == "H100:2"
    assert validate_gpu(None) is None


def test_validate_gpu_rejects_unknown() -> None:
    from loom_drivers.modal.gpu import ModalGPUError, validate_gpu

    with pytest.raises(ModalGPUError) as ei:
        validate_gpu("Z9000")
    assert "Z9000" in str(ei.value)
    assert "A10" in str(ei.value)


def test_validate_gpu_rejects_bad_suffix() -> None:
    from loom_drivers.modal.gpu import ModalGPUError, validate_gpu

    with pytest.raises(ModalGPUError):
        validate_gpu("H100:abc")
