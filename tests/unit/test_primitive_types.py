import pytest
from pydantic import TypeAdapter, ValidationError

from loom.models.types import (
    OS,
    GPUVendor,
    LogLevel,
    MultiStepRewardStrategy,
    NetworkPolicyKind,
    ResourceMode,
    VerifierEnvMode,
)


def test_os_values():
    adapter = TypeAdapter(OS)
    assert adapter.validate_python("linux") == "linux"
    assert adapter.validate_python("windows") == "windows"
    with pytest.raises(ValidationError):
        adapter.validate_python("macos")


def test_gpu_vendor_values():
    adapter = TypeAdapter(GPUVendor)
    assert adapter.validate_python("none") == "none"
    assert adapter.validate_python("nvidia") == "nvidia"
    with pytest.raises(ValidationError):
        adapter.validate_python("amd")  # v2 may add this; v1 rejects


def test_verifier_env_mode_values():
    adapter = TypeAdapter(VerifierEnvMode)
    assert adapter.validate_python("shared") == "shared"
    assert adapter.validate_python("separate") == "separate"


def test_resource_modes_v1_trimmed():
    """Spec §2.3 — Harbor had 5; v1 has 3 (auto, limit, guarantee)."""
    adapter = TypeAdapter(ResourceMode)
    assert adapter.validate_python("auto") == "auto"
    assert adapter.validate_python("limit") == "limit"
    assert adapter.validate_python("guarantee") == "guarantee"
    for legacy in ("ignore", "request"):
        with pytest.raises(ValidationError):
            adapter.validate_python(legacy)


def test_multi_step_reward_strategy_values():
    adapter = TypeAdapter(MultiStepRewardStrategy)
    for v in ("mean", "min", "weighted", "final"):
        assert adapter.validate_python(v) == v


def test_network_policy_kind_values():
    adapter = TypeAdapter(NetworkPolicyKind)
    for v in ("public", "no-network", "allowlist"):
        assert adapter.validate_python(v) == v


def test_log_level_values():
    adapter = TypeAdapter(LogLevel)
    for v in ("debug", "info", "warn", "error", "fatal"):
        assert adapter.validate_python(v) == v
