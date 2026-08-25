import pytest
from pydantic import ValidationError

from loom.models.capabilities import Capabilities, RequiredCapabilities


def test_capabilities_construction():
    caps = Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public", "no-network"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto", "limit"]),
    )
    assert caps.os == "linux"
    assert "public" in caps.network_policies


def test_required_caps_subset_match():
    """A worker offering linux + (public, no-network) satisfies a trial
    requiring linux + {public}."""
    worker = Capabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public", "no-network"]),
        dynamic_network_policy=False, mounted_fs=True,
        resource_modes=frozenset(["auto"]),
    )
    req = RequiredCapabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
    )
    assert req.satisfied_by(worker) is True


def test_required_caps_subset_mismatch_os():
    worker = Capabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=False, mounted_fs=True,
        resource_modes=frozenset(["auto"]),
    )
    req = RequiredCapabilities(
        os="windows", gpu_vendor="none",
        network_policies=frozenset(["public"]),
    )
    assert req.satisfied_by(worker) is False


def test_required_caps_rejects_different_sandbox_backend():
    worker = Capabilities(
        backend="modal",
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto"]),
    )
    req = RequiredCapabilities(
        backend="docker",
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public"]),
    )
    assert req.satisfied_by(worker) is False


def test_required_caps_subset_mismatch_network():
    worker = Capabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=False, mounted_fs=True,
        resource_modes=frozenset(["auto"]),
    )
    req = RequiredCapabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["allowlist"]),
    )
    assert req.satisfied_by(worker) is False


def test_required_caps_frozen():
    req = RequiredCapabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
    )
    with pytest.raises(ValidationError):
        req.os = "windows"  # type: ignore[misc]


def test_capabilities_default_gpu_types_empty():
    """gpu_types defaults to an empty frozenset (no GPU passthrough)."""
    c = Capabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=True, mounted_fs=True,
        resource_modes=frozenset(["auto"]),
    )
    assert c.gpu_types == frozenset()
    assert c.cpu_arch == "x86_64"


def test_capabilities_gpu_types_roundtrip():
    """gpu_types accepts a non-empty frozenset and is immutable like the rest."""
    c = Capabilities(
        os="linux", gpu_vendor="nvidia",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=False, mounted_fs=True,
        resource_modes=frozenset(["auto"]),
        gpu_types=frozenset({"A10", "H100"}),
    )
    assert "A10" in c.gpu_types
    assert "H100" in c.gpu_types
    with pytest.raises(ValidationError):
        c.gpu_types = frozenset()  # type: ignore[misc]


def test_required_caps_match_cpu_architecture():
    worker = Capabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=False, mounted_fs=True,
        resource_modes=frozenset(["auto"]),
        cpu_arch="arm64",
    )
    x86_req = RequiredCapabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
        cpu_arch="x86_64",
    )
    any_req = RequiredCapabilities(
        os="linux", gpu_vendor="none",
        network_policies=frozenset(["public"]),
        cpu_arch="any",
    )

    assert x86_req.satisfied_by(worker) is False
    assert any_req.satisfied_by(worker) is True
