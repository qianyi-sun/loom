"""Capabilities + RequiredCapabilities — what workers offer and trials demand
(spec §2.3, §3.1.1). Scalar fields match the §2.6 SQL claim query semantics
exactly."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from loom.models.types import OS, GPUVendor, NetworkPolicyKind, ResourceMode


class Capabilities(BaseModel):
    """A single backend configuration's offered capabilities."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    os: OS
    gpu_vendor: GPUVendor
    network_policies: frozenset[NetworkPolicyKind]
    dynamic_network_policy: bool
    mounted_fs: bool
    resource_modes: frozenset[ResourceMode]
    # GPU types this backend can attach to a single sandbox. Empty set ⇒
    # backend cannot attach a GPU even if gpu_vendor != "none". For Docker
    # and Daytona this is always empty (no GPU passthrough). For Modal it
    # lists the modal-supported GPU strings.
    gpu_types: frozenset[str] = frozenset()


class RequiredCapabilities(BaseModel):
    """What a trial requires. Persisted as `trials.requires_caps` JSONB."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    os: OS
    gpu_vendor: GPUVendor
    network_policies: frozenset[NetworkPolicyKind]

    def satisfied_by(self, caps: Capabilities) -> bool:
        """True iff `caps` offers everything this requires."""
        return (
            self.os == caps.os
            and self.gpu_vendor == caps.gpu_vendor
            and self.network_policies <= caps.network_policies
        )
