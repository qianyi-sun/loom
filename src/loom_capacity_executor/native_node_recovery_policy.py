"""Protected node scope, not sender authentication or no-writer evidence.

Only the fixed authenticated management entrypoint may use these bindings.
Matching a policy never authorizes pruning without the host lifecycle fence.
The installer derives and retains this policy before admitting worker launch;
neither worker JSON nor current filesystem contents create historical scope.
"""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from loom_capacity_agent.native_recovery import NativeRecoveryMappingRange
from loom_capacity_agent.native_recovery_publication import NativeRecoveryHostIdentityV1
from loom_capacity_executor.native_installed_release import _Observation, _path
from loom_capacity_executor.native_quarantine_prune import NativeQuarantineIdentity
from loom_capacity_manager.contracts import Digest, Identifier, StrictV1Model, canonical_bytes


class NativeNodeRecoveryScopeV1(StrictV1Model):
    installation_id: UUID
    pool_id: Identifier
    profile_sha256: Digest
    host: NativeRecoveryHostIdentityV1
    scratch_root: str
    quarantine_root: str
    scratch_device: int = Field(ge=0)
    scratch_mount_id: int = Field(gt=0)
    filesystem: Literal["ext4", "xfs", "btrfs", "tmpfs"]
    uid_map: Annotated[tuple[NativeRecoveryMappingRange, ...], Field(min_length=1, max_length=340)]
    gid_map: Annotated[tuple[NativeRecoveryMappingRange, ...], Field(min_length=1, max_length=340)]

    @model_validator(mode="after")
    def _scope(self) -> Self:
        scratch, quarantine = _path(self.scratch_root), _path(self.quarantine_root)
        if scratch == quarantine or scratch in quarantine.parents or quarantine in scratch.parents:
            raise ValueError("recovery scratch and protected quarantine must be disjoint")
        for ranges, original in ((self.uid_map, self.host.original_uid), (self.gid_map, self.host.original_gid)):
            root = ranges[0]
            if (root.inside, root.outside, root.count) != (0, original, 1):
                raise ValueError("recovery policy root map differs from retained host identity")
            for coordinate in ("inside", "outside"):
                intervals = sorted((getattr(item, coordinate), item.count) for item in ranges)
                if (any(start + count > 2**32 - 1 for start, count in intervals)
                    or any(start + count > following for (start, count), (following, _) in pairwise(intervals))):
                    raise ValueError("recovery policy mapping overlaps or exceeds identity range")
        # The inode is filled only from authenticated attempt history later.
        self.quarantine_identity(inode=1).validate()
        return self

    def quarantine_identity(self, *, inode: int) -> NativeQuarantineIdentity:
        return NativeQuarantineIdentity(device=self.scratch_device, inode=inode, mount_id=self.scratch_mount_id,
            uid_ranges=tuple((item.outside, item.count) for item in self.uid_map),
            gid_ranges=tuple((item.outside, item.count) for item in self.gid_map))


class NativeNodeRecoveryPolicyV1(StrictV1Model):
    management_uid: int = Field(gt=0, lt=2**32 - 1)
    scopes: Annotated[tuple[NativeNodeRecoveryScopeV1, ...], Field(min_length=1, max_length=4096)]

    @model_validator(mode="after")
    def _inventory(self) -> Self:
        keys = [(scope.installation_id, scope.pool_id, scope.profile_sha256, scope.host.node_id, scope.host.boot_id)
            for scope in self.scopes]
        if len(set(keys)) != len(keys):
            raise ValueError("recovery node policy contains duplicate retained scopes")
        if any(item.outside <= self.management_uid < item.outside + item.count
            for scope in self.scopes for item in scope.uid_map):
            raise ValueError("recovery management identity overlaps a workload mapping")
        return self


def read_native_node_recovery_policy(path: Path, *, expected_sha256: str) -> NativeNodeRecoveryPolicyV1:
    """The fixed installed entrypoint owns this path and digest, never stdin."""
    _path(str(path))
    observation = _Observation()
    wire = observation.read(path, digest=expected_sha256, size=None, mode=0o444,
        bound=4 * 1024**2, collect=True)
    policy = NativeNodeRecoveryPolicyV1.model_validate_json(wire)
    if canonical_bytes(policy) != wire:
        raise ValueError("recovery node policy must be canonical")
    observation.finish()
    return policy
