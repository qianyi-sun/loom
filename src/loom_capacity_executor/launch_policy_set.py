"""Exact typed controller policies for future application/build runtime routing.

Purpose must come from the authenticated persisted member/intent generation.
This resolver neither authenticates a supplied purpose nor authorizes a launch.
Legacy V2 runtime assembly/rendering deliberately does not accept these roots.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated, Literal

from pydantic import Field, field_validator

from loom_capacity_executor.launch_renderer import OperatorLaunchProfileV2
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, Digest, Identifier, PositiveQuantity
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2, StrictV2Model

LaunchPurpose = Literal["application-worker", "personal-build-worker"]
_MAX_PROFILE_ENTRIES = 512


class _StrictLaunchV3(StrictV2Model):
    schema_version: Literal[3] = 3  # type: ignore[assignment]

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("typed launch policy schema must be integer3")
        return value


class PurposeLaunchPolicyV3(_StrictLaunchV3):
    purpose: LaunchPurpose
    profile_sha256: Digest

    @field_validator("profile_sha256")
    @classmethod
    def _nonzero(cls, value: str) -> str:
        if value == "0" * 64:
            raise ValueError("typed launch profile digest must be nonzero")
        return value


class PoolLaunchPolicyV3(_StrictLaunchV3):
    pool_id: Identifier
    pool_generation: PositiveQuantity
    entries: Annotated[tuple[PurposeLaunchPolicyV3, ...], Field(min_length=1, max_length=_MAX_PROFILE_ENTRIES)]

    @field_validator("entries")
    @classmethod
    def _canonical(cls, value: tuple[PurposeLaunchPolicyV3, ...]) -> tuple[PurposeLaunchPolicyV3, ...]:
        if len({entry.profile_sha256 for entry in value}) != len(value):
            raise ValueError("typed launch profile must have one unambiguous purpose")
        return tuple(sorted(value, key=lambda item: (item.purpose, item.profile_sha256)))


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    if len(payload) > MAX_CONTRACT_BYTES:
        raise ValueError("typed launch policy exceeds byte bound")
    return hashlib.sha256(payload).hexdigest()


def full_launch_profile_digest(profile: OperatorLaunchProfileV2) -> str:
    """Commit all shape/resource/runtime fields; omit only the policy self-link."""
    if not isinstance(profile, OperatorLaunchProfileV2) or type(profile.schema_version) is not int or profile.schema_version != 2:
        raise ValueError("typed launch profile is invalid")
    checked = OperatorLaunchProfileV2.model_validate_json(profile.model_dump_json())
    return _digest(checked.model_dump(mode="json", exclude={"controller_authority_sha256"}))


def canonical_pool_launch_policy_digest(policy: PoolLaunchPolicyV3) -> str:
    if not isinstance(policy, PoolLaunchPolicyV3) or type(policy.schema_version) is not int or policy.schema_version != 3:
        raise ValueError("typed pool launch policy is invalid")
    checked = PoolLaunchPolicyV3.model_validate_json(policy.model_dump_json())
    return _digest(checked.model_dump(mode="json"))


def resolve_typed_runtime_profile(
    binding: ExecutableIntentBindingV2, profiles: tuple[OperatorLaunchProfileV2, ...], *,
    policy: PoolLaunchPolicyV3, purpose: LaunchPurpose, controller_authority_sha256: str,
) -> OperatorLaunchProfileV2:
    """Resolve one profile after verifying complete set membership and exact intent."""
    root = canonical_pool_launch_policy_digest(policy)
    if (
        root != controller_authority_sha256 or purpose not in ("application-worker", "personal-build-worker")
        or not isinstance(binding, ExecutableIntentBindingV2)
        or not isinstance(profiles, tuple) or not 0 < len(profiles) <= _MAX_PROFILE_ENTRIES
    ):
        raise ValueError("typed launch authority or purpose is invalid")
    binding = ExecutableIntentBindingV2.model_validate_json(binding.model_dump_json())
    if binding.pool_id != policy.pool_id or binding.pool_generation != policy.pool_generation:
        raise ValueError("typed launch policy pool binding changed")
    entries = {entry.profile_sha256: entry.purpose for entry in policy.entries}
    observed: set[str] = set()
    matches = []
    for profile in profiles:
        digest = full_launch_profile_digest(profile)
        if (
            digest in observed or digest not in entries
            or profile.controller_authority_sha256 != root
            or profile.pool_id != policy.pool_id or profile.pool_generation != policy.pool_generation
        ):
            raise ValueError("typed launch profile set differs from controller policy")
        observed.add(digest)
        if (
            entries[digest] == purpose
            and profile.profile_id == binding.profile_id
            and profile.profile_generation == binding.profile_generation
            and profile.profile_digest == binding.profile_digest
            and profile.shape_id == binding.shape_id
            and profile.concurrency_slots == binding.concurrency_slots
            and profile.resources == binding.resources
            and profile.trusted_launcher_release_sha256 == binding.execution.trusted_fleet_release_sha256
            and sum(set(binding.node_ids) <= set(domain.node_ids) for domain in profile.resource_domains) == 1
        ):
            matches.append(profile)
    if observed != set(entries) or len(matches) != 1:
        raise ValueError("typed launch intent does not resolve to one approved purpose profile")
    return matches[0]
