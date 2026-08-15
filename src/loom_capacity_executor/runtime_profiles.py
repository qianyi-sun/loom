"""Public runtime profile resolution shared by daemon assembly and executors."""

from __future__ import annotations

import hmac

from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    canonical_launch_policy_digest,
)
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2


class RuntimeAssemblyError(RuntimeError):
    """A positive executor runtime artifact failed exact immutable validation."""


def resolve_runtime_profile(
    binding: ExecutableIntentBindingV2,
    profiles: tuple[OperatorLaunchProfileV2, ...],
    *,
    controller_authority_sha256: str,
) -> OperatorLaunchProfileV2:
    """Select the one approved profile that exactly matches an intent binding."""

    if not isinstance(binding, ExecutableIntentBindingV2):
        raise RuntimeAssemblyError("runtime profile binding is not executable-v2")
    matches: list[OperatorLaunchProfileV2] = []
    for profile in profiles:
        if not isinstance(profile, OperatorLaunchProfileV2):
            raise RuntimeAssemblyError("runtime profile set contains an invalid profile")
        profile_digest = canonical_launch_policy_digest(profile)
        if (
            binding.pool_id == profile.pool_id
            and binding.pool_generation == profile.pool_generation
            and binding.profile_id == profile.profile_id
            and binding.profile_generation == profile.profile_generation
            and hmac.compare_digest(binding.profile_digest, profile.profile_digest)
            and binding.shape_id == profile.shape_id
            and binding.concurrency_slots == profile.concurrency_slots
            and binding.resources == profile.resources
            and hmac.compare_digest(
                profile.controller_authority_sha256, controller_authority_sha256
            )
            and hmac.compare_digest(profile_digest, controller_authority_sha256)
        ):
            matches.append(profile)
    if len(matches) != 1:
        raise RuntimeAssemblyError("runtime profile does not exactly match binding")
    return matches[0]
