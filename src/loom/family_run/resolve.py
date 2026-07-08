"""Resolver: catalog defaults + trial_config override -> ResolvedFamilyRunSpec (#672)."""

from __future__ import annotations

from typing import Any

from loom.family_run.spec import (
    FamilyRunSpec,
    PluginRef,
    ResolvedFamilyRunSpec,
)

_REQUIRED_ROLES = (
    "family_key_extractor",
    "sequencer",
    "advance_predicate",
    "adapter",
    "failure_policy",
    "state_backend",
)


class FamilyRunNotEnabledError(ValueError):
    """Raised when neither layer opts the batch into family-run mode."""


def resolve_family_run_spec(
    *,
    catalog: FamilyRunSpec | None,
    override: FamilyRunSpec | None,
) -> ResolvedFamilyRunSpec:
    """Merge catalog defaults with per-batch override.

    Rules:
    * ``enabled`` -- override wins; else catalog; else raise.
    * Per-role: override wins; else catalog; else raise.
    * ``mount_path`` -- override wins; else catalog; else framework default.
    """

    def _pick(role: str) -> PluginRef | None:
        for source in (override, catalog):
            if source is None:
                continue
            value = getattr(source, role)
            if value is not None:
                assert isinstance(value, PluginRef)
                return value
        return None

    enabled = _pick_scalar("enabled", override, catalog)
    if not enabled:
        raise FamilyRunNotEnabledError(
            "family_run.enabled must be True in either the catalog or the "
            "trial_config override",
        )

    resolved_roles: dict[str, PluginRef] = {}
    missing: list[str] = []
    for role in _REQUIRED_ROLES:
        value = _pick(role)
        if value is None:
            missing.append(role)
        else:
            resolved_roles[role] = value
    if missing:
        raise ValueError(
            f"family_run spec is missing required role(s): {', '.join(missing)}",
        )

    mount_path = _pick_scalar("mount_path", override, catalog) or "/root/.skills"
    return ResolvedFamilyRunSpec(enabled=True, mount_path=mount_path, **resolved_roles)


def _pick_scalar(name: str, *sources: FamilyRunSpec | None) -> Any:
    for source in sources:
        if source is None:
            continue
        value = getattr(source, name)
        if value is not None:
            return value
    return None
