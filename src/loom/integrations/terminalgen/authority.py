"""Code-owned admission policy for TerminalGen execution Attempts.

The Recipe is intentionally not registered yet. This module gives the Control
Plane and worker one canonical policy document so a future activation cannot
drift on pool, profile, network, node, or validation requirements.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType
from uuid import UUID, uuid5

from loom.pipeline.keys import canonical_digest
from loom.pipeline.work_protocol import (
    TerminalGenAuthoringGrantV1,
    TerminalTaskValidationGrantV1,
)

TERMINALGEN_RECIPE_NAME = "terminalgen-authoring"
TERMINALGEN_RECIPE_VERSION = 1
_AUTHORIZATION_NAMESPACE = UUID("3effb864-af98-5c2b-bca8-0eb4388d1b23")


class TerminalGenAuthorityError(RuntimeError):
    """The persisted Attempt is outside the code-owned authoring policy."""


@dataclass(frozen=True, slots=True)
class TerminalGenPoolPolicy:
    pool_name: str
    resource_profile: str
    network_profile: str
    node_pattern: str
    provider_required: bool
    validation_required: bool

    def matches_node(self, node_key: str) -> bool:
        return re.fullmatch(self.node_pattern, node_key) is not None

    def canonical_value(self) -> dict[str, object]:
        return {
            "pool_name": self.pool_name,
            "resource_profile": self.resource_profile,
            "network_profile": self.network_profile,
            "node_pattern": self.node_pattern,
            "provider_required": self.provider_required,
            "validation_required": self.validation_required,
        }


TERMINALGEN_POOL_POLICIES = MappingProxyType(
    {
        item.pool_name: item
        for item in (
            TerminalGenPoolPolicy(
                pool_name="terminalgen-generate-gateway",
                resource_profile="terminalgen-generate-gateway@1",
                network_profile="gateway",
                node_pattern=r"generate_card_(?:0[0-9]|1[0-7])",
                provider_required=True,
                validation_required=False,
            ),
            TerminalGenPoolPolicy(
                pool_name="terminalgen-package-none",
                resource_profile="terminalgen-package-none@1",
                network_profile="none",
                node_pattern=r"(?:package_(?:authoring|runtime)|publish_boundary)",
                provider_required=False,
                validation_required=False,
            ),
            TerminalGenPoolPolicy(
                pool_name="terminalgen-plan-none",
                resource_profile="terminalgen-plan-none@1",
                network_profile="none",
                node_pattern=(
                    r"(?:plan_batch|plan_card_(?:0[0-9]|1[0-7])|plan_audit|"
                    r"finalize_card_(?:0[0-9]|1[0-7])|global_finalize)"
                ),
                provider_required=False,
                validation_required=False,
            ),
            TerminalGenPoolPolicy(
                pool_name="terminalgen-validate-none",
                resource_profile="terminalgen-validate-none@1",
                network_profile="none",
                node_pattern=r"validate_card_(?:0[0-9]|1[0-7])",
                provider_required=False,
                validation_required=True,
            ),
        )
    }
)

TERMINALGEN_RUNTIME_POLICY_DIGEST = canonical_digest(
    {
        "schema_version": "loom.terminalgen-runtime-policy.v1",
        "recipe_name": TERMINALGEN_RECIPE_NAME,
        "recipe_version": TERMINALGEN_RECIPE_VERSION,
        "pools": [
            TERMINALGEN_POOL_POLICIES[name].canonical_value()
            for name in sorted(TERMINALGEN_POOL_POLICIES, key=str.encode)
        ],
    }
)


def policy_for_pool(pool_name: str) -> TerminalGenPoolPolicy:
    try:
        return TERMINALGEN_POOL_POLICIES[pool_name]
    except KeyError as exc:
        raise TerminalGenAuthorityError("terminalgen_pool_not_registered") from exc


def policy_for_attempt(
    *,
    node_key: str,
    resource_profile: str,
    network_profile: str,
) -> TerminalGenPoolPolicy:
    matches = [
        policy
        for policy in TERMINALGEN_POOL_POLICIES.values()
        if policy.resource_profile == resource_profile
        and policy.network_profile == network_profile
        and policy.matches_node(node_key)
    ]
    if len(matches) != 1:
        raise TerminalGenAuthorityError("terminalgen_attempt_policy_mismatch")
    return matches[0]


def build_terminalgen_authoring_grant(
    *,
    recipe_name: str,
    recipe_version: int,
    recipe_digest: str,
    pipeline_run_id: UUID,
    stage_run_id: UUID,
    execution_attempt_id: UUID,
    node_key: str,
    resource_profile: str,
    resource_profile_digest: str,
    image_runtime_contract_digest: str,
    resolved_input_bindings_digest: str,
    network_profile: str,
    provider_connection_ref: UUID | None,
    validation: TerminalTaskValidationGrantV1 | None = None,
) -> TerminalGenAuthoringGrantV1:
    """Build the deterministic server-side grant from frozen run state."""

    if (recipe_name, recipe_version) != (
        TERMINALGEN_RECIPE_NAME,
        TERMINALGEN_RECIPE_VERSION,
    ):
        raise TerminalGenAuthorityError("terminalgen_recipe_identity_mismatch")
    policy = policy_for_attempt(
        node_key=node_key,
        resource_profile=resource_profile,
        network_profile=network_profile,
    )
    if policy.provider_required != (provider_connection_ref is not None):
        raise TerminalGenAuthorityError("terminalgen_provider_authority_mismatch")
    if policy.validation_required != (validation is not None):
        reason = (
            "terminalgen_validation_authority_unavailable"
            if policy.validation_required
            else "terminalgen_validation_authority_unexpected"
        )
        raise TerminalGenAuthorityError(reason)

    authorization_name = canonical_digest(
        {
            "schema_version": "loom.terminalgen-authoring-authorization-name.v1",
            "execution_attempt_id": str(execution_attempt_id),
            "recipe_digest": recipe_digest,
            "runtime_policy_digest": TERMINALGEN_RUNTIME_POLICY_DIGEST,
        }
    )
    return TerminalGenAuthoringGrantV1(
        authorization_id=uuid5(_AUTHORIZATION_NAMESPACE, authorization_name),
        pipeline_run_id=pipeline_run_id,
        stage_run_id=stage_run_id,
        execution_attempt_id=execution_attempt_id,
        recipe_digest=recipe_digest,
        node_key=node_key,
        resource_profile_digest=resource_profile_digest,
        image_runtime_contract_digest=image_runtime_contract_digest,
        resolved_input_bindings_digest=resolved_input_bindings_digest,
        runtime_policy_digest=TERMINALGEN_RUNTIME_POLICY_DIGEST,
        validation=validation,
    )


__all__ = [
    "TERMINALGEN_POOL_POLICIES",
    "TERMINALGEN_RECIPE_NAME",
    "TERMINALGEN_RECIPE_VERSION",
    "TERMINALGEN_RUNTIME_POLICY_DIGEST",
    "TerminalGenAuthorityError",
    "TerminalGenPoolPolicy",
    "build_terminalgen_authoring_grant",
    "policy_for_attempt",
    "policy_for_pool",
]
