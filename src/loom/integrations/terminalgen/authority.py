"""Code-owned admission policy for TerminalGen execution Attempts.

The Recipe is intentionally not registered yet. This module gives the Control
Plane and worker one canonical policy document so a future activation cannot
drift on pool, profile, network, node, or validation requirements.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import UUID, uuid5

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import BindingSetV1
from loom.pipeline.work_protocol import (
    ResourceProfileV1,
    TerminalGenAuthoringGrantV1,
    TerminalTaskValidationGrantV1,
)

TERMINALGEN_RECIPE_NAME = "terminalgen-authoring"
TERMINALGEN_RECIPE_VERSION = 1
_AUTHORIZATION_NAMESPACE = UUID("3effb864-af98-5c2b-bca8-0eb4388d1b23")
_VALIDATION_AUTHORIZATION_NAMESPACE = UUID("7a1e8ceb-d22e-56ab-8908-2d2168d843cb")


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

TERMINALGEN_VALIDATION_POLICY_DIGEST = canonical_digest(
    {
        "schema_version": "loom.terminal-task-validation-policy.v1",
        "backend": "rootless-buildkit-oci-v1",
        "network_profile": "none",
        "repeat_count": 2,
        "modes": ["baseline_unsolved", "reference_solution"],
        "stage_receives_runtime_socket": False,
        "task_base_must_be_digest_pinned": True,
        "dependency_resolver_must_be_digest_pinned": True,
        "dependency_lock_required": True,
    }
)

_VALIDATION_ARGV_PREFIX = (
    "python",
    "-m",
    "loom.integrations.terminalgen.cli",
    "run",
)


def terminalgen_validation_argv(
    *,
    node_key: str,
    task_base_image: str,
    dependency_resolver_image: str,
    dependency_allowlist_digest: str,
) -> list[str]:
    """Return the one byte-stable validator argv accepted by claim authority."""

    return [
        *_VALIDATION_ARGV_PREFIX,
        node_key,
        "--validation-backend",
        "rootless-buildkit-oci-v1",
        "--task-base-image",
        task_base_image,
        "--dependency-resolver-image",
        dependency_resolver_image,
        "--dependency-allowlist-sha256",
        dependency_allowlist_digest,
        "--validation-policy-sha256",
        TERMINALGEN_VALIDATION_POLICY_DIGEST,
    ]


def _validation_argv_authority(
    *,
    node_key: str,
    argv: Sequence[object],
) -> tuple[str, str, str]:
    values = list(argv)
    if len(values) != 15 or values[:5] != [*_VALIDATION_ARGV_PREFIX, node_key]:
        raise TerminalGenAuthorityError("terminalgen_validation_argv_mismatch")
    expected_literals = {
        5: "--validation-backend",
        6: "rootless-buildkit-oci-v1",
        7: "--task-base-image",
        9: "--dependency-resolver-image",
        11: "--dependency-allowlist-sha256",
        13: "--validation-policy-sha256",
        14: TERMINALGEN_VALIDATION_POLICY_DIGEST,
    }
    if any(values[index] != expected for index, expected in expected_literals.items()):
        raise TerminalGenAuthorityError("terminalgen_validation_argv_mismatch")
    if not all(isinstance(values[index], str) for index in (8, 10, 12)):
        raise TerminalGenAuthorityError("terminalgen_validation_argv_mismatch")
    return str(values[8]), str(values[10]), str(values[12])


def build_terminal_task_validation_grant(
    *,
    pipeline_run_id: UUID,
    stage_run_id: UUID,
    execution_attempt_id: UUID,
    node_key: str,
    node: Mapping[str, Any],
    resource_profile: Mapping[str, Any],
    input_bindings: Sequence[Mapping[str, Any]],
) -> TerminalTaskValidationGrantV1:
    """Derive the closed validation grant from one immutable ExecutionSpec."""

    if re.fullmatch(r"validate_card_(?:0[0-9]|1[0-7])", node_key) is None:
        raise TerminalGenAuthorityError("terminalgen_validation_node_mismatch")
    if node.get("node_key") != node_key or node.get("network_profile") != "none":
        raise TerminalGenAuthorityError("terminalgen_validation_node_mismatch")
    argv = node.get("argv")
    if not isinstance(argv, list):
        raise TerminalGenAuthorityError("terminalgen_validation_argv_mismatch")
    task_base_image, dependency_resolver_image, dependency_allowlist_digest = (
        _validation_argv_authority(node_key=node_key, argv=argv)
    )
    validator_image = node.get("image")
    if not isinstance(validator_image, str):
        raise TerminalGenAuthorityError("terminalgen_validation_image_missing")

    try:
        profile = ResourceProfileV1.model_validate(resource_profile)
        bindings = [BindingSetV1.model_validate(item) for item in input_bindings]
    except ValueError as exc:
        raise TerminalGenAuthorityError("terminalgen_validation_snapshot_invalid") from exc
    if (
        f"{profile.name}@{profile.version}" != "terminalgen-validate-none@1"
        or profile.pids_limit is None
        or profile.network_profile != "none"
    ):
        raise TerminalGenAuthorityError("terminalgen_validation_profile_mismatch")
    task_bundle_items = [
        item
        for binding in bindings
        if binding.binding_name == "task_bundle"
        and binding.artifact_type == "terminalgen_task_bundle.v1"
        for item in binding.items
    ]
    if len(task_bundle_items) != 1:
        raise TerminalGenAuthorityError("terminalgen_validation_task_bundle_mismatch")

    authorization_name = canonical_digest(
        {
            "schema_version": "loom.terminal-task-validation-authorization-name.v1",
            "execution_attempt_id": str(execution_attempt_id),
            "task_bundle_content_sha256": task_bundle_items[0].content_sha256,
            "validator_image": validator_image,
            "task_base_image": task_base_image,
            "dependency_resolver_image": dependency_resolver_image,
            "policy_digest": TERMINALGEN_VALIDATION_POLICY_DIGEST,
            "dependency_allowlist_digest": dependency_allowlist_digest,
        }
    )
    return TerminalTaskValidationGrantV1(
        authorization_id=uuid5(_VALIDATION_AUTHORIZATION_NAMESPACE, authorization_name),
        pipeline_run_id=pipeline_run_id,
        stage_run_id=stage_run_id,
        execution_attempt_id=execution_attempt_id,
        task_bundle_content_sha256=task_bundle_items[0].content_sha256,
        validator_image=validator_image,
        task_base_image=task_base_image,
        dependency_resolver_image=dependency_resolver_image,
        policy_digest=TERMINALGEN_VALIDATION_POLICY_DIGEST,
        dependency_allowlist_digest=dependency_allowlist_digest,
        repeat_count=2,
        cpu_cores=profile.cpu_cores,
        memory_bytes=profile.memory_bytes,
        pids_limit=profile.pids_limit,
        timeout_seconds=min(int(node.get("timeout_seconds", 0)), profile.timeout_seconds_max),
        network_profile="none",
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
    "TERMINALGEN_VALIDATION_POLICY_DIGEST",
    "TerminalGenAuthorityError",
    "TerminalGenPoolPolicy",
    "build_terminal_task_validation_grant",
    "build_terminalgen_authoring_grant",
    "policy_for_attempt",
    "policy_for_pool",
    "terminalgen_validation_argv",
]
