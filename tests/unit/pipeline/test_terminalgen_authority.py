from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import pytest

from loom.integrations.terminalgen.authority import (
    TERMINALGEN_POOL_POLICIES,
    TERMINALGEN_RUNTIME_POLICY_DIGEST,
    TerminalGenAuthorityError,
    build_terminalgen_authoring_grant,
    policy_for_attempt,
)
from loom.pipeline.resource_profiles import load_resource_profiles
from loom_control_plane.routes.workers import _terminalgen_authorization_for_claim

DIGEST = "sha256:" + "a" * 64


class _FrozenResult:
    def mappings(self) -> _FrozenResult:
        return self

    def one_or_none(self) -> dict[str, UUID]:
        return {"id": UUID(int=3)}


class _FreezeSession:
    def __init__(self) -> None:
        self.params: dict[str, Any] | None = None

    async def execute(self, _statement: object, params: dict[str, Any]) -> _FrozenResult:
        self.params = params
        return _FrozenResult()


def _attempt_row() -> dict[str, Any]:
    return {
        "id": UUID(int=3),
        "pipeline_run_id": UUID(int=1),
        "stage_run_id": UUID(int=2),
        "node_key": "plan_batch",
        "recipe_name": "terminalgen-authoring",
        "recipe_version": 1,
        "recipe_digest": DIGEST,
        "resource_profile_json": {"name": "terminalgen-plan-none", "version": 1},
        "resource_profile_digest": DIGEST,
        "image_runtime_contract_digest": DIGEST,
        "provider_connection_ref": None,
        "execution_authorization_json": None,
        "execution_authorization_bytes": None,
        "execution_authorization_digest": None,
    }


def _grant(**overrides: object):  # type: ignore[no-untyped-def]
    values: dict[str, Any] = {
        "recipe_name": "terminalgen-authoring",
        "recipe_version": 1,
        "recipe_digest": DIGEST,
        "pipeline_run_id": UUID(int=1),
        "stage_run_id": UUID(int=2),
        "execution_attempt_id": UUID(int=3),
        "node_key": "plan_batch",
        "resource_profile": "terminalgen-plan-none@1",
        "resource_profile_digest": DIGEST,
        "image_runtime_contract_digest": DIGEST,
        "resolved_input_bindings_digest": DIGEST,
        "network_profile": "none",
        "provider_connection_ref": None,
    }
    values.update(overrides)
    return build_terminalgen_authoring_grant(**values)


def test_runtime_policy_is_closed_and_grant_identity_is_deterministic() -> None:
    first = _grant()
    replay = _grant()

    assert len(TERMINALGEN_POOL_POLICIES) == 4
    profiles = load_resource_profiles()
    for policy in TERMINALGEN_POOL_POLICIES.values():
        profile = profiles.get(policy.resource_profile).profile
        assert profile.network_profile == policy.network_profile
        assert profile.execution_variants[0].pool_class == policy.pool_name
    assert first == replay
    assert first.schema_version == "loom.terminalgen-authoring-grant.v1"
    assert first.authorization_id == replay.authorization_id
    assert first.runtime_policy_digest == TERMINALGEN_RUNTIME_POLICY_DIGEST
    assert (
        policy_for_attempt(
            node_key="package_runtime",
            resource_profile="terminalgen-package-none@1",
            network_profile="none",
        ).pool_name
        == "terminalgen-package-none"
    )
    for pool_name, prefix in (
        ("terminalgen-generate-gateway", "generate_card"),
        ("terminalgen-validate-none", "validate_card"),
        ("terminalgen-plan-none", "plan_card"),
        ("terminalgen-plan-none", "finalize_card"),
    ):
        policy = TERMINALGEN_POOL_POLICIES[pool_name]
        assert [
            ordinal for ordinal in range(100) if policy.matches_node(f"{prefix}_{ordinal:02d}")
        ] == list(range(18))


async def test_claim_authorization_is_frozen_once_and_replayed_byte_exact() -> None:
    session = _FreezeSession()
    row = _attempt_row()
    spec = {"resolved_input_bindings_digest": DIGEST}
    node = {"network_profile": "none"}

    first = await _terminalgen_authorization_for_claim(
        session,  # type: ignore[arg-type]
        attempt_row=row,
        spec=spec,
        node=node,
    )

    assert first is not None
    assert session.params is not None
    row.update(
        execution_authorization_json=json.loads(session.params["authorization_json"]),
        execution_authorization_bytes=session.params["authorization_bytes"],
        execution_authorization_digest=session.params["authorization_digest"],
    )
    replay_session = _FreezeSession()
    replay = await _terminalgen_authorization_for_claim(
        replay_session,  # type: ignore[arg-type]
        attempt_row=row,
        spec=spec,
        node=node,
    )
    assert replay == first
    assert replay.model_dump(mode="json")["schema_version"] == (
        "loom.terminalgen-authoring-grant.v1"
    )
    assert replay_session.params is None

    row["execution_authorization_digest"] = DIGEST
    with pytest.raises(TerminalGenAuthorityError, match="snapshot_drift"):
        await _terminalgen_authorization_for_claim(
            replay_session,  # type: ignore[arg-type]
            attempt_row=row,
            spec=spec,
            node=node,
        )


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"recipe_name": "lookalike"}, "recipe_identity_mismatch"),
        ({"node_key": "plan_card_18"}, "attempt_policy_mismatch"),
        ({"network_profile": "gateway"}, "attempt_policy_mismatch"),
        ({"provider_connection_ref": UUID(int=9)}, "provider_authority_mismatch"),
        (
            {
                "node_key": "validate_card_00",
                "resource_profile": "terminalgen-validate-none@1",
            },
            "validation_authority_unavailable",
        ),
    ],
)
def test_grant_builder_rejects_recipe_pool_and_authority_drift(
    overrides: dict[str, object], reason: str
) -> None:
    with pytest.raises(TerminalGenAuthorityError, match=reason):
        _grant(**overrides)
