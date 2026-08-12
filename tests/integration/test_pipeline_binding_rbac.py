from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import AuthContext
from loom.db.schema import ProviderConnection, Team, User
from loom.pipeline.control_bindings import (
    JudgeExecutionProfileApplyV1,
    RecipeProviderBindingApplyV1,
    registered_judge_adapter_digest,
)
from loom_service.pipeline_control_bindings import (
    SqlPipelineRecipeBindingResolver,
    apply_judge_profile,
    apply_provider_binding,
)
from loom_service.routes.pipeline import _site_admin_actor

D = "sha256:" + "e" * 64


def _context(*, admin: bool, user: bool = True) -> AuthContext:
    return AuthContext(
        token_hash=b"binding-rbac",
        type="user",
        scopes=["read:own", "submit"],
        team_id=uuid4(),
        expires_at=None,
        user_id=uuid4() if user else None,
        auth_kind="browser",
        role="platform_admin" if admin else "owner",
    )


def test_admin_binding_surface_requires_auditable_site_admin_actor() -> None:
    actor = _context(admin=True)
    assert _site_admin_actor(cast(Any, (SimpleNamespace(), actor))) == actor.user_id
    for context in (_context(admin=False), _context(admin=True, user=False)):
        with pytest.raises(HTTPException) as exc:
            _site_admin_actor(cast(Any, (SimpleNamespace(), context)))
        assert exc.value.status_code == 403


def _locks(logical_name: str, role: str) -> list[dict[str, object]]:
    return [
        {
            "role": role,
            "image_path": f"/opt/behavior/provider-assets/{logical_name}/{role}",
            "sha256": D,
        }
    ]


async def test_admin_apply_is_versioned_idempotent_and_public_list_is_redacted(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    team_id, actor_id, openai_id, anthropic_id = (uuid4() for _ in range(4))
    async with sessions() as session, session.begin():
        session.add(Team(id=team_id, name=f"bindings-{team_id}"))
        session.add(
            User(
                id=actor_id,
                username=f"binding-admin-{actor_id}",
                username_normalized=f"binding-admin-{actor_id}",
                status="active",
                is_platform_admin=True,
            )
        )
        await session.flush()
        for connection_id, provider_type in (
            (openai_id, "openai-compatible"),
            (anthropic_id, "anthropic"),
        ):
            session.add(
                ProviderConnection(
                    id=connection_id,
                    team_id=team_id,
                    provider_type=provider_type,
                    display_name=str(connection_id),
                    base_url="https://provider.invalid/v1",
                    upstream_host="provider.invalid",
                    resolved_egress_ips=[],
                    encrypted_api_key_ref="test://redacted",
                    allowed_models=(
                        ["gpt-5.6-sol"]
                        if provider_type == "openai-compatible"
                        else ["claude-sonnet-4-6", "claude-opus-4-7"]
                    ),
                    status="valid",
                    created_by="test",
                )
            )
        await session.flush()
        for name, adapter, connection_id in (
            ("codex_profile", "codex_pipeline_locked_home_v1", openai_id),
            ("alternate_profile", "synthetic_judge_v1", anthropic_id),
        ):
            codex = adapter.startswith("codex")
            payload = JudgeExecutionProfileApplyV1.model_validate(
                {
                    "status": "active",
                    "recipe_digest": D,
                    "environment": "staging",
                    "agent_name": "codex" if codex else "synthetic_judge",
                    "agent_version": "0.146.0" if codex else "1.0.0",
                    "agent_adapter": adapter,
                    "agent_adapter_digest": registered_judge_adapter_digest(adapter),
                    "provider_connection_id": connection_id,
                    "provider": "openai" if codex else "anthropic",
                    "model": "gpt-5.6-sol" if codex else "claude-sonnet-4-6",
                    "wire_api": "responses" if codex else "messages",
                    "runner_lock_sha256": D,
                    "provider_asset_manifest_sha256": D,
                    "provider_asset_locks": _locks("behavior_offline_judge", "judge"),
                    "mcp_server_locks": [
                        {
                            "logical_name": mcp,
                            "transport": "stdio",
                            "interface_version": "1",
                            "package_or_image_sha256": D,
                            "configuration_sha256": D,
                        }
                        for mcp in ("video", "video_demo")
                    ],
                    "provider_request_limit_per_attempt": 256,
                    "provider_cost_limit_microusd_per_attempt": 30_000_000,
                    "per_call_timeout_seconds": 60,
                    "allowed_team_ids": [team_id],
                }
            )
            created, replay = await apply_judge_profile(
                session,
                actor_id=actor_id,
                recipe_name="behavior-recovery",
                recipe_version=1,
                profile_name=name,
                payload=payload,
                idempotency_key=f"create-{name}",
                create_only=True,
                expected_version=None,
            )
            assert not replay and created["provider_connection_id"] == str(connection_id)
        primitive = RecipeProviderBindingApplyV1.model_validate(
            {
                "status": "active",
                "recipe_digest": D,
                "environment": "staging",
                "provider_connection_id": anthropic_id,
                "provider": "anthropic",
                "model": "claude-opus-4-7",
                "wire_api": "messages",
                "runner_lock_sha256": D,
                "provider_asset_manifest_sha256": D,
                "provider_asset_locks": _locks("behavior_recovery_primitive", "primitive"),
                "mcp_server_locks": [
                    {
                        "logical_name": "recovery_video",
                        "transport": "stdio",
                        "interface_version": "1",
                        "package_or_image_sha256": D,
                        "configuration_sha256": D,
                    }
                ],
                "provider_request_limit_per_attempt": 512,
                "provider_cost_limit_microusd_per_attempt": 30_000_000,
                "per_call_timeout_seconds": 600,
                "allowed_team_ids": [team_id],
            }
        )
        first, replay = await apply_provider_binding(
            session,
            actor_id=actor_id,
            recipe_name="behavior-recovery",
            recipe_version=1,
            logical_name="behavior_recovery_primitive",
            payload=primitive,
            idempotency_key="create-primitive",
            create_only=True,
            expected_version=None,
        )
        again, replay_again = await apply_provider_binding(
            session,
            actor_id=actor_id,
            recipe_name="behavior-recovery",
            recipe_version=1,
            logical_name="behavior_recovery_primitive",
            payload=primitive,
            idempotency_key="create-primitive",
            create_only=True,
            expected_version=None,
        )
        assert not replay and replay_again and first == again
    resolver = SqlPipelineRecipeBindingResolver(sessions)
    public = await resolver.list(
        team_id=team_id, recipe_name="behavior-recovery", recipe_version=1
    )
    assert [item["profile_name"] for item in public] == ["alternate_profile", "codex_profile"]
    assert all("provider_connection_id" not in item for item in public)
    await engine.dispose()
