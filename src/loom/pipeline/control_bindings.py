"""Immutable server-owned control bindings for official Pipeline recipes.

The public API accepts configuration only.  Identity, version, audit fields,
canonical bytes and digests are derived by the service and frozen again on
each PipelineRun.  No credential material is represented by these models.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime
from typing import Annotated, Literal, TypedDict
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import Digest, PipelineModel, PositiveSafeInt, PositiveVersion

ProfileStatus = Literal["active", "disabled"]
_Slug = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,62}$")]
_ProfileName = Annotated[
    str,
    StringConstraints(
        pattern=r"^(?:[a-z][a-z0-9_]{0,62}|behavior-judge-codex-gpt-5\.6-sol-v1)$"
    ),
]
_Text = Annotated[str, StringConstraints(min_length=1, max_length=512)]

class _JudgeAdapterRegistration(TypedDict):
    agent: str
    versions: list[str]
    provider: str
    wire: str
    models: list[str]
    mcps: list[str]


_JUDGE_ADAPTER_CATALOG: dict[str, _JudgeAdapterRegistration] = {
    "codex_pipeline_locked_home_v1": {
        "agent": "codex",
        "versions": ["0.146.0"],
        "provider": "openai",
        "wire": "responses",
        "models": ["gpt-5.6-sol"],
        "mcps": ["video", "video_demo"],
    },
    "synthetic_judge_v1": {
        "agent": "synthetic_judge",
        "versions": ["1.0.0"],
        "provider": "anthropic",
        "wire": "messages",
        "models": ["claude-sonnet-4-6"],
        "mcps": ["video", "video_demo"],
    },
}


class ProviderAssetLockV1(PipelineModel):
    role: _Slug
    image_path: Annotated[
        str,
        StringConstraints(pattern=r"^/opt/behavior/provider-assets/[a-z][a-z0-9_]{0,62}/.+$"),
    ]
    sha256: Digest

    @field_validator("image_path")
    @classmethod
    def path_is_closed(cls, value: str) -> str:
        if "//" in value or "/../" in value or value.endswith("/..") or "\\" in value:
            raise ValueError("provider asset path escapes its immutable image root")
        return unicodedata.normalize("NFC", value)


class McpServerLockV1(PipelineModel):
    logical_name: _Slug
    transport: Literal["stdio"]
    interface_version: _Text
    package_or_image_sha256: Digest
    configuration_sha256: Digest


class JudgeExecutionProfileApplyV1(PipelineModel):
    status: ProfileStatus
    recipe_digest: Digest
    environment: _Slug
    agent_name: _Slug
    agent_version: _Text
    agent_adapter: _Slug
    agent_adapter_digest: Digest
    provider_connection_id: UUID
    provider: Literal["openai", "anthropic"]
    model: _Text
    wire_api: Literal["responses", "messages"]
    runner_lock_sha256: Digest
    provider_asset_manifest_sha256: Digest
    provider_asset_locks: Annotated[list[ProviderAssetLockV1], Field(min_length=1, max_length=64)]
    mcp_server_locks: Annotated[list[McpServerLockV1], Field(min_length=1, max_length=16)]
    provider_request_limit_per_attempt: PositiveSafeInt
    provider_cost_limit_microusd_per_attempt: PositiveSafeInt
    per_call_timeout_seconds: PositiveSafeInt
    allowed_team_ids: Annotated[list[UUID], Field(max_length=10_000)]

    @model_validator(mode="after")
    def closed_behavior_profile(self) -> JudgeExecutionProfileApplyV1:
        if self.provider_request_limit_per_attempt > 256:
            raise ValueError("judge request limit exceeds 256")
        if self.provider_cost_limit_microusd_per_attempt > 30_000_000:
            raise ValueError("judge cost limit exceeds 30000000 microusd")
        if self.per_call_timeout_seconds > 60:
            raise ValueError("judge call timeout exceeds 60 seconds")
        roles = [item.role for item in self.provider_asset_locks]
        if roles != sorted(roles, key=lambda item: item.encode("utf-8")) or len(roles) != len(
            set(roles)
        ):
            raise ValueError("provider asset locks must be bytewise-role-sorted and unique")
        names = [item.logical_name for item in self.mcp_server_locks]
        if names != sorted(names, key=lambda item: item.encode("utf-8")) or len(names) != len(
            set(names)
        ):
            raise ValueError("MCP locks must be bytewise-logical-name-sorted and unique")
        teams = [str(item) for item in self.allowed_team_ids]
        if teams != sorted(teams, key=lambda item: item.encode("utf-8")) or len(teams) != len(
            set(teams)
        ):
            raise ValueError("allowed teams must be bytewise-UUID-sorted and unique")
        return self


class JudgeExecutionProfileV1(JudgeExecutionProfileApplyV1):
    schema_version: Literal["loom.judge-execution-profile.v1"]
    profile_id: UUID
    profile_name: _ProfileName
    version: PositiveVersion
    recipe_name: Literal["behavior-recovery"]
    recipe_version: Literal[1]
    node_key: Literal["offline_judge"]
    created_by: UUID
    created_at: datetime
    updated_by: UUID
    updated_at: datetime


class RecipeProviderBindingApplyV1(PipelineModel):
    status: ProfileStatus
    recipe_digest: Digest
    environment: _Slug
    provider_connection_id: UUID
    provider: Literal["anthropic"]
    model: Literal["claude-opus-4-7"]
    wire_api: Literal["messages"]
    runner_lock_sha256: Digest
    provider_asset_manifest_sha256: Digest
    provider_asset_locks: Annotated[list[ProviderAssetLockV1], Field(min_length=1, max_length=64)]
    mcp_server_locks: Annotated[list[McpServerLockV1], Field(min_length=1, max_length=16)]
    provider_request_limit_per_attempt: Literal[512]
    provider_cost_limit_microusd_per_attempt: Literal[30_000_000]
    per_call_timeout_seconds: Literal[600]
    allowed_team_ids: Annotated[list[UUID], Field(max_length=10_000)]

    @model_validator(mode="after")
    def closed_primitive_binding(self) -> RecipeProviderBindingApplyV1:
        roles = [item.role for item in self.provider_asset_locks]
        if roles != sorted(roles, key=lambda item: item.encode("utf-8")) or len(roles) != len(
            set(roles)
        ):
            raise ValueError("provider asset locks must be bytewise-role-sorted and unique")
        names = [item.logical_name for item in self.mcp_server_locks]
        if names != ["recovery_video"]:
            raise ValueError("primitive binding requires the exact recovery_video MCP lock")
        teams = [str(item) for item in self.allowed_team_ids]
        if teams != sorted(teams, key=lambda item: item.encode("utf-8")) or len(teams) != len(
            set(teams)
        ):
            raise ValueError("allowed teams must be bytewise-UUID-sorted and unique")
        return self


class RecipeProviderBindingV1(RecipeProviderBindingApplyV1):
    schema_version: Literal["loom.recipe-provider-binding.v1"]
    binding_id: UUID
    logical_name: Literal["behavior_recovery_primitive"]
    version: PositiveVersion
    recipe_name: Literal["behavior-recovery"]
    recipe_version: Literal[1]
    node_key: Literal["recovery_primitive"]
    created_by: UUID
    created_at: datetime
    updated_by: UUID
    updated_at: datetime


class ControlBindingSnapshotDocumentV1(PipelineModel):
    logical_name: Literal["behavior_offline_judge", "behavior_recovery_primitive"]
    kind: Literal["judge_profile", "provider"]
    node_key: Literal["offline_judge", "recovery_primitive"]
    object_id: UUID
    version: PositiveVersion
    snapshot_sha256: Digest
    snapshot: JudgeExecutionProfileV1 | RecipeProviderBindingV1

    @model_validator(mode="after")
    def reference_matches_snapshot(self) -> ControlBindingSnapshotDocumentV1:
        expected = (
            ("behavior_offline_judge", "judge_profile", "offline_judge")
            if isinstance(self.snapshot, JudgeExecutionProfileV1)
            else ("behavior_recovery_primitive", "provider", "recovery_primitive")
        )
        object_id = (
            self.snapshot.profile_id
            if isinstance(self.snapshot, JudgeExecutionProfileV1)
            else self.snapshot.binding_id
        )
        if (self.logical_name, self.kind, self.node_key) != expected:
            raise ValueError("control binding kind or node drift")
        if self.object_id != object_id or self.version != self.snapshot.version:
            raise ValueError("control binding source identity drift")
        if self.snapshot_sha256 != control_snapshot_digest(self.snapshot):
            raise ValueError("control binding snapshot digest drift")
        return self


def snapshot_bytes(value: JudgeExecutionProfileV1 | RecipeProviderBindingV1) -> bytes:
    return canonical_document(value.model_dump(mode="json", exclude_none=False))


def control_snapshot_digest(value: JudgeExecutionProfileV1 | RecipeProviderBindingV1) -> Digest:
    return canonical_digest(value.model_dump(mode="json", exclude_none=False))


def validate_registered_judge_adapter(value: JudgeExecutionProfileApplyV1) -> None:
    """Fail closed against the small repo-owned adapter catalog."""

    selected = _JUDGE_ADAPTER_CATALOG.get(value.agent_adapter)
    if selected is None:
        raise ValueError("unknown or disabled judge agent adapter")
    if (
        value.agent_name != selected["agent"]
        or value.agent_version not in selected["versions"]
        or value.provider != selected["provider"]
        or value.wire_api != selected["wire"]
        or value.model not in selected["models"]
        or sorted(item.logical_name for item in value.mcp_server_locks) != selected["mcps"]
    ):
        raise ValueError("agent, model, provider, output validator, or MCP lock is incompatible")
    if value.agent_adapter_digest != canonical_digest(
        {
            "schema_version": "loom.judge-adapter-registration.v1",
            "adapter": value.agent_adapter,
            **selected,
        }
    ):
        raise ValueError("registered judge adapter digest mismatch")
    if value.agent_adapter == "codex_pipeline_locked_home_v1" and value.agent_version != "0.146.0":
        raise ValueError("official Codex profile must pin version 0.146.0")


def validate_digest(value: str) -> str:
    if re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError("invalid sha256 digest")
    return value


def registered_judge_adapter_digest(adapter: str) -> Digest:
    try:
        selected = _JUDGE_ADAPTER_CATALOG[adapter]
    except KeyError as exc:
        raise ValueError("unknown judge adapter") from exc
    return canonical_digest(
        {
            "schema_version": "loom.judge-adapter-registration.v1",
            "adapter": adapter,
            **selected,
        }
    )


__all__ = [
    "ControlBindingSnapshotDocumentV1",
    "JudgeExecutionProfileApplyV1",
    "JudgeExecutionProfileV1",
    "McpServerLockV1",
    "ProviderAssetLockV1",
    "RecipeProviderBindingApplyV1",
    "RecipeProviderBindingV1",
    "control_snapshot_digest",
    "registered_judge_adapter_digest",
    "snapshot_bytes",
    "validate_registered_judge_adapter",
]
