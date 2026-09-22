"""Protected environment bindings, separate from a child's execution topology.

These contracts describe identities, not authorization. Management authenticates
owners and validates published candidates before constructing them. In particular,
importing an existing binding is an operator operation, never a create-request flag.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EnvironmentKind = Literal["development", "staging", "production"]
EnvironmentScope = Literal["personal", "shared"]
_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_PROVIDER_ID = r"^[a-zA-Z0-9_-]{1,128}$"
_SHARED_SLUGS = {"development": "dev", "staging": "staging", "production": "prod"}
_RESERVED_SLUGS = frozenset((*_SHARED_SLUGS.values(), "shared"))


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _hostname(value: str) -> str:
    if len(value) > 253 or "." not in value or any(
        re.fullmatch(_LABEL, part) is None for part in value.split(".")
    ):
        raise ValueError("expected a lowercase DNS hostname")
    return value


class FoundationBinding(_Contract):
    """Operator-owned shared infrastructure; never populated from feature source.

Retain the existing validated installation settings as JSON so this frozen model
does not expose a mutable nested configuration. Each reader gets its own copy.
Ingress uses the shared controller's default wildcard certificate, not a copy of
its private key in developer namespaces. This contract does not provision it.
"""

    platform_config_json: str = Field(min_length=2, max_length=262144)
    public_dns_zone: str
    ingress_class_name: str = Field(pattern="^" + _LABEL + "$")
    ingress_namespace: str = Field(pattern="^" + _LABEL + "$")
    ingress_controller_label: str = Field(pattern="^" + _LABEL + "$")
    min_nodes: int = Field(default=0, ge=0, le=100, strict=True)

    _dns_zone = field_validator("public_dns_zone")(_hostname)

    @property
    def platform_config(self) -> dict[str, Any]:
        config: dict[str, Any] = json.loads(self.platform_config_json)
        return config

    @model_validator(mode="after")
    def _validate_installation(self) -> FoundationBinding:
        from loom.nebius_platform_render import validate_environment

        config = self.platform_config
        if not isinstance(config, dict):
            raise ValueError("foundation configuration must be an object")
        if config.get("schema_version") != "loom.nebius-platform.v1":
            raise ValueError("foundation must be the operator's standalone installation")
        validate_environment(config)
        if config.get("regional_execution_targets"):
            raise ValueError("managed environments require one primary-region foundation")
        if self.min_nodes > config["capacity_policy"]["max_nodes"]:
            raise ValueError("warm floor exceeds the shared pool maximum")
        return self


class EnvironmentRegistrationV1(_Contract):
    """One environment's protected identity and desired deployment generation."""

    schema_version: Literal["loom.nebius-environment-registration.v1"] = (
        "loom.nebius-environment-registration.v1"
    )
    environment_id: UUID
    incarnation: UUID
    owner_user_id: UUID | None
    owner_team_id: UUID
    scope: EnvironmentScope
    kind: EnvironmentKind
    slug: str = Field(min_length=1, max_length=54, pattern="^" + _LABEL + "$")
    cluster_id: str = Field(pattern=_PROVIDER_ID)
    physical_pool_id: str = Field(pattern=_PROVIDER_ID)
    application_namespace: str = Field(pattern="^" + _LABEL + "$")
    execution_namespace: str = Field(pattern="^" + _LABEL + "$")
    build_namespace: str = Field(pattern="^" + _LABEL + "$")
    public_host: str
    target_id: str = Field(pattern="^" + _LABEL + "$")
    binding_mode: Literal["generated", "imported"] = "generated"
    candidate_id: UUID | None = None
    deployment_generation: int = Field(default=1, ge=1, strict=True)
    desired_state: Literal["active", "suspended", "destroyed"] = "active"

    _public_host = field_validator("public_host")(_hostname)

    @property
    def namespaces(self) -> tuple[str, str, str]:
        return self.application_namespace, self.execution_namespace, self.build_namespace

    @model_validator(mode="after")
    def _identity(self) -> EnvironmentRegistrationV1:
        for value in (self.environment_id, self.incarnation, self.owner_user_id,
                      self.owner_team_id, self.candidate_id):
            if value is not None and value.int == 0:
                raise ValueError("identity UUID must not be nil")
        if self.scope == "personal":
            if self.kind != "development" or self.owner_user_id is None:
                raise ValueError("personal environments require development and an owner user")
            if self.slug in _RESERVED_SLUGS:
                raise ValueError("personal slug is reserved for shared infrastructure")
            expected_application = "loom-dev-" + self.slug
        else:
            if self.slug != _SHARED_SLUGS[self.kind]:
                raise ValueError("shared slug must match its environment kind")
            expected_application = "loom-" + self.slug
        if len(set(self.namespaces)) != 3:
            raise ValueError("environment namespaces must be distinct")
        if self.build_namespace != self.execution_namespace + "-build":
            raise ValueError("build namespace must extend the execution binding")
        if self.binding_mode == "generated" and (
            self.application_namespace != expected_application
            or self.execution_namespace != "loom-run-" + self.incarnation.hex
            or self.target_id != "env-" + self.incarnation.hex
        ):
            raise ValueError("generated physical names must match environment identity")
        return self


class EnvironmentOperationV1(_Contract):
    """Public operation state; pending creation is never execution readiness."""

    operation_id: UUID
    environment_id: UUID
    deployment_generation: int = Field(ge=1, strict=True)
    action: Literal["create", "destroy_retained"]
    phase: Literal["pending", "running", "blocked", "completed"]
    error_code: str | None = None
    execution_enabled: Literal[False] = False


class EnvironmentCreateRequestV1(_Contract):
    slug: str = Field(min_length=1, max_length=54, pattern="^" + _LABEL + "$")
    candidate_id: UUID

    @model_validator(mode="after")
    def _personal_only(self) -> EnvironmentCreateRequestV1:
        if self.slug in _RESERVED_SLUGS or self.candidate_id.int == 0:
            raise ValueError("create requires a personal slug and candidate identity")
        return self


class EnvironmentStatusV1(_Contract):
    registration: EnvironmentRegistrationV1
    operation: EnvironmentOperationV1 | None


class EnvironmentOperationRequestV1(_Contract):
    """Only retained teardown is available before shared-execution lifecycle."""

    action: Literal["destroy_retained"]
    expected_generation: int = Field(ge=1, strict=True)


def new_environment_registration(
    foundation: FoundationBinding,
    *,
    environment_id: UUID,
    incarnation: UUID,
    owner_user_id: UUID | None,
    owner_team_id: UUID,
    slug: str,
    scope: EnvironmentScope = "personal",
    kind: EnvironmentKind = "development",
) -> EnvironmentRegistrationV1:
    """Derive fresh bindings; no caller-provided namespace or pool overrides."""
    config = foundation.platform_config
    execution_namespace = "loom-run-" + incarnation.hex
    return EnvironmentRegistrationV1(
        environment_id=environment_id, incarnation=incarnation,
        owner_user_id=owner_user_id, owner_team_id=owner_team_id,
        scope=scope, kind=kind, slug=slug,
        cluster_id=config["cluster_id"], physical_pool_id=config["execution_node_group_id"],
        application_namespace="loom-dev-" + slug if scope == "personal" else "loom-" + slug,
        execution_namespace=execution_namespace, build_namespace=execution_namespace + "-build",
        public_host=slug + "." + foundation.public_dns_zone, target_id="env-" + incarnation.hex,
    )
