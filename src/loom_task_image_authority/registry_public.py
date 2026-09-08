"""Shared secret-free validation of retained registry capability provenance."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import rfc8785
from pydantic import ConfigDict, TypeAdapter

from loom.db.schema import TaskImageRegistryCredentialGeneration
from loom.task_image_build_plan import TaskImageBuildPlanV1
from loom_task_image_authority.contracts import TaskImageRegistryCredentialV1

_CREDENTIAL_FIELDS: dict[str, TypeAdapter[Any]] = {
    name: TypeAdapter(field.rebuild_annotation(), config=ConfigDict(strict=True))
    for name, field in TaskImageRegistryCredentialV1.model_fields.items()
    if name != "bearer_token"
}


def validate_stored_registry_credential_public(
    row: TaskImageRegistryCredentialGeneration,
    *,
    registry_origin: str,
    plan: TaskImageBuildPlanV1,
) -> None:
    payload = row.response_public_json
    if (
        set(payload)
        != (set(TaskImageRegistryCredentialV1.model_fields) - {"bearer_token"})
        | {"bearer_token_sha256"}
        or hashlib.sha256(rfc8785.dumps(payload)).hexdigest() != row.response_sha256
    ):
        raise ValueError("credential canonical provenance changed")
    # Reuse the exact reviewed public field types without fabricating a bearer
    # token or touching its secret store. Cross-field bindings are checked below.
    try:
        for name, adapter in _CREDENTIAL_FIELDS.items():
            parsed = adapter.validate_json(json.dumps(payload[name]))
            if adapter.dump_python(parsed, mode="json") != payload[name]:
                raise ValueError("noncanonical credential public field")
    except ValueError as exc:
        raise ValueError("credential public schema changed") from exc
    mapping = {"attempt_id": "materialization_attempt_id", "bearer_token_sha256": "token_hash"}
    for name in (
        "credential_id",
        "request_id",
        "grant_id",
        "session_id",
        "session_generation",
        "attestation_generation",
        "attestation_sha256",
        "materialization_id",
        "attempt_id",
        "attempt_number",
        "lease_epoch",
        "builder_id",
        "component",
        "generation",
        "predecessor_credential_id",
        "lease_heartbeat_operation_id",
        "registry_origin",
        "registry_service",
        "registry_issuer",
        "repository",
        "registry_key_id",
        "bearer_token_sha256",
        "issued_at",
        "expires_at",
    ):
        actual = getattr(row, mapping.get(name, name))
        expected = payload[name]
        if isinstance(actual, datetime):
            expected = datetime.fromisoformat(expected)
        elif isinstance(actual, UUID):
            actual = str(actual)
        elif isinstance(actual, bytes):
            actual = actual.hex()
        if actual != expected:
            raise ValueError("credential row provenance changed")
    if (
        payload["purpose"] != "production"
        or payload["shadow_campaign_id"] is not None
        or payload["cpu_arch"] != plan.cpu_arch
        or payload["platform"] != plan.platform
        or payload["actions"] != ["pull", "push"]
        or payload["predecessor_generation"] != (row.generation - 1 if row.generation > 1 else None)
        or row.registry_origin != registry_origin
        or not row.issued_at < row.expires_at <= row.issued_at + timedelta(seconds=45)
        or row.issued_at.microsecond != 0
        or row.expires_at.microsecond != 0
        or (
            row.generation == 1
            and (
                row.predecessor_credential_id is not None
                or row.lease_heartbeat_operation_id is not None
            )
        )
        or (
            row.generation > 1
            and (
                row.predecessor_credential_id is None
                or row.predecessor_credential_id == row.credential_id
                or row.lease_heartbeat_operation_id is None
            )
        )
    ):
        raise ValueError("credential destination changed")
