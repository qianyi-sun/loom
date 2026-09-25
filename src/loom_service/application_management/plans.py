"""Freeze qualified renderer results; this module is not an HTTP input adapter."""
from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from loom.nebius_application_contract import (
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
    SharedDevelopmentBindingV1,
)
from loom.nebius_application_render import RenderedApplication
from loom.nebius_environment_render import _envelope
from loom_service.environment_management.registry import ManagementError


def freeze_plan(prepared: RenderedApplication, release: ApplicationReleaseV1,
                shared: SharedDevelopmentBindingV1) -> dict[str, Any]:
    """Trusted management must qualify publication/authority before this call.

    Validate binding and measured costs; this is not a malicious-manifest sandbox.
    Persist no secret values. Credentials will use a separate encrypted journal.
    """
    try:
        row = ApplicationRegistrationV1.model_validate(prepared.registration.model_dump())
        release = ApplicationReleaseV1.model_validate(release.model_dump())
        shared = SharedDevelopmentBindingV1.model_validate(shared.model_dump())
        if (row.release_id != release.release_id or row.data_environment_id != shared.data_environment_id
                or row.cluster_id != shared.cluster_id or release.schema_revision != shared.schema_revision
                or row.desired_state != "active" or prepared.platform_envelope != _envelope(prepared.files)
                or prepared.platform_envelope.storage_mib != 0
                or any(type(value) is not int or value < 0 for value in asdict(prepared.platform_envelope).values())):
            raise ValueError("inconsistent application plan")
        docs = [doc for group in prepared.files.values() for doc in group]
        if not docs or any(doc["kind"] not in {
            "Namespace", "ServiceAccount", "RoleBinding", "Deployment", "Service", "NetworkPolicy", "Ingress",
        } or (doc["metadata"]["name"] if doc["kind"] == "Namespace" else doc["metadata"].get("namespace"))
                != row.application_namespace for doc in docs):
            raise ValueError("non-application resource in plan")
        # Round-trip detaches nested caller-owned mappings from persisted intent.
        value: dict[str, Any] = json.loads(json.dumps({
            "schema_version": "loom.nebius-application-plan.v1", "registration": row.model_dump(mode="json"),
            "release": release.model_dump(mode="json"), "shared": shared.model_dump(mode="json"),
            "files": prepared.files, "platform_envelope": asdict(prepared.platform_envelope),
        }))
        return value
    except (ValueError, TypeError, KeyError) as exc:
        raise ManagementError("invalid_application_plan", 422) from exc
