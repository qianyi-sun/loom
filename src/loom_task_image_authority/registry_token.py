"""Canonical repository identities for retained publication provenance."""

from __future__ import annotations

import hashlib
import re
from uuid import UUID

_COMPONENT_RE = re.compile(r"(?:task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})")
MAX_REGISTRY_BEARER_TOKEN_BYTES = 16 * 1024


def publication_repository(
    *,
    purpose: str,
    shadow_campaign_id: UUID | None,
    cpu_arch: str,
    attempt_id: UUID,
    component: str,
) -> str:
    """Derive the only production repository authorized for a component."""

    if purpose != "production" or shadow_campaign_id is not None:
        raise ValueError("registry publication is available only for production attempts")
    if cpu_arch not in {"x86_64", "arm64"}:
        raise ValueError("registry publication architecture is invalid")
    if type(attempt_id) is not UUID or attempt_id.int == 0:
        raise TypeError("registry publication attempt ID must be a nonzero UUID")
    if type(component) is not str or _COMPONENT_RE.fullmatch(component) is None:
        raise ValueError("registry publication component is invalid")

    if component == "task":
        component_segment = "task"
    else:
        component_sha256 = hashlib.sha256(component.encode("ascii")).hexdigest()
        component_segment = f"sidecar-sha256-{component_sha256}"
    return (
        f"loom-task-image-attempts/{cpu_arch}/{attempt_id}/"
        f"{component_segment}"
    )
