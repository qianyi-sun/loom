"""Live read-only prerequisites for the protected management installation."""
from __future__ import annotations

import json
from uuid import UUID

import httpx
from scripts.ops.nebius_management_install import ManagementInstallRequest

from loom.execution_image_admission import ImageAdmissionKeyring
from loom_service.environment_management.candidates import GitHubCandidateCatalog


class ManagementPrerequisiteError(RuntimeError):
    """Private credential, artifact and inventory contents must not escape."""


async def qualify_management_publication(*, request: ManagementInstallRequest, candidate_id: UUID,
                                         http: httpx.AsyncClient) -> None:
    try:
        installation = request.deployment.installation
        catalog = GitHubCandidateCatalog(http, token=request.material["loom-management-publications"]["token"],
            publications=list(installation.publications), registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        selected = await catalog.resolve(candidate_id)
        if selected.candidate != request.candidate or selected.profile != request.profile:
            raise ValueError()
    except Exception:
        raise ManagementPrerequisiteError("management publication unqualified") from None
