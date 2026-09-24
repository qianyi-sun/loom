"""Connected read-only installation checks reuse publication authority, not labels."""
from __future__ import annotations

import copy
import io
import json
import zipfile
from dataclasses import replace

import httpx
import pytest
from tests.ops.test_nebius_management_install import installation as installation
from tests.ops.test_nebius_management_supplied import material as material
from tests.unit.test_nebius_candidate_catalog import github_transport
from tests.unit.test_nebius_candidate_catalog import publication as publication
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def published_request(installation, publication):
    from loom_service.environment_management.deployment import ManagementDeployment

    request, _ = installation
    reference, _, payload, keyring, candidate = publication
    deployment = request.deployment.model_dump(mode="json")
    deployment["installation"].update(publications=[{**reference, "candidate_id": str(reference["candidate_id"])}],
                                       registry_prefix=candidate["registry_prefix"], keyring=json.loads(keyring))
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        profile = json.loads(archive.read("runtime-profile.json"))
    material = copy.deepcopy(request.material)
    material["loom-management-publications"]["token"] = "test-github-secret"
    return replace(request, deployment=ManagementDeployment.model_validate(deployment), candidate=candidate,
                   profile=profile, material=material)


async def publication_check(request, publication):
    from scripts.ops.nebius_management_prerequisites import qualify_management_publication

    reference, responses, payload, _, _ = publication
    async with httpx.AsyncClient(transport=github_transport(responses, payload), trust_env=False) as client:
        await qualify_management_publication(request=request, candidate_id=reference["candidate_id"], http=client)


async def test_selected_management_bytes_require_exact_approved_publication(installation, publication):
    request = published_request(installation, publication)
    await publication_check(request, publication)


@pytest.mark.parametrize("mutation", ["unselected", "image", "profile", "failed_check", "expired"])
async def test_supplied_candidate_cannot_substitute_for_github_publication(installation, publication, mutation):
    from scripts.ops.nebius_management_prerequisites import ManagementPrerequisiteError

    request = published_request(installation, publication)
    if mutation == "unselected":
        request = replace(request, deployment=request.deployment.model_copy(update={
            "installation": request.deployment.installation.model_copy(update={"publications": ()})}))
    elif mutation == "image":
        request.candidate["images"]["service"]["image_ref"] = "cr.eu-north1.nebius.cloud/other/service@sha256:" + "a" * 64
    elif mutation == "profile":
        request.profile["candidate_sha"] = "b" * 40
    elif mutation == "failed_check":
        publication[1]["commits/" + "b" * 40 + "/check-runs"]["check_runs"][0]["conclusion"] = "failure"
    else:
        publication[1]["actions/artifacts/123"]["expired"] = True
    with pytest.raises(ManagementPrerequisiteError) as error:
        await publication_check(request, publication)
    assert "test-github-secret" not in str(error.value)
