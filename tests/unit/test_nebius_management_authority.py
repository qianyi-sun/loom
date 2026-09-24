"""Bootstrap authority is frozen into owned children, never inferred from names."""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.unit.test_nebius_environment_contract import foundation_from, registration_for
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

INSTALLATION = "30000000-0000-4000-8000-000000000001"


def authority():
    from loom.nebius_management_authority import ManagementNamespaceAuthority

    return ManagementNamespaceAuthority(installation_id=INSTALLATION, namespace="loom-nebius-management")


@pytest.mark.parametrize("change", [
    {"installation_id": "00000000-0000-0000-0000-000000000000"},
    {"namespace": "kube-system"}, {"subject": "admin"},
])
def test_authority_rejects_invalid_identity_or_caller_selected_subject(change):
    from loom.nebius_management_authority import ManagementNamespaceAuthority

    with pytest.raises(ValueError):
        ManagementNamespaceAuthority.model_validate({
            "installation_id": INSTALLATION, "namespace": "loom-nebius-management", **change,
        })


def test_child_steps_grant_owned_namespace_access_before_cloud_and_secrets(platform_inputs):
    from loom.nebius_environment_contract import FoundationBinding
    from loom.nebius_environment_render import render_environment
    from loom_service.environment_management.steps import creation_steps

    config, candidate, profile = platform_inputs
    legacy = foundation_from(config)
    foundation = FoundationBinding.model_validate({
        **legacy.model_dump(), "namespace_authority": authority(),
    })
    row = registration_for(foundation, "alice")
    rendered = render_environment(row, candidate, foundation, profile=profile, keyring={},
                                  repo_root=Path(__file__).resolve().parents[2])
    steps = creation_steps(rendered)
    assert [step.payload["kind"] for step in steps[:6]] == ["Namespace"] * 3 + ["RoleBinding"] * 3
    for step in steps[:3]:
        assert step.payload["metadata"]["labels"]["loom.nebius/namespace-installation"] == INSTALLATION
    for namespace, step in zip(row.namespaces, steps[3:6], strict=True):
        assert step.payload["metadata"]["namespace"] == namespace
        assert step.payload["subjects"] == [{
            "kind": "ServiceAccount", "namespace": "loom-nebius-management", "name": "loom-management-provisioner",
        }]
        assert step.payload["roleRef"] == {
            "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
            "name": "loom-management-30000000000040008000000000000001-resources",
        }
    old = render_environment(row, candidate, legacy, profile=profile, keyring={},
                             repo_root=Path(__file__).resolve().parents[2])
    assert len(old.files["00-namespaces.yaml"]) == 3
    assert all("loom.nebius/namespace-installation" not in doc["metadata"]["labels"]
               for doc in old.files["00-namespaces.yaml"])


def test_namespace_authority_cannot_adopt_imported_bindings(platform_inputs):
    from loom.nebius_environment_contract import FoundationBinding
    from loom.nebius_environment_render import render_environment

    config, candidate, profile = platform_inputs
    foundation = FoundationBinding.model_validate({
        **foundation_from(config).model_dump(), "namespace_authority": authority(),
    })
    row = registration_for(foundation, "alice").model_copy(update={"binding_mode": "imported"})
    with pytest.raises(ValueError, match="authority cannot adopt imported"):
        render_environment(row, candidate, foundation, profile=profile, keyring={},
                           repo_root=Path(__file__).resolve().parents[2])
