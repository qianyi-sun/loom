"""Personal lifecycle authority never inherits full-environment privileges."""
from __future__ import annotations

from uuid import UUID

import pytest

from tests.unit.test_nebius_application_render import inputs, named
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

INSTALLATION = "30000000-0000-4000-8000-000000000001"


def authority_for(shared):
    from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1

    return ApplicationNamespaceAuthorityV1(
        installation_id=INSTALLATION, namespace="loom-nebius-management",
        cluster_id=shared.cluster_id, data_environment_id=shared.data_environment_id,
        shared_namespace=shared.platform_namespace,
    )


@pytest.mark.parametrize("change", [
    {"installation_id": str(UUID(int=0))}, {"data_environment_id": str(UUID(int=0))},
    {"namespace": "kube-system"}, {"subject": "admin"}, {"shared_namespace": "bad/name"},
])
def test_application_authority_rejects_invalid_or_caller_selected_identity(platform_inputs, change):
    from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1

    authority = authority_for(inputs(platform_inputs)[2])
    with pytest.raises(ValueError):
        ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump() | change)


def test_application_renderer_emits_separate_authority_binding(platform_inputs):
    from loom.nebius_application_render import render_application
    from loom.nebius_environment_contract import FoundationBinding

    row, release, shared, foundation = inputs(platform_inputs)
    authority = authority_for(shared)
    # An installed legacy manager may coexist but grants no application authority.
    foundation = FoundationBinding.model_validate(foundation.model_dump() | {
        "namespace_authority": {"installation_id": INSTALLATION, "namespace": authority.namespace},
    })
    rendered = render_application(row, release, shared, foundation, authority=authority)
    labels = named(rendered, "Namespace", "loom-dev-alice")["metadata"]["labels"]
    assert labels["loom.nebius/application-installation"] == INSTALLATION
    assert labels["loom.nebius/application-id"] == str(row.application_id)
    assert labels["loom.nebius/data-environment-id"] == str(shared.data_environment_id)
    assert "loom.nebius/environment-id" not in labels
    assert "loom.nebius/namespace-installation" not in labels
    role = named(rendered, "RoleBinding", authority.name)
    assert role["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": authority.name + "-resources",
    }
    assert role["subjects"] == [{
        "kind": "ServiceAccount", "namespace": "loom-nebius-management", "name": "loom-application-provisioner",
    }]
    assert rendered.platform_envelope.storage_mib == 0


@pytest.mark.parametrize("change", [
    {"cluster_id": "different-cluster"}, {"shared_namespace": "loom-other"},
    {"data_environment_id": UUID("40000000-0000-4000-8000-000000000001")},
    {"namespace": "kube-system"},
])
def test_renderer_rejects_wrong_or_unchecked_application_authority(platform_inputs, change):
    from loom.nebius_application_render import render_application

    values = inputs(platform_inputs)
    authority = authority_for(values[2]).model_copy(update=change)
    with pytest.raises(ValueError):
        render_application(*values, authority=authority)


def test_application_authority_is_minimal_and_admission_precedes_bootstrap(platform_inputs):
    from loom.nebius_application_authority import render_application_authority

    authority = authority_for(inputs(platform_inputs)[2])
    docs = render_application_authority(authority)
    assert [doc["kind"] for doc in docs] == [
        "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding",
        "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding",
        "ClusterRole", "ClusterRole", "ClusterRoleBinding",
    ]
    roles = {doc["metadata"]["name"]: doc for doc in docs if doc["kind"] == "ClusterRole"}
    resources = roles[authority.name + "-resources"]["rules"]
    assert {resource for rule in resources for resource in rule["resources"]} == {
        "deployments", "services", "secrets", "serviceaccounts", "ingresses", "networkpolicies", "pods", "replicasets",
    }
    bootstrap = roles[authority.name + "-bootstrap"]["rules"]
    assert bootstrap == [
        {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "create"]},
        {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["rolebindings"], "verbs": ["get", "create"]},
        {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["clusterroles"],
         "resourceNames": [authority.name + "-resources"], "verbs": ["bind"]},
    ]
    assert not any("*" in rule[key] for rule in resources + bootstrap for key in ("resources", "verbs", "apiGroups"))
    assert all(doc["spec"]["failurePolicy"] == "Fail" for doc in docs if doc["kind"] == "ValidatingAdmissionPolicy")


def test_application_authority_renderer_revalidates_unchecked_input(platform_inputs):
    from loom.nebius_application_authority import render_application_authority

    authority = authority_for(inputs(platform_inputs)[2]).model_copy(update={"namespace": "kube-system"})
    with pytest.raises(ValueError):
        render_application_authority(authority)
