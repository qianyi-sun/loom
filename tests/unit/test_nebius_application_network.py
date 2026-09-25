"""Protected shared-side admission cannot become a personal shared-policy writer."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from tests.unit.test_nebius_application_authority import INSTALLATION, authority_for
from tests.unit.test_nebius_application_render import DATA_ID, inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


def test_shared_ingress_ands_managed_namespace_and_api_pod_on_exact_ports(platform_inputs):
    from loom.nebius_application_network import render_application_shared_access

    _, _, shared, foundation = inputs(platform_inputs)
    authority = authority_for(shared)
    before = [value.model_dump_json() for value in (authority, shared, foundation)]
    docs = render_application_shared_access(authority, shared, foundation)
    assert len(docs) == 3
    assert {doc["metadata"]["namespace"] for doc in docs} == {shared.platform_namespace}
    targets = {doc["spec"]["podSelector"]["matchLabels"]["app"]: doc for doc in docs}
    assert set(targets) == {"loom-postgres", "loom-control-plane", "loom-llm-gateway"}
    for app, port in (("loom-postgres", 5432), ("loom-control-plane", 8080), ("loom-llm-gateway", 9100)):
        doc = targets[app]
        assert doc["kind"] == "NetworkPolicy" and doc["apiVersion"] == "networking.k8s.io/v1"
        assert doc["spec"]["policyTypes"] == ["Ingress"] and "egress" not in doc["spec"]
        assert doc["spec"]["ingress"] == [{
            "from": [{
                "namespaceSelector": {
                    "matchLabels": {"loom.nebius/application-installation": INSTALLATION,
                                    "loom.nebius/data-environment-id": str(DATA_ID),
                                    "pod-security.kubernetes.io/enforce": "restricted"},
                    "matchExpressions": [{"key": key, "operator": "Exists"} for key in (
                        "loom.nebius/application-id", "loom.nebius/incarnation")],
                },
                "podSelector": {"matchLabels": {"app": "loom-service"}},
            }],
            "ports": [{"protocol": "TCP", "port": port}],
        }]
    assert len({doc["metadata"]["name"] for doc in docs}) == 3
    assert [value.model_dump_json() for value in (authority, shared, foundation)] == before


@pytest.mark.parametrize("change", ["data", "cluster", "namespace", "invalid-authority", "staging", "production"])
def test_shared_access_rejects_mismatched_or_non_development_bindings(platform_inputs, change):
    from loom.nebius_application_network import render_application_shared_access

    _, _, shared, foundation = inputs(platform_inputs)
    authority = authority_for(shared)
    if change == "data":
        authority = authority.model_copy(update={"data_environment_id": uuid4()})
    elif change == "cluster":
        authority = authority.model_copy(update={"cluster_id": "different"})
    elif change == "namespace":
        authority = authority.model_copy(update={"shared_namespace": "loom-foreign"})
    elif change == "invalid-authority":
        authority = authority.model_copy(update={"namespace": "kube-system"})
    else:
        foundation = foundation.model_copy(update={
            "platform_config_json": json.dumps(foundation.platform_config | {"environment": change}),
        })
    with pytest.raises(ValueError):
        render_application_shared_access(authority, shared, foundation)
