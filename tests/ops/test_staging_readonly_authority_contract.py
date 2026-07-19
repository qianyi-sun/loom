from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "deploy/k8s/staging-rollout-readonly.yaml"


def _documents() -> dict[str, dict[str, object]]:
    documents = list(yaml.safe_load_all(MANIFEST.read_text()))
    return {str(document["kind"]): document for document in documents}


def test_readonly_preflight_rbac_is_namespace_scoped_and_non_mutating() -> None:
    documents = _documents()

    assert set(documents) == {"ServiceAccount", "Role", "RoleBinding"}
    assert all(
        document["metadata"]["namespace"] == "loom-staging"
        for document in documents.values()
    )
    account = documents["ServiceAccount"]
    assert account["metadata"]["name"] == "loom-rollout-readonly"
    assert account["automountServiceAccountToken"] is False

    rules = documents["Role"]["rules"]
    assert rules
    assert all(set(rule["verbs"]) <= {"get", "list", "watch"} for rule in rules)
    resources = {
        resource
        for rule in rules
        for resource in rule["resources"]
    }
    assert "*" not in resources
    assert "secrets" not in resources
    assert "serviceaccounts" not in resources
    assert "serviceaccounts/token" not in resources

    binding = documents["RoleBinding"]
    assert binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-rollout-readonly",
            "namespace": "loom-staging",
        },
    ]
    assert binding["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "Role",
        "name": "loom-rollout-readonly",
    }


def test_readonly_preflight_rbac_does_not_embed_a_token_or_cluster_binding() -> None:
    payload = MANIFEST.read_text()

    assert "kind: ClusterRole" not in payload
    assert "kind: ClusterRoleBinding" not in payload
    assert "kind: Secret" not in payload
    assert "token:" not in payload
