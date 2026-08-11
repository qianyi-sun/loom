from __future__ import annotations

from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.rehearsal_authority import (
    DEFAULT_REHEARSAL_AUTHORITY_MANIFEST,
    rehearsal_authority_digest,
)


def test_checked_in_rehearsal_authority_is_exact_and_bounded() -> None:
    digest = rehearsal_authority_digest()
    assert len(digest) == 64
    payload = DEFAULT_REHEARSAL_AUTHORITY_MANIFEST.read_text()
    assert "loom-rehearsal-" in payload
    assert "failurePolicy: Fail" in payload
    assert 'validationActions: ["Deny"]' in payload
    assert "cluster-admin" not in payload
    assert 'resourceNames: ["loom-rollout-rehearsal-observer"]' in payload
    documents = tuple(yaml.safe_load_all(payload))
    cluster_binding = next(
        document
        for document in documents
        if document["kind"] == "ClusterRoleBinding"
        and document["metadata"]["name"] == "loom-rollout-rehearsal"
    )
    assert cluster_binding["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-rollout-rehearsal",
            "namespace": "loom-rollout-system",
        }
    ]
    ingress_role, ingress_binding = documents[-2:]
    assert ingress_role["metadata"] == {
        "name": "loom-rollout-rehearsal-ingress-observer",
        "namespace": "ingress-nginx",
    }
    assert ingress_role["rules"][0]["resourceNames"] == ["ingress-nginx-controller"]
    assert ingress_role["rules"][0]["resources"] == ["endpoints", "services"]
    assert ingress_binding["roleRef"]["kind"] == "Role"
    assert all(rule["resources"] != ["nodes"] for rule in documents[2]["rules"])


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("failurePolicy: Fail", "failurePolicy: Ignore"),
        ('validationActions: ["Deny"]', 'validationActions: ["Audit"]'),
        ('resources: ["namespaces"]', 'resources: ["*"]'),
        ('verbs: ["bind"]', 'verbs: ["impersonate"]'),
        ('      - "pods"', '      - "nodes"'),
        ("request.userInfo.username ==", "request.userInfo.username !="),
        ("startsWith('loom-rehearsal-')", "startsWith('loom-')"),
        (
            "pod-security.kubernetes.io/enforce']\n        == 'restricted'",
            "pod-security.kubernetes.io/enforce']\n        == 'baseline'",
        ),
        ("automountServiceAccountToken: false", "automountServiceAccountToken: true"),
    ],
)
def test_rehearsal_authority_rejects_privilege_or_admission_drift(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    payload = DEFAULT_REHEARSAL_AUTHORITY_MANIFEST.read_text()
    assert old in payload
    path = tmp_path / "authority.yaml"
    path.write_text(payload.replace(old, new, 1))
    with pytest.raises(ValueError, match="contract drifted"):
        rehearsal_authority_digest(path)
