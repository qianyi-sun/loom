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
    assert documents[-1]["kind"] == "ClusterRoleBinding"
    assert documents[-1]["subjects"] == [
        {
            "kind": "ServiceAccount",
            "name": "loom-rollout-rehearsal",
            "namespace": "loom-rollout-system",
        }
    ]


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
