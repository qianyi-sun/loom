from __future__ import annotations

import pytest

from loom_cli.rollout.readonly_authority import (
    ReadonlyAuthorityEvidence,
    readonly_authority_policy_digest,
)


def _evidence(**overrides: object) -> ReadonlyAuthorityEvidence:
    values: dict[str, object] = {
        "principal": "loom-rollout-readonly",
        "environment": "staging",
        "namespace": "loom-staging",
        "kubernetes_verbs": ("get", "list", "watch"),
        "kubernetes_resources": ("deployments", "pods", "services"),
        "http_methods": ("GET", "HEAD"),
        "capability_source_digest": "a" * 64,
    }
    values.update(overrides)
    return ReadonlyAuthorityEvidence(**values)  # type: ignore[arg-type]


def test_readonly_authority_accepts_only_non_mutating_non_secret_capabilities() -> None:
    evidence = _evidence()

    assert evidence.ready
    assert len(evidence.evidence_digest) == 64
    assert len(readonly_authority_policy_digest()) == 64


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("kubernetes_verbs", ("get", "create")),
        ("kubernetes_verbs", ("*",)),
        ("kubernetes_resources", ("pods", "secrets")),
        ("kubernetes_resources", ("*",)),
        ("http_methods", ("GET", "POST")),
    ),
)
def test_readonly_authority_reports_every_mutating_or_secret_grant(
    field: str,
    value: tuple[str, ...],
) -> None:
    assert not _evidence(**{field: value}).ready


def test_readonly_authority_rejects_cross_environment_identity() -> None:
    with pytest.raises(ValueError, match="invalid"):
        _evidence(environment="production")
