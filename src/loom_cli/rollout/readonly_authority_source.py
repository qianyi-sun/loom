"""Server-observed source for the dedicated staging readonly authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Protocol
from uuid import UUID

from loom_cli.rollout.readonly_authority import ReadonlyAuthorityEvidence

_MAX_RESPONSE_BYTES = 1 << 20
_VALUE_RE = re.compile(r"^[a-z0-9*][a-z0-9*/.:-]{0,127}$")
_NON_RESOURCE_URL_RE = re.compile(r"^/[a-z0-9*][a-z0-9*/.:-]{0,126}$")
_SAFE_REVIEW_RESOURCES = frozenset(
    {
        ("authentication.k8s.io", "selfsubjectreviews"),
        ("authorization.k8s.io", "selfsubjectaccessreviews"),
        ("authorization.k8s.io", "selfsubjectrulesreviews"),
    }
)


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...


JsonCommandRunner = Callable[[Sequence[str], bytes], CommandResult]
ApplicationObservation = Callable[[], bytes]


def _object(payload: bytes) -> Mapping[str, object]:
    if not payload or len(payload) > _MAX_RESPONSE_BYTES:
        raise ValueError("readonly authority response is unavailable")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("readonly authority response is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("readonly authority response is invalid")
    return value


def _strings(
    value: object,
    *,
    allow_empty: bool = False,
    allow_blank_items: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty) or len(value) > 512:
        raise ValueError("readonly authority rule is invalid")
    rendered = tuple(value)
    if (
        any(
            not isinstance(item, str)
            or (not allow_blank_items and _VALUE_RE.fullmatch(item) is None)
            or (allow_blank_items and item and _VALUE_RE.fullmatch(item) is None)
            for item in rendered
        )
        or len(set(rendered)) != len(rendered)
    ):
        raise ValueError("readonly authority rule is invalid")
    return rendered


def _urls(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or len(value) > 512:
        raise ValueError("readonly non-resource rule is invalid")
    rendered = tuple(value)
    if (
        any(
            not isinstance(item, str)
            or _NON_RESOURCE_URL_RE.fullmatch(item) is None
            or ".." in item.split("/")
            for item in rendered
        )
        or len(set(rendered)) != len(rendered)
    ):
        raise ValueError("readonly non-resource rule is invalid")
    return rendered


def _subject_username(payload: bytes, *, namespace: str) -> str:
    body = _object(payload)
    status = body.get("status")
    if not isinstance(status, dict):
        raise ValueError("readonly subject review is incomplete")
    user_info = status.get("userInfo")
    if not isinstance(user_info, dict):
        raise ValueError("readonly subject review is incomplete")
    username = user_info.get("username")
    expected = f"system:serviceaccount:{namespace}:loom-rollout-readonly"
    if username != expected:
        raise ValueError("readonly subject identity drifted")
    return expected


def _protected_rules(payload: bytes) -> tuple[tuple[str, ...], tuple[str, ...], str]:
    body = _object(payload)
    status = body.get("status")
    if not isinstance(status, dict) or status.get("incomplete") is True:
        raise ValueError("readonly rules review is incomplete")
    evaluation_error = status.get("evaluationError")
    if evaluation_error not in {None, ""}:
        raise ValueError("readonly rules review failed")
    resource_rules = status.get("resourceRules")
    non_resource_rules = status.get("nonResourceRules", [])
    if not isinstance(resource_rules, list) or not isinstance(non_resource_rules, list):
        raise ValueError("readonly rules review is invalid")
    if not resource_rules or len(resource_rules) > 512 or len(non_resource_rules) > 512:
        raise ValueError("readonly rules review is invalid")

    protected_verbs: set[str] = set()
    protected_resources: set[str] = set()
    canonical_rules: list[dict[str, object]] = []
    for raw in resource_rules:
        if not isinstance(raw, dict):
            raise ValueError("readonly resource rule is invalid")
        verbs = _strings(raw.get("verbs"))
        api_groups = _strings(
            raw.get("apiGroups"),
            allow_empty=True,
            allow_blank_items=True,
        )
        resources = _strings(raw.get("resources"))
        resource_names = _strings(raw.get("resourceNames", []), allow_empty=True)
        pairs = {(group, resource) for group in api_groups for resource in resources}
        safe_review = bool(pairs) and pairs <= _SAFE_REVIEW_RESOURCES and set(verbs) <= {"create"}
        if not safe_review:
            protected_verbs.update(verbs)
            protected_resources.update(resources)
        canonical_rules.append(
            {
                "apiGroups": sorted(api_groups),
                "resourceNames": sorted(resource_names),
                "resources": sorted(resources),
                "verbs": sorted(verbs),
            }
        )
    for raw in non_resource_rules:
        if not isinstance(raw, dict):
            raise ValueError("readonly non-resource rule is invalid")
        verbs = _strings(raw.get("verbs"))
        urls = _urls(raw.get("nonResourceURLs"))
        protected_verbs.update(verbs)
        if "*" in urls:
            protected_resources.add("*")
        canonical_rules.append(
            {"nonResourceURLs": sorted(urls), "verbs": sorted(verbs)}
        )
    digest = hashlib.sha256(
        json.dumps(canonical_rules, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return tuple(sorted(protected_verbs)), tuple(sorted(protected_resources)), digest


def _application_identity(payload: bytes) -> tuple[tuple[str, ...], str]:
    body = _object(payload)
    if (
        body.get("auth_kind") != "readonly_probe"
        or body.get("credential_type") != "staging_readonly_probe"
        or body.get("principal_type") != "readonly_probe"
        or body.get("readonly_authority_version") != "v1"
        or body.get("scopes") != ["read:own"]
    ):
        raise ValueError("readonly application identity drifted")
    raw_methods = body.get("allowed_http_methods")
    if raw_methods != ["GET", "HEAD"]:
        raise ValueError("readonly application methods drifted")
    methods = ("GET", "HEAD")
    team_id = body.get("team_id")
    try:
        if not isinstance(team_id, str):
            raise ValueError
        UUID(team_id)
    except ValueError as exc:
        raise ValueError("readonly application team is invalid") from exc
    digest = hashlib.sha256(
        json.dumps(
            {
                "auth_kind": "readonly_probe",
                "credential_type": "staging_readonly_probe",
                "methods": list(methods),
                "principal_type": "readonly_probe",
                "scopes": ["read:own"],
                "team_id": team_id,
                "version": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return methods, digest


def probe_readonly_authority(
    run: JsonCommandRunner,
    *,
    kubeconfig: Path,
    namespace: str,
    application_observation: ApplicationObservation,
) -> ReadonlyAuthorityEvidence:
    """Read exact Kubernetes and application authority without protected mutation."""
    if (
        not kubeconfig.is_absolute()
        or ".." in kubeconfig.parts
        or namespace != "loom-staging"
    ):
        raise ValueError("readonly authority target is invalid")
    review_specs = (
        (
            "/apis/authentication.k8s.io/v1/selfsubjectreviews",
            {"apiVersion": "authentication.k8s.io/v1", "kind": "SelfSubjectReview"},
        ),
        (
            "/apis/authorization.k8s.io/v1/selfsubjectrulesreviews",
            {
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectRulesReview",
                "spec": {"namespace": namespace},
            },
        ),
    )
    responses: list[bytes] = []
    for uri, spec in review_specs:
        result = run(
            (
                "kubectl",
                "--kubeconfig",
                str(kubeconfig),
                "create",
                "--raw",
                uri,
                "--request-timeout=10s",
                "-f",
                "-",
            ),
            json.dumps(spec, sort_keys=True, separators=(",", ":")).encode(),
        )
        if result.returncode != 0 or not isinstance(result.stdout, str):
            raise ValueError("readonly authority server review failed")
        responses.append(result.stdout.encode())
    subject = _subject_username(responses[0], namespace=namespace)
    verbs, resources, rules_digest = _protected_rules(responses[1])
    methods, application_digest = _application_identity(application_observation())
    capability_digest = hashlib.sha256(
        json.dumps(
            {
                "application": application_digest,
                "rules": rules_digest,
                "subject": subject,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return ReadonlyAuthorityEvidence(
        principal="loom-rollout-readonly",
        environment="staging",
        namespace=namespace,
        kubernetes_verbs=verbs,
        kubernetes_resources=resources,
        http_methods=methods,
        capability_source_digest=capability_digest,
    )


__all__ = ["JsonCommandRunner", "probe_readonly_authority"]
