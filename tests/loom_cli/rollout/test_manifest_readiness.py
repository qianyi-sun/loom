from __future__ import annotations

import hashlib
import subprocess

import pytest
import yaml

from loom_cli.rollout.image_readiness import ALL_BUILD_IMAGES, ROLLOUT_IMAGES
from loom_cli.rollout.manifest_readiness import (
    ManifestRenderSession,
    inspect_rendered_manifests,
    pin_rendered_manifest_images,
    render_checkpoint_guard_field_ownership_payload,
)


def _digests() -> dict[str, str]:
    return {
        name: f"sha256:{hashlib.sha256(name.encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }


def _rendered(*, image_tag: str = "staging-1111111") -> str:
    containers = "\n".join(
        f"        - name: {name}\n          image: {name}:{image_tag}"
        for name, _path in ROLLOUT_IMAGES
    )
    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: exact-candidate
  namespace: loom-staging
spec:
  template:
    spec:
      containers:
{containers}
"""


def _rendered_with_lifecycle_cronjob() -> str:
    return (
        _rendered()
        + """---
apiVersion: batch/v1
kind: CronJob
metadata:
  labels:
    app: loom-staging-data-lifecycle
  name: loom-staging-data-lifecycle
  namespace: loom-staging
spec:
  concurrencyPolicy: Forbid
  schedule: '*/5 * * * *'
  suspend: false
"""
    )


def test_manifest_render_binds_every_local_image_to_exact_id() -> None:
    artifact = inspect_rendered_manifests(
        _rendered(),
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )

    assert artifact.resource_count == 1
    assert artifact.image_identities == {name: _digests()[name] for name, _path in ROLLOUT_IMAGES}
    assert len(artifact.artifact_digest) == 64


def test_registry_manifest_pinning_replaces_every_mutable_rollout_tag() -> None:
    registry_digests = {
        name: f"sha256:{hashlib.sha256((name + '-manifest').encode()).hexdigest()}"
        for name, _path in ALL_BUILD_IMAGES
    }

    pinned = pin_rendered_manifest_images(
        _rendered(),
        image_tag="staging-1111111",
        container_registry="192.168.50.13:5000",
        registry_digests=registry_digests,
    )
    document = yaml.safe_load(pinned)
    images = {
        container["name"]: container["image"]
        for container in document["spec"]["template"]["spec"]["containers"]
    }

    assert images == {
        name: f"192.168.50.13:5000/{name}@{registry_digests[name]}"
        for name, _path in ROLLOUT_IMAGES
    }
    artifact = inspect_rendered_manifests(
        pinned,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
        container_registry="192.168.50.13:5000",
        registry_digests=registry_digests,
    )
    assert set(artifact.image_identities) == {name for name, _path in ROLLOUT_IMAGES}


def test_registry_manifest_pinning_accepts_exact_standing_image_digest_set() -> None:
    registry_digests = {
        name: f"sha256:{hashlib.sha256((name + '-manifest').encode()).hexdigest()}"
        for name, _path in ROLLOUT_IMAGES
    }

    pinned = pin_rendered_manifest_images(
        _rendered(),
        image_tag="staging-1111111",
        container_registry="192.168.50.13:5000",
        registry_digests=registry_digests,
    )

    assert "loom-family-orchestrator@sha256:" in pinned
    assert "loom-egress-xds@sha256:" in pinned
    assert ":staging-1111111" not in pinned


def test_manifest_render_ignores_only_empty_yaml_documents() -> None:
    artifact = inspect_rendered_manifests(
        "---\n---\n" + _rendered() + "---\n",
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )

    assert artifact.resource_count == 1
    with pytest.raises(ValueError, match="resource set is invalid"):
        inspect_rendered_manifests(
            _rendered() + "---\nscalar\n",
            image_tag="staging-1111111",
            namespace="loom-staging",
            image_digests=_digests(),
        )


def test_manifest_render_rejects_missing_or_stale_local_image() -> None:
    missing = _rendered().replace(
        "        - name: loom-worker\n          image: loom-worker:staging-1111111\n",
        "",
    )
    with pytest.raises(ValueError, match="image set is incomplete"):
        inspect_rendered_manifests(
            missing,
            image_tag="staging-1111111",
            namespace="loom-staging",
            image_digests=_digests(),
        )
    with pytest.raises(ValueError, match="image tag drifted"):
        inspect_rendered_manifests(
            _rendered(image_tag="staging-2222222"),
            image_tag="staging-1111111",
            namespace="loom-staging",
            image_digests=_digests(),
        )


def test_manifest_render_binds_profile_enabled_image_subset() -> None:
    expected = frozenset(name for name, _path in ROLLOUT_IMAGES if name != "loom-worker")
    rendered = _rendered().replace(
        "        - name: loom-worker\n          image: loom-worker:staging-1111111\n",
        "",
    )

    artifact = inspect_rendered_manifests(
        rendered,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
        expected_image_names=expected,
    )

    assert set(artifact.image_identities) == expected


def test_manifest_render_rejects_unexpected_profile_image() -> None:
    expected = frozenset(name for name, _path in ROLLOUT_IMAGES if name != "loom-worker")

    with pytest.raises(ValueError, match="disabled rollout image"):
        inspect_rendered_manifests(
            _rendered(),
            image_tag="staging-1111111",
            namespace="loom-staging",
            image_digests=_digests(),
            expected_image_names=expected,
        )


def test_manifest_render_rejects_ambiguous_nonroot_cronjob_identity() -> None:
    rendered = (
        _rendered()
        + """---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: lifecycle
  namespace: loom-staging
spec:
  jobTemplate:
    spec:
      template:
        spec:
          securityContext:
            runAsNonRoot: true
          containers:
            - name: lifecycle
              image: external.example/maintenance:exact
"""
    )

    with pytest.raises(ValueError, match="non-root identity is ambiguous"):
        inspect_rendered_manifests(
            rendered,
            image_tag="staging-1111111",
            namespace="loom-staging",
            image_digests=_digests(),
        )


def test_manifest_render_accepts_explicit_container_nonroot_identity() -> None:
    rendered = (
        _rendered()
        + """---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: lifecycle
  namespace: loom-staging
spec:
  jobTemplate:
    spec:
      template:
        spec:
          securityContext:
            runAsNonRoot: true
          containers:
            - name: lifecycle
              image: external.example/maintenance:exact
              securityContext:
                runAsUser: 65532
"""
    )

    artifact = inspect_rendered_manifests(
        rendered,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )

    assert artifact.resource_count == 2


def test_manifest_render_rejects_declared_egress_denied_by_target_ingress() -> None:
    rendered = _rendered() + _network_policy_graph(allow_source=False)

    with pytest.raises(ValueError, match="network policy graph denies declared egress"):
        inspect_rendered_manifests(
            rendered,
            image_tag="staging-1111111",
            namespace="loom-staging",
            image_digests=_digests(),
        )


def test_manifest_render_accepts_symmetric_network_policy_graph() -> None:
    artifact = inspect_rendered_manifests(
        _rendered() + _network_policy_graph(allow_source=True),
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )

    assert artifact.resource_count == 5


def _network_policy_graph(*, allow_source: bool) -> str:
    allowed_app = "lifecycle" if allow_source else "other"
    return f"""---
apiVersion: v1
kind: Pod
metadata:
  name: lifecycle-pod
  namespace: loom-staging
  labels: {{app: lifecycle}}
spec:
  containers: [{{name: lifecycle, image: external.example/lifecycle:exact}}]
---
apiVersion: v1
kind: Pod
metadata:
  name: object-store-pod
  namespace: loom-staging
  labels: {{app: object-store}}
spec:
  containers: [{{name: object-store, image: external.example/object-store:exact}}]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: lifecycle
  namespace: loom-staging
spec:
  podSelector: {{matchLabels: {{app: lifecycle}}}}
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector: {{matchLabels: {{app: object-store}}}}
      ports: [{{port: 9000, protocol: TCP}}]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: object-store
  namespace: loom-staging
spec:
  podSelector: {{matchLabels: {{app: object-store}}}}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector: {{matchLabels: {{app: {allowed_app}}}}}
      ports: [{{port: 9000, protocol: TCP}}]
"""


def test_manifest_session_renders_once_and_server_validates_same_bytes() -> None:
    render_calls: list[object] = []
    server_inputs: list[str] = []

    def render() -> str:
        render_calls.append(object())
        return _rendered()

    def server_dry_run(payload: str):
        server_inputs.append(payload)
        return subprocess.CompletedProcess([], 0, "", "")

    session = ManifestRenderSession(
        render,
        server_dry_run,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )

    first = session.render()
    second = session.render()
    checked = session.server_validate()

    assert first is second is checked
    assert len(render_calls) == 1
    assert server_inputs == [_rendered()]


def test_manifest_server_validation_fails_closed_without_rerender() -> None:
    session = ManifestRenderSession(
        _rendered,
        lambda _payload: subprocess.CompletedProcess([], 1, "", "rejected"),
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )
    session.render()

    with pytest.raises(ValueError, match="server-side dry-run"):
        session.server_validate()


def test_checkpoint_field_ownership_retry_changes_only_lifecycle_suspension() -> None:
    original = list(yaml.safe_load_all(_rendered_with_lifecycle_cronjob()))
    guarded = list(
        yaml.safe_load_all(
            render_checkpoint_guard_field_ownership_payload(_rendered_with_lifecycle_cronjob())
        )
    )
    original[1]["spec"]["suspend"] = True

    assert guarded == original


@pytest.mark.parametrize(
    "rendered",
    [
        _rendered(),
        _rendered_with_lifecycle_cronjob().replace("suspend: false", "suspend: true"),
        _rendered_with_lifecycle_cronjob()
        + "---"
        + _rendered_with_lifecycle_cronjob().split("---", maxsplit=1)[1],
        _rendered_with_lifecycle_cronjob() + "---\nscalar\n",
    ],
    ids=("missing", "already-suspended", "duplicate", "non-resource"),
)
def test_checkpoint_field_ownership_retry_rejects_ambiguous_guard_state(
    rendered: str,
) -> None:
    with pytest.raises(ValueError, match="checkpoint guard"):
        render_checkpoint_guard_field_ownership_payload(rendered)


def test_manifest_field_ownership_does_not_retry_a_passing_original() -> None:
    payloads: list[str] = []

    def field_ownership_dry_run(payload: str):
        payloads.append(payload)
        return subprocess.CompletedProcess([], 0, "", "")

    session = ManifestRenderSession(
        _rendered_with_lifecycle_cronjob,
        lambda _payload: subprocess.CompletedProcess([], 0, "", ""),
        field_ownership_dry_run=field_ownership_dry_run,
        field_ownership_retry_render=render_checkpoint_guard_field_ownership_payload,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )
    artifact = session.render()

    assert session.field_ownership_validate() is artifact
    assert payloads == [_rendered_with_lifecycle_cronjob()]


def test_manifest_field_ownership_retries_exact_guard_held_payload() -> None:
    payloads: list[str] = []

    def field_ownership_dry_run(payload: str):
        payloads.append(payload)
        cronjob = next(
            document
            for document in yaml.safe_load_all(payload)
            if document.get("kind") == "CronJob"
        )
        return subprocess.CompletedProcess(
            [],
            0 if cronjob["spec"]["suspend"] is True else 1,
            "",
            "",
        )

    session = ManifestRenderSession(
        _rendered_with_lifecycle_cronjob,
        lambda _payload: subprocess.CompletedProcess([], 0, "", ""),
        field_ownership_dry_run=field_ownership_dry_run,
        field_ownership_retry_render=render_checkpoint_guard_field_ownership_payload,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )
    artifact = session.render()

    assert session.field_ownership_validate() is artifact
    assert [
        next(
            document["spec"]["suspend"]
            for document in yaml.safe_load_all(payload)
            if document.get("kind") == "CronJob"
        )
        for payload in payloads
    ] == [False, True]


def test_manifest_field_ownership_guard_retry_does_not_hide_other_conflicts() -> None:
    session = ManifestRenderSession(
        _rendered_with_lifecycle_cronjob,
        lambda _payload: subprocess.CompletedProcess([], 0, "", ""),
        field_ownership_dry_run=lambda _payload: subprocess.CompletedProcess(
            [], 1, "", "unrelated conflict"
        ),
        field_ownership_retry_render=render_checkpoint_guard_field_ownership_payload,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )
    session.render()

    with pytest.raises(ValueError, match="field-ownership dry-run"):
        session.field_ownership_validate()


def test_seeded_manifest_session_never_rerenders_exact_artifact() -> None:
    artifact = inspect_rendered_manifests(
        _rendered(),
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
    )
    render_calls: list[object] = []
    server_inputs: list[str] = []

    def render() -> str:
        render_calls.append(object())
        return "must-not-render"

    def server_dry_run(payload: str):
        server_inputs.append(payload)
        return subprocess.CompletedProcess([], 0, "", "")

    session = ManifestRenderSession(
        render,
        server_dry_run,
        image_tag="staging-1111111",
        namespace="loom-staging",
        image_digests=_digests(),
        artifact=artifact,
    )

    assert session.render() is artifact
    assert session.server_validate() is artifact
    assert not render_calls
    assert server_inputs == [_rendered()]
