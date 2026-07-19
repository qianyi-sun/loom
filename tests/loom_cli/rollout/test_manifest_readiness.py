from __future__ import annotations

import hashlib
import subprocess

import pytest

from loom_cli.rollout.image_readiness import ALL_BUILD_IMAGES, ROLLOUT_IMAGES
from loom_cli.rollout.manifest_readiness import (
    ManifestRenderSession,
    inspect_rendered_manifests,
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
