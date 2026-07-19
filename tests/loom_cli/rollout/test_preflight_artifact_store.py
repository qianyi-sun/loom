from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    ImageArtifactSet,
    ImageDescriptor,
)
from loom_cli.rollout.manifest_readiness import ManifestArtifact
from loom_cli.rollout.preflight_artifact_store import (
    PreflightArtifactStore,
    PreflightArtifactStoreError,
)


def _images() -> ImageArtifactSet:
    descriptors = {
        name: ImageDescriptor(
            image_id=f"sha256:{index:064x}",
            revision="a" * 40,
            os="linux",
            architecture="amd64",
            entrypoint=("node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs")
            if name == "loom-staging-admin-browser-smoke"
            else (),
        )
        for index, (name, _dockerfile) in enumerate(ALL_BUILD_IMAGES, 1)
    }
    return ImageArtifactSet(
        descriptors=descriptors,
        plan_digest="b" * 64,
        artifact_digest="c" * 64,
    )


def _manifests(images: ImageArtifactSet) -> ManifestArtifact:
    rollout_names = {name for name, _dockerfile in ALL_BUILD_IMAGES if "browser" not in name}
    rendered = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: exact\n"
    return ManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=__import__("hashlib").sha256(rendered.encode()).hexdigest(),
        resource_count=1,
        resource_set_digest="d" * 64,
        image_identities={
            name: digest for name, digest in images.image_digests.items() if name in rollout_names
        },
        artifact_digest="e" * 64,
    )


def _publish(store: PreflightArtifactStore):
    images = _images()
    return store.publish(
        candidate_sha="a" * 40,
        candidate_tree="f" * 40,
        mutation_epoch=8,
        images=images,
        manifests=_manifests(images),
        migration_plan_sha256="1" * 64,
        browser_report_schema_sha256="2" * 64,
    )


def test_store_publishes_and_reuses_exact_private_artifacts(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")

    first = _publish(store)
    second = _publish(store)

    assert second == first == store.read(first.bundle_digest)
    assert first.rendered_manifest_path.read_text().startswith("apiVersion")
    assert first.descriptor_path.parent.name == first.bundle_digest
    for path in (first.descriptor_path, first.rendered_manifest_path):
        assert path.stat().st_uid == os.geteuid()
        assert path.stat().st_mode & 0o777 == 0o600
    assert first.descriptor_path.parent.stat().st_mode & 0o777 == 0o700


def test_store_rejects_content_and_path_authority_drift(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    publication = _publish(store)
    publication.rendered_manifest_path.write_text("changed\n")
    with pytest.raises(PreflightArtifactStoreError, match="drift"):
        store.read(publication.bundle_digest)

    publication.rendered_manifest_path.unlink()
    publication.rendered_manifest_path.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(PreflightArtifactStoreError, match="invalid"):
        store.read(publication.bundle_digest)


def test_store_rejects_descriptor_schema_and_identity_confusion(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    publication = _publish(store)
    record = json.loads(publication.descriptor_path.read_bytes())
    record["unknown"] = True
    publication.descriptor_path.write_text(json.dumps(record))

    with pytest.raises(PreflightArtifactStoreError, match="descriptor"):
        store.read(publication.bundle_digest)


def test_store_rejects_cross_artifact_image_drift(tmp_path: Path) -> None:
    images = _images()
    manifests = _manifests(images)
    drifted = ManifestArtifact(
        rendered_yaml=manifests.rendered_yaml,
        rendered_sha256=manifests.rendered_sha256,
        resource_count=manifests.resource_count,
        resource_set_digest=manifests.resource_set_digest,
        image_identities={**dict(manifests.image_identities), "loom-service": "sha256:" + "9" * 64},
        artifact_digest=manifests.artifact_digest,
    )
    with pytest.raises(PreflightArtifactStoreError, match="binding"):
        PreflightArtifactStore(tmp_path / "state").publish(
            candidate_sha="a" * 40,
            candidate_tree="f" * 40,
            mutation_epoch=8,
            images=images,
            manifests=drifted,
            migration_plan_sha256="1" * 64,
            browser_report_schema_sha256="2" * 64,
        )
