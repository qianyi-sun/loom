from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

from loom_cli.rollout.image_readiness import (
    ALL_BUILD_IMAGES,
    REHEARSAL_POSTGRES_ENTRYPOINT,
    REHEARSAL_POSTGRES_IMAGE,
    ROLLOUT_IMAGES,
    ImageArtifactSet,
    ImageDescriptor,
    inspect_exact_images,
)
from loom_cli.rollout.manifest_readiness import ManifestArtifact, inspect_rendered_manifests
from loom_cli.rollout.migration_manifest_readiness import (
    build_migration_manifest_artifact,
)
from loom_cli.rollout.preflight_artifact_store import (
    PreflightArtifactStore,
    PreflightArtifactStoreError,
)
from loom_cli.rollout.preflight_contract import CheckContext, CheckOperation
from loom_cli.rollout.preflight_registered_checks import (
    build_preflight_artifact_publication_check,
)
from loom_cli.rollout.production_defaults_readiness import ProductionDefaultsArtifact


def _images() -> ImageArtifactSet:
    descriptors = {
        name: ImageDescriptor(
            image_id=f"sha256:{index:064x}",
            revision="a" * 40,
            os="linux",
            architecture="amd64",
            entrypoint=("node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs")
            if name == "loom-staging-admin-browser-smoke"
            else (REHEARSAL_POSTGRES_ENTRYPOINT if name == REHEARSAL_POSTGRES_IMAGE else ()),
        )
        for index, (name, _dockerfile) in enumerate(ALL_BUILD_IMAGES, 1)
    }
    return ImageArtifactSet(
        descriptors=descriptors,
        plan_digest="b" * 64,
        artifact_digest="c" * 64,
    )


def _manifests(images: ImageArtifactSet) -> ManifestArtifact:
    rollout_names = {name for name, _dockerfile in ROLLOUT_IMAGES}
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
        migration=_migration(images),
        production_defaults=_production_defaults(),
        migration_plan_sha256="1" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="2" * 64,
    )


def _production_defaults(*, candidate_tree: str = "f" * 40) -> ProductionDefaultsArtifact:
    payload = {
        "schema_version": 1,
        "candidate_sha": "a" * 40,
        "candidate_tree": candidate_tree,
        "environment": "staging",
        "yibuapi_sync": None,
        "providers": [],
    }
    digest = (
        __import__("hashlib")
        .sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        .hexdigest()
    )
    return ProductionDefaultsArtifact(
        schema_version=1,
        candidate_sha="a" * 40,
        candidate_tree=candidate_tree,
        environment="staging",
        yibuapi_sync=None,
        providers=(),
        artifact_digest=digest,
    )


@dataclass(frozen=True)
class _Result:
    returncode: int
    stdout: str


def _migration(
    images: ImageArtifactSet,
    *,
    candidate_tree: str = "f" * 40,
    image_tag: str = "staging-aaaaaaa",
    migration_plan_sha256: str = "1" * 64,
    migration_target_revision: str = "0067",
):
    return build_migration_manifest_artifact(
        lambda _manifest: _Result(0, ""),
        candidate_sha="a" * 40,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
        image_id=images.image_digests["loom-control-plane"],
        namespace="loom-staging",
        migration_plan_sha256=migration_plan_sha256,
        migration_target_revision=migration_target_revision,
    )


def _docker(argv, _cwd):
    if tuple(argv[:2]) == ("docker", "run"):
        return _Result(0, "")
    tag = str(argv[-1])
    name = tag.split(":", 1)[0]
    index = next(
        index
        for index, (expected, _dockerfile) in enumerate(ALL_BUILD_IMAGES, 1)
        if expected == name
    )
    entrypoint: list[str] = []
    if name == "loom-staging-admin-browser-smoke":
        entrypoint = ["node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs"]
    elif name == REHEARSAL_POSTGRES_IMAGE:
        entrypoint = list(REHEARSAL_POSTGRES_ENTRYPOINT)
    return _Result(
        0,
        json.dumps(
            [
                {
                    "Id": f"sha256:{index:064x}",
                    "Os": "linux",
                    "Architecture": "amd64",
                    "Config": {
                        "Entrypoint": entrypoint,
                        "Labels": {"org.opencontainers.image.revision": "a" * 40},
                    },
                }
            ]
        ),
    )


def _loadable_artifacts():
    image_tag = "staging-aaaaaaa"
    images = inspect_exact_images(_docker, image_tag=image_tag, resolved_sha="a" * 40)
    containers = "\n".join(
        f"        - name: {name}\n          image: {name}:{image_tag}"
        for name, _dockerfile in ROLLOUT_IMAGES
    )
    rendered = (
        "apiVersion: apps/v1\n"
        "kind: Deployment\n"
        "metadata:\n"
        "  name: exact\n"
        "  namespace: loom-staging\n"
        "spec:\n"
        "  template:\n"
        "    spec:\n"
        "      containers:\n"
        f"{containers}\n"
    )
    manifests = inspect_rendered_manifests(
        rendered,
        image_tag=image_tag,
        namespace="loom-staging",
        image_digests=images.image_digests,
    )
    return image_tag, images, manifests


def test_store_publishes_and_reuses_exact_private_artifacts(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")

    first = _publish(store)
    second = _publish(store)

    assert second == first == store.read(first.bundle_digest)
    assert first.rendered_manifest_path.read_text().startswith("apiVersion")
    assert first.descriptor_path.parent.name == first.bundle_digest
    for path in (
        first.descriptor_path,
        first.rendered_manifest_path,
        first.migration_manifest_path,
        first.production_defaults_path,
    ):
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


def test_store_rejects_migration_artifact_drift(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    publication = _publish(store)
    publication.migration_manifest_path.write_text("changed\n")

    with pytest.raises(PreflightArtifactStoreError, match="drift"):
        store.read(publication.bundle_digest)


def test_store_rejects_production_defaults_artifact_drift(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    publication = _publish(store)
    publication.production_defaults_path.write_text("{}\n")

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
            migration=_migration(images),
            production_defaults=_production_defaults(),
            migration_plan_sha256="1" * 64,
            migration_target_revision="0067",
            browser_report_schema_sha256="2" * 64,
        )


def test_store_rejects_unbounded_migration_target(tmp_path: Path) -> None:
    images = _images()
    with pytest.raises(PreflightArtifactStoreError, match="input binding"):
        PreflightArtifactStore(tmp_path / "state").publish(
            candidate_sha="a" * 40,
            candidate_tree="f" * 40,
            mutation_epoch=8,
            images=images,
            manifests=_manifests(images),
            migration=_migration(images),
            production_defaults=_production_defaults(),
            migration_plan_sha256="1" * 64,
            migration_target_revision="head",
            browser_report_schema_sha256="2" * 64,
        )


def test_store_loads_one_exact_publication_without_rebuild_or_render(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    image_tag, images, manifests = _loadable_artifacts()
    publication = store.publish(
        candidate_sha="a" * 40,
        candidate_tree="f" * 40,
        mutation_epoch=8,
        images=images,
        manifests=manifests,
        migration=_migration(
            images,
            image_tag=image_tag,
            migration_target_revision="0066",
        ),
        production_defaults=_production_defaults(),
        migration_plan_sha256="1" * 64,
        migration_target_revision="0066",
        browser_report_schema_sha256="2" * 64,
    )

    loaded = store.load_exact(
        candidate_sha="a" * 40,
        candidate_tree="f" * 40,
        mutation_epoch=8,
        image_tag=image_tag,
        namespace="loom-staging",
        image_run=_docker,
    )

    assert loaded.publication == publication
    assert loaded.images == images
    assert loaded.manifests == manifests


def test_store_rejects_ambiguous_exact_publications(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    image_tag, images, manifests = _loadable_artifacts()
    for migration_digest in ("1" * 64, "3" * 64):
        store.publish(
            candidate_sha="a" * 40,
            candidate_tree="f" * 40,
            mutation_epoch=8,
            images=images,
            manifests=manifests,
            migration=_migration(
                images,
                image_tag=image_tag,
                migration_plan_sha256=migration_digest,
                migration_target_revision="0066",
            ),
            production_defaults=_production_defaults(),
            migration_plan_sha256=migration_digest,
            migration_target_revision="0066",
            browser_report_schema_sha256="2" * 64,
        )

    with pytest.raises(PreflightArtifactStoreError, match="ambiguous"):
        store.load_exact(
            candidate_sha="a" * 40,
            candidate_tree="f" * 40,
            mutation_epoch=8,
            image_tag=image_tag,
            namespace="loom-staging",
            image_run=_docker,
        )


def test_publication_check_is_the_single_tier_one_publication_boundary(tmp_path: Path) -> None:
    store = PreflightArtifactStore(tmp_path / "state")
    image_tag, images, manifests = _loadable_artifacts()
    check = build_preflight_artifact_publication_check(
        store=store,
        image_artifact=lambda: images,
        manifest_artifact=lambda: manifests,
        migration_manifest_artifact=lambda: _migration(
            images,
            image_tag=image_tag,
            migration_target_revision="0066",
        ),
        production_defaults_artifact=_production_defaults,
        candidate_sha="a" * 40,
        candidate_tree="f" * 40,
        mutation_epoch=8,
        migration_artifact=lambda: ("1" * 64, "0066"),
        expected_migration_policy_sha256="9" * 64,
        browser_report_schema_sha256="2" * 64,
    )
    context = CheckContext(
        {
            "browser.report-schema.sha256": "2" * 64,
            "candidate.sha": "a" * 40,
            "candidate.tree": "f" * 40,
            "migration.policy.sha256": "9" * 64,
            "staging.mutation-epoch": 8,
        }
    )

    outcome = check.operations[CheckOperation.PROBE](context)

    assert outcome.passed
    assert check.spec.tier == 1
    assert check.spec.dependencies == (
        "images.contract",
        "manifests.server-schema",
        "migration.plan",
        "migration.manifest",
        "browser.runtime",
        "production-defaults.plan",
    )
    assert outcome.evidence["production-defaults-digest"] == _production_defaults().artifact_digest
    loaded = store.load_exact(
        candidate_sha="a" * 40,
        candidate_tree="f" * 40,
        mutation_epoch=8,
        image_tag=image_tag,
        namespace="loom-staging",
        image_run=_docker,
    )
    assert outcome.evidence["bundle-digest"] == loaded.publication.bundle_digest
    assert loaded.publication.migration_plan_sha256 == "1" * 64
    assert loaded.publication.migration_target_revision == "0066"
    assert loaded.migration == _migration(
        images,
        image_tag=image_tag,
        migration_target_revision="0066",
    )
