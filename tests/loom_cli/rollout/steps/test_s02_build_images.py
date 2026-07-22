"""BuildImagesStep coverage tests (#340, #365).

Locks the rollout-image set to every locally-tagged container image
referenced by a managed Deployment in ``deploy/k8s/*.yaml``. Prevents
a repeat of #365, where ``loom-web`` was left out of the driver's
build+load set and the web pod hit ImagePullBackOff after cluster-up.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.s02_build_images import (
    AUXILIARY_ROLLOUT_IMAGES,
    ROLLOUT_IMAGES,
    BuildImagesStep,
)


def _repo_root() -> Path:
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "deploy" / "k8s").is_dir():
            return parent
    raise RuntimeError("could not locate repo root from " + str(p))


def _locally_tagged_deployment_images() -> set[str]:
    """Return the set of *image names* (part before ``:``) referenced by
    a container in any managed Deployment or StatefulSet under
    ``deploy/k8s/``.

    Registry-qualified images (``foo/bar``, ``registry.k8s.io/...``) are
    skipped — those are pulled from a public registry, not built by the
    rollout driver.

    StatefulSet coverage is required because loom-worker renders as a
    StatefulSet on dynamic-storage profiles (#673).
    """
    root = _repo_root() / "deploy" / "k8s"
    names: set[str] = set()
    for path in sorted(root.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if not isinstance(doc, dict) or doc.get("kind") not in (
                "Deployment",
                "StatefulSet",
            ):
                continue
            spec = doc.get("spec") or {}
            template = spec.get("template") or {}
            pod_spec = template.get("spec") or {}
            for container in pod_spec.get("containers") or []:
                image = container.get("image") if isinstance(container, dict) else None
                if not image:
                    continue
                head = str(image).split(":", 1)[0]
                if "/" in head:
                    continue
                # Skip unqualified external images (e.g. `postgres:16`
                # on the loom-postgres StatefulSet — pulled from Docker
                # Hub, not built locally). Only loom-* images are
                # produced by the rollout driver.
                if not head.startswith("loom-"):
                    continue
                names.add(head)
    return names


class TestBuildImagesCoverage:
    def test_rollout_images_covers_every_managed_deployment(self) -> None:
        rendered = _locally_tagged_deployment_images()
        rollout = {name for name, _ in ROLLOUT_IMAGES}
        missing = rendered - rollout
        assert not missing, (
            f"rollout driver build set is missing images referenced by "
            f"managed Deployments: {sorted(missing)}. Add them to "
            f"ROLLOUT_IMAGES in s02_build_images.py (regression for #365)."
        )

    def test_rollout_images_do_not_include_stale_entries(self) -> None:
        """Prevent silent orphans: every image the driver builds must
        still be referenced by a managed Deployment. If a service is
        retired, its Dockerfile + ROLLOUT_IMAGES entry should go too."""
        rendered = _locally_tagged_deployment_images()
        rollout = {name for name, _ in ROLLOUT_IMAGES}
        stale = rollout - rendered
        assert not stale, (
            f"ROLLOUT_IMAGES has entries not referenced by any managed "
            f"Deployment: {sorted(stale)}. Remove them or add the "
            f"corresponding Deployment."
        )

    @pytest.mark.parametrize("image,dockerfile", list(ROLLOUT_IMAGES))
    def test_every_rollout_image_has_a_dockerfile(
        self,
        image: str,
        dockerfile: str,
    ) -> None:
        path = _repo_root() / dockerfile
        assert path.is_file(), (
            f"ROLLOUT_IMAGES entry {image!r} points at {dockerfile!r} which does not exist"
        )

    @pytest.mark.parametrize("image,dockerfile", list(AUXILIARY_ROLLOUT_IMAGES))
    def test_every_auxiliary_rollout_image_has_a_dockerfile(
        self,
        image: str,
        dockerfile: str,
    ) -> None:
        path = _repo_root() / dockerfile
        assert path.is_file(), f"auxiliary rollout image {image!r} points at missing {dockerfile!r}"

    def test_browser_acceptance_image_is_content_addressed_and_revision_bound(
        self,
    ) -> None:
        dockerfile = (_repo_root() / "deploy/Dockerfile.staging-admin-browser-smoke").read_text(
            encoding="utf-8"
        )

        assert (
            "mcr.microsoft.com/playwright:v1.61.1-noble@sha256:"
            "5b8f294aff9041b7191c34a4bab3ac270157a28774d4b0660e9743297b697e48" in dockerfile
        )
        assert "ARG LOOM_BUILD_SHA" in dockerfile
        assert 'org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in dockerfile
        assert "npm ci --prefix web --ignore-scripts" in dockerfile
        assert "install -d -o root -g root -m 0755 /opt/loom/web/scripts" in dockerfile
        assert "COPY --chmod=0444 web/scripts/staging-admin-browser-smoke.mjs" in dockerfile
        assert (
            'ENTRYPOINT ["node", "/opt/loom/web/scripts/'
            'staging-admin-browser-smoke.mjs"]' in dockerfile
        )

    def test_rehearsal_postgres_image_is_content_addressed_and_revision_bound(self) -> None:
        dockerfile = (_repo_root() / "deploy/Dockerfile.rehearsal-postgres").read_text(
            encoding="utf-8"
        )

        assert (
            "postgres:16@sha256:"
            "33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20" in dockerfile
        )
        assert "ARG LOOM_BUILD_SHA" in dockerfile
        assert 'org.opencontainers.image.revision="${LOOM_BUILD_SHA}"' in dockerfile


def test_service_image_build_is_bound_to_resolved_candidate_sha(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolved_sha = "c" * 40
    ctx = make_ctx(tmp_path, resolved_sha=resolved_sha)
    rollout_dir = tmp_path / "rollout"
    (rollout_dir / "01-worktree" / "src").mkdir(parents=True)
    step_path = rollout_dir / "02-build-images"
    step_path.mkdir()
    step_dir = StepDir(number=2, name="build-images", path=step_path)
    calls: list[dict[str, object]] = []

    def fake_build(_run, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(image_digests={"loom-service": "sha256:" + "a" * 64})

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s02_build_images.build_exact_images",
        fake_build,
    )

    result = BuildImagesStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert calls == [
        {
            "candidate_root": rollout_dir / "01-worktree" / "src",
            "image_tag": ctx.image_tag,
            "resolved_sha": resolved_sha,
        }
    ]


def test_verify_rejects_service_image_with_stale_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="c" * 40)
    step_dir = StepDir(number=2, name="build-images", path=tmp_path)
    calls: list[dict[str, object]] = []

    def fake_inspect(_run, **kwargs):
        calls.append(kwargs)
        raise ValueError("stale revision")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s02_build_images.inspect_exact_images",
        fake_inspect,
    )

    outcome = BuildImagesStep().verify(ctx, step_dir)

    assert outcome.name == "MISMATCH"
    assert calls == [
        {
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
        }
    ]


def test_run_rebuilds_service_image_with_stale_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="c" * 40)
    rollout_dir = tmp_path / "rollout"
    (rollout_dir / "01-worktree" / "src").mkdir(parents=True)
    step_path = rollout_dir / "02-build-images"
    step_path.mkdir()
    step_dir = StepDir(number=2, name="build-images", path=step_path)

    def fake_build(_run, **_kwargs):
        raise ValueError("rollout image contract failed for loom-service")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s02_build_images.build_exact_images",
        fake_build,
    )

    result = BuildImagesStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error == "rollout image contract failed for loom-service"
