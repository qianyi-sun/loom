"""BuildImagesStep coverage tests (#340, #365).

Locks the rollout-image set to every locally-tagged container image
referenced by a managed Deployment in ``deploy/k8s/*.yaml``. Prevents
a repeat of #365, where ``loom-web`` was left out of the driver's
build+load set and the web pod hit ImagePullBackOff after cluster-up.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom_cli.rollout.steps.s02_build_images import ROLLOUT_IMAGES


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
        self, image: str, dockerfile: str,
    ) -> None:
        path = _repo_root() / dockerfile
        assert path.is_file(), (
            f"ROLLOUT_IMAGES entry {image!r} points at {dockerfile!r} "
            f"which does not exist"
        )
