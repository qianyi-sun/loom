"""#1146: image builds are refused on containment-required (non-exclusive)
Slurm workers, since a build's RUN steps run outside the job cgroup and could
escape onto shared nodes. Contained workers must use pre-built/cached images."""

from __future__ import annotations

import pytest

from loom.driver.build_containment import (
    ImageBuildForbiddenError,
    forbid_build_when_contained,
)


def test_forbid_build_when_contained_raises_when_required() -> None:
    with pytest.raises(ImageBuildForbiddenError, match="containment-required"):
        forbid_build_when_contained(True, "loom-task:abc")


def test_forbid_build_when_contained_noop_when_not_required() -> None:
    # Exclusive / unconstrained worker: builds are allowed.
    forbid_build_when_contained(False, "loom-task:abc")


class _FakeImages:
    def __init__(self) -> None:
        self.build_called = False

    def get(self, *_a: object, **_k: object) -> object:
        raise KeyError("not cached")  # force the build path

    def build(self, *_a: object, **_k: object) -> object:
        self.build_called = True
        return object(), []


class _FakeClient:
    def __init__(self) -> None:
        self.images = _FakeImages()


def test_layered_build_refuses_under_containment_without_building() -> None:
    from loom_worker.trial_cache import _build_layered_image_sync

    client = _FakeClient()
    with pytest.raises(ImageBuildForbiddenError):
        _build_layered_image_sync(
            client=client,
            tag="loom-trial:xyz",
            base_digest="sha256:deadbeef",
            install_script="echo hi",
            require_containment=True,
        )
    assert client.images.build_called is False  # guard fired before building


def test_layered_build_allowed_when_not_contained() -> None:
    from loom_worker.trial_cache import _build_layered_image_sync

    client = _FakeClient()
    _build_layered_image_sync(
        client=client,
        tag="loom-trial:xyz",
        base_digest="sha256:deadbeef",
        install_script="echo hi",
        require_containment=False,
    )
    assert client.images.build_called is True
