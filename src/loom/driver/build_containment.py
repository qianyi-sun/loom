"""Fail-closed guard for image builds on containment-required workers (#1146).

Increment 1 (#1145) binds every *runtime* trial/sandbox container into the
Slurm job cgroup via ``--cgroup-parent``. Image **builds** cannot be contained
the same way: ``docker-py``'s ``images.build()`` exposes no ``cgroup_parent``,
so a build's ``RUN`` steps execute in a host-daemon container **outside** the
job cgroup and could escape onto shared non-exclusive nodes (MinIO/k8s on
double-duty OLDLAB, other Slurm users on GB10).

Rather than run an uncontained build, a containment-required worker **refuses**
to build at runtime and requires the image to be pre-built / cached (the shared
trial-image registry, #547). Contained workers can still *run* any pre-built or
pulled image — they just cannot *build* one on a packed node.
"""

from __future__ import annotations


class ImageBuildForbiddenError(RuntimeError):
    """Raised when a build is attempted on a containment-required worker."""


def forbid_build_when_contained(require_containment: bool, image_tag: str) -> None:
    """Fail closed if a runtime image build is attempted under containment.

    ``require_containment`` is the worker's ``require_cgroup_parent`` signal
    (set only for non-exclusive Slurm workers). When set, the image must be
    pre-built/cached; building it here would escape the job cgroup.
    """
    if not require_containment:
        return
    raise ImageBuildForbiddenError(
        f"refusing to build image {image_tag!r} on a containment-required "
        "(non-exclusive Slurm) worker: image builds run outside the job "
        "cgroup and could escape onto shared nodes (#896/#1146). Pre-build "
        "and cache the image (trial-image registry) so no build runs on a "
        "packed node."
    )
