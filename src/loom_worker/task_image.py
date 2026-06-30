"""Back-compat re-export of `loom.driver.task_image`.

The module was hoisted out of `loom_worker` so `loom_cli` can use the
same Dockerfile-build flow on the laptop path. Existing imports of
`loom_worker.task_image` keep working; new code should import from
`loom.driver.task_image` directly.
"""

from loom.driver.task_image import (  # noqa: F401
    DEFAULT_BUILD_CONTEXT_MAX_BYTES,
    DEFAULT_BUILD_CONTEXT_MAX_FILES,
    DEFAULT_TASK_IMAGE,
    ENV_BUILD_CONTEXT_MAX_BYTES,
    ENV_BUILD_CONTEXT_MAX_FILES,
    TaskImageBuildError,
    _enforce_build_context_limits,
    _resolve_build_context_path,
    _resolve_dockerfile_path,
    resolve_task_image,
    task_image_tag,
)
