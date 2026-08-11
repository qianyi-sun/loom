from __future__ import annotations

import pytest

from loom.pipeline.artifact_commit import confined_relative_path


@pytest.mark.parametrize("path", ["../secret", "/absolute", "a\\b", "a//b", "./a"])
def test_untrusted_paths_fail_before_object_identity_exists(path: str) -> None:
    with pytest.raises(ValueError):
        confined_relative_path(path)
