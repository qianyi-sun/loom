import pytest

from loom.pipeline.artifact_commit import confined_relative_path, multipart_part_size


def test_output_upload_paths_and_parts_are_closed() -> None:
    assert confined_relative_path("results/aggregate.json") == "results/aggregate.json"
    assert multipart_part_size(10 * 1024 * 1024) >= 5 * 1024 * 1024
    for unsafe in ("../secret", "/absolute", "results//duplicate"):
        with pytest.raises(ValueError):
            confined_relative_path(unsafe)
