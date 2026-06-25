from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions

pytestmark = pytest.mark.docker


@pytest.fixture
async def docker_driver() -> AsyncGenerator[object, None]:
    pytest.importorskip("docker")
    import docker
    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not available")
    from loom.driver.docker import DockerDriver
    d = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    await d.start(options=StartOptions())
    try:
        yield d
    finally:
        await d.stop(delete=True)


async def test_upload_file_roundtrip(docker_driver, tmp_path: Path):  # type: ignore[no-untyped-def]
    src = tmp_path / "in.txt"
    src.write_bytes(b"hello docker\n")
    await docker_driver.upload(src, PurePosixPath("/workspace/in.txt"))

    r = await docker_driver.exec("cat /workspace/in.txt")
    assert r.stdout == b"hello docker\n"


async def test_download_file(docker_driver, tmp_path: Path):  # type: ignore[no-untyped-def]
    await docker_driver.exec("echo from-container > /workspace/out.txt")
    dst = tmp_path / "out.txt"
    await docker_driver.download(PurePosixPath("/workspace/out.txt"), dst)
    assert dst.read_bytes() == b"from-container\n"


async def test_download_missing_raises(docker_driver, tmp_path: Path):  # type: ignore[no-untyped-def]
    with pytest.raises(FileNotFoundError):
        await docker_driver.download(PurePosixPath("/missing"), tmp_path / "x")


async def test_download_directory_raises_not_silent(docker_driver, tmp_path: Path):  # type: ignore[no-untyped-def]
    """Regression for Risk 2: get_archive on a directory returns the whole
    subtree as a multi-entry tar. Driver.download is single-file only;
    silently extracting just one entry would be a quiet data loss."""
    from loom.errors import DriverError
    await docker_driver.exec("mkdir -p /workspace/multi && "
                             "echo a > /workspace/multi/a.txt && "
                             "echo b > /workspace/multi/b.txt")
    with pytest.raises(DriverError, match="directory"):
        await docker_driver.download(PurePosixPath("/workspace/multi"), tmp_path / "out")


async def test_upload_creates_nested_parent(docker_driver, tmp_path: Path):  # type: ignore[no-untyped-def]
    """Spec §2.2: upload() must create parent dirs as needed."""
    src = tmp_path / "in.txt"
    src.write_bytes(b"x")
    await docker_driver.upload(src, PurePosixPath("/workspace/deep/nested/file.txt"))
    r = await docker_driver.exec("cat /workspace/deep/nested/file.txt")
    assert r.stdout == b"x"


async def test_upload_creates_parent_when_path_contains_spaces(
    docker_driver,
    tmp_path: Path,
):  # type: ignore[no-untyped-def]
    src = tmp_path / "in.txt"
    src.write_bytes(b"path with spaces\n")

    dst = PurePosixPath("/workspace/dir with spaces/file.txt")
    await docker_driver.upload(src, dst)

    r = await docker_driver.exec("cat '/workspace/dir with spaces/file.txt'")
    assert r.stdout == b"path with spaces\n"
