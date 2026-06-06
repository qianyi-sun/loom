from pathlib import Path, PurePosixPath

import pytest

from loom.driver.base import StartOptions
from loom.driver.fake import FakeDriver
from loom.errors import DriverNotStartedError
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import Allowlist, NoNetwork


@pytest.fixture
async def started_fake() -> FakeDriver:
    fake = FakeDriver()
    await fake.start(options=StartOptions())
    return fake


async def test_upload_writes_to_filesystem(
    started_fake: FakeDriver, tmp_path: Path,
):
    src = tmp_path / "x.txt"
    src.write_bytes(b"hello")
    await started_fake.upload(src, PurePosixPath("/workspace/x.txt"))
    assert started_fake.filesystem[PurePosixPath("/workspace/x.txt")] == b"hello"


async def test_download_reads_filesystem(
    started_fake: FakeDriver, tmp_path: Path,
):
    started_fake.filesystem[PurePosixPath("/workspace/y.txt")] = b"world"
    dst = tmp_path / "out.txt"
    await started_fake.download(PurePosixPath("/workspace/y.txt"), dst)
    assert dst.read_bytes() == b"world"


async def test_download_creates_parent_dirs(
    started_fake: FakeDriver, tmp_path: Path,
):
    started_fake.filesystem[PurePosixPath("/workspace/z.txt")] = b"z"
    dst = tmp_path / "nested" / "deeper" / "out.txt"
    await started_fake.download(PurePosixPath("/workspace/z.txt"), dst)
    assert dst.read_bytes() == b"z"


async def test_download_missing_raises(started_fake: FakeDriver, tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        await started_fake.download(PurePosixPath("/missing"), tmp_path / "x")


async def test_set_network_policy_updates_attr(started_fake: FakeDriver):
    await started_fake.set_network_policy(NoNetwork())
    assert started_fake.network_policy.kind == "no-network"
    await started_fake.set_network_policy(Allowlist(domains=("a.com",)))
    assert started_fake.network_policy.kind == "allowlist"


async def test_healthcheck_invokes_stub():
    calls: list[HealthcheckSpec | None] = []

    def stub(hc: HealthcheckSpec | None) -> None:
        calls.append(hc)

    fake = FakeDriver(healthcheck_stub=stub)
    await fake.start(options=StartOptions())
    spec = HealthcheckSpec(command="true", retries=1)
    await fake.run_healthcheck(spec)
    await fake.run_healthcheck(None)
    assert calls == [spec, None]


async def test_io_requires_started(tmp_path: Path):
    fake = FakeDriver()
    with pytest.raises(DriverNotStartedError):
        await fake.upload(tmp_path / "x", PurePosixPath("/y"))
    with pytest.raises(DriverNotStartedError):
        await fake.download(PurePosixPath("/y"), tmp_path / "x")
    with pytest.raises(DriverNotStartedError):
        await fake.set_network_policy(NoNetwork())
    with pytest.raises(DriverNotStartedError):
        await fake.run_healthcheck(None)
