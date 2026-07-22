from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

import pytest
from scripts.ops import staging_rollout_data_reserve as reserve


def _findmnt(*, source: str = "/dev/nvme0n1p2", fstype: str = "ext4") -> str:
    return json.dumps(
        {
            "filesystems": [
                {"target": "/", "source": source, "fstype": fstype, "options": "rw,relatime"}
            ]
        }
    )


def _tune(*, reserved: int) -> str:
    return (
        "Block count:              1953230848\n"
        f"Reserved block count:     {reserved}\n"
        "Block size:               4096\n"
        "Reserved blocks uid:      0 (user root)\n"
        "Reserved blocks gid:      0 (group root)\n"
    )


class FakeRunner:
    def __init__(self, *, reserved: int = 97661542, source: str = "/dev/nvme0n1p2") -> None:
        self.reserved = reserved
        self.source = source
        self.calls: list[tuple[str, ...]] = []
        self.fail_readback = False
        self.active_unit = False
        self.device_stat = "61b0|0|6\n"

    def run(self, argv: Sequence[str]) -> reserve.CommandResult:
        call = tuple(argv)
        self.calls.append(call)
        if call[0] == "/usr/bin/findmnt":
            return reserve.CommandResult(0, _findmnt(source=self.source), "")
        if call[0] == "/usr/bin/stat":
            return reserve.CommandResult(0, self.device_stat, "")
        if call[0] == "/usr/bin/systemctl":
            output = "loom-staging-rollout-req-example-1.service loaded active running\n"
            return reserve.CommandResult(0, output if self.active_unit else "", "")
        if call[:2] == ("/usr/sbin/tune2fs", "-l"):
            value = self.reserved
            if self.fail_readback and any(item[1] == "-m" for item in self.calls):
                value += 20_000_000
            return reserve.CommandResult(0, _tune(reserved=value), "")
        if call[:2] == ("/usr/sbin/tune2fs", "-m"):
            self.reserved = round(1953230848 * reserve.TARGET_RESERVED_PERCENT / 100)
            return reserve.CommandResult(0, "", "")
        if call[:2] == ("/usr/sbin/tune2fs", "-r"):
            self.reserved = int(call[2])
            self.fail_readback = False
            return reserve.CommandResult(0, "", "")
        raise AssertionError(call)


def test_install_converges_fixed_large_ext4_device_and_is_idempotent(tmp_path: Path) -> None:
    runner = FakeRunner()

    first = reserve.install(runner, euid=0, lock_path=tmp_path / "lock")
    second = reserve.install(runner, euid=0, lock_path=tmp_path / "lock")

    assert first["ok"] is True
    assert first["changed"] is True
    assert first["previous_reserved_percent"] == pytest.approx(5.0, abs=0.01)
    assert first["released_bytes"] > 150_000_000_000
    assert second["changed"] is False
    assert sum(call[:2] == ("/usr/sbin/tune2fs", "-m") for call in runner.calls) == 1


def test_install_rejects_non_root_without_inspection(tmp_path: Path) -> None:
    runner = FakeRunner()

    with pytest.raises(reserve.ReserveError, match="requires root"):
        reserve.install(runner, euid=2005, lock_path=tmp_path / "lock")

    assert runner.calls == []


def test_install_refuses_active_rollout_before_filesystem_inspection(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.active_unit = True

    with pytest.raises(reserve.ReserveError, match="active rollout"):
        reserve.install(runner, euid=0, lock_path=tmp_path / "lock")

    assert [call[0] for call in runner.calls] == ["/usr/bin/systemctl"]


def test_install_rejects_device_owner_drift(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.device_stat = "61b0|0|0\n"

    with pytest.raises(reserve.ReserveError, match="block device identity"):
        reserve.install(runner, euid=0, lock_path=tmp_path / "lock")

    assert not any(call[:2] == ("/usr/sbin/tune2fs", "-m") for call in runner.calls)


@pytest.mark.parametrize(
    ("source", "reserved", "message"),
    (
        ("/dev/sda1", 97661542, "identity"),
        ("/dev/nvme0n1p2", 120000000, "supported transition"),
        ("/dev/nvme0n1p2", 39064617, "supported transition"),
    ),
)
def test_install_fails_closed_on_device_or_reserve_drift(
    tmp_path: Path, source: str, reserved: int, message: str
) -> None:
    runner = FakeRunner(reserved=reserved, source=source)

    with pytest.raises(reserve.ReserveError, match=message):
        reserve.install(runner, euid=0, lock_path=tmp_path / "lock")

    assert not any(call[:2] == ("/usr/sbin/tune2fs", "-m") for call in runner.calls)


def test_install_rolls_back_exact_reserved_block_count_on_bad_readback(tmp_path: Path) -> None:
    runner = FakeRunner()
    original = runner.reserved
    runner.fail_readback = True

    with pytest.raises(reserve.ReserveError, match="rolled back"):
        reserve.install(runner, euid=0, lock_path=tmp_path / "lock")

    assert runner.reserved == original
    assert any(call[:2] == ("/usr/sbin/tune2fs", "-r") for call in runner.calls)


def test_check_requires_exact_target_percent() -> None:
    runner = FakeRunner(reserved=round(1953230848 * 0.03))

    report = reserve.check(runner)

    assert report["ok"] is True
    assert report["reserved_percent"] == pytest.approx(3.0, abs=0.01)
