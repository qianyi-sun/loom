"""Capture the complete mapped identity before subordinate scratch is created."""

import io
from importlib import import_module
from pathlib import Path

import pytest


def _kernel(monkeypatch, uid_map, gid_map=None):
    monkeypatch.setattr("os.geteuid", lambda: 0)
    monkeypatch.setattr("os.getegid", lambda: 0)
    values = {"uid_map": uid_map, "gid_map": uid_map if gid_map is None else gid_map}
    monkeypatch.setattr(Path, "open", lambda path, *args: io.BytesIO(values[path.name]))


def test_complete_uid_and_gid_maps_are_separate_immutable_observations(monkeypatch):
    module = import_module("loom_capacity_executor.native_identity_mapping")
    _kernel(monkeypatch, b"         0      24850          1\n1 100000 65536\n",
        b"0 24851 1\n1 200000 32768\n32769 300000 32768\n")
    observed = module.observe_native_mapped_identity()
    assert [(r.inside, r.outside, r.count) for r in observed.uid_ranges] == [(0, 24850, 1), (1, 100000, 65536)]
    assert [(r.inside, r.outside, r.count) for r in observed.gid_ranges] == [
        (0, 24851, 1), (1, 200000, 32768), (32769, 300000, 32768)]
    with pytest.raises(AttributeError):
        observed.uid_ranges = ()
    with pytest.raises(AttributeError):
        observed.uid_ranges[0].outside = 0


@pytest.mark.parametrize("tail", [
    b"malformed\n", b"1 100000 0\n", b"1 0 1\n", b"1 24850 1\n",
    b"0 100000 1\n", b"1 100000 10\n5 200000 10\n",
    b"1 100000 10\n20 100005 10\n", b"1 4294967295 1\n",
    b"4294967295 100000 1\n", b"1 4294967294 2\n", b"4294967294 100000 2\n",
    b"1 -1 1\n", b"1 +100000 1\n", b"1 100000 1 trailing\n",
    b"1 " + b"9" * 17000 + b" 1\n",
])
@pytest.mark.parametrize("invalid_map", ["uid_map", "gid_map"])
def test_guard_rejects_invalid_subordinate_ranges_not_only_root(monkeypatch, tail, invalid_map):
    # Exercise the existing pre-unpack/mutation guard, not just a new parser.
    module = import_module("loom_capacity_executor.native_mapper_capabilities")
    valid = b"0 24850 1\n1 100000 65536\n"
    invalid = b"0 24850 1\n" + tail
    _kernel(monkeypatch, invalid if invalid_map == "uid_map" else valid,
        invalid if invalid_map == "gid_map" else valid)
    with pytest.raises(RuntimeError, match="mapping"):
        module._require_mapped_root()


def test_mapping_read_is_bounded_and_fails_closed_on_io_error(monkeypatch):
    module = import_module("loom_capacity_executor.native_identity_mapping")
    _kernel(monkeypatch, b"0 24850 1\n")

    class FailedRead(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 16385
            raise OSError("kernel mapping unavailable")

    monkeypatch.setattr(Path, "open", lambda *args: FailedRead())
    with pytest.raises(OSError, match="unavailable"):
        module.observe_native_mapped_identity()


def test_root_only_and_highest_valid_subordinate_id_are_recorded(monkeypatch):
    module = import_module("loom_capacity_executor.native_identity_mapping")
    _kernel(monkeypatch, b"0 24850 1\n", b"0 24851 1\n4294967294 4294967294 1\n")
    observed = module.observe_native_mapped_identity()
    assert len(observed.uid_ranges) == 1
    assert observed.gid_ranges[-1].outside == 4294967294
