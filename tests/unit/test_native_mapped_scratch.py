"""Mapped scratch pruning cannot escape its fixed attempt-local targets."""

import os
from importlib import import_module

import pytest

from tests.unit.test_native_material_launch import material_spec


def prepared(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_mapped_scratch")
    _runtime, spec, path, _digest = material_spec(tmp_path)
    monkeypatch.setattr(module, "_require_mapped_root", lambda: None)
    roots = [tmp_path / "material", tmp_path / "runsc", path.parent / "output", path.parent / "buildkit-run"]
    for root in roots:
        root.mkdir(mode=0o700)
        (root / "nested").mkdir(mode=0o700)
        (root / "nested/data").write_text("scratch")
    return module, module.capture_native_mapped_scratch(spec), roots


def test_only_fixed_scratch_removed_and_symlinks_never_followed(tmp_path, monkeypatch):
    module, spec, roots = prepared(tmp_path, monkeypatch)
    keep = tmp_path / "recovery.json"
    keep.write_text("locator")
    state = tmp_path / "work/rootlesskit"
    state.mkdir()
    (state / "keep").write_text("active mapper")
    (roots[0] / "link").symlink_to(state, target_is_directory=True)
    (roots[0] / "nested").chmod(0o500)

    module.clean_native_mapped_scratch(spec)

    assert all(not root.exists() for root in roots)
    assert keep.read_text() == "locator" and (state / "keep").read_text() == "active mapper"
    assert (tmp_path / "work/runtime-spec.json").is_file()


def test_unmapped_caller_rejected_before_any_delete(tmp_path, monkeypatch):
    module, spec, roots = prepared(tmp_path, monkeypatch)

    def unmapped():
        raise RuntimeError("unmapped")

    monkeypatch.setattr(module, "_require_mapped_root", unmapped)
    with pytest.raises(RuntimeError, match="unmapped"):
        module.clean_native_mapped_scratch(spec)
    assert all((root / "nested/data").read_text() == "scratch" for root in roots)


def test_same_device_mount_boundary_is_rejected(tmp_path, monkeypatch):
    module, spec, roots = prepared(tmp_path, monkeypatch)
    real = module._mount_id

    def mounted(fd):
        result = real(fd)
        return result + 1 if os.readlink(f"/proc/self/fd/{fd}") == str(roots[0] / "nested") else result

    monkeypatch.setattr(module, "_mount_id", mounted)
    with pytest.raises(ValueError, match="mount"):
        module.clean_native_mapped_scratch(spec)
    assert (roots[0] / "nested/data").read_text() == "scratch"


def test_entry_bound_stops_without_removing_unvisited_content(tmp_path, monkeypatch):
    module, spec, roots = prepared(tmp_path, monkeypatch)
    monkeypatch.setattr(module, "_MAX_ENTRIES", 1)
    with pytest.raises(ValueError, match="bound"):
        module.clean_native_mapped_scratch(spec)
    assert (roots[0] / "nested/data").read_text() == "scratch"


def test_replaced_attempt_is_not_adopted_at_cleanup(tmp_path, monkeypatch):
    module, snapshot, _roots = prepared(tmp_path, monkeypatch)
    retained = tmp_path.with_name(tmp_path.name + "-retained")
    tmp_path.rename(retained)
    tmp_path.mkdir(mode=0o700)
    (tmp_path / "keep").write_text("replacement")
    try:
        with pytest.raises(ValueError, match="changed"):
            module.clean_native_mapped_scratch(snapshot)
        assert (tmp_path / "keep").read_text() == "replacement"
        assert (retained / "material/nested/data").read_text() == "scratch"
    finally:
        (tmp_path / "keep").unlink()
        tmp_path.rmdir()
        retained.rename(tmp_path)


@pytest.mark.parametrize("parent", ["attempt", "workspace"])
def test_parent_must_remain_private_before_cleanup(tmp_path, monkeypatch, parent):
    module, snapshot, roots = prepared(tmp_path, monkeypatch)
    path = getattr(snapshot, parent).path
    path.chmod(0o777)
    try:
        with pytest.raises(ValueError, match="private"):
            module.clean_native_mapped_scratch(snapshot)
        assert all((root / "nested/data").read_text() == "scratch" for root in roots)
    finally:
        path.chmod(0o700)


def test_root_replacement_between_precheck_and_traversal_is_not_adopted(tmp_path, monkeypatch):
    module, snapshot, roots = prepared(tmp_path, monkeypatch)
    original = module.os.stat
    moved = tmp_path / "retained-material"
    swapped = False

    def replace_at_traversal(path, *args, **kwargs):
        nonlocal swapped
        if path == "material" and kwargs.get("dir_fd") is not None and not swapped:
            swapped = True
            roots[0].rename(moved)
            roots[0].mkdir(mode=0o700)
            (roots[0] / "foreign").write_text("preserve")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(module.os, "stat", replace_at_traversal)
    with pytest.raises(ValueError, match="changed"):
        module.clean_native_mapped_scratch(snapshot)
    assert (roots[0] / "foreign").read_text() == "preserve"
    assert (moved / "nested/data").read_text() == "scratch"


@pytest.mark.parametrize("bound", ["depth", "time"])
def test_depth_and_time_bound_retain_unvisited_content(tmp_path, monkeypatch, bound):
    module, snapshot, roots = prepared(tmp_path, monkeypatch)
    if bound == "depth":
        monkeypatch.setattr(module, "_MAX_DEPTH", 0)
    else:
        monkeypatch.setattr(module, "_MAX_SECONDS", 0)
    with pytest.raises(ValueError, match="bound"):
        module.clean_native_mapped_scratch(snapshot)
    assert all((root / "nested/data").read_text() == "scratch" for root in roots)
