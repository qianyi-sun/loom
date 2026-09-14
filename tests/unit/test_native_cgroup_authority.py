"""Recovery-capable launch refuses writable historical cgroup escape paths."""

import os
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def cgroups(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_cgroup_authority")
    root = tmp_path / "cgroup"
    job = root / "system.slice/slurmstepd.scope/job_101"
    step = job / "step_batch/task_0"
    step.mkdir(parents=True)
    for directory in (step, *step.parents):
        if directory == tmp_path:
            break
        directory.chmod(0o755)
        for name in ("cgroup.procs", "cgroup.threads"):
            (directory / name).write_bytes(b"101\n")
            (directory / name).chmod(0o644)
    original = os.fstat

    def metadata(fd):
        value = original(fd)
        fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
        fields["st_uid"] = fields["st_gid"] = 0
        return SimpleNamespace(**fields)

    monkeypatch.setattr(module.os, "fstat", metadata)
    return module, root, job, step


@pytest.mark.parametrize("fault", ["exact", "ancestor", "job", "descendant", "procs", "threads", "owner", "mount", "symlink", "bound"])
def test_cgroup_authority_covers_ancestors_and_whole_job(cgroups, monkeypatch, fault):
    module, root, job, step = cgroups
    if fault in {"ancestor", "job", "descendant"}:
        {"ancestor": job.parent, "job": job, "descendant": step}[fault].chmod(0o775)
    elif fault in {"procs", "threads"}:
        (step / ("cgroup." + fault)).chmod(0o664)
    elif fault == "owner":
        original = module.os.fstat

        def metadata(fd):
            result = original(fd)
            if Path(os.readlink(f"/proc/self/fd/{fd}")) == step / "cgroup.procs":
                result.st_uid = 24850
            return result

        monkeypatch.setattr(module.os, "fstat", metadata)
    elif fault == "mount":
        original = module._mount_id
        monkeypatch.setattr(module, "_mount_id", lambda fd: original(fd) + int(Path(os.readlink(f"/proc/self/fd/{fd}")) == step))
    elif fault == "symlink":
        (step / "cgroup.procs").unlink()
        (step / "cgroup.procs").symlink_to(job / "cgroup.procs")
    elif fault == "bound":
        monkeypatch.setattr(module, "_MAX_NODES", 2)
    with module.ExitStack() as stack:
        root_fd = module._open_directory(root, stack)
        if fault == "exact":
            module.require_native_cgroup_authority(root_fd, relative=job.relative_to(root))
        else:
            with pytest.raises((ValueError, OSError)):
                module.require_native_cgroup_authority(root_fd, relative=job.relative_to(root))
