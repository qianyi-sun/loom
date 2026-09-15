"""Only the disposable container's private cgroup namespace is modified."""

import os
import subprocess
from contextlib import ExitStack
from pathlib import Path

from loom_capacity_executor.native_cgroup_authority import require_native_cgroup_authority
from loom_capacity_executor.native_oci_material import _open_directory


def main():
    root = Path("/run/native-cgroups")
    root.mkdir()
    subprocess.run(["mount", "-t", "cgroup2", "none", str(root)], check=True, timeout=5)
    job = root / "slurm/job_101"
    step = job / "step_batch"
    try:
        step.mkdir(parents=True)
        with ExitStack() as stack:
            descriptor = _open_directory(root, stack)
            require_native_cgroup_authority(descriptor, relative=job.relative_to(root))
            # Real DAC must prevent an unrelated non-root host identity from
            # obtaining a migration descriptor, not merely reject fake metadata.
            child = os.fork()
            if child == 0:
                os.setgroups([])
                os.setgid(24851)
                os.setuid(24850)
                try:
                    os.open(step / "cgroup.procs", os.O_WRONLY)
                except PermissionError:
                    os._exit(0)
                os._exit(1)
            _, status = os.waitpid(child, 0)
            assert os.waitstatus_to_exitcode(status) == 0
            os.chown(step / "cgroup.procs", 24850, 24851)
            try:
                require_native_cgroup_authority(descriptor, relative=job.relative_to(root))
            except ValueError:
                pass
            else:
                raise AssertionError("real delegated migration control accepted")
        print("native-cgroup-authority-verified", flush=True)
    finally:
        step.rmdir()
        job.rmdir()
        job.parent.rmdir()
        subprocess.run(["umount", str(root)], check=True, timeout=5)


if __name__ == "__main__":
    main()
