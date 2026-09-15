"""Disposable same-subid pruning proof, not installed recovery authority."""

import ctypes
import os
import subprocess
import sys
from pathlib import Path

from loom_capacity_executor.native_mapped_scratch import (
    capture_native_mapped_scratch,
    clean_native_mapped_scratch,
)
from loom_capacity_executor.native_rootless_runtime import NativeRootlessSpecV2


def main():
    mode = sys.argv[1]
    attempt = Path("/tmp/native-attempt")
    if mode == "outer":
        assert os.getuid() == 1000
        attempt.mkdir(mode=0o700)
        (attempt / "work").mkdir(mode=0o700)
        (attempt / "recovery.json").write_text("retained locator")
        subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
            "--state-dir=/tmp/scratch-mapper", sys.executable, "-I", __file__, sys.argv[2]],
            check=True, timeout=30)
        assert (attempt / "recovery.json").read_text() == "retained locator"
        return
    assert os.getuid() == 0
    spec = NativeRootlessSpecV2.model_validate_json(Path("/fixture/spec.json").read_bytes())
    material = attempt / "material"
    material.mkdir(mode=0o700)
    nested = material / "nested"
    nested.mkdir(mode=0o700)
    (nested / "data").write_text("private data")
    output = attempt / "work/output"
    output.mkdir(mode=0o700)
    (output / "data").write_text("subordinate output")
    os.chown(output / "data", 1000, 1000)
    os.chown(output, 1000, 1000)
    nested.chmod(0o500)
    snapshot = capture_native_mapped_scratch(spec)
    if mode == "mounted":
        foreign = Path("/tmp/foreign")
        foreign.mkdir(mode=0o700)
        (foreign / "keep").write_text("foreign inode")
        libc = ctypes.CDLL(None, use_errno=True)
        assert libc.mount(os.fsencode(foreign), os.fsencode(nested), None, 4096, None) == 0, ctypes.get_errno()
        try:
            try:
                clean_native_mapped_scratch(snapshot)
            except ValueError as error:
                assert "mount" in str(error)
            else:
                raise AssertionError("same-device bind mount was traversed")
            assert (foreign / "keep").read_text() == "foreign inode"
            assert foreign.stat().st_mode & 0o777 == 0o700
            print("same-device-bind-mount-preserved", flush=True)
        finally:
            assert libc.umount2(os.fsencode(nested), 2) == 0, ctypes.get_errno()
    elif mode == "clean":
        clean_native_mapped_scratch(snapshot)
        assert not material.exists() and not output.exists()
        print("subordinate-private-scratch-pruned", flush=True)
    else:
        raise AssertionError(mode)


if __name__ == "__main__":
    main()
