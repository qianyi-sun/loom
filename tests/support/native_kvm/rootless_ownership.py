"""Disposable mapping/ownership probe; never a native worker entrypoint."""

import os
import subprocess
import sys
from pathlib import Path


def main():
    root = Path("/tmp/native-ownership-output")
    artifact = root / "build" / "artifact"
    mode = sys.argv[1]
    if mode == "feature":
        assert os.getuid() == 1000
        for descriptor in (3, 4):
            try:
                os.fstat(descriptor)
            except OSError:
                pass
            else:
                raise AssertionError("fixture worker inherited a private IO channel")
        artifact.parent.mkdir(mode=0o700)
        artifact.write_bytes(b"fixture-artifact")
    elif mode == "mapped":
        assert os.getuid() == 0
        root.mkdir(mode=0o700)
        os.chown(root, 1000, 1000)
        subprocess.run([sys.executable, __file__, "feature"], user=1000, group=1000,
            extra_groups=(), check=True, timeout=10)
        assert artifact.read_bytes() == b"fixture-artifact"
        print("mapped-helper-read-private-output", flush=True)
    elif mode == "outer":
        assert os.getuid() == 1000
        subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--state-dir=/tmp/rootless-probe",
            sys.executable, __file__, "mapped"], check=True, timeout=20)
        try:
            artifact.read_bytes()
        except PermissionError:
            print("outer-helper-cannot-read-private-output", flush=True)
        else:
            raise AssertionError("outer helper unexpectedly accessed mapped private output")
    else:
        raise AssertionError("unknown fixture mode")


if __name__ == "__main__":
    main()
