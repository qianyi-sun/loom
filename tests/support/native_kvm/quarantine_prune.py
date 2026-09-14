"""Disposable root filesystem mechanism; no Slurm or installed cleanup authority."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from loom_capacity_executor.native_quarantine_prune import (
    NativeQuarantineIdentity,
    _mount_id,
    prune_native_quarantine,
)


def main():
    mode = sys.argv[1]
    base = Path(tempfile.mkdtemp(prefix="native-prune-", dir="/tmp"))
    base.chmod(0o755)
    root, foreign = base / "quarantine", base / "foreign"
    root.mkdir(mode=0o700)
    foreign.mkdir(mode=0o700)
    os.chown(root, 24850, 24851)
    (root / "recovery.json").write_bytes(b"locator")
    (root / "recovery.json").chmod(0o400)
    os.chown(root / "recovery.json", 24850, 24851)
    (foreign / "sentinel").write_bytes(b"not-owned-by-attempt")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    mounted = False
    try:
        observed = os.fstat(descriptor)
        identity = NativeQuarantineIdentity(observed.st_dev, observed.st_ino, _mount_id(descriptor),
            ((24850, 1), (100000, 65536)), ((24851, 1), (200000, 65536)))
        if mode == "mounted":
            (root / "mount").mkdir()
            os.chown(foreign, 24850, 24851)
            subprocess.run(["mount", "--bind", str(foreign), str(root / "mount")], check=True, timeout=5)
            mounted = True
            assert (root / "mount").stat().st_dev == observed.st_dev
        elif mode == "foreign":
            (root / "other-owner").write_bytes(b"retain")
            os.chown(root / "other-owner", 99999, 99999)
        elif mode == "unprivileged":
            os.setgroups([])
            os.setgid(24851)
            os.setuid(24850)
        else:
            (root / "subordinate").mkdir()
            (root / "subordinate/data").write_bytes(b"mapped-data")
            os.chown(root / "subordinate/data", 100001, 200001)
            (root / "subordinate/data").chmod(0o000)
            os.chown(root / "subordinate", 100000, 200000)
            (root / "subordinate").chmod(0o000)
        try:
            prune_native_quarantine(descriptor, identity=identity)
        except ValueError as error:
            expected = {"mounted": "mount", "foreign": "ownership", "unprivileged": "initial host root"}
            assert mode in expected and expected[mode] in str(error)
        else:
            assert mode == "clean" and sorted(item.name for item in root.iterdir()) == ["recovery.json"]
        assert (root / "recovery.json").read_bytes() == b"locator"
        if mode != "unprivileged":
            assert (foreign / "sentinel").read_bytes() == b"not-owned-by-attempt"
        print("quarantine-prune-" + mode + "-verified", flush=True)
    finally:
        if mounted:
            subprocess.run(["umount", str(root / "mount")], check=True, timeout=5)
        os.close(descriptor)


if __name__ == "__main__":
    main()
