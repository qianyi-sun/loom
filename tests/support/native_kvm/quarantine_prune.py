"""Disposable root filesystem mechanism; no Slurm or installed cleanup authority."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

from loom_capacity_executor.native_quarantine_prune import (
    NativeQuarantineIdentity,
    _mount_id,
    prune_native_quarantine,
)


def main():
    mode = sys.argv[1]
    if mode.startswith("journal-"):
        journal_main(mode.removeprefix("journal-"))
        return
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


def journal_main(interruption):
    from loom_capacity_executor import native_quarantine_journal as journal_module

    base = Path(tempfile.mkdtemp(prefix="native-journal-", dir="/run"))
    base.chmod(0o700)
    ledger, scratch = base / "ledger", base / "scratch"
    ledger.mkdir(mode=0o700)
    scratch.mkdir(mode=0o700)
    os.chown(scratch, 24850, 24851)
    attempt = scratch / ("attempt-" + str(uuid4()))
    attempt.mkdir(mode=0o700)
    os.chown(attempt, 24850, 24851)
    (attempt / "recovery.json").write_bytes(b"locator")
    (attempt / "recovery.json").chmod(0o400)
    os.chown(attempt / "recovery.json", 24850, 24851)
    (attempt / "data").write_bytes(b"subordinate")
    os.chown(attempt / "data", 100001, 200001)
    descriptor = os.open(attempt, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        observed = os.fstat(descriptor)
        identity = NativeQuarantineIdentity(observed.st_dev, observed.st_ino, _mount_id(descriptor),
            ((24850, 1), (100000, 65536)), ((24851, 1), (200000, 65536)))
    finally:
        os.close(descriptor)
    key = "a" * 64
    original_save = journal_module.NativeQuarantineJournal._save
    fired = False

    def interrupted_save(self, phase):
        nonlocal fired
        if phase == interruption and not fired:
            fired = True
            raise InterruptedError("simulated process death before progress publication")
        return original_save(self, phase)

    if interruption != "complete":
        journal_module.NativeQuarantineJournal._save = interrupted_save
        try:
            with journal_module.NativeQuarantineJournal(ledger, key=key, source=attempt, identity=identity) as journal:
                journal.reconcile()
        except InterruptedError:
            assert fired
        else:
            raise AssertionError("interruption boundary was not reached")
        finally:
            journal_module.NativeQuarantineJournal._save = original_save
    for _ in range(2):
        with journal_module.NativeQuarantineJournal(ledger, key=key, source=attempt, identity=identity) as journal:
            assert journal.reconcile() == "completed"
        assert not attempt.exists() and not (ledger / key / "attempt").exists()
    print("quarantine-prune-journal-" + interruption + "-verified", flush=True)


if __name__ == "__main__":
    main()
