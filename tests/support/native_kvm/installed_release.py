"""Disposable real-ownership fixture; synthetic payload is not a tooling release."""

import hashlib
import os
import subprocess
import sys
from pathlib import Path

from loom_capacity_executor.native_installed_release import (
    NativeInstalledFileV1,
    NativeInstalledReleaseV1,
    verify_native_installed_release,
)
from loom_capacity_manager.contracts import canonical_bytes

ROOT = Path("/opt/native-release-fixture")
MANIFEST = ROOT / "manifest.json"


def main():
    if sys.argv[1] == "mapped":
        assert os.getuid() == 0
        # This is precisely why mapped st_uid cannot establish host-root trust.
        assert (ROOT / "rootfs.tar").stat().st_uid == 65534
        assert (ROOT / "rootfs.tar").read_bytes() == b"rootfs.tar"
        try:
            verify_native_installed_release(MANIFEST, expected_sha256=sys.argv[2],
                expected_source_sha="a" * 40, expected_platform="linux/amd64")
        except ValueError as error:
            assert "original" in str(error)
        else:
            raise AssertionError("mapped namespace was mistaken for host-root trust")
        print("mapped-consumption-without-host-root-trust-ok", flush=True)
        return

    assert os.getuid() == 0
    files = []
    for name in ("gvisor/runsc", "gvisor/containerd-shim-runsc-v1", "gvisor/gvisor-bin/checkpointgofer",
        "gvisor/gvisor-bin/gvisor_sentry", "gvisor/gvisor-bin/runsc-metric-server", "python/bin/python3",
        "python/lib/loom_capacity_executor/__init__.py", "rootlesskit", "rootfs.tar", "seccomp.json"):
        path = ROOT / name
        path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        content = name.encode()
        path.write_bytes(content)
        mode = 0o444 if name.endswith((".py", ".json", ".tar")) else 0o555
        path.chmod(mode)
        files.append(NativeInstalledFileV1(path=str(path), sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content), mode=mode))
    manifest = NativeInstalledReleaseV1(source_sha="a" * 40, platform="linux/amd64",
        runsc_root=str(ROOT / "gvisor"), python_root=str(ROOT / "python"),
        python=str(ROOT / "python/bin/python3"), rootlesskit=str(ROOT / "rootlesskit"),
        rootfs=str(ROOT / "rootfs.tar"), seccomp=str(ROOT / "seccomp.json"),
        files=tuple(sorted(files, key=lambda item: item.path)))
    wire = canonical_bytes(manifest)
    MANIFEST.write_bytes(wire)
    MANIFEST.chmod(0o444)
    digest = hashlib.sha256(wire).hexdigest()
    if sys.argv[1] == "untrusted-owner":
        os.chown(ROOT / "gvisor/gvisor-bin/gvisor_sentry", 1000, 1000)
    os.setgroups([])
    os.setgid(1000)
    os.setuid(1000)
    if sys.argv[1] == "untrusted-owner":
        try:
            verify_native_installed_release(MANIFEST, expected_sha256=digest,
                expected_source_sha="a" * 40, expected_platform="linux/amd64")
        except ValueError as error:
            assert "protected" in str(error)
        else:
            raise AssertionError("owner-controlled installed helper was accepted")
        print("owner-controlled-helper-rejected", flush=True)
        return
    result = verify_native_installed_release(MANIFEST, expected_sha256=digest,
        expected_source_sha="a" * 40, expected_platform="linux/amd64")
    assert result.manifest == manifest
    try:
        (ROOT / "rootfs.tar").write_bytes(b"replacement")
    except PermissionError:
        pass
    else:
        raise AssertionError("original worker could mutate protected material")
    print("original-uid-protected-release-verified", flush=True)
    subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
        "--state-dir=/tmp/native-release-mapping", sys.executable, "-I", __file__, "mapped", digest],
        check=True, timeout=20)


if __name__ == "__main__":
    main()
