"""Disposable fixture observation before pruning; never installed in a release.

The production mapped process has a single JSON stdout channel. Capture probe
output separately and require the outer fixture to read the bound evidence, so
a missing startup hook cannot silently turn this test into a false pass.
"""

import hashlib
import io
import json
import os
import runpy
import sys
import traceback
from contextlib import redirect_stdout
from pathlib import Path


def install():
    if sys.orig_argv[1:5] != ["-I", "-m", "loom_capacity_executor.native_rootless_runtime", "mapped"]:
        return
    identity = json.loads(Path("/fixtures/identity.json").read_bytes())
    if identity["root_stop"] not in {"monitored-rootless-outer-v2", "monitored-rootless-outer-v2-expiry"}:
        return

    def failure(kind, value, tb):
        # Synthetic fixture inputs only; never log locals or whole environments.
        text = "".join(traceback.format_exception(kind, value, tb, limit=12))
        Path("/result/native-mapped-error.txt").write_text(text[-8192:])

    sys.excepthook = failure
    from loom_capacity_executor import native_mapped_scratch

    original = native_mapped_scratch.clean_native_mapped_scratch

    def observed(snapshot):
        stream = io.StringIO()
        with redirect_stdout(stream):
            runpy.run_path("/test-support/rootless_full_io.py")["verify_runtime"]()
        mount_id = native_mapped_scratch._mount_id

        def checked_mount(descriptor):
            actual = mount_id(descriptor)
            if actual != snapshot.attempt.mount:
                Path("/result/native-mount-boundary.json").write_text(json.dumps({
                    "path": os.readlink(f"/proc/self/fd/{descriptor}"),
                    "expected": snapshot.attempt.mount, "actual": actual,
                }))
            return actual

        native_mapped_scratch._mount_id = checked_mount
        try:
            original(snapshot)
        finally:
            native_mapped_scratch._mount_id = mount_id
        spec = Path("/tmp/native-attempt/work/runtime-spec.json").read_bytes()
        evidence = {"spec_sha256": hashlib.sha256(spec).hexdigest(), "pruned": True,
            "observations": stream.getvalue()}
        with Path("/result/native-pre-prune.json").open("x") as destination:
            json.dump(evidence, destination)

    native_mapped_scratch.clean_native_mapped_scratch = observed


install()
