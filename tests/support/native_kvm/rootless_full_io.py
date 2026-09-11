"""Disposable full-build IO split; the synthetic authority stays outside mapping."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from loom_capacity_agent.build_admission import (
    BuildClaimRequestV1,
    BuildExecutionPermitV1,
    BuildOutcomeReceiptV1,
    BuildSourceContextV1,
)
from loom_capacity_agent.build_artifact_stream import BuildArtifactUploadReceiptV1
from loom_capacity_executor.native_allocated_io import scoped_native_allocated_io
from loom_capacity_executor.native_build_source import NativeStagedBuildSource
from loom_capacity_executor.native_outer_build import run_native_outer_build
from loom_capacity_executor.native_rootless_runtime import (
    NativeRootlessSpecV1,
    exec_native_rootless_runtime,
)
from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest


def launch():
    exec_native_rootless_runtime(Path("/tmp/native-work/runtime-spec.json"),
        expected_sha256=sys.argv[5], expected_parent_pid=int(sys.argv[2]),
        authority_fd=int(sys.argv[3]), artifact_fd=int(sys.argv[4]))


def verify_runtime():
    """Independent mapped readback; never repeat production runtime deletions."""
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
    layout = NativeRunscLayout(Path("/runtime/runsc"), Path("/tmp/runsc-state"), Path("/fixtures"), context.claim_digest)
    output = Path("/tmp/native-work/output")
    if expiry:
        pulse = output / "lifecycle-pulse"
        stopped = pulse.read_bytes()
        assert int(stopped) > 0, "deadline test never reached a live sandbox client"
        late = "late-" + layout.identity("pause")
        command = [*layout.command("start", "client")[:-1], late]
        try:
            assert subprocess.run(command, capture_output=True, timeout=5).returncode != 0
            assert pulse.read_bytes() == stopped
        finally:
            subprocess.run([*layout.command("delete", "client")[:-1], late], check=True, timeout=10)
        print("native-supervised-expiry-stopped-live-client", flush=True)
    else:
        assert list((output / "build/images").iterdir()) == []
        print("native-allocated-client-artifact-ok", flush=True)
    state = subprocess.run(layout.command("list", "pause"), check=True, capture_output=True, timeout=10)
    assert not json.loads(state.stdout)
    print("native-allocated-runtime-cleanup-ok", flush=True)


async def main():
    assert os.getuid() == 1000
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
    # Trusted disposable material preparation needs the same static UID mapping
    # to restore rootfs capabilities. It starts no feature/runtime process.
    runtime_workspace = Path("/tmp/native-work")
    runtime_workspace.mkdir(mode=0o700)
    subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
        "--state-dir=/tmp/rootless-preparation", sys.executable, "/test-support/execute.py", "prepare"],
        check=True, timeout=60)
    claim = BuildClaimRequestV1.model_validate_json(Path("/fixtures/claim.json").read_bytes())
    spec = NativeRootlessSpecV1(claim=claim, context=context, runsc="/runtime/runsc",
        state_root="/tmp/runsc-state", bundle_root="/fixtures", workspace="/tmp/native-work",
        max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2)
    wire = canonical_bytes(spec)
    spec_path = Path("/tmp/native-work/runtime-spec.json")
    spec_path.write_bytes(wire)
    spec_path.chmod(0o400)
    workspace = Path("/tmp/native-outer-io")
    workspace.mkdir(mode=0o700)

    class FixtureClient:
        calls = 0

        async def authorize_execution(self, request, *, worker_credential):
            assert worker_credential == "x" * 43
            self.calls += 1
            if expiry and self.calls > 1:
                await asyncio.Future()  # Independent mapped monitor must expire.
            now = datetime.now(UTC)
            return BuildExecutionPermitV1(request=request, request_digest=canonical_digest(request),
                issued_at=now, not_after=now + timedelta(seconds=10))

        async def upload_artifact(self, observed_claim, *, worker_credential, artifact, chunks):
            assert not expiry and observed_claim == claim and worker_credential == "x" * 43
            observed_size, digest = 0, hashlib.sha256()
            with open("/result/artifacts.tar", "wb") as output:
                async for chunk in chunks:
                    output.write(chunk)
                    digest.update(chunk)
                    observed_size += len(chunk)
            assert observed_size == artifact.archive_size_bytes and digest.hexdigest() == artifact.archive_sha256
            try:
                Path("/tmp/native-work/output/build/artifacts.tar").read_bytes()
            except PermissionError:
                pass
            else:
                raise AssertionError("outer IO directly read private mapped output")
            print("native-outer-io-received-private-artifact", flush=True)
            return BuildArtifactUploadReceiptV1(claim_digest=context.claim_digest, artifact=artifact)

        async def record_outcome(self, request, *, worker_credential):
            assert request.claim == claim and worker_credential == "x" * 43
            assert request.result == ("failed" if expiry else "artifact-ready")
            return BuildOutcomeReceiptV1(request=request, request_digest=canonical_digest(request))

    async with scoped_native_allocated_io(claim=claim,
        source=NativeStagedBuildSource(context, Path("/fixtures/input/source.tar")),
        client=FixtureClient(), worker_credential="x" * 43) as owner:
        outcome = await run_native_outer_build(owner, spec_path=spec_path, expected_sha256=hashlib.sha256(wire).hexdigest(),
            artifact_workspace=workspace, timeout_seconds=120)
    assert (outcome.request.artifact is None) is expiry
    if not expiry:
        print("native-supervised-build-completed", flush=True)
    print("native-supervised-cleanup-confirmed", flush=True)
    subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
        "--state-dir=/tmp/rootless-verification", sys.executable, __file__, "verify"], check=True, timeout=30)
    assert list(workspace.iterdir()) == []
    print("native-outer-io-session-settled", flush=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["verify"]:
        verify_runtime()
    elif len(sys.argv) > 1:
        launch()
    else:
        asyncio.run(main())
