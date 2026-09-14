"""Disposable full-build IO split; the synthetic authority stays outside mapping."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
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
    NativeRootlessSpecV2,
    exec_native_rootless_runtime,
    read_native_rootless_spec,
)
from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_bytes


def paths():
    v2 = "-v2" in json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"]
    return (Path("/tmp/native-attempt/work"), Path("/tmp/native-attempt/runsc"), Path("/tmp/native-attempt/material/bundles")) if v2 else (
        Path("/tmp/native-work"), Path("/tmp/runsc-state"), Path("/tmp/native-material/bundles"))


def launch():
    exec_native_rootless_runtime(paths()[0] / "runtime-spec.json",
        expected_sha256=sys.argv[5], expected_parent_pid=int(sys.argv[2]),
        authority_fd=int(sys.argv[3]), artifact_fd=int(sys.argv[4]))


def verify_runtime():
    """Independent mapped readback; never repeat production runtime deletions."""
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
    workspace, state, bundles = paths()
    layout = NativeRunscLayout(Path("/runtime/runsc"), state, bundles, context.claim_digest)
    output = workspace / "output"
    if "-v2" in json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"]:
        from loom_capacity_executor.native_oci_bundles import render_native_oci_bundles

        spec_path = workspace / "runtime-spec.json"
        spec = read_native_rootless_spec(spec_path, expected_sha256=hashlib.sha256(spec_path.read_bytes()).hexdigest())
        assert isinstance(spec, NativeRootlessSpecV2)
        rendered = render_native_oci_bundles(context, spec.material.policy(workspace))
        for role in ("pause", "buildkit", "client"):
            assert (bundles / role / "config.json").read_bytes() == getattr(rendered, role)
    if expiry:
        material_v2 = "-v2" in json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"]
        if material_v2:
            assert Path("/result/live-step").read_text() == "observed-live-step"
        else:
            pulse = output / "lifecycle-pulse"
            stopped = pulse.read_bytes()
            assert int(stopped) > 0, "deadline test never reached a live sandbox client"
        late = "late-" + layout.identity("pause")
        command = [*layout.command("start", "client")[:-1], late]
        try:
            assert subprocess.run(command, capture_output=True, timeout=5).returncode != 0
            if not material_v2:
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


def observe_live_step():
    """Read-only original-UID observer, not material preparation or authority."""
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    _workspace, state, bundles = paths()
    layout = NativeRunscLayout(Path("/runtime/runsc"), state, bundles, context.claim_digest)
    deadline = time.monotonic() + 45
    # runsc JSON reports only PIDs. Its table exposes the guest command name;
    # the exact private sidecar ID scopes this observation to the fixture build.
    command = [*layout.command("list", "pause")[:-2], "ps", "--format=table", layout.identity("buildkit")]
    while time.monotonic() < deadline:
        result = subprocess.run(command, capture_output=True, timeout=5)
        if result.returncode == 0:
            rows = [line.split(maxsplit=8) for line in result.stdout.decode().splitlines()[1:]]
            if any(len(row) == 9 and row[1].isdigit() and int(row[1]) > 0 and row[8] == "step" for row in rows):
                Path("/result/live-step").write_text("observed-live-step")
                return
        time.sleep(0.1)
    raise AssertionError("never observed feature Dockerfile RUN step")


async def main():
    assert os.getuid() == 1000
    context = BuildSourceContextV1.model_validate_json(Path("/fixtures/context.json").read_bytes())
    expiry = json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"].endswith("expiry")
    # Trusted disposable material preparation needs the same static UID mapping
    # to restore rootfs capabilities. It starts no feature/runtime process.
    material_v2 = "-v2" in json.loads(Path("/fixtures/identity.json").read_bytes())["root_stop"]
    runtime_workspace, state, bundles = paths()
    if material_v2:
        runtime_workspace.parent.mkdir(mode=0o700)
    runtime_workspace.mkdir(mode=0o700)
    claim = BuildClaimRequestV1.model_validate_json(Path("/fixtures/claim.json").read_bytes())
    fields = dict(claim=claim, context=context, runsc="/runtime/runsc",
        state_root=str(state), bundle_root=str(bundles), workspace=str(runtime_workspace),
        max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2)
    if material_v2:
        from loom_capacity_executor.native_rootless_material import NativeRootlessMaterialV1

        binding = json.loads(Path("/fixtures/rootfs-binding.json").read_bytes())
        seccomp = Path("/fixtures/client-seccomp.json").read_bytes()
        spec = NativeRootlessSpecV2(**fields, material=NativeRootlessMaterialV1(
            archive="/fixtures/rootfs.tar", archive_sha256=binding["sha256"], archive_size_bytes=binding["size_bytes"],
            max_unpacked_bytes=1024**3, max_entries=100000,
            client_seccomp=seccomp.decode(), client_seccomp_sha256=hashlib.sha256(seccomp).hexdigest(),
            tmp_bytes=64 * 1024**2, buildkit_state_bytes=1024**3))
        wire = canonical_executable_bytes(spec)
    else:
        subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
            "--state-dir=/tmp/rootless-preparation", sys.executable, "/test-support/execute.py", "prepare"],
            check=True, timeout=60)
        spec = NativeRootlessSpecV1(**fields)
        wire = canonical_bytes(spec)
    spec_path = runtime_workspace / "runtime-spec.json"
    spec_path.write_bytes(wire)
    spec_path.chmod(0o400)
    workspace = Path("/tmp/native-outer-io")
    workspace.mkdir(mode=0o700)

    class FixtureClient:
        calls = 0

        async def authorize_execution(self, request, *, worker_credential):
            assert worker_credential == "x" * 43
            self.calls += 1
            if expiry and self.calls > 1 and (not material_v2 or Path("/result/live-step").exists()):
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
            if material_v2:
                assert not (runtime_workspace / "output").exists()
            else:
                try:
                    (runtime_workspace / "output/build/artifacts.tar").read_bytes()
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

    observer = None
    try:
        if material_v2 and expiry:
            observer = await asyncio.create_subprocess_exec(sys.executable, __file__, "observe")
        async with scoped_native_allocated_io(claim=claim,
            source=NativeStagedBuildSource(context, Path("/fixtures/input/source.tar")),
            client=FixtureClient(), worker_credential="x" * 43) as owner:
            outcome = await run_native_outer_build(owner, spec_path=spec_path, expected_sha256=hashlib.sha256(wire).hexdigest(),
                artifact_workspace=workspace, timeout_seconds=120)
        if observer is not None:
            assert await asyncio.wait_for(observer.wait(), 5) == 0
    finally:
        if observer is not None and observer.returncode is None:
            observer.kill()
            await asyncio.wait_for(observer.wait(), 5)
    assert (outcome.request.artifact is None) is expiry
    if not expiry:
        print("native-supervised-build-completed", flush=True)
    if material_v2:
        assert not Path("/tmp/rootless-preparation").exists()
        print("native-v2-one-launch-session-settled", flush=True)
    print("native-supervised-cleanup-confirmed", flush=True)
    if material_v2:
        evidence = json.loads(Path("/result/native-pre-prune.json").read_bytes())
        assert evidence["spec_sha256"] == hashlib.sha256(wire).hexdigest()
        assert evidence["pruned"] is True
        for scratch in (bundles.parent, runtime_workspace / "output", runtime_workspace / "buildkit-run"):
            assert not scratch.exists() and not scratch.is_symlink()
        # The exact launcher has exited through the outer result/wait path.
        # Check retained-file accessibility in the original namespace only;
        # this does not prove every namespace reference or Slurm process died.
        from loom_capacity_executor.native_mapped_scratch import _mount_id

        attempt_fd = os.open(runtime_workspace.parent, os.O_PATH | os.O_NOFOLLOW)
        namespace_fd = os.open(state / "null-netns", os.O_PATH | os.O_NOFOLLOW)
        try:
            assert _mount_id(attempt_fd) == _mount_id(namespace_fd)
            assert os.fstat(namespace_fd).st_uid == os.getuid()
        finally:
            os.close(attempt_fd)
            os.close(namespace_fd)
        print(evidence["observations"], end="", flush=True)
        print("native-v2-mapped-scratch-pruned", flush=True)
    else:
        subprocess.run(["/usr/bin/rootlesskit", "--net=none", "--subid-source=static",
            "--state-dir=/tmp/rootless-verification", sys.executable, __file__, "verify"], check=True, timeout=30)
    assert list(workspace.iterdir()) == []
    print("native-outer-io-session-settled", flush=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["verify"]:
        verify_runtime()
    elif sys.argv[1:] == ["observe"]:
        observe_live_step()
    elif len(sys.argv) > 1:
        launch()
    else:
        asyncio.run(main())
