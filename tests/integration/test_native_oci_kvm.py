"""Actual rendered KVM bundles, restricted client, BuildKit RUN and artifacts.

Requires Docker, /dev/kvm and a native static C compiler. Missing KVM is a skip,
not certification. The privileged outer fixture is disposable and networkless;
this is not proof of rootless host installation or Slurm containment.
"""

import hashlib
import io
import json
import os
import platform
import shutil
import subprocess
import tarfile
import urllib.request
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import pytest

from loom.personal_dev_builder_artifact import verify_personal_dev_build_artifact
from loom.personal_dev_candidate import CandidateRegistration
from loom.personal_dev_sandbox_builder import _DOCKERFILES
from loom.personal_dev_source import create_personal_dev_source_snapshot
from loom_capacity_executor.native_oci_bundles import (
    NativeOciBundlePolicy,
    render_native_oci_bundles,
)
from loom_capacity_executor.native_sandbox_contract import render_native_sandbox_contract
from tests.unit.test_native_sandbox_consumer import bound_context
from tests.unit.test_personal_dev_builder import _attempt, _candidate

pytestmark = [pytest.mark.docker, pytest.mark.timeout(600)]
ROOT = Path(__file__).resolve().parents[2]
PYTHON = "python@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534"
BUILDERS = {
    "x86_64": "sha256:23099633b78bce84207e7a2418df1b941a360163a7d13b3f514f180bc29a89f9",
    "aarch64": "sha256:fff10d1d2fe52187693498edbda9e40b88c9a3c0a4b66515a7c113b89b825ff6",
}


def checked(*args, **kwargs):
    return subprocess.run(list(args), check=True, timeout=180, **kwargs)


def prepare_runtime(tmp_path, arch):
    profile_path = ROOT / ("deploy/dev-fleet/personal-dev-builder-runtime-profile.json"
        if arch == "x86_64" else "deploy/personal-dev-native-builder/runtime-profile-v1.json")
    archive_spec = json.loads(profile_path.read_bytes())["archive"]
    archive_path = tmp_path / "gvisor.tar.bz2"
    cached = os.environ.get("LOOM_TEST_GVISOR_ARCHIVE")
    if cached:
        archive_path = Path(cached)
    else:
        with urllib.request.urlopen(archive_spec["url"], timeout=60) as response, archive_path.open("wb") as output:
            shutil.copyfileobj(response, output)
    with archive_path.open("rb") as stream:
        assert hashlib.file_digest(stream, "sha512").hexdigest() == archive_spec["sha512"]
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o755)
    with tarfile.open(archive_path) as archive:
        for name, expected in archive_spec["members"].items():
            member = archive.getmember(name)
            assert member.isfile() and member.size == expected["size"]
            payload = archive.extractfile(member).read()
            assert hashlib.sha256(payload).hexdigest() == expected["sha256"]
            target = runtime / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
            target.chmod(0o555)
    return runtime


@pytest.mark.parametrize("root_stop", ["signal", "launcher-death"])
def test_rendered_native_kvm_client_builds_and_verifies_all_components(tmp_path, root_stop):
    arch = platform.machine()
    if arch not in BUILDERS or not Path("/dev/kvm").exists():
        pytest.skip("native KVM acceptance requires x86_64/aarch64 with /dev/kvm")
    runtime = prepare_runtime(tmp_path, arch)
    fixtures, result_dir = tmp_path / "fixtures", tmp_path / "result"
    fixtures.mkdir()
    result_dir.mkdir()
    image = "ghcr.io/qianyi-sun/loom-personal-dev-builder@" + BUILDERS[arch]
    name = "loom-native-oci-test-" + uuid4().hex
    try:
        checked("docker", "create", "--name", name, image, capture_output=True)
        with (fixtures / "rootfs.tar").open("wb") as output:
            checked("docker", "export", name, stdout=output)
    finally:
        subprocess.run(["docker", "rm", name], capture_output=True, timeout=20, check=False)
    repo = tmp_path / "source"
    repo.mkdir()
    (repo / "deploy").mkdir()
    checked("cc", "-static", "-O2", "-Wall", "-Wextra", "-Werror",
        str(ROOT / "tests/support/native_kvm/step.c"), "-o", str(repo / "step"))
    (repo / "payload").write_text("committed\n")
    for dockerfile in _DOCKERFILES.values():
        (repo / dockerfile).write_text("FROM scratch\nCOPY step /step\nCOPY payload /payload\nRUN [\"/step\"]\n")
    checked("git", "init", "-q", str(repo))
    checked("git", "-C", str(repo), "add", ".")
    checked("git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.com",
        "commit", "-qm", "fixture")
    # Prove actual local modifications, not only the committed fixture, are built.
    (repo / "payload").write_text("native-uncommitted-source\n")
    (fixtures / "input").mkdir()
    source_path = fixtures / "input/source.tar"
    snapshot = create_personal_dev_source_snapshot(repo, source_path)
    source_path.chmod(0o444)
    candidate = _candidate(source_sha256=snapshot.source_digest, archive_sha256=snapshot.archive_sha256,
        archive_size_bytes=source_path.stat().st_size, source_commit=snapshot.manifest.source_commit,
        dirty=snapshot.manifest.dirty, manifest_json=asdict(snapshot.manifest))
    registration = CandidateRegistration(candidate=candidate, build_attempt=_attempt(state="running"), created=False)
    context = bound_context(registration, "oldlab" if arch == "x86_64" else "gb10")
    wire = (ROOT / "deploy/personal-dev-builder/client-seccomp-v1.json").read_bytes()
    policy = NativeOciBundlePolicy(rootfs=Path("/tmp/native-rootfs"), workspace=Path("/tmp/native-work"),
        client_seccomp=wire, client_seccomp_sha256=hashlib.sha256(wire).hexdigest(),
        tmp_bytes=64 * 1024**2, buildkit_state_bytes=1024**3)
    bundles = render_native_oci_bundles(context, policy)
    (fixtures / "identity.json").write_text(json.dumps({"sandbox_id": bundles.sandbox_id,
        "buildkit_id": bundles.buildkit_id, "client_id": bundles.client_id, "root_stop": root_stop}))
    for component in ("pause", "buildkit", "client"):
        (fixtures / component).mkdir()
        (fixtures / component / "config.json").write_bytes(getattr(bundles, component))
    (fixtures / "input/contract.json").write_bytes(render_native_sandbox_contract(context,
        max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2))
    modules = fixtures / "client-modules"
    modules.mkdir()
    for module in ("__init__", "personal_dev_builder_artifact", "personal_dev_candidate",
        "personal_dev_sandbox_builder", "personal_dev_source"):
        shutil.copyfile(ROOT / "src/loom" / (module + ".py"), modules / (module + ".py"))
    try:
        # Detached runsc helpers need an orphan reaper. Python as container PID1
        # leaves zombies that runsc's kill(pid, 0) liveness test sees as running.
        output = checked("docker", "run", "--init", "--name", name, "--network=none", "--cpus=2", "--memory=4g",
            "--pids-limit=512", "--device=/dev/kvm", "--cap-add=SYS_ADMIN", "--cap-add=SYS_PTRACE",
            "--security-opt=apparmor=unconfined", "--security-opt=seccomp=unconfined", "--read-only",
            "--tmpfs=/tmp:rw,nodev,size=2g",
            "--mount", f"type=bind,src={fixtures},dst=/fixtures,readonly",
            "--mount", f"type=bind,src={runtime},dst=/runtime,readonly",
            "--mount", f"type=bind,src={result_dir},dst=/result",
            "--mount", f"type=bind,src={ROOT / 'tests/support/native_kvm'},dst=/test-support,readonly",
            PYTHON, "python3", "/test-support/execute.py", capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        pytest.fail(f"rendered native KVM fixture failed:\n{exc.stdout}\n{exc.stderr}")
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=20, check=False)
    assert "native-allocated-client-artifact-ok" in output.stdout
    assert "native-client-isolation-probes-ok" in output.stdout
    assert "native-root-stop-children-and-late-start-ok" in output.stdout
    assert "native-allocated-runtime-cleanup-ok" in output.stdout
    verified_dir = tmp_path / "verified"
    verified_dir.mkdir()
    verified = verify_personal_dev_build_artifact(result_dir / "artifacts.tar", registration,
        platform=context.platform, output_directory=verified_dir,
        max_artifact_bytes=32 * 1024**2, max_image_archive_bytes=3 * 1024**2)
    assert set(verified.images) == set(_DOCKERFILES)
    for image in verified.images.values():
        with tarfile.open(image.archive_path) as archive:
            def blob(descriptor):
                return archive.extractfile("blobs/sha256/" + descriptor["digest"].split(":")[1]).read()
            index = json.load(archive.extractfile("index.json"))
            manifest = json.loads(blob(index["manifests"][0]))
            observed = {}
            for descriptor in manifest["layers"]:
                with tarfile.open(fileobj=io.BytesIO(blob(descriptor)), mode="r:*") as layer:
                    for member in layer:
                        if member.name in {"payload", "executed"}:
                            observed[member.name] = layer.extractfile(member).read()
            assert observed == {"payload": b"native-uncommitted-source\n",
                "executed": b"native-buildkit-kvm-executed\n"}
