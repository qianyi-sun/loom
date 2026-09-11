"""Versioned one-launch material preparation never widens V1 authority."""

import hashlib
import json
import os
import socket
from importlib import import_module

import pytest

from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.unit.test_native_rootless_runtime import spec_file


def material_spec(tmp_path):
    module, previous, path, _digest = spec_file(tmp_path)
    seccomp = '{"defaultAction":"SCMP_ACT_ERRNO","syscalls":[{"names":["read"],"action":"SCMP_ACT_ALLOW"}]}'
    document = json.loads(previous.model_dump_json())
    document.update(schema_version=2, bundle_root=str(tmp_path / "material/bundles"),
        state_root=str(tmp_path / "runsc"), material={
            "schema_version": 1, "archive": "/release/rootfs.tar", "archive_sha256": "a" * 64,
            "archive_size_bytes": 10240, "max_unpacked_bytes": 1024**3, "max_entries": 100000,
            "client_seccomp": seccomp, "client_seccomp_sha256": hashlib.sha256(seccomp.encode()).hexdigest(),
            "tmp_bytes": 64 * 1024**2, "buildkit_state_bytes": 1024**3})
    spec = module.NativeRootlessSpecV2.model_validate_json(json.dumps(document))
    path.chmod(0o600)
    path.write_bytes(canonical_executable_bytes(spec))
    path.chmod(0o400)
    return module, spec, path, hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("fault", [None, "v1", "version", "digest", "archive-overlap", "bundles",
    "state", "limit", "unknown", "root-workspace"])
def test_versioned_material_spec_is_canonical_fixed_and_disjoint(tmp_path, fault):
    module, spec, path, digest = material_spec(tmp_path)
    if fault == "v1":
        # V1 still rejects all material fields rather than reinterpreting them.
        document = json.loads(path.read_bytes())
        document["schema_version"] = 1
    elif fault is not None:
        document = json.loads(path.read_bytes())
        if fault == "version":
            document["schema_version"] = 3
        elif fault == "digest":
            document["material"]["client_seccomp_sha256"] = "f" * 64
        elif fault == "archive-overlap":
            document["material"]["archive"] = str(tmp_path / "material/rootfs.tar")
        elif fault == "bundles":
            document["bundle_root"] = str(tmp_path / "alternate")
        elif fault == "state":
            document["state_root"] = str(tmp_path / "material/runsc")
        elif fault == "limit":
            document["material"]["max_entries"] = 0
        elif fault == "unknown":
            document["material"]["command"] = ["/bin/sh"]
        else:
            document["workspace"] = "/work"
    if fault is None:
        assert module.read_native_rootless_spec(path, expected_sha256=digest) == spec
    else:
        wire = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        path.chmod(0o600)
        path.write_bytes(wire)
        path.chmod(0o400)
        with pytest.raises(ValueError):
            module.read_native_rootless_spec(path, expected_sha256=hashlib.sha256(wire).hexdigest())


@pytest.mark.parametrize("fail_at", [None, "prepare"])
def test_mapped_v2_prepares_after_parent_binding_before_any_session(tmp_path, monkeypatch, fail_at):
    from loom_capacity_executor.native_build_session import NativeBuildSessionResult
    from loom_capacity_executor.native_runtime_cleanup import NativeRuntimeCleanupResult
    from loom_capacity_executor.native_supervisor import NativeSupervisionResult

    module, spec, path, digest = material_spec(tmp_path)
    events = []
    authority, peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, artifact_peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)

    def parent(*args, **kwargs):
        events.append("parent")
        return 321

    def prepare(observed):
        assert observed == spec and events == ["parent"]
        events.append("prepare")
        if fail_at == "prepare":
            raise ValueError("fixture invalid material")

    def execute(**kwargs):
        assert events == ["parent", "prepare"]
        assert kwargs["layout"] == spec.layout()
        events.append("execute")
        return NativeBuildSessionResult(NativeSupervisionResult(False, "expired", True),
            NativeRuntimeCleanupResult(True, "fixture"), None)

    monkeypatch.setenv("LISTEN_PID", str(os.getpid()))
    monkeypatch.setenv("LISTEN_FDS", "2")
    monkeypatch.setattr(module, "bind_native_rootless_parent", parent)
    monkeypatch.setattr(module, "_activation_channels", lambda: (authority, artifact))
    monkeypatch.setattr(module, "prepare_native_rootless_material", prepare)
    monkeypatch.setattr(module, "execute_native_build_session", execute)
    try:
        if fail_at:
            with pytest.raises(ValueError, match="fixture invalid material"):
                module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert events == ["parent", "prepare"]
        else:
            result = module.run_native_mapped_runtime(path, expected_sha256=digest, expected_rootless_pid=123)
            assert result.artifact is None and events == ["parent", "prepare", "execute"]
        assert authority.fileno() == artifact.fileno() == -1
    finally:
        for channel in (authority, peer, artifact, artifact_peer):
            channel.close()


@pytest.mark.parametrize("failed", [None, "unpack", "capabilities", "bundles", "reused", "replaced-after-unpack"])
def test_material_composition_is_ordered_and_never_reuses_scratch(tmp_path, monkeypatch, failed):
    _runtime, spec, _path, _digest = material_spec(tmp_path)
    module = import_module("loom_capacity_executor.native_rootless_material")
    events = []
    material = tmp_path / "material"
    if failed == "reused":
        material.mkdir()
        (material / "keep").write_text("foreign")

    def step(name):
        def run(*args, **kwargs):
            events.append(name)
            if name == "unpack":
                assert kwargs["destination"] == material / "rootfs"
                assert kwargs["expected_sha256"] == spec.material.archive_sha256
                if failed == "replaced-after-unpack":
                    material.rename(tmp_path / "retained-material")
                    material.mkdir(mode=0o700)
                    (material / "keep").write_text("foreign")
            elif name == "capabilities":
                assert args == (material / "rootfs",)
            else:
                assert args == (spec.context,)
                assert kwargs["bundle_root"] == material / "bundles"
            if failed == name:
                raise ValueError("fixture " + name)
        return run

    monkeypatch.setattr(module, "_require_mapped_root", lambda: None)
    monkeypatch.setattr(module, "unpack_native_rootfs_archive", step("unpack"))
    monkeypatch.setattr(module, "restore_native_mapper_capabilities", step("capabilities"))
    monkeypatch.setattr(module, "prepare_native_oci_material", step("bundles"))
    if failed:
        with pytest.raises((ValueError, OSError)):
            module.prepare_native_rootless_material(spec)
        if failed == "reused":
            assert (material / "keep").read_text() == "foreign" and events == []
        elif failed == "replaced-after-unpack":
            assert (material / "keep").read_text() == "foreign" and events == ["unpack"]
        else:
            assert events == ["unpack", "capabilities", "bundles"][:["unpack", "capabilities", "bundles"].index(failed) + 1]
    else:
        module.prepare_native_rootless_material(spec)
        assert events == ["unpack", "capabilities", "bundles"]
