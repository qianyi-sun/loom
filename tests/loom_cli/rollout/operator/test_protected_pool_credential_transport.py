from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.capacity_control_plane import CapacityPoolExecutorProfile
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_execution_credentials import (
    _credentials,
)

MODULE = "loom_cli.rollout.operator.protected_pool_credential_transport"


def _payload(tmp_path: Path, pool: str = "gb10"):  # type: ignore[no-untyped-def]
    credentials = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_execution_credentials"
    )
    bundle = credentials.load_execution_credential_bundle(
        _credentials(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    module = importlib.import_module(MODULE)
    return module.pool_execution_credential_payload(bundle, pool_id=pool)


def test_local_transport_recovers_partial_publication_without_replacement(
    tmp_path: Path,
) -> None:
    assert importlib.util.find_spec(MODULE) is not None, "pool credential transport is missing"
    module = importlib.import_module(MODULE)
    transport_type = getattr(module, "FixedLocalPoolCredentialTransport", None)
    assert transport_type is not None, "local pool credential transport is missing"
    payload = _payload(tmp_path)
    parent = tmp_path / "runtime"
    parent.mkdir(mode=0o700)
    target = parent / "gb10"
    target.mkdir(mode=0o700)
    first_name = "bearer-token"
    first = target / first_name
    first.write_bytes(payload.files[first_name])
    first.chmod(0o600)
    original_inode = first.stat().st_ino
    transport = transport_type(
        pool_id="gb10",
        target_directory=target,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )

    assert transport.observe(payload) is None
    evidence = transport.publish(payload)

    assert first.stat().st_ino == original_inode
    assert set(path.name for path in target.iterdir()) == set(payload.files)
    assert evidence.pool_id == "gb10"
    assert evidence.credential_metadata_sha256 == payload.credential_metadata_sha256
    assert transport.observe(payload) == evidence


def test_pool_payload_contains_only_its_bound_controller_material(tmp_path: Path) -> None:
    credentials = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_execution_credentials"
    )
    bundle = credentials.load_execution_credential_bundle(
        _credentials(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    module = importlib.import_module(MODULE)

    gb10 = module.pool_execution_credential_payload(bundle, pool_id="gb10")
    oldlab = bundle.clients["pool-executor-oldlab"]

    assert gb10.files == {
        "bearer-token": bundle.clients["pool-executor-gb10"].bearer_token,
        "client-certificate.pem": bundle.clients["pool-executor-gb10"].certificate,
        "client-private-key.pem": bundle.clients["pool-executor-gb10"].private_key,
        "manager-ca.pem": bundle.clients["pool-executor-gb10"].manager_ca,
        "ownership-private-key": bundle.ownership_private_keys["gb10"],
    }
    assert oldlab.bearer_token not in gb10.files.values()
    assert oldlab.certificate not in gb10.files.values()
    assert oldlab.private_key not in gb10.files.values()
    assert bundle.ownership_private_keys["oldlab"] not in gb10.files.values()


def test_pool_credential_wire_contracts_round_trip_canonically_without_plaintext(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)
    parent = tmp_path / "runtime"
    parent.mkdir(mode=0o700)
    transport = module.FixedLocalPoolCredentialTransport(
        pool_id="gb10",
        target_directory=parent / "gb10",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )
    evidence = transport.publish(payload)

    payload_wire = payload.to_bytes()
    evidence_wire = evidence.to_bytes()

    assert module.PoolExecutionCredentialPayload.from_bytes(payload_wire) == payload
    assert module.PoolExecutionCredentialEvidence.from_bytes(evidence_wire) == evidence
    assert payload_wire.endswith(b"\n")
    assert evidence_wire.endswith(b"\n")
    assert payload.files["bearer-token"] not in payload_wire
    assert payload.files["ownership-private-key"] not in payload_wire
    assert payload.files["bearer-token"].decode("ascii") not in repr(payload)


def test_pool_credential_wire_contracts_reject_unknown_duplicate_and_noncanonical_fields(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)
    canonical = payload.to_bytes()
    value = json.loads(canonical)

    value["unknown"] = True
    unknown = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with pytest.raises(ValueError, match="credential payload"):
        module.PoolExecutionCredentialPayload.from_bytes(unknown)

    duplicate = canonical.replace(
        b'{"credential_metadata_sha256":', b'{"pool_id":"gb10","credential_metadata_sha256":'
    )
    with pytest.raises(ValueError, match="credential payload"):
        module.PoolExecutionCredentialPayload.from_bytes(duplicate)

    with pytest.raises(ValueError, match="credential payload"):
        module.PoolExecutionCredentialPayload.from_bytes(canonical.rstrip(b"\n") + b" \n")


def test_local_transport_rejects_metadata_drift(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)
    parent = tmp_path / "runtime"
    parent.mkdir(mode=0o700)
    target = parent / "gb10"
    transport = module.FixedLocalPoolCredentialTransport(
        pool_id="gb10",
        target_directory=target,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
    )
    transport.publish(payload)
    (target / "client-private-key.pem").chmod(0o640)

    with pytest.raises(ValueError, match="unsafe"):
        transport.observe(payload)


def test_transport_factory_binds_exact_profile_runtime_paths() -> None:
    module = importlib.import_module(MODULE)
    factory = getattr(module, "local_pool_credential_transport_from_binding", None)
    assert factory is not None, "bound local pool credential transport factory is missing"
    profile = CapacityPoolExecutorProfile.model_validate(
        tomllib.loads(
            Path("deploy/dev-fleet/capacity-pool-executor.toml.example").read_text(encoding="utf-8")
        )
    )
    gb10 = next(pool for pool in profile.pools if pool.pool_id == "gb10")

    transport = factory(gb10, service_gid=2007)

    assert transport.target_directory == Path("/run/loom-capacity-executor/gb10")
    assert transport.service_uid == gb10.local_uid
    assert transport.service_gid == 2007
    with pytest.raises(ValueError, match="paths"):
        factory(
            gb10.model_copy(update={"bearer_token_file": "/tmp/foreign-token"}),
            service_gid=2007,
        )


def test_fixed_controller_transport_uses_only_typed_pool_operations(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)
    expected = module.PoolExecutionCredentialEvidence(
        pool_id="gb10",
        file_sha256={
            name: hashlib.sha256(content).hexdigest() for name, content in payload.files.items()
        },
        credential_metadata_sha256=payload.credential_metadata_sha256,
        uid=993,
        gid=993,
        directory_mode=0o700,
        file_mode=0o600,
    )
    calls: list[tuple[str, bytes]] = []

    def invoke(operation: str, wire: bytes) -> SimpleNamespace:
        calls.append((operation, wire))
        response = b"null\n" if operation == "observe-credential" else expected.to_bytes()
        return SimpleNamespace(returncode=0, stdout=response, stderr=b"")

    transport = module.FixedControllerPoolCredentialTransport(
        pool_id="gb10",
        invoke=invoke,
    )

    assert transport.observe(payload) is None
    assert transport.publish(payload) == expected
    assert calls == [
        ("observe-credential", payload.to_bytes()),
        ("publish-credential", payload.to_bytes()),
    ]
    oldlab_root = tmp_path / "oldlab"
    oldlab_root.mkdir()
    with pytest.raises(ValueError, match="binding"):
        transport.observe(_payload(oldlab_root, "oldlab"))


def test_oldlab_controller_transport_binds_fixed_image_and_host_pid_channel(
    tmp_path: Path,
) -> None:
    """Catch using the cluster-only logical image for later credential calls."""
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path, "oldlab")
    digest = "a" * 64
    image = f"192.168.50.13:5000/loom-capacity-executor@sha256:{digest}"
    runtime_image = f"localhost:5000/loom-capacity-executor@sha256:{digest}"
    calls: list[tuple[tuple[str, ...], str]] = []

    def run(argv, input_payload):  # type: ignore[no-untyped-def]
        calls.append((tuple(argv), input_payload))
        return SimpleNamespace(returncode=0, stdout=b"null\n", stderr=b"")

    transport = module.build_fixed_oldlab_pool_credential_transport(image=image, run=run)

    assert transport.observe(payload) is None
    argv, wire = calls[0]
    assert argv == (
        "/usr/bin/docker",
        "run",
        "--rm",
        "--user",
        "0:0",
        "--privileged",
        "--pid=host",
        "--network=none",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=64m,mode=0700",
        "--mount",
        "type=bind,src=/,dst=/host,bind-propagation=rslave",
        "--entrypoint",
        "/usr/local/bin/python",
        runtime_image,
        "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py",
        "--host-root",
        "/host",
        "--operation",
        "observe-credential",
    )
    assert wire == payload.to_bytes().decode("ascii")


def test_gb10_controller_transport_rejects_trust_source_rotation(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)

    class Controller:
        authority = "a" * 64

        @property
        def controller_prerequisite_authority_sha256(self) -> str:
            return self.authority

        def invoke_pool_credential(self, operation: str, wire: bytes) -> SimpleNamespace:
            assert operation == "observe-credential"
            assert wire == payload.to_bytes()
            return SimpleNamespace(returncode=0, stdout=b"null\n", stderr=b"")

    controller = Controller()
    transport = module.build_fixed_gb10_pool_credential_transport(controller=controller)

    assert transport.observe(payload) is None
    controller.authority = "b" * 64
    with pytest.raises(ValueError, match="authority"):
        transport.observe(payload)


def test_gb10_controller_transport_rejects_trust_rotation_during_operation(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)

    class Controller:
        authority = "a" * 64

        @property
        def controller_prerequisite_authority_sha256(self) -> str:
            return self.authority

        def invoke_pool_credential(self, operation: str, wire: bytes) -> SimpleNamespace:
            assert operation == "observe-credential"
            assert wire == payload.to_bytes()
            self.authority = "b" * 64
            return SimpleNamespace(returncode=0, stdout=b"null\n", stderr=b"")

    transport = module.build_fixed_gb10_pool_credential_transport(controller=Controller())

    with pytest.raises(ValueError, match="authority"):
        transport.observe(payload)


def test_controller_transport_rejects_malformed_or_foreign_evidence(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    payload = _payload(tmp_path)
    oldlab_root = tmp_path / "oldlab"
    oldlab_root.mkdir()
    oldlab_payload = _payload(oldlab_root, "oldlab")
    foreign = module.PoolExecutionCredentialEvidence(
        pool_id="oldlab",
        file_sha256={name: "a" * 64 for name in oldlab_payload.files},
        credential_metadata_sha256=oldlab_payload.credential_metadata_sha256,
        uid=993,
        gid=993,
        directory_mode=0o700,
        file_mode=0o600,
    )

    for result in (
        SimpleNamespace(returncode=1, stdout=b"null\n", stderr=b""),
        SimpleNamespace(returncode=0, stdout=b"not-json\n", stderr=b""),
        SimpleNamespace(returncode=0, stdout=foreign.to_bytes(), stderr=b""),
    ):
        transport = module.FixedControllerPoolCredentialTransport(
            pool_id="gb10",
            invoke=lambda _operation, _wire, result=result: result,
        )
        with pytest.raises(RuntimeError, match="failed safely"):
            transport.observe(payload)
