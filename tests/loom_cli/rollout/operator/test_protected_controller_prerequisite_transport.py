from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_cli.rollout.operator.protected_controller_prerequisite_transport as prerequisite_transport
from loom_cli.rollout.operator.protected_controller_discovery import (
    ControllerDiscoveryRequest,
)
from loom_cli.rollout.operator.protected_controller_prerequisite_transport import (
    FixedControllerPrerequisiteTransport,
    build_fixed_oldlab_controller_prerequisite_transport,
)
from tests.loom_cli.rollout.operator.test_protected_controller_discovery import (
    _evidence as _discovery_evidence,
)
from tests.loom_cli.rollout.operator.test_protected_controller_prerequisite_component import (
    _component,
)


def test_transport_observes_absence_and_converges_canonical_evidence(tmp_path: Path) -> None:
    component, _fixture_transport, plan, artifact, evidence = _component(
        tmp_path, pool_id="gb10", evidence_present=False
    )
    request = component._request(plan, artifact)
    calls: list[tuple[str, bytes]] = []

    def invoke(operation: str, payload: bytes):  # type: ignore[no-untyped-def]
        calls.append((operation, payload))
        response = b"null\n" if operation == "observe-prerequisite" else evidence.to_bytes()
        return SimpleNamespace(returncode=0, stdout=response, stderr=b"")

    transport = FixedControllerPrerequisiteTransport(
        pool_id="gb10",
        authority_sha256=request.transport_authority_sha256,
        invoke=invoke,
    )

    assert transport.observe(request) is None
    assert transport.converge(request) == evidence
    assert calls == [
        ("observe-prerequisite", request.to_bytes()),
        ("converge-prerequisite", request.to_bytes()),
    ]


def test_transport_discovers_controller_authority_without_a_prerequisite_binding() -> None:
    """Catch retaining the artifact-before-discovery bootstrap cycle."""
    calls: list[tuple[str, bytes]] = []
    evidence = _discovery_evidence()

    def invoke(operation: str, payload: bytes):  # type: ignore[no-untyped-def]
        calls.append((operation, payload))
        return SimpleNamespace(returncode=0, stdout=evidence.to_bytes(), stderr=b"")

    transport = FixedControllerPrerequisiteTransport(
        pool_id="gb10",
        authority_sha256="1" * 64,
        invoke=invoke,
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="gb10",
        transport_authority_sha256="1" * 64,
    )

    assert transport.discover(request) == evidence
    assert calls == [("discover-controller", request.to_bytes())]


def test_transport_rejects_drifted_controller_discovery_response() -> None:
    """Catch accepting discovery evidence from another authenticated route."""
    evidence = _discovery_evidence(pool_id="oldlab")
    transport = FixedControllerPrerequisiteTransport(
        pool_id="gb10",
        authority_sha256="1" * 64,
        invoke=lambda _operation, _payload: SimpleNamespace(
            returncode=0,
            stdout=evidence.to_bytes(),
            stderr=b"",
        ),
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="gb10",
        transport_authority_sha256="1" * 64,
    )

    with pytest.raises(RuntimeError, match="failed safely"):
        transport.discover(request)


@pytest.mark.parametrize(
    "result",
    (
        SimpleNamespace(returncode=1, stdout=b"null\n", stderr=b""),
        SimpleNamespace(returncode=0, stdout=b"null\n", stderr=b"failure"),
        SimpleNamespace(returncode=0, stdout=b"{}\n", stderr=b""),
    ),
)
def test_transport_rejects_failed_or_malformed_controller_response(
    tmp_path: Path,
    result: object,
) -> None:
    component, _fixture_transport, plan, artifact, _evidence = _component(
        tmp_path, pool_id="oldlab", evidence_present=False
    )
    request = component._request(plan, artifact)
    transport = FixedControllerPrerequisiteTransport(
        pool_id="oldlab",
        authority_sha256=request.transport_authority_sha256,
        invoke=lambda _operation, _payload: result,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="failed safely"):
        transport.observe(request)


def test_transport_rejects_cross_pool_or_authority_request_before_invocation(
    tmp_path: Path,
) -> None:
    component, _fixture_transport, plan, artifact, _evidence = _component(
        tmp_path, pool_id="gb10", evidence_present=False
    )
    request = component._request(plan, artifact)
    invoked = False

    def invoke(_operation: str, _payload: bytes):  # type: ignore[no-untyped-def]
        nonlocal invoked
        invoked = True
        raise AssertionError("must not invoke")

    transport = FixedControllerPrerequisiteTransport(
        pool_id="oldlab",
        authority_sha256="9" * 64,
        invoke=invoke,
    )

    with pytest.raises(ValueError, match="binding"):
        transport.observe(request)
    assert invoked is False


def test_oldlab_transport_uses_only_the_fixed_host_pid_installer_channel(
    tmp_path: Path,
) -> None:
    """Catch using the cluster-only logical image through host Docker."""
    component, _fixture_transport, plan, artifact, evidence = _component(
        tmp_path, pool_id="oldlab", evidence_present=False
    )
    initial = component._request(plan, artifact)
    digest = initial.image.rsplit("@sha256:", 1)[1]
    initial = replace(
        initial,
        image=f"192.168.50.13:5000/loom-capacity-executor@sha256:{digest}",
    )
    calls: list[tuple[tuple[str, ...], str]] = []

    def run(argv, payload):  # type: ignore[no-untyped-def]
        calls.append((tuple(argv), payload))
        return SimpleNamespace(
            returncode=0,
            stdout=replace(
                evidence,
                image=initial.image,
                transport_authority_sha256=transport.authority_sha256,
            ).to_bytes(),
            stderr=b"",
        )

    transport = build_fixed_oldlab_controller_prerequisite_transport(
        run=run,
        image=initial.image,
    )
    request = replace(
        initial,
        transport_authority_sha256=transport.authority_sha256,
    )

    assert transport.converge(request).pool_id == "oldlab"

    argv, payload = calls[0]
    runtime_image = f"localhost:5000/loom-capacity-executor@sha256:{digest}"
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
        "--runtime-image",
        runtime_image,
        "--operation",
        "converge-prerequisite",
    )
    assert payload.encode("ascii") == request.to_bytes()


def test_oldlab_transport_discovers_through_the_bound_executor_image() -> None:
    """Catch requiring a candidate-bound prerequisite before OLDLAB discovery."""
    calls: list[tuple[tuple[str, ...], str]] = []
    evidence = _discovery_evidence(pool_id="oldlab")
    image = "192.168.50.13:5000/loom-capacity-executor@sha256:" + "a" * 64
    runtime_image = "localhost:5000/loom-capacity-executor@sha256:" + "a" * 64

    def run(argv, payload):  # type: ignore[no-untyped-def]
        calls.append((tuple(argv), payload))
        return SimpleNamespace(
            returncode=0,
            stdout=replace(
                evidence,
                transport_authority_sha256=transport.authority_sha256,
            ).to_bytes(),
            stderr=b"",
        )

    transport = build_fixed_oldlab_controller_prerequisite_transport(
        run=run,
        image=image,
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="oldlab",
        transport_authority_sha256=transport.authority_sha256,
    )

    assert transport.discover(request).pool_id == "oldlab"
    assert calls == [
        (
            (
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
                "discover-controller",
            ),
            request.to_bytes().decode("ascii"),
        )
    ]


def test_gb10_transport_exposes_controller_discovery() -> None:
    """Catch leaving discovery inaccessible through the forced-SSH adapter."""
    evidence = _discovery_evidence(pool_id="gb10")

    class _Controller:
        controller_prerequisite_authority_sha256 = "8" * 64

        def invoke_controller_prerequisite(self, operation: str, payload: bytes):
            assert operation == "discover-controller"
            assert ControllerDiscoveryRequest.from_bytes(payload).pool_id == "gb10"
            return SimpleNamespace(
                returncode=0,
                stdout=replace(
                    evidence,
                    transport_authority_sha256=self.controller_prerequisite_authority_sha256,
                ).to_bytes(),
                stderr=b"",
            )

    transport = prerequisite_transport.build_fixed_gb10_controller_prerequisite_transport(
        controller=_Controller()
    )
    request = ControllerDiscoveryRequest(
        schema_version=1,
        pool_id="gb10",
        transport_authority_sha256="8" * 64,
    )

    assert transport.discover(request).pool_id == "gb10"


def test_gb10_transport_revalidates_the_live_forced_ssh_authority(
    tmp_path: Path,
) -> None:
    component, _fixture_transport, plan, artifact, evidence = _component(
        tmp_path, pool_id="gb10", evidence_present=False
    )
    initial = component._request(plan, artifact)

    class _Controller:
        controller_prerequisite_authority_sha256 = "8" * 64

        def __init__(self) -> None:
            self.calls = 0

        def invoke_controller_prerequisite(self, _operation: str, _payload: bytes):
            self.calls += 1
            return SimpleNamespace(
                returncode=0,
                stdout=replace(
                    evidence,
                    transport_authority_sha256=(self.controller_prerequisite_authority_sha256),
                ).to_bytes(),
                stderr=b"",
            )

    controller = _Controller()
    transport = prerequisite_transport.build_fixed_gb10_controller_prerequisite_transport(
        controller=controller
    )
    request = replace(
        initial,
        transport_authority_sha256=transport.authority_sha256,
    )

    assert transport.observe(request) is not None
    controller.controller_prerequisite_authority_sha256 = "9" * 64
    assert transport.authority_sha256 == "9" * 64
    with pytest.raises(ValueError, match="binding"):
        transport.observe(request)
    assert controller.calls == 1
