from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    PreparedControllerRequest,
)
from loom_cli.rollout.operator.protected_prepared_controller_transport import (
    FixedPreparedControllerTransport,
    build_fixed_gb10_prepared_controller_transport,
    build_fixed_oldlab_prepared_controller_transport,
)
from tests.loom_cli.rollout.operator.test_protected_capacity_execution_preparation_component import (
    _component,
    _controller_evidence,
)


def _request(tmp_path: Path, *, pool_id: str) -> PreparedControllerRequest:
    component, _manager, transports, plan, _artifact, _guard_calls = _component(tmp_path)
    component.apply(plan)
    return transports[pool_id].requests[-1][1]


def test_transport_uses_only_fixed_prepared_operations_and_canonical_documents(
    tmp_path: Path,
) -> None:
    request = _request(tmp_path, pool_id="gb10")
    disabled = _controller_evidence(request, timer=False, tick=False)
    enabled = _controller_evidence(request, timer=True, tick=False)
    ticked = _controller_evidence(request, timer=True, tick=True)
    calls: list[tuple[str, bytes]] = []

    def invoke(operation: str, payload: bytes):  # type: ignore[no-untyped-def]
        calls.append((operation, payload))
        assert PreparedControllerRequest.from_bytes(payload) == request
        responses = {
            "observe-prepared": b"null\n",
            "converge-prepared-files": disabled.to_bytes(),
            "enable-prepared-timer": enabled.to_bytes(),
            "run-prepared-tick": ticked.to_bytes(),
            "disable-prepared-timer": disabled.to_bytes(),
        }
        return SimpleNamespace(returncode=0, stdout=responses[operation], stderr=b"")

    transport = FixedPreparedControllerTransport(
        pool_id="gb10",
        authority_sha256=request.transport_authority_sha256,
        invoke=invoke,
    )

    assert transport.observe(request) is None
    assert transport.converge_files(request) == disabled
    assert transport.enable_timer(request) == enabled
    assert transport.run_tick(request) == ticked
    assert transport.disable_timer(request) == disabled
    assert calls == [
        ("observe-prepared", request.to_bytes()),
        ("converge-prepared-files", request.to_bytes()),
        ("enable-prepared-timer", request.to_bytes()),
        ("run-prepared-tick", request.to_bytes()),
        ("disable-prepared-timer", request.to_bytes()),
    ]


def test_transport_accepts_absent_file_evidence_after_successful_timer_disable(
    tmp_path: Path,
) -> None:
    """Catch rejecting a controller that proved an absent prepared file set inert."""

    request = _request(tmp_path, pool_id="oldlab")
    transport = FixedPreparedControllerTransport(
        pool_id="oldlab",
        authority_sha256=request.transport_authority_sha256,
        invoke=lambda operation, _payload: SimpleNamespace(
            returncode=0,
            stdout=b"null\n" if operation == "disable-prepared-timer" else b"failure\n",
            stderr=b"",
        ),
    )

    assert transport.disable_timer(request) is None


def test_transport_rejects_semantically_wrong_operation_evidence(tmp_path: Path) -> None:
    request = _request(tmp_path, pool_id="gb10")
    enabled_without_tick = _controller_evidence(request, timer=True, tick=False)
    transport = FixedPreparedControllerTransport(
        pool_id="gb10",
        authority_sha256=request.transport_authority_sha256,
        invoke=lambda _operation, _payload: SimpleNamespace(
            returncode=0,
            stdout=enabled_without_tick.to_bytes(),
            stderr=b"",
        ),
    )

    with pytest.raises(RuntimeError, match="failed safely"):
        transport.run_tick(request)


@pytest.mark.parametrize(
    "result",
    (
        SimpleNamespace(returncode=1, stdout=b"null\n", stderr=b""),
        SimpleNamespace(returncode=0, stdout=b"null\n", stderr=b"failure"),
        SimpleNamespace(returncode=0, stdout=b"{}\n", stderr=b""),
        SimpleNamespace(returncode=0, stdout=b"x" * (2 * 1024 * 1024 + 1), stderr=b""),
    ),
)
def test_transport_rejects_failed_malformed_or_oversized_responses(
    tmp_path: Path,
    result: object,
) -> None:
    request = _request(tmp_path, pool_id="oldlab")
    transport = FixedPreparedControllerTransport(
        pool_id="oldlab",
        authority_sha256=request.transport_authority_sha256,
        invoke=lambda _operation, _payload: result,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="failed safely"):
        transport.converge_files(request)


def test_transport_rejects_cross_pool_request_before_invocation(tmp_path: Path) -> None:
    request = _request(tmp_path, pool_id="gb10")
    invoked = False

    def invoke(_operation: str, _payload: bytes):  # type: ignore[no-untyped-def]
        nonlocal invoked
        invoked = True
        raise AssertionError("must not invoke")

    transport = FixedPreparedControllerTransport(
        pool_id="oldlab",
        authority_sha256="9" * 64,
        invoke=invoke,
    )

    with pytest.raises(ValueError, match="binding"):
        transport.observe(request)
    assert invoked is False


def test_transport_rejects_evidence_for_another_request(tmp_path: Path) -> None:
    request = _request(tmp_path, pool_id="gb10")
    evidence = replace(
        _controller_evidence(request, timer=False, tick=False),
        request_sha256="a" * 64,
    )
    transport = FixedPreparedControllerTransport(
        pool_id="gb10",
        authority_sha256=request.transport_authority_sha256,
        invoke=lambda _operation, _payload: SimpleNamespace(
            returncode=0,
            stdout=evidence.to_bytes(),
            stderr=b"",
        ),
    )

    with pytest.raises(RuntimeError, match="failed safely"):
        transport.observe(request)


def test_oldlab_transport_uses_only_fixed_host_pid_installer_channel(
    tmp_path: Path,
) -> None:
    """Catch using the cluster-only logical image for prepared controller calls."""
    initial = _request(tmp_path, pool_id="oldlab")
    digest = initial.prerequisite.image.rsplit("@sha256:", 1)[1]
    logical_image = f"192.168.50.13:5000/loom-capacity-executor@sha256:{digest}"
    runtime_image = f"localhost:5000/loom-capacity-executor@sha256:{digest}"
    initial_prerequisite = replace(initial.prerequisite, image=logical_image)
    initial = replace(initial, prerequisite=initial_prerequisite)
    calls: list[tuple[tuple[str, ...], str]] = []

    def run(argv, payload):  # type: ignore[no-untyped-def]
        calls.append((tuple(argv), payload))
        bound_request = PreparedControllerRequest.from_bytes(payload.encode("ascii"))
        return SimpleNamespace(
            returncode=0,
            stdout=_controller_evidence(
                bound_request,
                timer=False,
                tick=False,
            ).to_bytes(),
            stderr=b"",
        )

    transport = build_fixed_oldlab_prepared_controller_transport(
        run=run,
        image=initial.prerequisite.image,
    )
    prerequisite = replace(
        initial.prerequisite,
        transport_authority_sha256=transport.authority_sha256,
    )
    request = replace(
        initial,
        transport_authority_sha256=transport.authority_sha256,
        prerequisite=prerequisite,
    )

    assert transport.converge_files(request).pool_id == "oldlab"
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
                (
                    "/opt/loom-capacity-executor-release/payload/installer/"
                    "install_capacity_executor.py"
                ),
                "--host-root",
                "/host",
                "--operation",
                "converge-prepared-files",
            ),
            request.to_bytes().decode("ascii"),
        )
    ]


def test_gb10_transport_revalidates_live_forced_ssh_authority(tmp_path: Path) -> None:
    initial = _request(tmp_path, pool_id="gb10")

    class _Controller:
        controller_prerequisite_authority_sha256 = "8" * 64

        def __init__(self) -> None:
            self.calls = 0

        def invoke_prepared_controller(self, operation: str, payload: bytes):
            self.calls += 1
            assert operation == "observe-prepared"
            bound_request = PreparedControllerRequest.from_bytes(payload)
            return SimpleNamespace(
                returncode=0,
                stdout=_controller_evidence(
                    bound_request,
                    timer=False,
                    tick=False,
                ).to_bytes(),
                stderr=b"",
            )

    controller = _Controller()
    transport = build_fixed_gb10_prepared_controller_transport(controller=controller)
    prerequisite = replace(
        initial.prerequisite,
        transport_authority_sha256=transport.authority_sha256,
    )
    request = replace(
        initial,
        transport_authority_sha256=transport.authority_sha256,
        prerequisite=prerequisite,
    )

    assert transport.observe(request) is not None
    controller.controller_prerequisite_authority_sha256 = "9" * 64
    assert transport.authority_sha256 == "9" * 64
    with pytest.raises(ValueError, match="binding"):
        transport.observe(request)
    assert controller.calls == 1
