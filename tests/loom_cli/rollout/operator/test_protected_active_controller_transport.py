"""Check both fixed command channels and exact activation evidence binding."""

import hashlib
import json
import os
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
import scripts.ops.gb10_external_supervisor_broker as broker

from loom_cli.rollout.operator import protected_gb10_external_supervisor_transport as remote
from loom_cli.rollout.operator.protected_active_controller import (
    ActiveControllerEvidence,
    ActiveControllerRequest,
)
from loom_cli.rollout.operator.protected_active_controller_transport import (
    FixedActiveControllerTransport,
    build_fixed_gb10_active_controller_transport,
    build_fixed_oldlab_active_controller_transport,
)
from tests.loom_cli.rollout.operator.test_protected_active_controller import _request
from tests.loom_cli.rollout.operator.test_protected_gb10_external_supervisor_transport import (
    _CONTROLLER_KNOWN_HOST,
    _controller_artifact,
    _Run,
    _transport,
)
from tests.loom_cli.rollout.operator.test_protected_prepared_controller_transport import (
    _request as _prepared_request,
)
from tests.ops.test_install_capacity_executor import _controller_request


def _evidence(request, state="staged"):
    prefix = "loom-capacity-pool-executor"
    units = {
        prefix + suffix
        for suffix in (
            ".service",
            "-prepared.service",
            "-prepared.timer",
            "-active.service",
            "-active.timer",
        )
    }
    active = dict.fromkeys(units, "inactive")
    enabled = {unit: "disabled" if unit.endswith(".timer") else "static" for unit in units}
    if state != "staged":
        enabled[prefix + "-active.timer"] = "enabled"
    if state == "active":
        active[prefix + "-active.timer"] = "active"
    return ActiveControllerEvidence(
        request.operation_id,
        request.pool_id,
        request.request_sha256,
        request.transport_authority_sha256,
        {path: hashlib.sha256(payload).hexdigest() for path, payload in request.files.items()},
        active,
        enabled,
    )


def _gb10_request(tmp_path):
    root = tmp_path / "prerequisite"
    root.mkdir()
    prerequisite = _prepared_request(root, pool_id="gb10").prerequisite
    root = tmp_path / "active"
    root.mkdir()
    return _request(root, prerequisite=prerequisite)


def test_transport_fixed_operations_and_exact_response(tmp_path):
    request = _request(tmp_path)
    calls = []

    def invoke(operation, payload):
        assert ActiveControllerRequest.from_bytes(payload) == request
        calls.append(operation)
        response = (
            b"null\n"
            if operation == "observe-active"
            else _evidence(
                request, "active" if operation == "enable-active-timer" else "staged"
            ).to_bytes()
        )
        return SimpleNamespace(returncode=0, stdout=response, stderr=b"")

    transport = FixedActiveControllerTransport(
        request.pool_id, request.transport_authority_sha256, invoke
    )
    assert transport.observe(request) is None
    assert transport.converge_files(request).state == "staged"
    assert transport.enable_timer(request).state == "active"
    assert calls == ["observe-active", "converge-active-files", "enable-active-timer"]


@pytest.mark.parametrize(
    "mutation",
    [
        "operation",
        "request",
        "files",
        "pool",
        "authority",
        "state",
        "stderr",
        "absent",
        "noncanonical",
    ],
)
def test_transport_rejects_wrong_or_unproven_activation(tmp_path, mutation):
    request = _request(tmp_path)
    evidence = _evidence(request, "active")
    if mutation == "operation":
        evidence = replace(evidence, operation_id=uuid4())
    elif mutation == "request":
        evidence = replace(evidence, request_sha256="a" * 64)
    elif mutation == "files":
        evidence = replace(evidence, file_sha256=dict.fromkeys(request.files, "a" * 64))
    elif mutation == "authority":
        evidence = replace(evidence, transport_authority_sha256="a" * 64)
    elif mutation == "state":
        evidence = _evidence(request)
    payload = evidence.to_bytes()
    if mutation == "pool":
        value = json.loads(payload)
        value["pool_id"] = "gb10"
        payload = broker._canonical_json(value)
    elif mutation == "noncanonical":
        payload += b" "
    elif mutation == "absent":
        payload = b"null\n"
    transport = FixedActiveControllerTransport(
        request.pool_id,
        request.transport_authority_sha256,
        lambda *_: SimpleNamespace(
            returncode=0, stdout=payload, stderr=b"error" if mutation == "stderr" else b""
        ),
    )
    with pytest.raises(RuntimeError, match="failed safely"):
        transport.enable_timer(request)


def _bind_transport(request, digest):
    prerequisite = replace(request.prepared.prerequisite, transport_authority_sha256=digest)
    return replace(
        request,
        prepared=replace(
            request.prepared, prerequisite=prerequisite, transport_authority_sha256=digest
        ),
    )


def test_oldlab_uses_fixed_host_namespace_channel(tmp_path):
    prerequisite = _controller_request(tmp_path)
    digest = prerequisite.image.rsplit("@sha256:", 1)[1]
    request = _request(
        tmp_path,
        prerequisite=replace(
            prerequisite, image=f"192.168.50.13:5000/loom-capacity-executor@sha256:{digest}"
        ),
    )
    calls = []

    def run(argv, payload):
        calls.append((tuple(argv), payload))
        actual = ActiveControllerRequest.from_bytes(payload.encode())
        return SimpleNamespace(returncode=0, stdout=_evidence(actual).to_bytes(), stderr=b"")

    transport = build_fixed_oldlab_active_controller_transport(
        run=run, image=request.profile.executor_image
    )
    request = _bind_transport(request, transport.authority_sha256)
    assert transport.converge_files(request).state == "staged"
    argv, payload = calls[0]
    assert argv[:4] == ("/usr/bin/docker", "run", "--rm", "--interactive")
    assert "--pid=host" in argv and "--network=none" in argv
    assert argv[-4:] == ("--host-root", "/host", "--operation", "converge-active-files")
    assert payload.encode() == request.to_bytes()


def test_gb10_uses_fixed_ssh_channel_and_revalidates_transport(tmp_path, monkeypatch):
    request = _gb10_request(tmp_path)
    artifact = _controller_artifact(tmp_path / "supervisor")
    known = tmp_path / "known_hosts"
    known.write_text(_CONTROLLER_KNOWN_HOST)
    known.chmod(0o644)
    monkeypatch.setattr(remote, "_KNOWN_HOSTS", known)
    monkeypatch.setattr(remote, "_KNOWN_HOSTS_OWNER_UID", os.geteuid())
    run = _Run("")
    channel = _transport(artifact, run, tmp_path / "key")
    transport = build_fixed_gb10_active_controller_transport(controller=channel)
    request = _bind_transport(request, transport.authority_sha256)
    run.response = _evidence(request, "active").to_bytes().decode()
    assert transport.enable_timer(request).state == "active"
    argv, payload = run.calls[0]
    assert argv[-1] == "loom-external-supervisor-v1"
    outer = json.loads(payload)
    assert outer == {
        "schema_version": 1,
        "operation": "enable_active_timer",
        "candidate_sha": artifact.candidate_sha,
        "candidate_tree": artifact.candidate_tree,
        "active_controller": json.loads(request.to_bytes()),
    }
    assert broker.parse_request_identity(payload.encode()) == (
        artifact.candidate_sha,
        artifact.candidate_tree,
    )
    known.write_text("changed\n")
    with pytest.raises(ValueError):
        transport.enable_timer(request)
    assert len(run.calls) == 1


@pytest.mark.parametrize(
    "operation,state",
    [
        ("observe_active_controller", "staged"),
        ("converge_active_files", "staged"),
        ("enable_active_timer", "active"),
        ("refresh_active_preparation", "staged"),
    ],
)
def test_gb10_broker_invokes_only_bound_candidate_installer(
    tmp_path, monkeypatch, operation, state
):
    request = _gb10_request(tmp_path)
    outer = {
        "schema_version": 1,
        "operation": operation,
        "candidate_sha": request.prepared.prerequisite.source_sha,
        "candidate_tree": "a" * 40,
        "active_controller": json.loads(request.to_bytes()),
    }
    payload = broker._canonical_json(outer)
    evidence = _evidence(request, state).to_bytes()
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return SimpleNamespace(returncode=0, stdout=evidence.decode(), stderr="")

    monkeypatch.setattr(broker, "_run", run)
    candidate = broker.CANDIDATES_ROOT / outer["candidate_sha"]
    assert broker.run_controller_active(candidate, payload) == evidence
    argv, kwargs = calls[0]
    assert argv[:4] == [
        str(candidate / "venv/bin/python"),
        "-I",
        "-B",
        str(candidate / "repo/scripts/ops/install_capacity_executor.py"),
    ]
    assert argv[4:] == [
        "--operation",
        {
            "observe_active_controller": "observe-active",
            "converge_active_files": "converge-active-files",
            "enable_active_timer": "enable-active-timer",
            "refresh_active_preparation": "refresh-active-preparation",
        }[operation],
    ]
    assert kwargs["input_payload"].encode() == request.to_bytes()
    # An envelope cannot smuggle an arbitrary file map to the trusted installer.
    outer["active_controller"]["files"] = {"/unrelated": "bytes"}
    with pytest.raises(broker.BrokerError):
        broker.run_controller_active(candidate, broker._canonical_json(outer))
    assert len(calls) == 1
