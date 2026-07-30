from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pytest
from scripts.ops import developer_sandbox_node_docker_request as renderer

SHA = "a" * 40
TREE = "b" * 40


def _args(
    tmp_path: Path,
    *,
    action: str = "readback",
    expectation: str = "absent",
    operation_id: str | None = "d" * 64,
) -> argparse.Namespace:
    bundle = tmp_path / "candidate.bundle"
    bundle.write_bytes(b"bundle")
    inputs = tmp_path / "input"
    inputs.mkdir(exist_ok=True)
    return argparse.Namespace(
        action=action,
        candidate_sha=SHA,
        candidate_tree=TREE,
        candidate_bundle=bundle,
        expected_node="trt-gb10-7",
        input_root=inputs,
        operation_id=operation_id,
        transport_expectation=expectation,
    )


def test_renderer_binds_operation_phase_candidate_node_bundle_and_inputs(
    tmp_path: Path,
) -> None:
    args = _args(tmp_path)

    payload = renderer.render(args)
    request = json.loads(payload)
    unsigned = dict(request)
    unsigned.pop("request_id")

    assert payload == renderer._canonical(request)
    assert request["schema_version"] == 2
    assert request["operation_id"] == "d" * 64
    assert request["transport_expectation"] == "absent"
    assert request["expected_node"] == "trt-gb10-7"
    assert request["candidate_bundle_sha256"] == hashlib.sha256(b"bundle").hexdigest()
    assert request["inputs"] == {}
    assert request["request_id"] == hashlib.sha256(renderer._canonical(unsigned)).hexdigest()


def test_renderer_generates_a_fresh_operation_id_when_omitted(
    tmp_path: Path,
) -> None:
    first = json.loads(renderer.render(_args(tmp_path, operation_id=None)))
    second = json.loads(renderer.render(_args(tmp_path, operation_id=None)))

    assert first["operation_id"] != second["operation_id"]
    assert renderer.SHA256_RE.fullmatch(first["operation_id"])
    assert renderer.SHA256_RE.fullmatch(second["operation_id"])


def test_renderer_hashes_client_secrets_without_emitting_them(tmp_path: Path) -> None:
    args = _args(
        tmp_path,
        action="transport-client-bootstrap",
        expectation="not-checked",
    )
    secret = b"private-secret-material"
    (args.input_root / "role").write_bytes(secret)
    (args.input_root / "role.pub").write_bytes(b"ssh-ed25519 public")
    (args.input_root / "known_hosts").write_bytes(b"host ssh-ed25519 public")

    payload = renderer.render(args)
    request = json.loads(payload)

    assert secret not in payload
    assert request["inputs"]["role"] == hashlib.sha256(secret).hexdigest()


@pytest.mark.parametrize("action", sorted(renderer.ENVIRONMENT_AUTHORITY_ACTIONS))
def test_environment_authority_requests_are_pinned_to_oldlab_2_without_inputs(
    tmp_path: Path,
    action: str,
) -> None:
    args = _args(tmp_path, action=action, expectation="not-checked")
    args.expected_node = "oldlab-2"

    request = json.loads(renderer.render(args))

    assert request["action"] == action
    assert request["expected_node"] == "oldlab-2"
    assert request["inputs"] == {}

    args.expected_node = "oldlab-1"
    with pytest.raises(renderer.RequestRenderError, match="binding is invalid"):
        renderer.render(args)

    args.expected_node = "oldlab-2"
    (args.input_root / "caller-path").write_text("forbidden\n", encoding="ascii")
    with pytest.raises(renderer.RequestRenderError, match="unexpected inputs"):
        renderer.render(args)


@pytest.mark.parametrize(
    ("action", "expectation"),
    [
        ("readback", "not-checked"),
        ("authority-upgrade", "server"),
    ],
)
def test_renderer_rejects_phase_expectation_drift(
    tmp_path: Path,
    action: str,
    expectation: str,
) -> None:
    with pytest.raises(renderer.RequestRenderError):
        renderer.render(_args(tmp_path, action=action, expectation=expectation))
