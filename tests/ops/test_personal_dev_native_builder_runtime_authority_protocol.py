from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from io import BytesIO
from pathlib import Path

import pytest
from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
    AuthorityRequestHeader,
    ProtocolError,
    encode_request,
    parse_request,
)

ROOT = Path(__file__).resolve().parents[2]
CLIENT = ROOT / "scripts/ops/personal_dev_native_builder_runtime_authority_client.py"
REQUEST_ID = "123e4567-e89b-42d3-a456-426614174000"
INSTANCE_ID = "123e4567-e89b-42d3-a456-426614174001"
AGENT_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent"
BUILDER_REPOSITORY = "ghcr.io/qianyi-sun/loom-personal-dev-builder"


def _image(repository: str, value: str) -> str:
    return f"{repository}@sha256:{value * 64}"


def _common(operation: str) -> dict[str, object]:
    return {
        "authority_source_sha": "a" * 40,
        "authority_source_tree": "b" * 40,
        "operation": operation,
        "request_id": REQUEST_ID,
        "runtime_profile_sha256": "c" * 64,
        "schema_version": 1,
    }


def _header(operation: str) -> dict[str, object]:
    header = _common(operation)
    if operation == "prepare":
        header.update(
            {
                "archive_path": (
                    f"/var/tmp/loom-personal-dev-native-builder/{REQUEST_ID}/"
                    "gvisor-release-20260810.0-aarch64.tar.bz2"
                ),
                "archive_sha512": "d" * 128,
                "current_agent": _image(AGENT_REPOSITORY, "e"),
                "current_builder": _image(BUILDER_REPOSITORY, "f"),
                "current_revision": "1" * 40,
                "previous_agent": _image(AGENT_REPOSITORY, "2"),
                "previous_builder": _image(BUILDER_REPOSITORY, "3"),
                "previous_revision": "4" * 40,
                "public_store_origin": "https://objects.example",
            }
        )
    elif operation == "stage-agent":
        header.update(
            {
                "agent_image": _image(AGENT_REPOSITORY, "e"),
                "agent_instance_id": INSTANCE_ID,
                "agent_key_id": "gb10-native-builder-v1",
                "builder_image": _image(BUILDER_REPOSITORY, "f"),
                "expected_public_key_sha256": "d" * 64,
                "expected_state_sha256": "e" * 64,
                "private_key_length": 32,
                "service_ca_length": 3,
                "service_origin": "https://agent.example",
            }
        )
    elif operation in {"activate", "remove"}:
        header["expected_state_sha256"] = "d" * 64
    return header


@pytest.mark.parametrize("operation", ["status", "prepare", "stage-agent", "activate", "remove"])
def test_round_trips_every_operation(operation: str) -> None:
    """Catches a parser that loses valid operation data or payload bytes."""
    header = _header(operation)
    payload = b"k" * 32 + b"ca!" if operation == "stage-agent" else b""

    request = parse_request(BytesIO(encode_request(header, payload)))

    assert request.header == AuthorityRequestHeader.from_mapping(header)
    assert request.payload == payload


def test_encode_is_canonical_and_parse_rejects_noncanonical_field_order() -> None:
    """Catches accepting equivalent JSON that is not the signed canonical wire form."""
    header = _header("status")
    encoded = encode_request(header)
    header_bytes = encoded[12:]
    assert header_bytes == json.dumps(
        header, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")

    noncanonical = json.dumps(
        dict(reversed(tuple(header.items()))), separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    frame = b"LOOMNBR1" + len(noncanonical).to_bytes(4, "big") + noncanonical
    with pytest.raises(ProtocolError, match="canonical"):
        parse_request(BytesIO(frame))


def test_rejects_duplicate_keys_and_invalid_frame_boundaries() -> None:
    """Catches ambiguous JSON and frame parsers that accept malformed input."""
    duplicate = (
        b'{"authority_source_sha":"' + b"a" * 40 + b'","authority_source_sha":"'
        + b"a" * 40
        + b'","authority_source_tree":"'
        + b"b" * 40
        + b'","operation":"status","request_id":"'
        + REQUEST_ID.encode()
        + b'","runtime_profile_sha256":"'
        + b"c" * 64
        + b'","schema_version":1}'
    )
    duplicate_frame = b"LOOMNBR1" + len(duplicate).to_bytes(4, "big") + duplicate
    wrong_version = _header("status")
    wrong_version["schema_version"] = 2
    cases = [
        duplicate_frame,
        b"NOPEMAG!" + encode_request(_header("status"))[8:],
        b"LOOMNBR1\x00\x01",
        b"LOOMNBR1" + (65537).to_bytes(4, "big"),
        b"LOOMNBR1"
        + len(json.dumps(wrong_version, sort_keys=True, separators=(",", ":")).encode()).to_bytes(4, "big")
        + json.dumps(wrong_version, sort_keys=True, separators=(",", ":")).encode(),
        encode_request(_header("status")) + b"trailing",
    ]

    for frame in cases:
        with pytest.raises(ProtocolError):
            parse_request(BytesIO(frame))


@pytest.mark.parametrize("field", ["schema_version", "private_key_length", "service_ca_length"])
def test_rejects_booleans_where_schema_requires_integers(field: str) -> None:
    """Catches Python's bool-is-int loophole in protocol validation."""
    operation = "stage-agent" if field != "schema_version" else "status"
    header = _header(operation)
    header[field] = True

    with pytest.raises(ProtocolError):
        encode_request(header, b"k" * 32 + b"ca!" if operation == "stage-agent" else b"")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agent_image", "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent:latest"),
        ("builder_image", _image(AGENT_REPOSITORY, "a")),
        ("service_origin", "http://agent.example"),
        ("service_origin", "https://user:secret@agent.example"),
        ("agent_instance_id", "not-a-uuid"),
        ("agent_key_id", "Invalid"),
        ("expected_state_sha256", "A" * 64),
    ],
)
def test_rejects_invalid_stage_agent_security_identity(field: str, value: str) -> None:
    """Catches mutable identities, wrong repositories, and malformed security inputs."""
    header = _header("stage-agent")
    header[field] = value
    with pytest.raises(ProtocolError):
        encode_request(header, b"k" * 32 + b"ca!")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("public_store_origin", "http://objects.example"),
        ("public_store_origin", "https://user:secret@objects.example"),
        ("public_store_origin", "https://127.0.0.1"),
        ("archive_sha512", "0" * 128),
    ],
)
def test_rejects_invalid_prepare_security_identity(field: str, value: str) -> None:
    """Catches untrusted store origins and invalid archive identity."""
    header = _header("prepare")
    header[field] = value
    with pytest.raises(ProtocolError):
        encode_request(header)


def test_rejects_unknown_fields_incomplete_previous_values_and_public_payloads() -> None:
    """Catches schema widening and public requests carrying secret payloads."""
    unknown = _header("status")
    unknown["unexpected"] = "value"
    incomplete_previous = _header("prepare")
    incomplete_previous["previous_agent"] = ""

    with pytest.raises(ProtocolError):
        encode_request(unknown)
    with pytest.raises(ProtocolError):
        encode_request(incomplete_previous)
    with pytest.raises(ProtocolError):
        encode_request(_header("activate"), b"forbidden")


def test_stage_agent_header_excludes_secret_bytes_and_digests() -> None:
    """Catches accidental inclusion of staged private material in JSON metadata."""
    key = bytes(range(32))
    ca = b"-----BEGIN SECRET CA-----\nunique certificate bytes\n"
    header = _header("stage-agent")
    header["service_ca_length"] = len(ca)
    frame = encode_request(header, key + ca)
    header_bytes = frame[12 : 12 + int.from_bytes(frame[8:12], "big")]

    assert key not in header_bytes
    assert ca not in header_bytes
    assert hashlib.sha256(key).hexdigest().encode() not in header_bytes
    assert hashlib.sha256(ca).hexdigest().encode() not in header_bytes
    assert parse_request(BytesIO(frame)).payload == key + ca


def test_client_status_emits_exactly_one_frame() -> None:
    """Catches CLI output contamination or a client/header mismatch."""
    command = [
        sys.executable,
        str(CLIENT),
        "status",
        "--authority-source-sha", "a" * 40,
        "--authority-source-tree", "b" * 40,
        "--request-id", REQUEST_ID,
        "--runtime-profile-sha256", "c" * 64,
        "--schema-version", "1",
    ]
    result = subprocess.run(command, cwd=ROOT, check=False, capture_output=True)

    assert result.returncode == 0
    assert result.stderr == b""
    assert parse_request(BytesIO(result.stdout)).header.operation == "status"


def test_client_stage_agent_reads_only_open_file_descriptors(tmp_path: Path) -> None:
    """Catches secret path arguments or a client that leaks descriptor metadata."""
    key_path = tmp_path / "key"
    ca_path = tmp_path / "ca"
    key = bytes(range(32))
    ca = b"local test CA"
    key_path.write_bytes(key)
    ca_path.write_bytes(ca)
    key_fd = os.open(key_path, os.O_RDONLY)
    ca_fd = os.open(ca_path, os.O_RDONLY)
    try:
        command = [
            sys.executable, str(CLIENT), "stage-agent",
            "--authority-source-sha", "a" * 40,
            "--authority-source-tree", "b" * 40,
            "--request-id", REQUEST_ID,
            "--runtime-profile-sha256", "c" * 64,
            "--schema-version", "1",
            "--expected-state-sha256", "d" * 64,
            "--agent-image", _image(AGENT_REPOSITORY, "e"),
            "--builder-image", _image(BUILDER_REPOSITORY, "f"),
            "--service-origin", "https://agent.example",
            "--agent-instance-id", INSTANCE_ID,
            "--agent-key-id", "gb10-native-builder-v1",
            "--expected-public-key-sha256", "e" * 64,
            "--private-key-fd", str(key_fd),
            "--service-ca-fd", str(ca_fd),
        ]
        result = subprocess.run(
            command, cwd=ROOT, check=False, capture_output=True, pass_fds=(key_fd, ca_fd)
        )
    finally:
        os.close(key_fd)
        os.close(ca_fd)

    assert result.returncode == 0
    assert result.stderr == b""
    request = parse_request(BytesIO(result.stdout))
    assert request.payload == key + ca
    assert "private_key_fd" not in request.header.as_mapping()
    assert "service_ca_fd" not in request.header.as_mapping()
