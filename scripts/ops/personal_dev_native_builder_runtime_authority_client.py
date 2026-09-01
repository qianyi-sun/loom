#!/usr/bin/python3 -Es
"""Secret-safe CLI encoder for native-builder authority requests."""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from personal_dev_native_builder_runtime_authority_protocol import (
    PRIVATE_KEY_LENGTH,
    SERVICE_CA_MAX_BYTES,
    ProtocolError,
    encode_request,
)


class ClientError(ValueError):
    """A client input or descriptor is invalid."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ClientError(message)


def _common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--schema-version", required=True, type=int)
    parser.add_argument("--authority-source-sha", required=True)
    parser.add_argument("--authority-source-tree", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--runtime-profile-sha256", required=True)


def _header_from_namespace(args: argparse.Namespace) -> dict[str, object]:
    return {
        "authority_source_sha": args.authority_source_sha,
        "authority_source_tree": args.authority_source_tree,
        "operation": args.operation,
        "request_id": args.request_id,
        "runtime_profile_sha256": args.runtime_profile_sha256,
        "schema_version": args.schema_version,
    }


def _add_prepare_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--archive-path", required=True)
    parser.add_argument("--archive-sha512", required=True)
    parser.add_argument("--current-agent", required=True)
    parser.add_argument("--current-builder", required=True)
    parser.add_argument("--current-revision", required=True)
    parser.add_argument("--previous-agent", required=True)
    parser.add_argument("--previous-builder", required=True)
    parser.add_argument("--previous-revision", required=True)
    parser.add_argument("--public-store-origin", required=True)


def _add_stage_agent_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--expected-state-sha256", required=True)
    parser.add_argument("--agent-image", required=True)
    parser.add_argument("--builder-image", required=True)
    parser.add_argument("--service-origin", required=True)
    parser.add_argument("--agent-instance-id", required=True)
    parser.add_argument("--agent-key-id", required=True)
    parser.add_argument("--expected-public-key-sha256", required=True)
    private_key = parser.add_mutually_exclusive_group(required=True)
    private_key.add_argument("--private-key-fd", type=int)
    private_key.add_argument("--private-key-path", type=Path)
    service_ca = parser.add_mutually_exclusive_group(required=True)
    service_ca.add_argument("--service-ca-fd", type=int)
    service_ca.add_argument("--service-ca-path", type=Path)


def _add_state_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--expected-state-sha256", required=True)


def _parser() -> _Parser:
    parser = _Parser(add_help=False)
    commands = parser.add_subparsers(dest="operation", required=True)
    for operation in ("status", "prepare", "stage-agent", "activate", "remove"):
        command = commands.add_parser(operation, add_help=False)
        _common_arguments(command)
        if operation == "prepare":
            _add_prepare_arguments(command)
        elif operation == "stage-agent":
            _add_stage_agent_arguments(command)
        elif operation in {"activate", "remove"}:
            _add_state_arguments(command)
    return parser


def _read_descriptor(fd: int, bound: int) -> bytes:
    if fd < 3:
        raise ClientError("descriptor is invalid")
    try:
        payload = bytearray()
        while len(payload) < bound:
            chunk = os.read(fd, bound - len(payload))
            if not chunk:
                break
            payload.extend(chunk)
        return bytes(payload)
    except OSError as exc:
        raise ClientError("descriptor is unreadable") from exc


def _read_root_file(path: Path, bound: int, mode: int) -> bytes:
    descriptor: int | None = None
    try:
        lexical = os.lstat(path)
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        payload = _read_descriptor(descriptor, bound)
        after = os.fstat(descriptor)
        lexical_after = os.lstat(path)
        identity = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_nlink,
            before.st_uid,
            before.st_gid,
            before.st_size,
        )
        if (
            not stat.S_ISREG(lexical.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != mode
            or before.st_nlink != 1
            or before.st_uid != 0
            or before.st_gid != 0
            or identity
            != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_nlink,
                after.st_uid,
                after.st_gid,
                after.st_size,
            )
            or identity
            != (
                lexical_after.st_dev,
                lexical_after.st_ino,
                lexical_after.st_mode,
                lexical_after.st_nlink,
                lexical_after.st_uid,
                lexical_after.st_gid,
                lexical_after.st_size,
            )
        ):
            raise ClientError("protected file is invalid")
        return payload
    except (OSError, ValueError) as exc:
        if isinstance(exc, ClientError):
            raise
        raise ClientError("protected file is invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _stage_payload(args: argparse.Namespace, header: dict[str, object]) -> bytes:
    private_key_fd = args.private_key_fd
    service_ca_fd = args.service_ca_fd
    if private_key_fd is not None and private_key_fd == service_ca_fd:
        raise ClientError("descriptors must differ")
    private_key = (
        _read_descriptor(private_key_fd, PRIVATE_KEY_LENGTH + 1)
        if private_key_fd is not None
        else _read_root_file(args.private_key_path, PRIVATE_KEY_LENGTH + 1, 0o400)
    )
    service_ca = (
        _read_descriptor(service_ca_fd, SERVICE_CA_MAX_BYTES + 1)
        if service_ca_fd is not None
        else _read_root_file(args.service_ca_path, SERVICE_CA_MAX_BYTES + 1, 0o444)
    )
    if len(private_key) != PRIVATE_KEY_LENGTH:
        raise ClientError("private key is invalid")
    if not 1 <= len(service_ca) <= SERVICE_CA_MAX_BYTES:
        raise ClientError("service CA is invalid")
    header["private_key_length"] = len(private_key)
    header["service_ca_length"] = len(service_ca)
    return private_key + service_ca


def _request(args: argparse.Namespace) -> bytes:
    header = _header_from_namespace(args)
    payload = b""
    if args.operation == "prepare":
        header.update(
            {
                "archive_path": args.archive_path,
                "archive_sha512": args.archive_sha512,
                "current_agent": args.current_agent,
                "current_builder": args.current_builder,
                "current_revision": args.current_revision,
                "previous_agent": args.previous_agent,
                "previous_builder": args.previous_builder,
                "previous_revision": args.previous_revision,
                "public_store_origin": args.public_store_origin,
            }
        )
    elif args.operation == "stage-agent":
        header.update(
            {
                "agent_image": args.agent_image,
                "agent_instance_id": args.agent_instance_id,
                "agent_key_id": args.agent_key_id,
                "builder_image": args.builder_image,
                "expected_public_key_sha256": args.expected_public_key_sha256,
                "expected_state_sha256": args.expected_state_sha256,
                "service_origin": args.service_origin,
            }
        )
        payload = _stage_payload(args, header)
    elif args.operation in {"activate", "remove"}:
        header["expected_state_sha256"] = args.expected_state_sha256
    return encode_request(header, payload)


def _emit_public_key(argv: Sequence[str]) -> bytes:
    parser = _Parser(add_help=False)
    parser.add_argument("--private-key-path", required=True, type=Path)
    parser.add_argument("--expected-public-key-sha256", required=True)
    args = parser.parse_args(argv)
    private_key = _read_root_file(
        args.private_key_path,
        PRIVATE_KEY_LENGTH + 1,
        0o400,
    )
    if len(private_key) != PRIVATE_KEY_LENGTH:
        raise ClientError("private key is invalid")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    public_key = (
        Ed25519PrivateKey.from_private_bytes(private_key)
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
    )
    if hashlib.sha256(public_key).hexdigest() != args.expected_public_key_sha256:
        raise ClientError("public key is invalid")
    return public_key


def main(argv: Sequence[str] | None = None) -> int:
    """Emit one frame, or one secret-free error with no stdout output."""
    try:
        arguments = list(sys.argv[1:] if argv is None else argv)
        payload = (
            _emit_public_key(arguments[1:])
            if arguments[:1] == ["emit-public-key"]
            else _request(_parser().parse_args(arguments))
        )
        sys.stdout.buffer.write(payload)
        return 0
    except (ClientError, ProtocolError, OSError, ValueError):
        sys.stderr.write("native runtime authority request failed\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
