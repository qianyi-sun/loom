#!/usr/bin/python3 -I
"""Unprivileged CLI for the developer environment registry authority."""

from __future__ import annotations

import argparse
import array
import hashlib
import json
import os
import socket
import stat
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

SCHEMA_VERSION: Final = 1
SOCKET_PATH: Final = Path("/run/loom-developer-environment-authority/authority.sock")
MAX_RESPONSE_BYTES: Final = 4 * 1024 * 1024

REGISTER_KIND: Final = "loom.developer-environment.register"
IMPORT_KIND: Final = "loom.developer-environment.candidate-import"
STATUS_KIND: Final = "loom.developer-environment.status"
SNAPSHOT_KIND: Final = "loom.developer-environment.snapshot"
BEGIN_DEPLOY_KIND: Final = "loom.developer-environment.begin-deploy"
CREATE_KIND: Final = "loom.developer-environment.create"
UPDATE_KIND: Final = "loom.developer-environment.update"
CHECK_KIND: Final = "loom.developer-environment.check"
ROLLBACK_KIND: Final = "loom.developer-environment.rollback"
DESTROY_KIND: Final = "loom.developer-environment.destroy"


class ClientError(RuntimeError):
    """A bounded, secret-safe CLI failure."""


def _stable_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _canonical(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(payload),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, UnicodeEncodeError) as exc:
        raise ClientError("request is not canonical JSON") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)

    register = commands.add_parser("register", allow_abbrev=False)
    register.add_argument("--idempotency-key", required=True)
    register.add_argument("--display-name", required=True)

    candidate_import = commands.add_parser("import", allow_abbrev=False)
    candidate_import.add_argument("--idempotency-key", required=True)
    candidate_import.add_argument("--env-id", required=True)
    candidate_import.add_argument("--bundle", type=Path, required=True)
    candidate_import.add_argument("--candidate-sha", required=True)
    candidate_import.add_argument("--candidate-tree", required=True)
    candidate_import.add_argument("--amd64-image-digest", required=True)
    candidate_import.add_argument("--arm64-image-digest", required=True)

    status = commands.add_parser("status", allow_abbrev=False)
    status.add_argument("--env-id", required=True)

    commands.add_parser("snapshot", allow_abbrev=False)

    begin = commands.add_parser("begin-deploy", allow_abbrev=False)
    begin.add_argument("--idempotency-key", required=True)
    begin.add_argument("--env-id", required=True)
    begin.add_argument("--candidate-id", required=True)
    begin.add_argument("--expected-resource-generation", required=True, type=int)

    create = commands.add_parser("create", allow_abbrev=False)
    create.add_argument("--idempotency-key", required=True)
    create.add_argument("--display-name", required=True)
    _candidate_arguments(create)

    update = commands.add_parser("update", allow_abbrev=False)
    update.add_argument("--idempotency-key", required=True)
    _candidate_arguments(update)

    commands.add_parser("check", allow_abbrev=False)

    rollback = commands.add_parser("rollback", allow_abbrev=False)
    rollback.add_argument("--idempotency-key", required=True)

    destroy = commands.add_parser("destroy", allow_abbrev=False)
    destroy.add_argument("--idempotency-key", required=True)
    return parser


def _candidate_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-tree", required=True)
    parser.add_argument("--amd64-image-digest", required=True)
    parser.add_argument("--arm64-image-digest", required=True)


def _bundle_binding(path: Path) -> tuple[int, int, str]:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.getuid()
            or before.st_size < 1
        ):
            raise ClientError("bundle metadata is unsafe")
        digest = hashlib.sha256()
        offset = 0
        while offset < before.st_size:
            chunk = os.pread(
                descriptor,
                min(1024 * 1024, before.st_size - offset),
                offset,
            )
            if not chunk:
                raise ClientError("bundle content is incomplete")
            digest.update(chunk)
            offset += len(chunk)
        if os.pread(descriptor, 1, before.st_size):
            raise ClientError("bundle size changed")
        after = os.fstat(descriptor)
        if _stable_identity(before) != _stable_identity(after):
            raise ClientError("bundle metadata changed")
        return descriptor, before.st_size, digest.hexdigest()
    except ClientError:
        if descriptor >= 0:
            os.close(descriptor)
        raise
    except OSError as exc:
        if descriptor >= 0:
            os.close(descriptor)
        raise ClientError("bundle is unavailable") from exc


def _build_request(
    arguments: argparse.Namespace,
) -> tuple[dict[str, Any], int | None]:
    common = {"schema_version": SCHEMA_VERSION}
    if arguments.command == "register":
        return (
            {
                **common,
                "kind": REGISTER_KIND,
                "idempotency_key": arguments.idempotency_key,
                "display_name": arguments.display_name,
            },
            None,
        )
    if arguments.command == "status":
        return (
            {
                **common,
                "kind": STATUS_KIND,
                "env_id": arguments.env_id,
            },
            None,
        )
    if arguments.command == "snapshot":
        return ({**common, "kind": SNAPSHOT_KIND}, None)
    if arguments.command == "check":
        return ({**common, "kind": CHECK_KIND}, None)
    if arguments.command == "rollback":
        return (
            {
                **common,
                "kind": ROLLBACK_KIND,
                "idempotency_key": arguments.idempotency_key,
            },
            None,
        )
    if arguments.command == "destroy":
        return (
            {
                **common,
                "kind": DESTROY_KIND,
                "idempotency_key": arguments.idempotency_key,
            },
            None,
        )
    if arguments.command == "begin-deploy":
        return (
            {
                **common,
                "kind": BEGIN_DEPLOY_KIND,
                "idempotency_key": arguments.idempotency_key,
                "env_id": arguments.env_id,
                "candidate_id": arguments.candidate_id,
                "expected_resource_generation": (arguments.expected_resource_generation),
            },
            None,
        )
    if arguments.command == "import":
        descriptor, size, digest = _bundle_binding(arguments.bundle)
        return (
            {
                **common,
                "kind": IMPORT_KIND,
                "idempotency_key": arguments.idempotency_key,
                "env_id": arguments.env_id,
                "candidate_sha": arguments.candidate_sha,
                "candidate_tree": arguments.candidate_tree,
                "bundle_sha256": digest,
                "bundle_size": size,
                "image_digests": {
                    "amd64": arguments.amd64_image_digest,
                    "arm64": arguments.arm64_image_digest,
                },
            },
            descriptor,
        )
    if arguments.command in {"create", "update"}:
        descriptor, size, digest = _bundle_binding(arguments.bundle)
        request = {
            **common,
            "kind": CREATE_KIND if arguments.command == "create" else UPDATE_KIND,
            "idempotency_key": arguments.idempotency_key,
            "candidate_sha": arguments.candidate_sha,
            "candidate_tree": arguments.candidate_tree,
            "bundle_sha256": digest,
            "bundle_size": size,
            "image_digests": {
                "amd64": arguments.amd64_image_digest,
                "arm64": arguments.arm64_image_digest,
            },
        }
        if arguments.command == "create":
            request["display_name"] = arguments.display_name
        return request, descriptor
    raise ClientError("command is unsupported")


def _exchange(
    request: Mapping[str, Any],
    descriptor: int | None,
) -> dict[str, Any]:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        connection.connect(str(SOCKET_PATH))
        ancillary: list[tuple[int, int, bytes]] = []
        if descriptor is not None:
            descriptors = array.array("i", [descriptor])
            ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, descriptors.tobytes())]
        connection.sendmsg([_canonical(request)], ancillary)
        raw, received_ancillary, flags, _address = connection.recvmsg(
            MAX_RESPONSE_BYTES + 1,
            socket.CMSG_SPACE(array.array("i").itemsize),
        )
    except OSError as exc:
        raise ClientError("authority is unavailable") from exc
    finally:
        connection.close()
    if (
        not raw
        or len(raw) > MAX_RESPONSE_BYTES
        or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC)
        or received_ancillary
    ):
        raise ClientError("authority response transport is invalid")
    try:
        response = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClientError("authority response is invalid") from exc
    if (
        not isinstance(response, dict)
        or raw != _canonical(response)
        or response.get("schema_version") != SCHEMA_VERSION
    ):
        raise ClientError("authority response binding is invalid")
    if response.get("status") == "succeeded":
        if (
            set(response) != {"schema_version", "kind", "status", "result"}
            or response.get("kind") != f"{request['kind']}.response"
        ):
            raise ClientError("authority response binding is invalid")
    elif response.get("status") == "failed":
        if set(response) != {
            "schema_version",
            "kind",
            "status",
            "error",
        }:
            raise ClientError("authority response binding is invalid")
    else:
        raise ClientError("authority response binding is invalid")
    return response


def main(argv: Sequence[str] | None = None) -> int:
    descriptor: int | None = None
    try:
        arguments = _parser().parse_args(list(argv) if argv is not None else None)
        request, descriptor = _build_request(arguments)
        response = _exchange(request, descriptor)
    except ClientError:
        sys.stderr.write("error: developer environment request failed safely\n")
        return 1
    finally:
        if descriptor is not None:
            os.close(descriptor)
    sys.stdout.buffer.write(_canonical(response))
    return 0 if response["status"] == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
