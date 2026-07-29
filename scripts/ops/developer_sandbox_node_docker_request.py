#!/usr/bin/python3 -I
"""Render one canonical, phase-bound developer-sandbox Docker request."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import secrets
import stat
import sys
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2
KIND = "loom.developer-sandbox.node-docker-bootstrap"
ACTIONS = (
    "authority-bootstrap",
    "authority-upgrade",
    "transport-server-bootstrap",
    "transport-client-bootstrap",
    "transport-upgrade",
    "readback",
)
EXPECTATIONS = ("not-checked", "absent", "server", "client-server")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NODE_RE = re.compile(r"^(?:oldlab-[1-5]|trt-gb10-(?:[1-9]|1[0-5]))$")
INPUT_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}(?:[.]pub)?$|^known_hosts$")
MAX_BUNDLE_BYTES = 256 * 1024 * 1024
MAX_INPUT_BYTES = 2 * 1024 * 1024


class RequestRenderError(RuntimeError):
    """A secret-safe request rendering failure."""


def _canonical(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"


def _regular_payload(path: Path, *, limit: int) -> bytes:
    try:
        metadata = path.lstat()
        payload = path.read_bytes()
    except OSError as exc:
        raise RequestRenderError("request input is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size > limit
        or len(payload) != metadata.st_size
    ):
        raise RequestRenderError("request input is unsafe")
    return payload


def _input_digests(root: Path) -> dict[str, str]:
    try:
        metadata = root.lstat()
        entries = tuple(root.iterdir())
    except OSError as exc:
        raise RequestRenderError("request input inventory is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RequestRenderError("request input inventory is unsafe")
    if len({entry.name for entry in entries}) != len(entries):
        raise RequestRenderError("request input inventory is not closed")
    digests: dict[str, str] = {}
    for entry in sorted(entries):
        if INPUT_RE.fullmatch(entry.name) is None:
            raise RequestRenderError("request input inventory is not closed")
        digests[entry.name] = hashlib.sha256(
            _regular_payload(entry, limit=MAX_INPUT_BYTES),
        ).hexdigest()
    return digests


def _validate_semantics(
    *,
    action: str,
    expectation: str,
    inputs: dict[str, str],
) -> None:
    if action == "readback":
        if expectation == "not-checked" or inputs:
            raise RequestRenderError("readback request contract is invalid")
    elif expectation != "not-checked":
        raise RequestRenderError("non-readback request contract is invalid")
    if action in {"authority-bootstrap", "authority-upgrade"} and inputs:
        raise RequestRenderError("authority request has unexpected inputs")
    if action == "transport-server-bootstrap" and (
        not inputs or any(not name.endswith(".pub") for name in inputs)
    ):
        raise RequestRenderError("transport server input set is invalid")
    if action == "transport-client-bootstrap":
        names = set(inputs)
        if "known_hosts" not in names:
            raise RequestRenderError("transport client known_hosts is missing")
        names.remove("known_hosts")
        private = {name for name in names if not name.endswith(".pub")}
        public = {name.removesuffix(".pub") for name in names if name.endswith(".pub")}
        if not private or private != public:
            raise RequestRenderError("transport client role set is invalid")


def render(args: argparse.Namespace) -> bytes:
    operation_id = args.operation_id or secrets.token_hex(32)
    if (
        SHA_RE.fullmatch(args.candidate_sha) is None
        or SHA_RE.fullmatch(args.candidate_tree) is None
        or SHA256_RE.fullmatch(operation_id) is None
        or NODE_RE.fullmatch(args.expected_node) is None
    ):
        raise RequestRenderError("request binding is invalid")
    inputs = _input_digests(args.input_root)
    _validate_semantics(
        action=args.action,
        expectation=args.transport_expectation,
        inputs=inputs,
    )
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "operation_id": operation_id,
        "transport_expectation": args.transport_expectation,
        "action": args.action,
        "candidate_sha": args.candidate_sha,
        "candidate_tree": args.candidate_tree,
        "candidate_bundle_sha256": hashlib.sha256(
            _regular_payload(args.candidate_bundle, limit=MAX_BUNDLE_BYTES),
        ).hexdigest(),
        "expected_node": args.expected_node,
        "inputs": inputs,
    }
    return _canonical(
        {
            **unsigned,
            "request_id": hashlib.sha256(_canonical(unsigned)).hexdigest(),
        },
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--action", choices=ACTIONS, required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-tree", required=True)
    parser.add_argument("--candidate-bundle", type=Path, required=True)
    parser.add_argument("--expected-node", required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--operation-id")
    parser.add_argument("--transport-expectation", choices=EXPECTATIONS, required=True)
    return parser


def main() -> int:
    try:
        payload = render(_parser().parse_args())
    except RequestRenderError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
