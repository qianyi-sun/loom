#!/usr/bin/env python3
"""Parse and verify bounded Slurm prerequisite readback without mutation."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import sys
from collections.abc import Mapping, Sequence
from typing import Any

MAX_INPUT_BYTES = 1024 * 1024
_UNSIGNED_INTEGER = re.compile(r"[0-9]+")
_WALL_TIME = re.compile(r"(?:(?P<days>[0-9]+)-)?(?P<hours>[0-9]{1,2}):(?P<minutes>[0-9]{2}):(?P<seconds>[0-9]{2})")
_MEMORY = re.compile(r"(?P<amount>[0-9]+)(?P<unit>[KMGT])")


class ReadbackError(ValueError):
    """Raised when Slurm readback does not match its exact contract."""


def parse_parsable2_row(
    payload: str,
    field_names: tuple[str, ...],
    *,
    allow_absent: bool,
) -> dict[str, str] | None:
    """Return one semantic ``--parsable2`` row, accepting its optional sentinel."""
    if not field_names or len(set(field_names)) != len(field_names):
        raise ReadbackError("invalid field contract")
    if payload == "":
        if allow_absent:
            return None
        raise ReadbackError("required row is absent")

    row = payload
    if row.endswith("\r\n"):
        row = row[:-2]
    elif row.endswith("\n"):
        row = row[:-1]
    if not row or "\n" in row or "\r" in row or "\x00" in row:
        raise ReadbackError("readback must contain exactly one row")

    parts = row.split("|")
    if len(parts) == len(field_names) + 1 and parts[-1] == "":
        parts.pop()
    if len(parts) != len(field_names):
        raise ReadbackError("readback field count is invalid")
    return dict(zip(field_names, parts, strict=True))


def _parse_unsigned_integer(value: str) -> int:
    if len(value) > 20 or _UNSIGNED_INTEGER.fullmatch(value) is None:
        raise ReadbackError("integer field is invalid")
    return int(value)


def _parse_wall_seconds(value: str) -> int:
    matched = _WALL_TIME.fullmatch(value)
    if matched is None:
        raise ReadbackError("wall time is invalid")
    days = _parse_unsigned_integer(matched.group("days") or "0")
    hours = _parse_unsigned_integer(matched.group("hours"))
    minutes = _parse_unsigned_integer(matched.group("minutes"))
    seconds = _parse_unsigned_integer(matched.group("seconds"))
    if hours > 23 or minutes > 59 or seconds > 59:
        raise ReadbackError("wall time is invalid")
    return (((days * 24) + hours) * 60 + minutes) * 60 + seconds


def _canonical_wall(value: str) -> str:
    total = _parse_wall_seconds(value)
    days, remainder = divmod(total, 24 * 60 * 60)
    hours, remainder = divmod(remainder, 60 * 60)
    minutes, seconds = divmod(remainder, 60)
    clock = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{days}-{clock}" if days else clock


def _parse_list(value: str) -> list[str]:
    if value == "":
        return []
    items = value.split(",")
    if any(not item for item in items):
        raise ReadbackError("list field is invalid")
    return sorted(set(items))


def _parse_memory_mib(value: str) -> int:
    matched = _MEMORY.fullmatch(value)
    if matched is None:
        raise ReadbackError("memory TRES is invalid")
    amount = _parse_unsigned_integer(matched.group("amount"))
    unit = matched.group("unit")
    if unit == "K":
        if amount % 1024:
            raise ReadbackError("memory TRES is not an integral MiB value")
        return amount // 1024
    multipliers = {"M": 1, "G": 1024, "T": 1024 * 1024}
    return amount * multipliers[unit]


def _parse_group_tres(value: str) -> dict[str, int]:
    if value == "":
        return {}
    canonical: dict[str, int] = {}
    key_mapping = {"cpu": "cpu", "mem": "memory_mib", "node": "nodes"}
    for item in value.split(","):
        if item.count("=") != 1:
            raise ReadbackError("TRES field is invalid")
        raw_key, raw_value = item.split("=", 1)
        key = key_mapping.get(raw_key)
        if key is None or key in canonical:
            raise ReadbackError("TRES key is invalid")
        if raw_key == "mem":
            canonical[key] = _parse_memory_mib(raw_value)
        else:
            canonical[key] = _parse_unsigned_integer(raw_value)
    return canonical


def _require_exact(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    if dict(actual) != dict(expected):
        raise ReadbackError("Slurm readback drifted from policy")


def verify_account(
    payload: str,
    *,
    name: str,
    allow_absent: bool,
) -> dict[str, str] | None:
    """Verify an account row and return its canonical state."""
    row = parse_parsable2_row(payload, ("name",), allow_absent=allow_absent)
    if row is None:
        return None
    canonical = {"name": row["name"]}
    _require_exact(canonical, {"name": name})
    return canonical


def verify_qos(
    payload: str,
    *,
    name: str,
    flags: Sequence[str],
    priority: int,
    max_jobs_per_user: int,
    max_submit_jobs_per_user: int,
    max_wall: str,
    group_tres: Mapping[str, int],
    allow_absent: bool,
) -> dict[str, object] | None:
    """Verify a QoS row and return normalized resource semantics."""
    row = parse_parsable2_row(
        payload,
        (
            "name",
            "flags",
            "priority",
            "max_jobs_per_user",
            "max_submit_jobs_per_user",
            "max_wall",
            "group_tres",
        ),
        allow_absent=allow_absent,
    )
    if row is None:
        return None
    canonical: dict[str, object] = {
        "name": row["name"],
        "flags": _parse_list(row["flags"]),
        "priority": _parse_unsigned_integer(row["priority"]),
        "max_jobs_per_user": _parse_unsigned_integer(row["max_jobs_per_user"]),
        "max_submit_jobs_per_user": _parse_unsigned_integer(
            row["max_submit_jobs_per_user"]
        ),
        "max_wall": _canonical_wall(row["max_wall"]),
        "group_tres": _parse_group_tres(row["group_tres"]),
    }
    expected: dict[str, object] = {
        "name": name,
        "flags": sorted(set(flags)),
        "priority": priority,
        "max_jobs_per_user": max_jobs_per_user,
        "max_submit_jobs_per_user": max_submit_jobs_per_user,
        "max_wall": _canonical_wall(max_wall),
        "group_tres": dict(group_tres),
    }
    _require_exact(canonical, expected)
    return canonical


def verify_association(
    payload: str,
    *,
    cluster: str,
    account: str,
    user: str,
    partition: str | None,
    qos: Sequence[str],
    default_qos: str,
    allow_absent: bool,
) -> dict[str, object] | None:
    """Verify a user association with exact, order-independent QoS membership."""
    field_names = (
        ("cluster", "account", "user", "qos", "default_qos")
        if partition is None
        else ("cluster", "account", "user", "partition", "qos", "default_qos")
    )
    row = parse_parsable2_row(payload, field_names, allow_absent=allow_absent)
    if row is None:
        return None
    canonical: dict[str, object] = {
        "cluster": row["cluster"],
        "account": row["account"],
        "user": row["user"],
    }
    expected: dict[str, object] = {
        "cluster": cluster,
        "account": account,
        "user": user,
    }
    if partition is not None:
        canonical["partition"] = row["partition"]
        expected["partition"] = partition
    canonical.update(
        {
            "qos": _parse_list(row["qos"]),
            "default_qos": row["default_qos"],
        }
    )
    expected.update(
        {
            "qos": sorted(set(qos)),
            "default_qos": default_qos,
        }
    )
    _require_exact(canonical, expected)
    return canonical


def _reservation_tokens(payload: str) -> dict[str, str]:
    if not payload or "\x00" in payload:
        raise ReadbackError("reservation readback is absent")
    try:
        tokens = shlex.split(payload)
    except ValueError as exc:
        raise ReadbackError("reservation readback is invalid") from exc
    parsed: dict[str, str] = {}
    for token in tokens:
        if "=" not in token:
            raise ReadbackError("reservation token is invalid")
        key, value = token.split("=", 1)
        if not key or key in parsed:
            raise ReadbackError("reservation token is duplicated")
        parsed[key] = value
    return parsed


def verify_reservation(
    payload: str,
    *,
    name: str,
    node: str,
    node_count: int,
    partition: str,
    users: Sequence[str],
    accounts: Sequence[str],
    state: str,
    flags: Sequence[str],
) -> dict[str, object]:
    """Verify the immutable legacy reservation fields from ``scontrol`` output."""
    row = _reservation_tokens(payload)
    required = {
        "ReservationName",
        "Nodes",
        "NodeCnt",
        "PartitionName",
        "Users",
        "Accounts",
        "State",
        "Flags",
    }
    if not required.issubset(row):
        raise ReadbackError("reservation readback is incomplete")
    canonical: dict[str, object] = {
        "name": row["ReservationName"],
        "node": row["Nodes"],
        "node_count": _parse_unsigned_integer(row["NodeCnt"]),
        "partition": row["PartitionName"],
        "users": _parse_list(row["Users"]),
        "accounts": _parse_list(row["Accounts"]),
        "state": row["State"],
        "flags": _parse_list(row["Flags"]),
    }
    expected: dict[str, object] = {
        "name": name,
        "node": node,
        "node_count": node_count,
        "partition": partition,
        "users": sorted(set(users)),
        "accounts": sorted(set(accounts)),
        "state": state,
        "flags": sorted(set(flags)),
    }
    _require_exact(canonical, expected)
    return canonical


def _csv_argument(value: str) -> tuple[str, ...]:
    if value == "":
        return ()
    items = value.split(",")
    if any(not item for item in items):
        raise ReadbackError("CLI list is invalid")
    return tuple(items)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    account = subparsers.add_parser("account")
    account.add_argument("--name", required=True)
    account.add_argument("--allow-absent", action="store_true")

    qos = subparsers.add_parser("qos")
    qos.add_argument("--name", required=True)
    qos.add_argument("--flags", required=True)
    qos.add_argument("--priority", required=True, type=int)
    qos.add_argument("--max-jobs", required=True, type=int)
    qos.add_argument("--max-submit", required=True, type=int)
    qos.add_argument("--max-wall", required=True)
    qos.add_argument("--group-tres", required=True)
    qos.add_argument("--allow-absent", action="store_true")

    association = subparsers.add_parser("association")
    association.add_argument("--cluster", required=True)
    association.add_argument("--account", required=True)
    association.add_argument("--user", required=True)
    association.add_argument("--partition")
    association.add_argument("--qos", required=True)
    association.add_argument("--default-qos", required=True)
    association.add_argument("--allow-absent", action="store_true")

    reservation = subparsers.add_parser("reservation")
    reservation.add_argument("--name", required=True)
    reservation.add_argument("--node", required=True)
    reservation.add_argument("--node-count", required=True, type=int)
    reservation.add_argument("--partition", required=True)
    reservation.add_argument("--users", required=True)
    reservation.add_argument("--accounts", required=True)
    reservation.add_argument("--state", required=True)
    reservation.add_argument("--flags", required=True)
    return parser


def _read_stdin() -> str:
    raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ReadbackError("readback exceeds the size limit")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ReadbackError("readback is not UTF-8") from exc


def _run(arguments: argparse.Namespace, payload: str) -> object:
    if arguments.command == "account":
        return verify_account(
            payload,
            name=arguments.name,
            allow_absent=arguments.allow_absent,
        )
    if arguments.command == "qos":
        return verify_qos(
            payload,
            name=arguments.name,
            flags=_csv_argument(arguments.flags),
            priority=arguments.priority,
            max_jobs_per_user=arguments.max_jobs,
            max_submit_jobs_per_user=arguments.max_submit,
            max_wall=arguments.max_wall,
            group_tres=_parse_group_tres(arguments.group_tres),
            allow_absent=arguments.allow_absent,
        )
    if arguments.command == "association":
        return verify_association(
            payload,
            cluster=arguments.cluster,
            account=arguments.account,
            user=arguments.user,
            partition=arguments.partition,
            qos=_csv_argument(arguments.qos),
            default_qos=arguments.default_qos,
            allow_absent=arguments.allow_absent,
        )
    return verify_reservation(
        payload,
        name=arguments.name,
        node=arguments.node,
        node_count=arguments.node_count,
        partition=arguments.partition,
        users=_csv_argument(arguments.users),
        accounts=_csv_argument(arguments.accounts),
        state=arguments.state,
        flags=_csv_argument(arguments.flags),
    )


def main() -> int:
    arguments = _parser().parse_args()
    try:
        result = _run(arguments, _read_stdin())
    except ReadbackError:
        print("error: Slurm readback is invalid", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
