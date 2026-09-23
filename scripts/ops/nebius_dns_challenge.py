#!/usr/bin/env python3
"""Narrow GoDaddy v3 DNS-01 boundary for protected certificate operations.

Only the selected certificate domain's TXT challenge can be added or removed.
This is not an ACME client or a generic DNS administration tool.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import ipaddress
import json
import math
import os
import re
import stat
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Self

import dns.exception
import dns.flags
import dns.message
import dns.name
import dns.query
import dns.rcode
import dns.rdatatype
import dns.resolver
import httpx

_API = "https://api.godaddy.com/v3/domains/zones/"
_HOST = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+")
_RECORD_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")
_VALIDATION = re.compile(r"[A-Za-z0-9_-]{43}")
_MAX_RESPONSE = 262_144


class DNSChallengeError(RuntimeError):
    """A fixed, payload-free diagnostic safe for protected operation output."""


def _private_json(path: Path, *, limit: int = 16_384) -> dict[str, Any]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise DNSChallengeError("private regular file required")
            content = stream.read(limit + 1)
        if len(content) > limit:
            raise DNSChallengeError("private input exceeds bound")
        value = json.loads(content)
        if not isinstance(value, dict):
            raise DNSChallengeError("private input must be an object")
        return value
    except (OSError, ValueError, RecursionError):
        raise DNSChallengeError("private input unavailable") from None


def _validate_token(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9._~+/=-]{1,8192}", value):
        raise DNSChallengeError("invalid DNS credential")
    return value


def load_token(path: Path, *, today: date | None = None) -> str:
    value = _private_json(path)
    try:
        expires = date.fromisoformat(value["expires_on"])
        if expires <= (today or datetime.now(UTC).date()):
            raise DNSChallengeError("DNS credential expired; renew through its owner route")
        return _validate_token(value["token"])
    except (KeyError, TypeError, ValueError):
        raise DNSChallengeError("invalid DNS credential metadata") from None


class GoDaddyDNS:
    """One certificate scope; no redirects, ambient proxies or write retries."""

    def __init__(self, zone: str, certificate_domain: str, token: str, *,
                 transport: httpx.BaseTransport | None = None) -> None:
        if (any(len(value) > 253 or not _HOST.fullmatch(value) for value in (zone, certificate_domain))
                or not certificate_domain.endswith("." + zone)):
            raise DNSChallengeError("certificate domain must be a child of the selected DNS zone")
        self.zone = zone
        self.certificate_domain = certificate_domain
        self.name = "_acme-challenge." + certificate_domain[:-(len(zone) + 1)]
        if len("_acme-challenge." + certificate_domain) > 253:
            raise DNSChallengeError("challenge name exceeds DNS bound")
        self._url = _API + zone + "/dns-records"
        self._client = httpx.Client(
            headers={"Authorization": "Bearer " + _validate_token(token), "Accept": "application/json",
                     "Accept-Encoding": "identity"},
            timeout=25, follow_redirects=False, trust_env=False,
            transport=transport or httpx.HTTPTransport(retries=0),
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self._client.close()

    def _request(self, method: str, *, page: int = 1, record_id: str | None = None,
                 body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = self._url if record_id is None else self._url + "/" + record_id
        params = {"type": "TXT", "name": self.name, "page": str(page), "pageSize": "100"} if method == "GET" else None
        diagnostic = "DNS inventory unavailable" if method == "GET" else "DNS write outcome ambiguous; preserve journal and reconcile"
        try:
            with self._client.stream(method, url, params=params, json=body) as response:
                if (response.status_code != {"GET": 200, "POST": 201, "DELETE": 204}[method]
                        or response.headers.get("content-encoding", "identity").lower() != "identity"):
                    raise DNSChallengeError(diagnostic)
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16_384):
                    if len(content) + len(chunk) > _MAX_RESPONSE:
                        raise DNSChallengeError(diagnostic)
                    content.extend(chunk)
                if method == "DELETE":
                    return {}
                value = json.loads(content)
                if not isinstance(value, dict):
                    raise DNSChallengeError(diagnostic)
                return value
        except (httpx.HTTPError, ValueError, RecursionError):
            raise DNSChallengeError(diagnostic) from None

    def _record(self, value: Any) -> dict[str, Any]:
        if (not isinstance(value, dict) or not isinstance(value.get("recordId"), str)
                or not _RECORD_ID.fullmatch(value["recordId"])
                or value.get("type") != "TXT" or value.get("name") != self.name
                or not isinstance(value.get("data"), str) or len(value["data"]) > 4096
                or type(value.get("ttl")) is not int or not 600 <= value["ttl"] <= 86400):
            raise DNSChallengeError("invalid scoped DNS record")
        return {key: value[key] for key in ("recordId", "type", "name", "data", "ttl")}

    def records(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        identities: set[str] = set()
        for page in range(1, 11):
            items = self._request("GET", page=page).get("items")
            if not isinstance(items, list) or len(items) > 100:
                raise DNSChallengeError("invalid DNS inventory")
            for item in items:
                record = self._record(item)
                if record["recordId"] in identities:
                    raise DNSChallengeError("duplicate DNS inventory identity")
                identities.add(record["recordId"])
                result.append(record)
            if len(items) < 100:
                return result
        raise DNSChallengeError("DNS inventory exceeds page bound")

    def create(self, validation: str) -> dict[str, Any]:
        if not _VALIDATION.fullmatch(validation):
            raise DNSChallengeError("invalid DNS-01 validation")
        desired = {"type": "TXT", "name": self.name, "data": validation, "ttl": 600}
        record = self._record(self._request("POST", body=desired))
        if any(record[key] != value for key, value in desired.items()):
            raise DNSChallengeError("DNS create readback differs; preserve pending journal")
        return record

    def delete_owned(self, expected: dict[str, Any]) -> None:
        expected = self._record(expected)
        if not _VALIDATION.fullmatch(expected["data"]) or expected["ttl"] != 600:
            raise DNSChallengeError("record is not an owned DNS-01 challenge")
        current = next((row for row in self.records() if row["recordId"] == expected["recordId"]), None)
        if current is None:
            return
        if current != expected:
            raise DNSChallengeError("DNS challenge changed; cleanup requires reconciliation")
        self._request("DELETE", record_id=expected["recordId"])


@contextmanager
def _journal_lock(root: Path, key: str) -> Iterator[Path]:
    try:
        root.mkdir(mode=0o700, parents=False, exist_ok=True)
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise DNSChallengeError("private journal directory required")
        # fsync(root) cannot make root's own entry durable in its parent. Do
        # this even on replay: an earlier process could have stopped after mkdir.
        parent = os.open(root.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        descriptor = os.open(root / (key + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "r+b") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise DNSChallengeError("private journal lock required")
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield root / (key + ".json")
    except OSError:
        raise DNSChallengeError("challenge journal unavailable or already in use") from None


def _save_journal(path: Path, value: dict[str, Any]) -> None:
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, prefix=".challenge-", delete=False) as stream:
            temporary = stream.name
            stream.write(json.dumps(value, sort_keys=True).encode())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            os.unlink(temporary)


def run_hook(dns: GoDaddyDNS, *, state_dir: Path, action: str, certbot_domain: str,
             validation: str, wait: Callable[[str, str, str], None]) -> str:
    """Journal ownership before reporting auth success; never infer write recovery."""
    if (action not in {"auth", "cleanup"} or not _VALIDATION.fullmatch(validation)
            or certbot_domain.removeprefix("*.") != dns.certificate_domain):
        raise DNSChallengeError("certificate hook is outside the protected scope")
    identity = {"schema": "loom.dns-challenge.v1", "zone": dns.zone,
                "domain": dns.certificate_domain, "validation": validation}
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    with _journal_lock(state_dir, key) as path:
        try:
            path.lstat()
        except FileNotFoundError:
            journal = None
        else:
            journal = _private_json(path, limit=262_144)
            before_ids = journal.get("before_record_ids")
            if (any(journal.get(key) != value for key, value in identity.items())
                    or journal.get("stage") not in {"pending", "created", "deleted"}
                    or not isinstance(before_ids, list) or len(before_ids) > 1000
                    or any(not isinstance(item, str) or not _RECORD_ID.fullmatch(item) for item in before_ids)
                    or len(set(before_ids)) != len(before_ids)):
                raise DNSChallengeError("invalid challenge journal")
        if journal is not None and journal["stage"] == "pending":
            raise DNSChallengeError("pending DNS write requires reconciliation; no automatic retry or cleanup")
        if action == "cleanup" and (journal is None or journal["stage"] == "deleted"):
            return "cleaned"
        if journal is not None and journal["stage"] == "deleted":
            raise DNSChallengeError("cleaned challenge cannot be reused")
        if journal is None:
            before = dns.records()
            if any(row["data"] == validation for row in before):
                raise DNSChallengeError("preexisting challenge has no ownership journal")
            journal = {**identity, "stage": "pending", "prepared_at": datetime.now(UTC).isoformat(),
                       "before_record_ids": [row["recordId"] for row in before]}
            _save_journal(path, journal)
            created = dns.create(validation)
            if any(row["recordId"] == created["recordId"] for row in before):
                raise DNSChallengeError("DNS creation reused an existing record identity")
            journal = {**journal, "stage": "created", "record": created}
            _save_journal(path, journal)
        record = dns._record(journal.get("record"))
        if (record["data"] != validation or record["ttl"] != 600
                or record["recordId"] in journal["before_record_ids"]):
            raise DNSChallengeError("journal record does not match its challenge")
        if action == "cleanup":
            dns.delete_owned(record)
            _save_journal(path, {**journal, "stage": "deleted"})
            return "cleaned"
        if not any(row == record for row in dns.records()):
            raise DNSChallengeError("owned DNS challenge is absent or changed")
        wait(dns.zone, dns.name + "." + dns.zone, validation)
        return "present"


def _authorities(zone: str, deadline: float) -> list[str]:
    """Resolve the selected zone's authority, not a recursive TXT cache."""
    resolver = dns.resolver.Resolver()

    def resolve(name: str, kind: str) -> dns.resolver.Answer:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DNSChallengeError("DNS propagation deadline")
        return resolver.resolve(name, kind, lifetime=min(5, remaining))

    try:
        answer = resolve(zone, "NS")
        if answer.canonical_name != dns.name.from_text(zone) or not 1 <= len(answer) <= 8:
            raise DNSChallengeError("invalid authoritative DNS inventory")
        servers = []
        for authority in answer:
            addresses = resolve(str(authority.target), "A")
            if not 1 <= len(addresses) <= 8:
                raise DNSChallengeError("invalid authoritative DNS addresses")
            for address in addresses:
                value = str(address.address)
                if not ipaddress.ip_address(value).is_global:
                    raise DNSChallengeError("authoritative DNS address is not public")
                servers.append(value)
        return list(dict.fromkeys(servers))
    except (dns.exception.DNSException, ValueError):
        raise DNSChallengeError("authoritative DNS discovery unavailable") from None


def wait_for_txt(zone: str, name: str, validation: str, *, timeout: float = 600) -> None:
    if not math.isfinite(timeout) or not 0 < timeout <= 600:
        raise DNSChallengeError("invalid DNS propagation deadline")
    deadline = time.monotonic() + timeout
    servers = _authorities(zone, deadline)
    wanted = dns.name.from_text(name)
    while time.monotonic() < deadline:
        complete = True
        for server in servers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DNSChallengeError("DNS propagation deadline")
            query = dns.message.make_query(wanted, "TXT")
            query.flags &= ~dns.flags.RD
            try:
                reply = dns.query.udp(query, server, timeout=min(3, remaining))
                if reply.flags & dns.flags.TC:
                    reply = dns.query.tcp(query, server, timeout=min(3, max(0.01, deadline - time.monotonic())))
            except dns.exception.DNSException:
                complete = False
                continue
            if not reply.flags & dns.flags.AA:
                raise DNSChallengeError("challenge is outside the selected authoritative DNS zone")
            if any(row.rdtype == dns.rdatatype.CNAME for row in reply.answer):
                raise DNSChallengeError("aliased DNS challenge requires separate qualification")
            values = [b"".join(item.strings) for row in reply.answer
                      if row.name == wanted and row.rdtype == dns.rdatatype.TXT for item in row]
            if reply.rcode() != dns.rcode.NOERROR or validation.encode() not in values:
                complete = False
        if complete:
            return
        time.sleep(min(5, max(0, deadline - time.monotonic())))
    raise DNSChallengeError("DNS propagation deadline")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("auth", "cleanup"))
    parser.add_argument("--zone", required=True)
    parser.add_argument("--certificate-domain", required=True)
    parser.add_argument("--credential-file", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        with GoDaddyDNS(args.zone, args.certificate_domain, load_token(args.credential_file)) as provider:
            status = run_hook(provider, state_dir=args.state_dir, action=args.action,
                              certbot_domain=os.environ.get("CERTBOT_DOMAIN", ""),
                              validation=os.environ.get("CERTBOT_VALIDATION", ""), wait=wait_for_txt)
        print(json.dumps({"status": status}))
        return 0
    except DNSChallengeError as exc:
        # This exception class contains only our fixed, payload-free categories.
        print(str(exc), file=sys.stderr)
        return 1
    except Exception:
        # Neither remote diagnostics nor filesystem/credential values belong in logs.
        print("DNS challenge failed; preserve private journal for scoped reconciliation", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
