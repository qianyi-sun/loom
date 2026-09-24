"""Fixed personal/management A-record publication; no generic DNS mutation API."""
from __future__ import annotations

import ipaddress
import json
import math
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, Self
from uuid import UUID, uuid4

import dns.exception
import dns.flags
import dns.message
import dns.name
import dns.query
import dns.rcode
import dns.rdatatype
import dns.resolver
import httpx
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_dns_challenge import _authorities, _validate_token

_HOST = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+")
_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


class PublicationError(RuntimeError):
    """Fixed diagnostic; preserve private journals and never expose credentials."""


def validate_target(target: dict[str, Any]) -> tuple[str, str]:
    try:
        if (set(target) != {"installation_id", "service_uid", "candidate", "fingerprint_sha256",
                            "zone", "child_domain", "management_host", "address"}
                or any(not isinstance(value, str) for value in target.values())):
            raise ValueError()
        for key in ("installation_id", "service_uid"):
            if str(UUID(target[key])) != target[key] or UUID(target[key]).int == 0:
                raise ValueError()
        if (not re.fullmatch(r"[0-9a-f]{40}", target["candidate"])
                or not re.fullmatch(r"[0-9a-f]{64}", target["fingerprint_sha256"])):
            raise ValueError()
        for key in ("zone", "child_domain", "management_host"):
            if len(target[key]) > 251 or not _HOST.fullmatch(target[key]):
                raise ValueError()
        zone, child, management = target["zone"], target["child_domain"], target["management_host"]
        if (not child.endswith("." + zone) or not management.endswith("." + zone)
                or management == child or management.endswith("." + child)):
            raise ValueError()
        address = ipaddress.IPv4Address(target["address"])
        if not address.is_global or address.is_multicast or str(address) != target["address"]:
            raise ValueError()
        return "*." + child[:-(len(zone) + 1)], management[:-(len(zone) + 1)]
    except (ValueError, TypeError, KeyError, AttributeError):
        raise PublicationError("invalid fixed DNS publication target") from None


def _record(value: Any, name: str) -> dict[str, Any]:
    if (not isinstance(value, dict) or value.get("name") != name
            or not isinstance(value.get("recordId"), str) or not _ID.fullmatch(value["recordId"])
            or not isinstance(value.get("type"), str) or not re.fullmatch(r"[A-Z0-9]{1,12}", value["type"])
            or not isinstance(value.get("data"), str) or len(value["data"]) > 4096
            or type(value.get("ttl")) is not int or not 600 <= value["ttl"] <= 86400):
        raise PublicationError("invalid exact-name DNS inventory")
    return {key: value[key] for key in ("recordId", "name", "type", "data", "ttl")}


class DNSProvider(Protocol):
    def records(self, name: str) -> list[dict[str, Any]]: ...
    def create(self, name: str, address: str) -> dict[str, Any]: ...


class GoDaddyPublication:
    """GET exact names; POST only their fixed A value. No redirects/write retries."""

    def __init__(self, target: dict[str, Any], token: str, *, transport: httpx.BaseTransport | None = None):
        self.names = validate_target(target)
        self.address = target["address"]
        self.url = "https://api.godaddy.com/v3/domains/zones/" + target["zone"] + "/dns-records"
        self.client = httpx.Client(headers={"Authorization": "Bearer " + _validate_token(token),
                                           "Accept": "application/json", "Accept-Encoding": "identity"},
                                   timeout=25, follow_redirects=False, trust_env=False,
                                   transport=transport or httpx.HTTPTransport(retries=0))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.client.close()

    def _request(self, method: str, name: str, *, page: int = 1) -> dict[str, Any]:
        if name not in self.names or method not in {"GET", "POST"}:
            raise PublicationError("DNS request outside fixed publication scope")
        params = {"name": name, "page": str(page), "pageSize": "100"} if method == "GET" else None
        body = {"name": name, "type": "A", "data": self.address, "ttl": 600} if method == "POST" else None
        try:
            with self.client.stream(method, self.url, params=params, json=body) as response:
                if (response.status_code != (200 if method == "GET" else 201)
                        or response.headers.get("content-encoding", "identity").lower() != "identity"):
                    raise ValueError()
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(content) + len(chunk) > 262144:
                        raise ValueError()
                    content.extend(chunk)
                result = json.loads(content)
                if not isinstance(result, dict):
                    raise ValueError()
                return result
        except (httpx.HTTPError, ValueError, RecursionError):
            raise PublicationError("DNS inventory unavailable" if method == "GET" else
                                   "DNS write outcome uncertain; preserve publication journal") from None

    def records(self, name: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        ids: set[str] = set()
        for page in range(1, 11):
            response = self._request("GET", name, page=page)
            items = response.get("items")
            if (not isinstance(items, list) or len(items) > 100
                    or (len(items) < 100 and response.get("nextPage") is not None)):
                raise PublicationError("incomplete DNS inventory")
            for item in items:
                row = _record(item, name)
                if row["recordId"] in ids:
                    raise PublicationError("duplicate DNS inventory identity")
                ids.add(row["recordId"])
                rows.append(row)
            if len(items) < 100:
                return rows
        raise PublicationError("DNS inventory exceeds page bound")

    def create(self, name: str, address: str) -> dict[str, Any]:
        if address != self.address:
            raise PublicationError("DNS address outside fixed publication scope")
        row = _record(self._request("POST", name), name)
        if row["type"] != "A" or row["data"] != address or row["ttl"] != 600:
            raise PublicationError("DNS create response differs; preserve publication journal")
        return row


def _selected(provider: DNSProvider, name: str, address: str) -> dict[str, Any] | None:
    rows = provider.records(name)
    if not isinstance(rows, list) or len(rows) > 1000:
        raise PublicationError("conflicting DNS records at publication name")
    selected = None
    identities: set[str] = set()
    for value in rows:
        row = _record(value, name)
        if row["recordId"] in identities:
            raise PublicationError("duplicate DNS inventory identity")
        identities.add(row["recordId"])
        if row["type"] == "TXT":
            continue  # Verification text does not change HTTP address resolution.
        if row["type"] != "A" or row["data"] != address or selected is not None:
            raise PublicationError("foreign DNS record at publication name")
        selected = row
    return selected


def _dns_deadline(target: dict[str, Any], timeout: float) -> float:
    validate_target(target)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 0 < timeout <= 600:
        raise PublicationError("invalid DNS observation deadline")
    return time.monotonic() + timeout


def _observe_addresses(target: dict[str, Any], names: tuple[str, str], servers: list[str], *,
                       deadline: float, require_address: bool) -> bool:
    complete = True
    for server in servers:
        for host in names:
            wanted = dns.name.from_text(host)
            for kind in (dns.rdatatype.A, dns.rdatatype.AAAA):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PublicationError("DNS observation deadline")
                request = dns.message.make_query(wanted, kind)
                request.flags &= ~dns.flags.RD
                try:
                    response = dns.query.udp(request, server, timeout=min(3, remaining))
                    if response.flags & dns.flags.TC:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise PublicationError("DNS observation deadline")
                        response = dns.query.tcp(request, server, timeout=min(3, remaining))
                except dns.exception.DNSException:
                    if not require_address:
                        raise PublicationError("DNS authority unavailable before publication") from None
                    complete = False
                    continue
                if (not response.flags & dns.flags.AA or response.flags & dns.flags.TC
                        or any(row.rdtype in {dns.rdatatype.CNAME, dns.rdatatype.DNAME} for row in response.answer)):
                    raise PublicationError("DNS alias or delegation requires separate qualification")
                code = response.rcode()
                values = [str(item) for row in response.answer if row.name == wanted and row.rdtype == kind for item in row]
                if kind == dns.rdatatype.AAAA and values:
                    raise PublicationError("IPv6 DNS route is outside the qualified IPv4 ingress")
                if not require_address:
                    if code not in {dns.rcode.NOERROR, dns.rcode.NXDOMAIN} or (values and values != [target["address"]]):
                        raise PublicationError("DNS authority conflicts with the qualified ingress")
                elif code != dns.rcode.NOERROR or (kind == dns.rdatatype.A and values != [target["address"]]):
                    complete = False
    return complete


def qualify_authority(target: dict[str, Any], *, timeout: float = 30) -> None:
    """Pre-write authority proof permits absent names, never aliases/delegation."""
    deadline = _dns_deadline(target, timeout)
    try:
        servers = _authorities(target["zone"], deadline)
        names = ("*." + target["child_domain"], target["management_host"])
        _observe_addresses(target, names, servers, deadline=deadline, require_address=False)
    except PublicationError:
        raise
    except Exception:
        raise PublicationError("DNS authority unavailable before publication") from None


def wait_for_addresses(target: dict[str, Any], *, timeout: float = 600) -> None:
    """Prove wildcard synthesis and management A on every selected authority."""
    deadline = _dns_deadline(target, timeout)
    try:
        servers = _authorities(target["zone"], deadline)
        label = uuid4().hex[:min(32, 252 - len(target["child_domain"]))]
        names = (label + "." + target["child_domain"], target["management_host"])
        while time.monotonic() < deadline:
            if _observe_addresses(target, names, servers, deadline=deadline, require_address=True):
                return
            time.sleep(min(5, max(0, deadline - time.monotonic())))
        raise PublicationError("DNS propagation deadline")
    except PublicationError:
        raise
    except Exception:
        raise PublicationError("authoritative DNS propagation unavailable") from None


def qualify_public_routes(target: dict[str, Any], *, timeout: float = 30) -> None:
    """Verify normal recursive resolution and trusted wildcard/management TLS."""
    from scripts.ops.nebius_ingress_probe import probe_management

    deadline = _dns_deadline(target, timeout)
    try:
        resolver = dns.resolver.Resolver()
        label = uuid4().hex[:min(32, 252 - len(target["child_domain"]))]
        names = (label + "." + target["child_domain"], target["management_host"])
        for hostname in names:
            for kind in ("A", "AAAA"):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PublicationError("recursive DNS observation deadline")
                answer = resolver.resolve(hostname, kind, lifetime=remaining, search=False, raise_on_no_answer=False)
                if (answer.canonical_name != dns.name.from_text(hostname)
                        or [str(row) for row in answer] != ([target["address"]] if kind == "A" else [])):
                    raise PublicationError("recursive DNS differs from qualified ingress")
            probe_management(address=target["address"], port=443, hostname=hostname,
                             fingerprint=target["fingerprint_sha256"])
        if time.monotonic() >= deadline:
            raise PublicationError("public route qualification deadline")
    except PublicationError:
        raise
    except Exception:
        raise PublicationError("public DNS or trusted HTTPS route unavailable") from None


def publish_dns(provider: DNSProvider, *, target: dict[str, Any], state_dir: Path,
                qualify: Callable[[], dict[str, Any]], wait: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
    """One create per exact name; replay never recreates a deleted/uncertain record."""
    names = validate_target(target)
    target = dict(target)

    def verify() -> None:
        if qualify() != target:
            raise PublicationError("qualified ingress target changed")

    try:
        with private_state._locked_state(state_dir):
            path = state_dir / "dns-publication.json"
            verify()
            # Inventory both names before the first write, including a conflict
            # at the second name. This does not claim a provider-side transaction.
            current = {name: _selected(provider, name, target["address"]) for name in names}
            if path.exists() or path.is_symlink():
                journal = json.loads(private_state._private_read(path))
                if (set(journal) != {"schema", "target", "status", "records"}
                        or journal["schema"] != "loom.nebius-dns-publication.v1" or journal["target"] != target
                        or journal["status"] not in {"pending", "complete"} or set(journal["records"]) != set(names)):
                    raise PublicationError("DNS publication journal differs")
                for name, entry in journal["records"].items():
                    if (set(entry) != {"phase", "record", "origin"}
                            or entry["phase"] not in {"unstarted", "intent", "acknowledged", "qualified"}):
                        raise PublicationError("invalid DNS publication journal")
                    if entry["phase"] in {"unstarted", "intent"}:
                        if entry["record"] is not None or entry["origin"] is not None:
                            raise PublicationError("invalid pending DNS record")
                    else:
                        recorded = _record(entry["record"], name)
                        if (recorded["type"] != "A" or recorded["data"] != target["address"]
                                or entry["origin"] not in {"created", "external", "uncertain"}
                                or (entry["phase"] == "acknowledged" and entry["origin"] != "created")
                                or (entry["origin"] == "created" and recorded["ttl"] != 600)):
                            raise PublicationError("invalid recorded DNS target")
                    if journal["status"] == "complete" and entry["phase"] != "qualified":
                        raise PublicationError("incomplete DNS publication journal")
            else:
                journal = {"schema": "loom.nebius-dns-publication.v1", "target": target, "status": "pending",
                           "records": {name: {"phase": "qualified" if current[name] else "unstarted",
                                              "record": current[name], "origin": "external" if current[name] else None}
                                       for name in names}}
                private_state._atomic_json(path, journal)

            for name in names:
                verify()
                entry = journal["records"][name]
                row = _selected(provider, name, target["address"])
                if entry["phase"] == "unstarted":
                    if row is not None:
                        entry.update(phase="qualified", record=row, origin="external")
                    else:
                        entry["phase"] = "intent"
                        private_state._atomic_json(path, journal)
                        try:
                            created = _record(provider.create(name, target["address"]), name)
                            if created["type"] != "A" or created["data"] != target["address"] or created["ttl"] != 600:
                                raise PublicationError("DNS create response differs")
                        except Exception:
                            pass  # Reconcile exact readback only, never repeat POST.
                        else:
                            entry.update(phase="acknowledged", record=created, origin="created")
                            private_state._atomic_json(path, journal)
                        row = _selected(provider, name, target["address"])
                if entry["phase"] == "intent":
                    if row is None:
                        raise PublicationError("DNS write unresolved; preserve journal without retry")
                    entry.update(phase="qualified", record=row, origin="uncertain")
                elif entry["phase"] in {"qualified", "acknowledged"}:
                    if row != entry["record"]:
                        raise PublicationError("published DNS record changed or disappeared")
                    entry["phase"] = "qualified"
                private_state._atomic_json(path, journal)
            wait(dict(target))
            verify()
            for name in names:
                if _selected(provider, name, target["address"]) != journal["records"][name]["record"]:
                    raise PublicationError("DNS record changed during propagation")
            journal["status"] = "complete"
            private_state._atomic_json(path, journal)
            return {"status": "dns_published", **target,
                    "records": [{"name": name, "record_id": journal["records"][name]["record"]["recordId"],
                                 "origin": journal["records"][name]["origin"]} for name in names]}
    except PublicationError:
        raise
    except Exception:
        raise PublicationError("DNS publication incomplete; preserve private journal") from None
