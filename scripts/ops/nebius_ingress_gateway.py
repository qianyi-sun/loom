#!/usr/bin/env python3
"""Private, journaled ingress installation primitives; no public cutover on import."""
from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from cryptography import x509
from scripts.ops import nebius_certificates as certificates


class IngressError(RuntimeError):
    """Payload-free failure; private TLS/API diagnostics must not reach Actions."""


@dataclass(frozen=True)
class TLSBinding:
    installation_id: str
    certificate_installation_id: str
    namespace: str
    namespace_uid: str
    kube_system_uid: str
    child_domain: str
    management_host: str

    def __post_init__(self) -> None:
        try:
            for value in (self.installation_id, self.certificate_installation_id,
                          self.namespace_uid, self.kube_system_uid):
                if str(UUID(value)) != value or UUID(value).int == 0:
                    raise ValueError()
            if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", self.namespace):
                raise ValueError()
            certificates.certificate_names(self.child_domain, self.management_host)
        except (ValueError, TypeError, certificates.CertificateError):
            raise IngressError("invalid protected ingress binding") from None


class TLSAPI(Protocol):
    def verify_identity(self, binding: TLSBinding) -> None:
        """Read back exact kube-system and destination namespace UIDs before writes."""
        ...

    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None: ...

    def create_secret(self, document: dict[str, Any]) -> None:
        """Create only; never apply/replace or silently retry an ambiguous write."""
        ...


def _verify_secret(observed: dict[str, Any], desired: dict[str, Any], recorded_uid: str | None) -> str:
    try:
        metadata = observed["metadata"]
        wanted = desired["metadata"]
        uid = str(UUID(metadata["uid"]))
        if (uid != metadata["uid"] or UUID(uid).int == 0 or (recorded_uid is not None and uid != recorded_uid)
                or metadata.get("deletionTimestamp") is not None or metadata.get("ownerReferences")
                or metadata["namespace"] != wanted["namespace"] or metadata["name"] != wanted["name"]
                or any(metadata.get("labels", {}).get(k) != v for k, v in wanted["labels"].items())
                or any(metadata.get("annotations", {}).get(k) != v for k, v in wanted["annotations"].items())
                or observed.get("apiVersion") != "v1" or observed.get("kind") != "Secret"
                or observed.get("type") != "kubernetes.io/tls" or observed.get("immutable") is not True
                or observed.get("data") != desired["data"]):
            raise ValueError()
        return uid
    except (ValueError, TypeError, KeyError):
        raise IngressError("TLS Secret ownership, identity or material differs") from None


def deliver_tls(config: dict[str, Any], *, binding: TLSBinding, api: TLSAPI,
                now: datetime | None = None, roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    """Deliver one selected generation; retain all old Secrets and recovery state.

    Operational callers use system trust. Tests may inject a local trust root.
    The API adapter must be bound by the protected installer, not a user request.
    A persisted create intent with no observed Secret blocks subsequent writes;
    an exact object readback resolves a lost reply without submitting another.
    """
    try:
        root = Path(config["state_dir"])
        if (config["installation_id"] != binding.certificate_installation_id
                or config["child_domain"] != binding.child_domain or config["management_host"] != binding.management_host):
            raise IngressError("certificate and ingress installation bindings differ")
        with certificates._locked_state(root):
            if certificates.load_installation(root / "installation.json") != config:
                raise IngressError("certificate installation state differs")
            selected = certificates._selected(root)
            if selected is None:
                raise IngressError("qualified certificate selection is unavailable")
            generation = selected["generation"]
            directory = root / "generations" / generation
            for parent in (directory.parent, directory):
                certificates._private_directory(parent)
            chain = certificates._private_read(directory / "fullchain.pem")
            key = certificates._private_read(directory / "privkey.pem", limit=16_384)
            report = certificates.validate_certificate(chain, key, child_domain=binding.child_domain,
                                                       management_host=binding.management_host, now=now, roots=roots)
            if hashlib.sha256(chain).hexdigest() != generation or any(selected[k] != v for k, v in report.items()):
                raise IngressError("selected certificate differs from freshly validated generation")
            name = "loom-ingress-tls-" + hashlib.sha256((binding.installation_id + ":" + generation).encode()).hexdigest()[:40]
            desired = {
                "apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/tls", "immutable": True,
                "metadata": {"name": name, "namespace": binding.namespace,
                             "labels": {"app.kubernetes.io/name": "loom-shared-ingress",
                                        "loom.openai.com/ingress-installation": binding.installation_id},
                             "annotations": {"loom.openai.com/certificate-generation": generation}},
                "data": {"tls.crt": base64.b64encode(chain).decode(), "tls.key": base64.b64encode(key).decode()},
            }
            api.verify_identity(binding)
            deliveries = root / "deliveries"
            certificates._private_directory(deliveries)
            receipt_path = deliveries / (binding.installation_id + "-" + generation + ".json")
            identity = {"schema": "loom.nebius-ingress-tls.v1", "binding": asdict(binding),
                        "certificate_generation": generation, "fingerprint_sha256": report["fingerprint_sha256"],
                        "secret_name": name}
            receipt = None
            if receipt_path.exists() or receipt_path.is_symlink():
                receipt = json.loads(certificates._private_read(receipt_path))
                if (not isinstance(receipt, dict) or set(receipt) != {*identity, "status", "secret_uid"}
                        or any(receipt[k] != v for k, v in identity.items())
                        or receipt["status"] not in {"create_intent", "tls_delivered"}
                        or (receipt["status"] == "create_intent") != (receipt["secret_uid"] is None)):
                    raise IngressError("TLS delivery receipt differs; reconcile before mutation")
            observed = api.get_secret(binding.namespace, name)
            if receipt is None:
                if observed is not None:
                    raise IngressError("untracked TLS Secret exists; refusing adoption")
                receipt = {**identity, "status": "create_intent", "secret_uid": None}
                certificates._atomic_json(receipt_path, receipt)
                try:
                    api.create_secret(desired)
                except Exception:
                    # No retry: only exact readback may resolve an unknown write.
                    pass
                observed = api.get_secret(binding.namespace, name)
            if observed is None:
                raise IngressError("TLS create outcome unresolved; preserve intent and reconcile")
            uid = _verify_secret(observed, desired, receipt["secret_uid"])
            result = {**identity, "status": "tls_delivered", "secret_uid": uid}
            if result != receipt:
                certificates._atomic_json(receipt_path, result)
            return result
    except IngressError:
        raise
    except Exception:
        raise IngressError("private TLS delivery failed; preserve state for reconciliation") from None
