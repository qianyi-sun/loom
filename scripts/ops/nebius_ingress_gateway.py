#!/usr/bin/env python3
"""Private, journaled ingress installation primitives; no public cutover on import."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import select
import socket
import ssl
import subprocess
import time
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


class KubectlTLSAPI:
    """Gateway-local adapter, callable only through the protected installation."""

    def __init__(self, kubeconfig: Path, *, binding: TLSBinding, executable: Path):
        if (not kubeconfig.is_absolute() or kubeconfig != kubeconfig.resolve()
                or not executable.is_absolute()):
            raise IngressError("protected Kubernetes tooling paths required")
        try:
            certificates._private_read(kubeconfig, limit=512 * 1024)
            cache = kubeconfig.parent / ".loom-ingress-kubectl-cache"
            certificates._private_directory(cache)
        except Exception:
            raise IngressError("private Kubernetes configuration unavailable") from None
        self.prefix = [str(executable), "--kubeconfig", str(kubeconfig), "--request-timeout=30s", "--cache-dir", str(cache)]
        self.binding = binding

    def _run(self, arguments: list[str], *, payload: bytes | None = None) -> bytes:
        try:
            result = subprocess.run([*self.prefix, *arguments], input=payload, capture_output=True,
                                    timeout=40, check=False, env={"PATH": os.defpath, "LANG": "C.UTF-8"})
            if result.returncode or len(result.stdout) > 4 * 1024 * 1024:
                raise IngressError("protected Kubernetes operation failed")
            return result.stdout
        except (OSError, subprocess.TimeoutExpired):
            raise IngressError("protected Kubernetes outcome unavailable") from None

    def _get(self, arguments: list[str]) -> dict[str, Any] | None:
        raw = self._run([*arguments, "--ignore-not-found", "-o", "json"])
        if not raw.strip():
            return None
        try:
            document = json.loads(raw)
            if not isinstance(document, dict):
                raise ValueError()
            return document
        except ValueError:
            raise IngressError("protected Kubernetes readback is invalid") from None

    def verify_identity(self, binding: TLSBinding) -> None:
        if binding != self.binding:
            raise IngressError("Kubernetes adapter binding differs")
        for namespace, uid in (("kube-system", binding.kube_system_uid), (binding.namespace, binding.namespace_uid)):
            observed = self._get(["get", "namespace", namespace])
            metadata = (observed or {}).get("metadata", {})
            if (not observed or observed.get("kind") != "Namespace" or metadata.get("name") != namespace
                    or metadata.get("uid") != uid or metadata.get("deletionTimestamp") is not None):
                raise IngressError("Kubernetes cluster or namespace identity differs")

    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None:
        if namespace != self.binding.namespace:
            raise IngressError("TLS Secret namespace outside protected binding")
        return self._get(["get", "secret", name, "-n", namespace])

    def create_secret(self, document: dict[str, Any]) -> None:
        if document.get("metadata", {}).get("namespace") != self.binding.namespace:
            raise IngressError("TLS Secret namespace outside protected binding")
        self.verify_identity(self.binding)
        self._run(["create", "-n", self.binding.namespace, "-f", "-", "-o", "name"],
                  payload=json.dumps(document).encode())


class ControllerAPI(TLSAPI, Protocol):
    def get_deployment(self, namespace: str, name: str) -> dict[str, Any] | None: ...
    def list_controller_pods(self, namespace: str) -> list[dict[str, Any]]: ...
    def list_controller_replicasets(self, namespace: str) -> list[dict[str, Any]]: ...
    def get_pod(self, namespace: str, name: str) -> dict[str, Any] | None: ...
    def probe_tls(self, namespace: str, name: str, uid: str, server_name: str) -> str:
        """Authenticate the exact Pod's TLS using system trust; return leaf SHA256."""
        ...


def _forward_port(process: subprocess.Popen[bytes], *, timeout: float = 10) -> int:
    if process.stdout is None:
        raise IngressError("private Pod forwarder unavailable")
    deadline = time.monotonic() + timeout
    output = bytearray()
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise IngressError("private Pod forwarder exited")
        if not select.select([process.stdout], [], [], min(0.1, max(0, deadline - time.monotonic())))[0]:
            continue
        chunk = os.read(process.stdout.fileno(), 4096)
        output.extend(chunk)
        if not chunk or len(output) > 16384:
            raise IngressError("private Pod forwarder report unavailable")
        match = re.search(rb"(?:^|\n)Forwarding from 127\.0\.0\.1:([0-9]{1,5}) -> 8443\r?\n", output)
        if match and 1024 <= int(match[1]) <= 65535:
            return int(match[1])
    raise IngressError("private Pod forwarder readiness timed out")


class KubectlControllerAPI(KubectlTLSAPI):
    def _namespace(self, namespace: str) -> None:
        if namespace != self.binding.namespace:
            raise IngressError("controller namespace outside protected binding")

    def get_deployment(self, namespace: str, name: str) -> dict[str, Any] | None:
        self._namespace(namespace)
        if name != "loom-shared-ingress":
            raise IngressError("controller name outside protected binding")
        return self._get(["get", "deployment", name, "-n", namespace])

    def _list(self, kind: str, namespace: str) -> list[dict[str, Any]]:
        self._namespace(namespace)
        document = self._get(["get", kind, "-n", namespace, "-l", "app=loom-shared-ingress"])
        items = (document or {}).get("items")
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise IngressError("controller membership readback unavailable")
        return items

    def list_controller_pods(self, namespace: str) -> list[dict[str, Any]]:
        return self._list("pods", namespace)

    def list_controller_replicasets(self, namespace: str) -> list[dict[str, Any]]:
        return self._list("replicasets", namespace)

    def get_pod(self, namespace: str, name: str) -> dict[str, Any] | None:
        self._namespace(namespace)
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", name):
            raise IngressError("invalid controller Pod name")
        return self._get(["get", "pod", name, "-n", namespace])

    def probe_tls(self, namespace: str, name: str, uid: str, server_name: str) -> str:
        self._namespace(namespace)
        if server_name != self.binding.management_host:
            raise IngressError("TLS server name outside protected binding")
        pod = self.get_pod(namespace, name)
        if not pod or pod["metadata"].get("uid") != uid:
            raise IngressError("TLS probe Pod identity differs")
        # Let kubectl reserve the local port: never release/rebind a guessed
        # free port. Bind loopback only, and retain the forwarder while probing.
        process = subprocess.Popen(
            [*self.prefix, "port-forward", "--address=127.0.0.1", "-n", namespace, f"pod/{name}", ":8443"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={"PATH": os.defpath, "LANG": "C.UTF-8"},
        )
        try:
            port = _forward_port(process)
            with socket.create_connection(("127.0.0.1", port), timeout=5) as connection:
                with ssl.create_default_context().wrap_socket(connection, server_hostname=server_name) as secured:
                    certificate = secured.getpeercert(binary_form=True)
            current = self.get_pod(namespace, name)
            if (not certificate or process.poll() is not None or not current
                    or current["metadata"].get("uid") != uid):
                raise IngressError("TLS probe identity changed")
            return hashlib.sha256(certificate).hexdigest()
        except Exception:
            raise IngressError("private Pod TLS verification failed") from None
        finally:
            # Inherit the protected gateway operation's supervised process group.
            # Never detach kubectl from its parent-death/timeout cleanup boundary.
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()


def qualify_controller(*, binding: TLSBinding, api: ControllerAPI, deployment_uid: str,
                       image: str, tls_receipt: dict[str, Any]) -> dict[str, Any]:
    """One read-only observation, not a readiness retry or a public cutover.

    A caller may poll this observation within a bounded rollout deadline. Every
    selected Pod must be current and serve the delivered certificate; replicas
    from an old generation, including terminating Pods, prevent qualification.
    """
    try:
        if (str(UUID(deployment_uid)) != deployment_uid
                or not re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[A-Za-z0-9_./-]+@sha256:[0-9a-f]{64}", image)
                or tls_receipt["binding"] != asdict(binding) or tls_receipt["status"] != "tls_delivered"):
            raise IngressError("controller qualification binding differs")
        api.verify_identity(binding)
        deployment = api.get_deployment(binding.namespace, "loom-shared-ingress")
        if deployment is None:
            raise IngressError("controller is absent")
        metadata, spec, status = deployment["metadata"], deployment["spec"], deployment.get("status", {})
        generation = metadata["generation"]
        if (metadata["uid"] != deployment_uid or metadata["namespace"] != binding.namespace
                or metadata.get("deletionTimestamp") is not None
                or metadata.get("labels", {}).get("loom.nebius/ingress-installation-id") != binding.installation_id
                or type(generation) is not int or generation < 1 or spec.get("replicas") != 1
                or status.get("observedGeneration") != generation
                or any(status.get(field) != 1 for field in ("replicas", "updatedReplicas", "readyReplicas", "availableReplicas"))
                or spec.get("selector") != {"matchLabels": {"app": "loom-shared-ingress"}}):
            raise IngressError("controller generation is not fully available")

        def current_spec(value: dict[str, Any]) -> bool:
            containers = value.get("containers", [])
            tls_volumes = [v for v in value.get("volumes", []) if v.get("name") == "tls"]
            return (len(containers) == 1 and containers[0].get("image") == image
                    and len(tls_volumes) == 1
                    and tls_volumes[0].get("secret", {}).get("secretName") == tls_receipt["secret_name"])

        if not current_spec(spec["template"]["spec"]):
            raise IngressError("controller image or TLS generation differs")
        def check_secret() -> None:
            secret = api.get_secret(binding.namespace, tls_receipt["secret_name"])
            meta = (secret or {}).get("metadata", {})
            if (not secret or meta.get("uid") != tls_receipt["secret_uid"]
                    or meta.get("name") != tls_receipt["secret_name"] or meta.get("namespace") != binding.namespace
                    or secret.get("immutable") is not True or secret.get("type") != "kubernetes.io/tls"
                    or meta.get("deletionTimestamp") is not None or meta.get("ownerReferences")
                    or meta.get("labels", {}).get("loom.openai.com/ingress-installation") != binding.installation_id
                    or meta.get("annotations", {}).get("loom.openai.com/certificate-generation") != tls_receipt["certificate_generation"]):
                raise IngressError("delivered TLS Secret identity differs")

        check_secret()
        owned_sets = {
            row["metadata"]["uid"] for row in api.list_controller_replicasets(binding.namespace)
            if row["metadata"].get("deletionTimestamp") is None and any(
                owner.get("controller") is True and owner.get("kind") == "Deployment" and owner.get("uid") == deployment_uid
                for owner in row["metadata"].get("ownerReferences", [])
            )
        }
        pods = api.list_controller_pods(binding.namespace)
        if len(pods) != 1:
            raise IngressError("controller has absent or mixed-generation Pods")
        for pod in pods:
            meta, state = pod["metadata"], pod.get("status", {})
            if (meta["namespace"] != binding.namespace or meta.get("deletionTimestamp") is not None
                    or not meta.get("resourceVersion") or meta.get("labels", {}).get("app") != "loom-shared-ingress"
                    or not current_spec(pod["spec"]) or state.get("phase") != "Running"
                    or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in state.get("conditions", []))
                    or not any(o.get("controller") is True and o.get("kind") == "ReplicaSet" and o.get("uid") in owned_sets
                               for o in meta.get("ownerReferences", []))):
                raise IngressError("controller Pod is not current and ready")
            if api.probe_tls(binding.namespace, meta["name"], meta["uid"], binding.management_host) != tls_receipt["fingerprint_sha256"]:
                raise IngressError("controller Pod serves a different certificate")
            observed = api.get_pod(binding.namespace, meta["name"])
            if (not observed or observed["metadata"].get("uid") != meta["uid"]
                    or observed["metadata"].get("resourceVersion") != meta["resourceVersion"]):
                raise IngressError("controller Pod changed during TLS qualification")
        current = api.get_deployment(binding.namespace, "loom-shared-ingress")
        if not current or current["metadata"]["uid"] != deployment_uid or current["metadata"]["generation"] != generation:
            raise IngressError("controller changed during TLS qualification")
        final_pods = api.list_controller_pods(binding.namespace)
        if sorted((p["metadata"]["uid"], p["metadata"]["resourceVersion"]) for p in final_pods) != sorted(
            (p["metadata"]["uid"], p["metadata"]["resourceVersion"]) for p in pods
        ):
            raise IngressError("controller membership changed during TLS qualification")
        check_secret()
        api.verify_identity(binding)
        return {"status": "controller_qualified", "deployment_uid": deployment_uid, "generation": generation,
                "pod_uids": [pod["metadata"]["uid"] for pod in pods],
                "fingerprint_sha256": tls_receipt["fingerprint_sha256"], "secret_uid": tls_receipt["secret_uid"]}
    except IngressError:
        raise
    except Exception:
        raise IngressError("controller qualification unavailable") from None


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
