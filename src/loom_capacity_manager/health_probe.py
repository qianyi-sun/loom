"""Strict mutual-TLS readiness probe for the global capacity manager."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import ssl
import stat
import sys
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from cryptography import x509

_MAX_HEALTH_BYTES = 1024
_MAX_SERVER_CERTIFICATE_BYTES = 64 * 1024
_HEALTH_FIELDS = {"status", "executable_new_capacity_ceiling"}
_MANAGER_SERVICE_DNS = "loom-capacity-manager.loom-dev.svc.cluster.local"
_LOOPBACK_IP = ipaddress.ip_address("127.0.0.1")
_DEFAULT_CREDENTIALS = "/var/run/loom-capacity-manager/runtime/credentials"


class CapacityHealthProbeError(RuntimeError):
    """The manager did not prove ready at the zero-execution boundary."""


def capacity_health_probe_argv(
    credentials_directory: str = _DEFAULT_CREDENTIALS,
    *,
    observe: bool = False,
) -> tuple[str, ...]:
    """Return the fixed in-container zero-ceiling health-probe command."""

    command = (
        "python",
        "-m",
        "loom_capacity_manager.health_probe",
        "--url",
        "https://127.0.0.1:8443/healthz",
        "--ca-file",
        f"{credentials_directory}/server-ca.pem",
        "--certificate-file",
        f"{credentials_directory}/health-certificate.pem",
        "--private-key-file",
        f"{credentials_directory}/health-private-key.pem",
        "--server-certificate-file",
        f"{credentials_directory}/server-certificate.pem",
    )
    return (*command, "--observe") if observe else command


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise CapacityHealthProbeError("capacity health contains duplicate fields")
        value[key] = item
    return value


def parse_observed_capacity_health_response(
    status_code: int,
    payload: bytes,
) -> dict[str, object]:
    """Parse one bounded exact health observation without requiring zero."""

    if status_code not in {200, 503} or not 0 < len(payload) <= _MAX_HEALTH_BYTES:
        raise CapacityHealthProbeError("capacity manager is not ready")
    try:
        document = json.loads(payload, object_pairs_hook=_unique_object)
    except (CapacityHealthProbeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityHealthProbeError("capacity health response is invalid") from exc
    if (
        not isinstance(document, dict)
        or set(document) != _HEALTH_FIELDS
        or document["status"] != ("ready" if status_code == 200 else "not-ready")
        or type(document["executable_new_capacity_ceiling"]) is not int
        or document["executable_new_capacity_ceiling"] < 0
    ):
        raise CapacityHealthProbeError("capacity manager health observation is invalid")
    return document


def parse_capacity_health_response(
    status_code: int,
    payload: bytes,
) -> dict[str, object]:
    """Validate one bounded response without Python's bool/int coercion."""

    document = parse_observed_capacity_health_response(status_code, payload)
    if document["status"] != "ready" or document["executable_new_capacity_ceiling"] != 0:
        raise CapacityHealthProbeError(
            "capacity manager did not prove the zero-execution readiness boundary"
        )
    return document


def _validate_server_certificate_identities(path: Path) -> None:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or not 0 < opened.st_size <= _MAX_SERVER_CERTIFICATE_BYTES
            ):
                raise ValueError("capacity manager server certificate is not bounded")
            payload = bytearray()
            while len(payload) <= _MAX_SERVER_CERTIFICATE_BYTES:
                chunk = os.read(
                    descriptor,
                    min(
                        16 * 1024,
                        _MAX_SERVER_CERTIFICATE_BYTES + 1 - len(payload),
                    ),
                )
                if not chunk:
                    break
                payload.extend(chunk)
            closed = os.fstat(descriptor)
            if (closed.st_dev, closed.st_ino, closed.st_size) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
            ) or len(payload) != opened.st_size:
                raise ValueError("capacity manager server certificate changed while reading")
        finally:
            os.close(descriptor)
        certificate = x509.load_pem_x509_certificate(bytes(payload))
        identities = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
    except (OSError, ValueError, x509.ExtensionNotFound) as exc:
        raise CapacityHealthProbeError(
            "capacity manager server certificate identities are invalid"
        ) from exc
    dns_names = set(identities.get_values_for_type(x509.DNSName))
    ip_addresses = set(identities.get_values_for_type(x509.IPAddress))
    if _MANAGER_SERVICE_DNS not in dns_names or _LOOPBACK_IP not in ip_addresses:
        raise CapacityHealthProbeError("capacity manager server certificate identities are invalid")


def probe_capacity_manager(
    *,
    url: str,
    ca_file: Path,
    certificate_file: Path,
    private_key_file: Path,
    server_certificate_file: Path,
    timeout_seconds: float = 3.0,
    observe: bool = False,
) -> dict[str, object]:
    """Perform one bounded, server-verified, client-authenticated health request."""

    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/healthz"
        or parsed.query
        or parsed.fragment
    ):
        raise CapacityHealthProbeError("capacity health URL is invalid")
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds <= 30
        or type(observe) is not bool
    ):
        raise CapacityHealthProbeError("capacity health timeout is invalid")
    _validate_server_certificate_identities(server_certificate_file)
    try:
        context = ssl.create_default_context(cafile=str(ca_file))
        context.load_cert_chain(
            certfile=str(certificate_file),
            keyfile=str(private_key_file),
        )
        with httpx.Client(
            verify=context,
            timeout=float(timeout_seconds),
            trust_env=False,
        ) as client:
            with client.stream("GET", url) as response:
                payload = bytearray()
                for chunk in response.iter_bytes(chunk_size=_MAX_HEALTH_BYTES + 1):
                    payload.extend(chunk)
                    if len(payload) > _MAX_HEALTH_BYTES:
                        raise CapacityHealthProbeError(
                            "capacity health response exceeds its size bound"
                        )
                status_code = response.status_code
    except CapacityHealthProbeError:
        raise
    except (OSError, ValueError, ssl.SSLError, httpx.HTTPError) as exc:
        raise CapacityHealthProbeError("capacity health transport failed") from exc
    parser = parse_observed_capacity_health_response if observe else parse_capacity_health_response
    return parser(status_code, bytes(payload))


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe the zero-capacity manager boundary")
    parser.add_argument("--url", required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--certificate-file", type=Path, required=True)
    parser.add_argument("--private-key-file", type=Path, required=True)
    parser.add_argument("--server-certificate-file", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    parser.add_argument(
        "--observe",
        action="store_true",
        help="Emit an exact read-only health observation without requiring ceiling zero.",
    )
    arguments = parser.parse_args()
    try:
        document = probe_capacity_manager(
            url=arguments.url,
            ca_file=arguments.ca_file,
            certificate_file=arguments.certificate_file,
            private_key_file=arguments.private_key_file,
            server_certificate_file=arguments.server_certificate_file,
            timeout_seconds=arguments.timeout_seconds,
            observe=arguments.observe,
        )
    except CapacityHealthProbeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = [
    "CapacityHealthProbeError",
    "capacity_health_probe_argv",
    "main",
    "parse_capacity_health_response",
    "parse_observed_capacity_health_response",
    "probe_capacity_manager",
]
