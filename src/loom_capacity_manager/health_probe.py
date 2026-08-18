"""Strict mutual-TLS readiness probe for the global capacity manager."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import ssl
import stat
import sys
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from cryptography import x509

from loom_capacity_manager.auth import MAX_BEARER_TOKEN_BYTES
from loom_capacity_manager.config import read_owner_only_bytes

_MAX_HEALTH_BYTES = 1024
_MAX_SERVER_CERTIFICATE_BYTES = 64 * 1024
_HEALTH_FIELDS = {"status", "executable_new_capacity_ceiling"}
_IDENTITY_FIELDS = {
    "authority_incarnation",
    "observer_principal_id",
    "configuration_epoch",
    "execution_state",
    "execution_epoch",
    "executable_new_capacity_ceiling",
}
_PRINCIPAL_ID = re.compile(r"[a-z0-9-]{1,128}")
_MANAGER_SERVICE_DNS = "loom-capacity-manager.loom-dev.svc.cluster.local"
_LOOPBACK_IP = ipaddress.ip_address("127.0.0.1")
_DEFAULT_CREDENTIALS = "/var/run/loom-capacity-manager/runtime/credentials"


class CapacityHealthProbeError(RuntimeError):
    """The manager did not prove ready at the zero-execution boundary."""


def capacity_health_probe_argv(
    credentials_directory: str = _DEFAULT_CREDENTIALS,
    *,
    observe: bool = False,
    allow_positive_ceiling: bool = False,
    observe_identity: bool = False,
) -> tuple[str, ...]:
    """Return one fixed in-container health-probe command."""

    if (
        type(observe) is not bool
        or type(allow_positive_ceiling) is not bool
        or type(observe_identity) is not bool
    ):
        raise ValueError("capacity health probe mode is invalid")
    if sum((observe, allow_positive_ceiling, observe_identity)) > 1:
        raise ValueError("capacity health probe modes are mutually exclusive")

    if observe_identity:
        return (
            "python",
            "-m",
            "loom_capacity_manager.health_probe",
            "--url",
            "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status",
            "--ca-file",
            f"{credentials_directory}/capacity-lifecycle-ca.pem",
            "--certificate-file",
            f"{credentials_directory}/capacity-lifecycle-certificate.pem",
            "--private-key-file",
            f"{credentials_directory}/capacity-lifecycle-private-key.pem",
            "--bearer-token-file",
            f"{credentials_directory}/capacity-lifecycle-token",
            "--observe-identity",
        )
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
    if observe:
        return (*command, "--observe")
    if allow_positive_ceiling:
        return (*command, "--allow-positive-ceiling")
    return command


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise CapacityHealthProbeError("capacity health contains duplicate fields")
        value[key] = item
    return value


def _read_owner_only_bearer_token(path: Path) -> str:
    payload = read_owner_only_bytes(path, max_bytes=MAX_BEARER_TOKEN_BYTES)
    try:
        value = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("capacity bearer credential is not UTF-8") from exc
    if value.endswith("\n"):
        value = value[:-1]
    if (
        not value
        or value != value.strip()
        or any(character in value for character in ("\r", "\n", "\x00"))
    ):
        raise ValueError("capacity bearer credential must contain one exact nonempty line")
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
    *,
    allow_positive_ceiling: bool = False,
) -> dict[str, object]:
    """Validate one bounded response without Python's bool/int coercion."""

    if type(allow_positive_ceiling) is not bool:
        raise CapacityHealthProbeError("capacity health probe mode is invalid")
    document = parse_observed_capacity_health_response(status_code, payload)
    if document["status"] != "ready":
        raise CapacityHealthProbeError("capacity manager is not ready")
    if not allow_positive_ceiling and document["executable_new_capacity_ceiling"] != 0:
        raise CapacityHealthProbeError(
            "capacity manager did not prove the zero-execution readiness boundary"
        )
    return document


def parse_observed_capacity_manager_identity_response(
    status_code: int,
    payload: bytes,
) -> dict[str, object]:
    """Parse a bounded status response into a secret-free manager binding."""

    if status_code != 200 or not 0 < len(payload) <= 64 * 1024:
        raise CapacityHealthProbeError("capacity manager identity is unavailable")
    try:
        document = json.loads(payload, object_pairs_hook=_unique_object)
        if not isinstance(document, dict) or not _IDENTITY_FIELDS.issubset(document):
            raise ValueError
        authority_value = document["authority_incarnation"]
        if not isinstance(authority_value, str):
            raise ValueError
        authority = UUID(authority_value)
        if str(authority) != authority_value:
            raise ValueError
        principal = document["observer_principal_id"]
        if not isinstance(principal, str) or _PRINCIPAL_ID.fullmatch(principal) is None:
            raise ValueError
        configuration_epoch = document["configuration_epoch"]
        execution_state = document["execution_state"]
        execution_epoch = document["execution_epoch"]
        ceiling = document["executable_new_capacity_ceiling"]
        if (
            authority.int == 0
            or type(configuration_epoch) is not int
            or configuration_epoch <= 0
            or execution_state not in {"shadow", "prepared", "active", "drain-only"}
            or type(execution_epoch) is not int
            or execution_epoch < 0
            or type(ceiling) is not int
            or ceiling < 0
        ):
            raise ValueError
        if execution_state == "shadow":
            coherent = execution_epoch == 0 and ceiling == 0
        elif execution_state in {"prepared", "drain-only"}:
            coherent = execution_epoch > 0 and ceiling == 0
        else:
            coherent = execution_epoch > 0 and ceiling > 0
        if not coherent:
            raise ValueError
    except (
        CapacityHealthProbeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise CapacityHealthProbeError("capacity manager identity observation is invalid") from exc
    return {
        "authority_incarnation": str(authority),
        "configuration_epoch": configuration_epoch,
        "executable_new_capacity_ceiling": ceiling,
        "execution_epoch": execution_epoch,
        "execution_state": execution_state,
        "observer_principal_id": principal,
    }


def _validate_server_certificate_identities(
    path: Path,
    *,
    required_ip_address: ipaddress.IPv4Address | ipaddress.IPv6Address | None = None,
) -> None:
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
    if (
        _MANAGER_SERVICE_DNS not in dns_names
        or _LOOPBACK_IP not in ip_addresses
        or (required_ip_address is not None and required_ip_address not in ip_addresses)
    ):
        raise CapacityHealthProbeError("capacity manager server certificate identities are invalid")


def probe_capacity_manager(
    *,
    url: str,
    ca_file: Path,
    certificate_file: Path,
    private_key_file: Path,
    server_certificate_file: Path | None = None,
    required_server_ip_san: str | None = None,
    bearer_token_file: Path | None = None,
    timeout_seconds: float = 3.0,
    observe: bool = False,
    allow_positive_ceiling: bool = False,
    observe_identity: bool = False,
) -> dict[str, object]:
    """Perform one bounded, server-verified, client-authenticated health request."""

    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != ("/v1/status" if observe_identity else "/healthz")
        or (
            observe_identity
            and url
            != "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443/v1/status"
        )
        or parsed.query
        or parsed.fragment
    ):
        raise CapacityHealthProbeError("capacity health URL is invalid")
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds <= 30
        or type(observe) is not bool
        or type(allow_positive_ceiling) is not bool
        or type(observe_identity) is not bool
        or sum((observe, allow_positive_ceiling, observe_identity)) > 1
        or (observe_identity and bearer_token_file is None)
        or (not observe_identity and bearer_token_file is not None)
        or (not observe_identity and server_certificate_file is None)
        or (required_server_ip_san is not None and server_certificate_file is None)
    ):
        raise CapacityHealthProbeError("capacity health timeout is invalid")
    if server_certificate_file is not None:
        if required_server_ip_san is None:
            _validate_server_certificate_identities(server_certificate_file)
        else:
            try:
                required_ip_address = ipaddress.ip_address(required_server_ip_san)
            except ValueError as exc:
                raise CapacityHealthProbeError(
                    "capacity manager server certificate identities are invalid"
                ) from exc
            if required_server_ip_san != required_ip_address.compressed:
                raise CapacityHealthProbeError(
                    "capacity manager server certificate identities are invalid"
                )
            _validate_server_certificate_identities(
                server_certificate_file,
                required_ip_address=required_ip_address,
            )
    try:
        bearer_token = (
            _read_owner_only_bearer_token(bearer_token_file)
            if bearer_token_file is not None
            else None
        )
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
            headers = (
                {
                    "Authorization": f"Bearer {bearer_token}",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                }
                if bearer_token is not None
                else None
            )
            response_limit = 64 * 1024 if observe_identity else _MAX_HEALTH_BYTES
            with client.stream("GET", url, headers=headers) as response:
                if observe_identity and response.headers.get(
                    "content-encoding", ""
                ).strip().lower() not in {"", "identity"}:
                    raise CapacityHealthProbeError(
                        "capacity health response encoding is invalid"
                    )
                payload = bytearray()
                for chunk in response.iter_bytes(chunk_size=response_limit + 1):
                    payload.extend(chunk)
                    if len(payload) > response_limit:
                        raise CapacityHealthProbeError(
                            "capacity health response exceeds its size bound"
                        )
                status_code = response.status_code
    except CapacityHealthProbeError:
        raise
    except (OSError, ValueError, ssl.SSLError, httpx.HTTPError) as exc:
        raise CapacityHealthProbeError("capacity health transport failed") from exc
    if observe_identity:
        return parse_observed_capacity_manager_identity_response(
            status_code,
            bytes(payload),
        )
    if observe:
        return parse_observed_capacity_health_response(status_code, bytes(payload))
    return parse_capacity_health_response(
        status_code,
        bytes(payload),
        allow_positive_ceiling=allow_positive_ceiling,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe the zero-capacity manager boundary")
    parser.add_argument("--url", required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--certificate-file", type=Path, required=True)
    parser.add_argument("--private-key-file", type=Path, required=True)
    parser.add_argument("--server-certificate-file", type=Path)
    parser.add_argument("--required-server-ip-san")
    parser.add_argument("--bearer-token-file", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument(
        "--observe",
        action="store_true",
        help="Emit an exact read-only health observation without requiring ceiling zero.",
    )
    modes.add_argument(
        "--allow-positive-ceiling",
        action="store_true",
        help="Require ready while accepting an exact nonnegative executable ceiling.",
    )
    modes.add_argument(
        "--observe-identity",
        action="store_true",
        help="Emit the authenticated manager identity and execution boundary.",
    )
    arguments = parser.parse_args()
    try:
        document = probe_capacity_manager(
            url=arguments.url,
            ca_file=arguments.ca_file,
            certificate_file=arguments.certificate_file,
            private_key_file=arguments.private_key_file,
            server_certificate_file=arguments.server_certificate_file,
            required_server_ip_san=arguments.required_server_ip_san,
            bearer_token_file=arguments.bearer_token_file,
            timeout_seconds=arguments.timeout_seconds,
            observe=arguments.observe,
            allow_positive_ceiling=arguments.allow_positive_ceiling,
            observe_identity=arguments.observe_identity,
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
    "parse_observed_capacity_manager_identity_response",
    "probe_capacity_manager",
]
