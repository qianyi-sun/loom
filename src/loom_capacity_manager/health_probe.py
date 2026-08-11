"""Strict mutual-TLS readiness probe for the global capacity manager."""

from __future__ import annotations

import argparse
import json
import ssl
import sys
from pathlib import Path
from urllib.parse import urlsplit

import httpx

_MAX_HEALTH_BYTES = 1024
_HEALTH_FIELDS = {"status", "executable_new_capacity_ceiling"}


class CapacityHealthProbeError(RuntimeError):
    """The manager did not prove ready at the zero-execution boundary."""


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise CapacityHealthProbeError("capacity health contains duplicate fields")
        value[key] = item
    return value


def parse_capacity_health_response(
    status_code: int,
    payload: bytes,
) -> dict[str, object]:
    """Validate one bounded response without Python's bool/int coercion."""

    if status_code != 200 or not 0 < len(payload) <= _MAX_HEALTH_BYTES:
        raise CapacityHealthProbeError("capacity manager is not ready")
    try:
        document = json.loads(payload, object_pairs_hook=_unique_object)
    except (CapacityHealthProbeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CapacityHealthProbeError("capacity health response is invalid") from exc
    if (
        not isinstance(document, dict)
        or set(document) != _HEALTH_FIELDS
        or document["status"] != "ready"
        or type(document["executable_new_capacity_ceiling"]) is not int
        or document["executable_new_capacity_ceiling"] != 0
    ):
        raise CapacityHealthProbeError(
            "capacity manager did not prove the zero-execution readiness boundary"
        )
    return document


def probe_capacity_manager(
    *,
    url: str,
    ca_file: Path,
    certificate_file: Path,
    private_key_file: Path,
    timeout_seconds: float = 3.0,
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
    ):
        raise CapacityHealthProbeError("capacity health timeout is invalid")
    try:
        context = ssl.create_default_context(cafile=str(ca_file))
        context.load_cert_chain(
            certfile=str(certificate_file),
            keyfile=str(private_key_file),
        )
        with httpx.Client(verify=context, timeout=float(timeout_seconds)) as client:
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
    return parse_capacity_health_response(status_code, bytes(payload))


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe the zero-capacity manager boundary")
    parser.add_argument("--url", required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--certificate-file", type=Path, required=True)
    parser.add_argument("--private-key-file", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    arguments = parser.parse_args()
    try:
        document = probe_capacity_manager(
            url=arguments.url,
            ca_file=arguments.ca_file,
            certificate_file=arguments.certificate_file,
            private_key_file=arguments.private_key_file,
            timeout_seconds=arguments.timeout_seconds,
        )
    except CapacityHealthProbeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = [
    "CapacityHealthProbeError",
    "main",
    "parse_capacity_health_response",
    "probe_capacity_manager",
]
