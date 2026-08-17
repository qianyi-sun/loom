#!/usr/bin/env python3
"""Delete one fenced task-image registry claim and complete its lease."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import socket
import ssl
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID, uuid4

_MAX_RESPONSE_BYTES = 1 << 20
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_GC_ID_RE = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


class RegistryGcError(RuntimeError):
    """The fenced registry operation could not be completed safely."""


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: bytes


class Transport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        ca_file: Path | None,
    ) -> HttpResponse: ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


@dataclass(frozen=True, slots=True)
class UrllibTransport:
    timeout_seconds: float = 20.0

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes | None,
        ca_file: Path | None,
    ) -> HttpResponse:
        context = ssl.create_default_context(cafile=str(ca_file)) if ca_file else None
        opener = urllib.request.build_opener(
            urllib.request.HTTPHandler(),
            urllib.request.HTTPSHandler(context=context),
            _NoRedirect(),
        )
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with opener.open(request, timeout=self.timeout_seconds) as response:
                payload = response.read(_MAX_RESPONSE_BYTES + 1)
                status = response.status
        except urllib.error.HTTPError as exc:
            payload = exc.read(_MAX_RESPONSE_BYTES + 1)
            status = exc.code
        except (OSError, urllib.error.URLError) as exc:
            raise RegistryGcError("registry GC transport failed") from exc
        if len(payload) > _MAX_RESPONSE_BYTES:
            raise RegistryGcError("registry GC response is too large")
        return HttpResponse(status=status, body=payload)


@dataclass(frozen=True, slots=True)
class GcConfig:
    cp_url: str
    cp_token: str = field(repr=False)
    registry_url: str
    registry_namespace: str
    registry_authorization: str = field(repr=False)
    ca_file: Path

    def __post_init__(self) -> None:
        cp = urllib.parse.urlsplit(self.cp_url)
        registry = urllib.parse.urlsplit(self.registry_url)
        if (
            cp.scheme not in {"http", "https"}
            or not cp.netloc
            or cp.path not in {"", "/"}
            or cp.query
            or cp.fragment
            or registry.scheme != "https"
            or not registry.netloc
            or registry.path not in {"", "/"}
            or registry.query
            or registry.fragment
            or not self.cp_token
            or any(character.isspace() for character in self.cp_token)
            or not self.registry_authorization.startswith("Basic ")
            or not re.fullmatch(r"[a-z0-9][a-z0-9._/-]{0,254}", self.registry_namespace)
            or self.registry_namespace.endswith("/")
            or not self.ca_file.is_absolute()
        ):
            raise ValueError("registry GC configuration is invalid")


def _json_body(value: dict[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _parse_json_response(response: HttpResponse, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(response.body)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RegistryGcError(f"{label} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RegistryGcError(f"{label} returned an invalid object")
    return payload


def _manifest_delete_url(config: GcConfig, image: object) -> str:
    if not isinstance(image, str) or len(image) > 2048 or image.count("@") != 1:
        raise RegistryGcError("registry GC claim contains an invalid image reference")
    name, digest = image.split("@", 1)
    registry = urllib.parse.urlsplit(config.registry_url)
    expected_prefix = f"{registry.netloc}/"
    if not name.startswith(expected_prefix):
        raise RegistryGcError("registry GC claim is outside the configured registry")
    repository = name[len(expected_prefix) :]
    if not (
        repository == config.registry_namespace
        or repository.startswith(config.registry_namespace + "/")
    ):
        raise RegistryGcError("registry GC claim is outside the configured registry namespace")
    if _DIGEST_RE.fullmatch(digest) is None:
        raise RegistryGcError("registry GC claim contains an invalid manifest digest")
    encoded_repository = urllib.parse.quote(repository, safe="/")
    return f"{config.registry_url.rstrip('/')}/v2/{encoded_repository}/manifests/{digest}"


def run_gc_once(
    config: GcConfig,
    *,
    transport: Transport,
    gc_id: str,
) -> dict[str, int | bool]:
    if _GC_ID_RE.fullmatch(gc_id) is None:
        raise RegistryGcError("registry GC id is invalid")
    cp_headers = {
        "Authorization": f"Bearer {config.cp_token}",
        "Content-Type": "application/json",
    }
    claim_url = (
        f"{config.cp_url.rstrip('/')}/api/v1/internal/"
        "task-image-materializations/registry-gc/claim"
    )
    claim = transport.request(
        "POST",
        claim_url,
        headers=cp_headers,
        body=_json_body({"gc_id": gc_id}),
        ca_file=None,
    )
    if claim.status == 204:
        return {"claimed": False, "deleted_manifests": 0}
    if claim.status != 200:
        raise RegistryGcError(f"registry GC claim failed with HTTP {claim.status}")
    payload = _parse_json_response(claim, label="registry GC claim")
    try:
        materialization_id = str(UUID(str(payload["id"])))
        lease_epoch = payload["lease_epoch"]
        images = payload["registry_images"]
    except (KeyError, TypeError, ValueError) as exc:
        raise RegistryGcError("registry GC claim shape is invalid") from exc
    if type(lease_epoch) is not int or lease_epoch <= 0 or not isinstance(images, dict) or not images:
        raise RegistryGcError("registry GC claim shape is invalid")
    delete_urls = sorted({_manifest_delete_url(config, image) for image in images.values()})
    registry_headers = {
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
        "Authorization": config.registry_authorization,
    }
    for delete_url in delete_urls:
        deleted = transport.request(
            "DELETE",
            delete_url,
            headers=registry_headers,
            body=None,
            ca_file=config.ca_file,
        )
        if deleted.status not in {202, 404}:
            raise RegistryGcError(
                f"registry manifest deletion failed with HTTP {deleted.status}"
            )
    complete_url = (
        f"{config.cp_url.rstrip('/')}/api/v1/internal/task-image-materializations/"
        f"registry-gc/{materialization_id}/complete"
    )
    completed = transport.request(
        "POST",
        complete_url,
        headers=cp_headers,
        body=_json_body({"gc_id": gc_id, "lease_epoch": lease_epoch}),
        ca_file=None,
    )
    if completed.status != 200:
        raise RegistryGcError(f"registry GC completion failed with HTTP {completed.status}")
    _parse_json_response(completed, label="registry GC completion")
    return {"claimed": True, "deleted_manifests": len(delete_urls)}


def _read_private_file(path: Path, *, label: str, max_bytes: int) -> bytes:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_size <= 0
            or metadata.st_size > max_bytes
        ):
            raise RegistryGcError(f"registry GC {label} metadata is unsafe")
        payload = path.read_bytes()
    except RegistryGcError:
        raise
    except OSError as exc:
        raise RegistryGcError(f"registry GC {label} is unavailable") from exc
    if len(payload) > max_bytes:
        raise RegistryGcError(f"registry GC {label} is too large")
    return payload


def _load_registry_authorization(config_dir: Path, *, registry_url: str) -> str:
    directory = config_dir.lstat()
    if (
        not stat.S_ISDIR(directory.st_mode)
        or stat.S_ISLNK(directory.st_mode)
        or directory.st_uid != os.geteuid()
        or stat.S_IMODE(directory.st_mode) & 0o077
    ):
        raise RegistryGcError("registry GC Docker config directory metadata is unsafe")
    payload = _read_private_file(
        config_dir / "config.json",
        label="Docker config",
        max_bytes=64 * 1024,
    )
    try:
        parsed = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RegistryGcError("registry GC Docker config is invalid") from exc
    registry_host = urllib.parse.urlsplit(registry_url).netloc
    auths = parsed.get("auths") if isinstance(parsed, dict) else None
    entry = auths.get(registry_host) if isinstance(auths, dict) else None
    auth = entry.get("auth") if isinstance(entry, dict) else None
    if not isinstance(auth, str) or not auth:
        raise RegistryGcError("registry GC Docker config lacks registry credentials")
    try:
        decoded = base64.b64decode(auth, validate=True)
    except (ValueError, TypeError) as exc:
        raise RegistryGcError("registry GC Docker config credentials are invalid") from exc
    if b":" not in decoded or not all(decoded.split(b":", 1)):
        raise RegistryGcError("registry GC Docker config credentials are invalid")
    return f"Basic {auth}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cp-url", required=True)
    parser.add_argument("--cp-token-file", type=Path, required=True)
    parser.add_argument("--registry-url", required=True)
    parser.add_argument("--registry-namespace", required=True)
    parser.add_argument("--registry-docker-config-dir", type=Path, required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    return parser


def main() -> None:
    args = _parser().parse_args()
    try:
        token = _read_private_file(
            args.cp_token_file,
            label="control-plane token",
            max_bytes=64 * 1024,
        ).strip().decode("ascii")
        authorization = _load_registry_authorization(
            args.registry_docker_config_dir,
            registry_url=args.registry_url,
        )
        config = GcConfig(
            cp_url=args.cp_url,
            cp_token=token,
            registry_url=args.registry_url,
            registry_namespace=args.registry_namespace,
            registry_authorization=authorization,
            ca_file=args.ca_file,
        )
        gc_id = f"{socket.gethostname()}:{os.getpid()}:{uuid4().hex}"
        result = run_gc_once(
            config,
            transport=UrllibTransport(timeout_seconds=args.timeout_seconds),
            gc_id=gc_id,
        )
    except (OSError, UnicodeError, ValueError, RegistryGcError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
