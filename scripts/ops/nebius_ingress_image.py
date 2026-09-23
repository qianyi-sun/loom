"""Pinned shared-ingress image publication into the protected region registry."""
from __future__ import annotations

import base64
import hashlib
import json
import re
import subprocess
from pathlib import Path

from scripts.ops import nebius_certificates as private_state

DIGEST = "sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18"
ROOT = Path(__file__).resolve().parents[2]


class ImageError(RuntimeError):
    """Fixed publication failure without registry payloads or credentials."""


def _run(arguments: list[str], *, timeout: int = 60) -> bytes:
    try:
        result = subprocess.run(["skopeo", *arguments], capture_output=True, timeout=timeout, check=False)
        if result.returncode or len(result.stdout) > 1024 * 1024:
            raise ImageError("ingress registry operation unavailable")
        return result.stdout
    except (OSError, subprocess.TimeoutExpired):
        raise ImageError("ingress registry outcome unavailable; reconcile before retry") from None


def _inspect(image: str, auth_file: Path) -> None:
    manifest_bytes = _run(["inspect", "--authfile", str(auth_file), "--raw", "docker://" + image])
    if "sha256:" + hashlib.sha256(manifest_bytes).hexdigest() != DIGEST:
        raise ImageError("ingress manifest differs from qualified pin")
    manifest = json.loads(manifest_bytes)
    if (manifest["schemaVersion"] != 2
            or manifest["mediaType"] != "application/vnd.oci.image.manifest.v1+json"):
        raise ImageError("ingress requires the qualified single-platform OCI manifest")
    config_bytes = _run(["inspect", "--authfile", str(auth_file), "--config", "--raw", "docker://" + image])
    if ("sha256:" + hashlib.sha256(config_bytes).hexdigest() != manifest["config"]["digest"]
            or len(config_bytes) != manifest["config"]["size"]):
        raise ImageError("ingress configuration differs from manifest")
    config = json.loads(config_bytes)
    if (config["architecture"] != "amd64" or config["os"] != "linux"
            or config["config"]["Labels"]["org.opencontainers.image.version"] != "v3.7.13"):
        raise ImageError("ingress platform or version differs from qualification")


def mirror_ingress_image(*, registry_prefix: str, region: str, auth_file: Path,
                         state_dir: Path) -> dict[str, str]:
    """One digest-preserving copy; subsequent calls reconcile by readback only.

    Destination uses a digest, never a mutable tag. No credential refresh, paid
    registry provisioning or Kubernetes mutation occurs here. The protected caller
    supplies freshly minted registry-only auth and retains the private journal.
    """
    try:
        if (not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", region)
                or not re.fullmatch(r"cr\." + re.escape(region) + r"\.nebius\.cloud/[a-z0-9]+", registry_prefix)):
            raise ImageError("ingress registry differs from protected region")
        for path in (auth_file, state_dir):
            if not path.is_absolute() or path != path.resolve() or path.is_relative_to(ROOT):
                raise ImageError("ingress registry state must be private and outside the repository")
        auth = json.loads(private_state._private_read(auth_file))
        host = registry_prefix.split("/", 1)[0]
        if (set(auth) != {"auths"} or set(auth["auths"]) != {host}
                or set(auth["auths"][host]) != {"auth"}
                or not base64.b64decode(auth["auths"][host]["auth"], validate=True).startswith(b"iam:")):
            raise ImageError("ingress requires registry-only authentication")
        source = "docker.io/library/traefik@" + DIGEST
        destination = registry_prefix + "/loom-shared-ingress@" + DIGEST
        identity = {"schema": "loom.nebius-ingress-image.v1", "source": source, "destination": destination,
                    "region": region}
        with private_state._locked_state(state_dir):
            path = state_dir / "image-mirror.json"
            if path.exists() or path.is_symlink():
                record = json.loads(private_state._private_read(path))
                if (not isinstance(record, dict) or set(record) != {*identity, "status"}
                        or any(record[key] != value for key, value in identity.items())
                        or record["status"] not in {"copy_intent", "mirrored"}):
                    raise ImageError("ingress image journal differs from protected input")
            else:
                _inspect(source, auth_file)
                record = {**identity, "status": "copy_intent"}
                private_state._atomic_json(path, record)
                try:
                    _run(["copy", "--authfile", str(auth_file), "--preserve-digests",
                          "docker://" + source, "docker://" + destination], timeout=900)
                except ImageError:
                    pass  # Only exact destination readback resolves an uncertain copy.
            _inspect(destination, auth_file)
            if record["status"] != "mirrored":
                private_state._atomic_json(path, {**identity, "status": "mirrored"})
            return {"status": "mirrored", "image": destination, "platform": "linux/amd64", "version": "v3.7.13"}
    except ImageError:
        raise
    except Exception:
        raise ImageError("ingress image publication unavailable; preserve private journal") from None
