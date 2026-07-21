"""Strict reader and live verifier for the root-issued runner install attestation."""

from __future__ import annotations

import hashlib
import json
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from loom_cli.rollout.credential_authority import TrustedFileRead, read_trusted_file

INSTALL_ATTESTATION_PATH = Path("/etc/loom/staging-rollout.install-attestation.json")
INSTALL_ASSETS = MappingProxyType(
    {
        "broker": (Path("/usr/local/libexec/loom-staging-rollout-broker"), 0o755, False),
        "client": (Path("/usr/local/bin/loom-staging-rollout"), 0o755, False),
        "config": (Path("/etc/loom/staging-rollout.toml"), 0o640, True),
        "gb10-known-hosts": (
            Path("/etc/loom/staging-rollout-gb10-known-hosts"),
            0o644,
            False,
        ),
        "gb10-trust-tool": (
            Path("/usr/local/libexec/loom-staging-rollout-gb10-trust"),
            0o755,
            False,
        ),
        "final-gate-helper": (
            Path("/usr/local/libexec/loom-staging-rollout-final-gate"),
            0o755,
            False,
        ),
        "rehearsal-helper": (
            Path("/usr/local/libexec/loom-staging-rollout-rehearsal"),
            0o755,
            False,
        ),
        "rehearsal-authority": (
            Path(
                "/opt/loom-staging-runner/source/"
                "deploy/k8s/staging-rollout-rehearsal-authority.yaml"
            ),
            0o644,
            False,
        ),
        "readonly-authority": (
            Path("/opt/loom-staging-runner/source/deploy/k8s/staging-rollout-readonly.yaml"),
            0o644,
            False,
        ),
        "shared-work2-mount-unit": (
            Path("/etc/systemd/system/shared_work2.mount"),
            0o644,
            False,
        ),
        "sysctl": (Path("/etc/sysctl.d/90-loom-staging-rollout.conf"), 0o644, False),
        "tmpfiles": (Path("/etc/tmpfiles.d/loom-staging-rollout.conf"), 0o644, False),
    }
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("runner install attestation contains duplicate keys")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class RunnerInstallAttestation:
    source_mode: str
    source_sha: str
    source_tree_sha: str
    source_base_sha: str
    install_record_sha256: str
    asset_sha256: Mapping[str, str]

    def __post_init__(self) -> None:
        if self.source_mode not in {"merged-dev", "sealed-cumulative"}:
            raise ValueError("runner install attestation source mode is invalid")
        if _SHA_RE.fullmatch(self.source_sha) is None:
            raise ValueError("runner install attestation source SHA is invalid")
        if self.source_mode == "sealed-cumulative":
            if (
                _SHA_RE.fullmatch(self.source_tree_sha) is None
                or _SHA_RE.fullmatch(self.source_base_sha) is None
            ):
                raise ValueError("runner install attestation sealed identity is invalid")
        elif self.source_tree_sha != "none" or self.source_base_sha != "none":
            raise ValueError("runner install attestation merged identity is invalid")
        if _SHA256_RE.fullmatch(self.install_record_sha256) is None:
            raise ValueError("runner install record digest is invalid")
        assets = dict(self.asset_sha256)
        if set(assets) != set(INSTALL_ASSETS) or any(
            _SHA256_RE.fullmatch(value) is None for value in assets.values()
        ):
            raise ValueError("runner install asset digests are invalid")
        object.__setattr__(self, "asset_sha256", MappingProxyType(assets))

    @classmethod
    def from_payload(cls, payload: bytes) -> RunnerInstallAttestation:
        if not payload or len(payload) > 64 * 1024:
            raise ValueError("runner install attestation payload is invalid")
        try:
            raw = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("runner install attestation payload is invalid") from exc
        if not isinstance(raw, dict) or set(raw) != {
            "asset_sha256",
            "install_record_sha256",
            "schema_version",
            "source_base_sha",
            "source_mode",
            "source_sha",
            "source_tree_sha",
        }:
            raise ValueError("runner install attestation schema is invalid")
        if raw.get("schema_version") != 1 or type(raw.get("schema_version")) is not int:
            raise ValueError("runner install attestation schema version is invalid")
        assets = raw.get("asset_sha256")
        if not isinstance(assets, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in assets.items()
        ):
            raise ValueError("runner install attestation asset digests are invalid")
        string_fields = {
            key: raw.get(key)
            for key in (
                "install_record_sha256",
                "source_base_sha",
                "source_mode",
                "source_sha",
                "source_tree_sha",
            )
        }
        if any(not isinstance(value, str) for value in string_fields.values()):
            raise ValueError("runner install attestation identity is invalid")
        return cls(
            source_mode=str(string_fields["source_mode"]),
            source_sha=str(string_fields["source_sha"]),
            source_tree_sha=str(string_fields["source_tree_sha"]),
            source_base_sha=str(string_fields["source_base_sha"]),
            install_record_sha256=str(string_fields["install_record_sha256"]),
            asset_sha256={str(key): str(value) for key, value in assets.items()},
        )

    @property
    def payload_digest(self) -> str:
        return hashlib.sha256(self.to_payload()).hexdigest()

    def to_payload(self) -> bytes:
        value = {
            "asset_sha256": dict(self.asset_sha256),
            "install_record_sha256": self.install_record_sha256,
            "schema_version": 1,
            "source_base_sha": self.source_base_sha,
            "source_mode": self.source_mode,
            "source_sha": self.source_sha,
            "source_tree_sha": self.source_tree_sha,
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


@dataclass(frozen=True, slots=True)
class VerifiedRunnerInstall:
    attestation: RunnerInstallAttestation
    metadata_fingerprint: str
    acl_fingerprint: str
    failed_assets: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.failed_assets


def _read_asset(
    path: Path,
    *,
    service_uid: int,
    expected_mode: int,
    expected_root_uid: int,
    private: bool,
) -> TrustedFileRead:
    trusted = read_trusted_file(
        path,
        service_uid=service_uid,
        private=private,
        max_bytes=4 << 20,
        require_nonempty=True,
    )
    metadata = trusted.metadata
    if (
        metadata.st_uid != expected_root_uid
        or stat.S_IMODE(metadata.st_mode) != expected_mode
        or metadata.st_nlink != 1
    ):
        raise ValueError("runner install asset metadata is unsafe")
    return trusted


def verify_runner_install(
    *,
    service_uid: int,
    attestation_path: Path = INSTALL_ATTESTATION_PATH,
    assets: Mapping[str, tuple[Path, int, bool]] | None = None,
    expected_root_uid: int = 0,
) -> VerifiedRunnerInstall:
    """Read the root-issued statement and re-hash every declared live asset."""
    statement = read_trusted_file(
        attestation_path,
        service_uid=service_uid,
        private=True,
        max_bytes=64 * 1024,
        require_nonempty=True,
    )
    if (
        statement.metadata.st_uid != expected_root_uid
        or stat.S_IMODE(statement.metadata.st_mode) != 0o640
    ):
        raise ValueError("runner install attestation metadata is unsafe")
    attestation = RunnerInstallAttestation.from_payload(statement.payload)
    selected = dict(INSTALL_ASSETS if assets is None else assets)
    if set(selected) != set(INSTALL_ASSETS):
        raise ValueError("runner install asset paths are incomplete")
    failed: list[str] = []
    for label in sorted(selected):
        path, mode, private = selected[label]
        try:
            payload = _read_asset(
                path,
                service_uid=service_uid,
                expected_mode=mode,
                expected_root_uid=expected_root_uid,
                private=private,
            ).payload
        except (OSError, ValueError):
            failed.append(label)
            continue
        if hashlib.sha256(payload).hexdigest() != attestation.asset_sha256[label]:
            failed.append(label)
    return VerifiedRunnerInstall(
        attestation=attestation,
        metadata_fingerprint=statement.metadata_fingerprint,
        acl_fingerprint=statement.acl_fingerprint,
        failed_assets=tuple(failed),
    )


__all__ = [
    "INSTALL_ASSETS",
    "INSTALL_ATTESTATION_PATH",
    "RunnerInstallAttestation",
    "VerifiedRunnerInstall",
    "verify_runner_install",
]
