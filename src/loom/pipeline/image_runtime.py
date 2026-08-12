"""Immutable multi-arch image runtime allowlist and provider-asset checks."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

from loom.pipeline.keys import canonical_digest
from loom.pipeline.work_protocol import ImageRuntimeContractV1

_DRIVER_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)+$")


class ImageRuntimeRegistryError(ValueError):
    pass


def dotted_integer_version(value: str) -> tuple[int, ...]:
    if _DRIVER_RE.fullmatch(value) is None:
        raise ImageRuntimeRegistryError("driver version must contain dotted integers only")
    return tuple(int(part) for part in value.split("."))


def driver_version_satisfies(actual: str, minimum: str) -> bool:
    actual_parts = dotted_integer_version(actual)
    minimum_parts = dotted_integer_version(minimum)
    width = max(len(actual_parts), len(minimum_parts))
    return actual_parts + (0,) * (width - len(actual_parts)) >= minimum_parts + (0,) * (
        width - len(minimum_parts)
    )


@dataclass(frozen=True)
class ImageRuntimeRecord:
    contract: ImageRuntimeContractV1
    snapshot_sha256: str

    @property
    def key(self) -> tuple[str, str]:
        return self.contract.image_index_digest, self.contract.platform


class ImageRuntimeRegistry:
    def __init__(self, records: Mapping[tuple[str, str], ImageRuntimeRecord]) -> None:
        manifest_platforms: dict[str, str] = {}
        for key, record in records.items():
            if key != record.key:
                raise ImageRuntimeRegistryError("image runtime registry key drift")
            digest = record.contract.platform_manifest_digest
            existing_platform = manifest_platforms.setdefault(
                digest, record.contract.platform
            )
            if existing_platform != record.contract.platform:
                raise ImageRuntimeRegistryError(
                    "platform manifest digest cannot be substituted across platforms"
                )
        self._records = MappingProxyType(dict(records))

    @classmethod
    def load(
        cls, path: Path = Path("config/image-runtime-contracts.toml")
    ) -> ImageRuntimeRegistry:
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ImageRuntimeRegistryError(f"cannot load image runtime registry: {exc}") from exc
        if set(raw) != {"schema_version", "contracts"}:
            raise ImageRuntimeRegistryError("image runtime registry keys are not closed")
        if raw["schema_version"] != "loom.image-runtime-registry.v1":
            raise ImageRuntimeRegistryError("unsupported image runtime registry version")
        contracts = raw["contracts"]
        if not isinstance(contracts, list):
            raise ImageRuntimeRegistryError("contracts must be an array")
        records: dict[tuple[str, str], ImageRuntimeRecord] = {}
        previous: tuple[bytes, bytes] | None = None
        for value in contracts:
            if not isinstance(value, dict):
                raise ImageRuntimeRegistryError("image runtime entry must be a table")
            normalized = dict(value)
            normalized.setdefault("cuda_userspace_version", None)
            normalized.setdefault("min_nvidia_driver_version", None)
            contract = ImageRuntimeContractV1.model_validate(normalized)
            key = (contract.image_index_digest, contract.platform)
            ordering = (key[0].encode("utf-8"), key[1].encode("utf-8"))
            if previous is not None and ordering <= previous:
                raise ImageRuntimeRegistryError(
                    "image runtime entries must be unique and bytewise sorted"
                )
            previous = ordering
            records[key] = ImageRuntimeRecord(contract, canonical_digest(contract))
        return cls(records)

    def resolve(
        self,
        *,
        image_index_digest: str,
        cpu_arch: str,
        expected_snapshot_sha256: str | None = None,
    ) -> ImageRuntimeRecord:
        platform = {"x86_64": "linux/amd64", "arm64": "linux/arm64"}.get(cpu_arch)
        if platform is None:
            raise ImageRuntimeRegistryError("unsupported image CPU architecture")
        try:
            record = self._records[(image_index_digest, platform)]
        except KeyError as exc:
            raise ImageRuntimeRegistryError("image_contract_mismatch") from exc
        if (
            expected_snapshot_sha256 is not None
            and record.snapshot_sha256 != expected_snapshot_sha256
        ):
            raise ImageRuntimeRegistryError("image_contract_mismatch")
        return record

    def get(self, image_index_digest: str, platform: str) -> ImageRuntimeRecord:
        try:
            return self._records[(image_index_digest, platform)]
        except KeyError as exc:
            raise ImageRuntimeRegistryError("image_contract_mismatch") from exc

    def list(self) -> tuple[ImageRuntimeRecord, ...]:
        return tuple(self._records.values())


def validate_provider_asset_payloads(
    contract: ImageRuntimeContractV1,
    *,
    observed_sha256_by_path: Mapping[str, str],
) -> None:
    """Require the attested image paths and hashes without a runtime fetch."""

    expected = {entry.image_path: entry.sha256 for entry in contract.provider_assets}
    if dict(observed_sha256_by_path) != expected:
        raise ImageRuntimeRegistryError("provider_asset_manifest_mismatch")
    for entry in contract.provider_assets:
        path = PurePosixPath(entry.image_path)
        root = PurePosixPath("/opt/behavior/provider-assets") / entry.logical_name
        if path == root or not path.is_relative_to(root):
            raise ImageRuntimeRegistryError("provider asset escaped its immutable image root")


def image_contract_digest(value: Mapping[str, Any] | ImageRuntimeContractV1) -> str:
    contract = (
        value
        if isinstance(value, ImageRuntimeContractV1)
        else ImageRuntimeContractV1.model_validate(value)
    )
    return canonical_digest(contract)
