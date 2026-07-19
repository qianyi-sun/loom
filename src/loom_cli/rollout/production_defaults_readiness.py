"""Canonical immutable production-defaults input for preflight and final apply."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from loom_cli.environment_state import EnvironmentStateProfile, load_environment_state_profile
from loom_llm_gateway.yibuapi_pricing import DEFAULT_YIBUAPI_PRICING_URL

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class ProviderPricingDefault:
    """One normalized hosted-provider pricing default."""

    name: str
    pricing_source: str
    rate_card_provider: str | None
    required: bool

    def __post_init__(self) -> None:
        if (
            not self.name
            or self.pricing_source not in {"rate-card", "tokens-only"}
            or (self.pricing_source == "rate-card" and not self.rate_card_provider)
            or (self.rate_card_provider is not None and not self.rate_card_provider)
        ):
            raise ValueError("production provider default is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "pricing_source": self.pricing_source,
            "rate_card_provider": self.rate_card_provider,
            "required": self.required,
        }


@dataclass(frozen=True, slots=True)
class ProductionDefaultsArtifact:
    """Exact secret-free defaults consumed by protected final convergence."""

    schema_version: int
    candidate_sha: str
    candidate_tree: str
    environment: str
    yibuapi_sync: Mapping[str, str] | None
    providers: tuple[ProviderPricingDefault, ...]
    artifact_digest: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or self.environment != "staging"
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
            or tuple(sorted(self.providers, key=lambda item: item.name)) != self.providers
            or len({item.name for item in self.providers}) != len(self.providers)
        ):
            raise ValueError("production defaults artifact identity is invalid")
        if self.yibuapi_sync is not None:
            expected = {"group", "source_url"}
            if (
                not self.yibuapi_sync
                or not set(self.yibuapi_sync).issubset(expected)
                or not self.yibuapi_sync.get("group")
                or any(
                    not isinstance(value, str) or not value for value in self.yibuapi_sync.values()
                )
            ):
                raise ValueError("production defaults rate-card sync is invalid")
            object.__setattr__(self, "yibuapi_sync", MappingProxyType(dict(self.yibuapi_sync)))
        if _hash_json(self.payload()) != self.artifact_digest:
            raise ValueError("production defaults artifact digest drifted")

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "environment": self.environment,
            "yibuapi_sync": None if self.yibuapi_sync is None else dict(self.yibuapi_sync),
            "providers": [provider.to_dict() for provider in self.providers],
        }

    def to_bytes(self) -> bytes:
        return _json_bytes({**self.payload(), "artifact_digest": self.artifact_digest})

    @classmethod
    def from_bytes(cls, payload: bytes) -> ProductionDefaultsArtifact:
        try:
            raw = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError("production defaults artifact is invalid") from exc
        if not isinstance(raw, dict) or set(raw) != {
            "schema_version",
            "candidate_sha",
            "candidate_tree",
            "environment",
            "yibuapi_sync",
            "providers",
            "artifact_digest",
        }:
            raise ValueError("production defaults artifact fields are invalid")
        sync = raw["yibuapi_sync"]
        if sync is not None and not isinstance(sync, dict):
            raise ValueError("production defaults rate-card sync is invalid")
        if sync is not None and any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in sync.items()
        ):
            raise ValueError("production defaults rate-card sync is invalid")
        providers = raw["providers"]
        if not isinstance(providers, list):
            raise ValueError("production defaults providers are invalid")
        parsed: list[ProviderPricingDefault] = []
        for provider in providers:
            if not isinstance(provider, dict) or set(provider) != {
                "name",
                "pricing_source",
                "rate_card_provider",
                "required",
            }:
                raise ValueError("production defaults provider fields are invalid")
            values = provider
            rate_card_provider = values["rate_card_provider"]
            if rate_card_provider is not None and not isinstance(rate_card_provider, str):
                raise ValueError("production defaults provider is invalid")
            if type(values["required"]) is not bool:
                raise ValueError("production defaults provider is invalid")
            parsed.append(
                ProviderPricingDefault(
                    name=_string(values, "name"),
                    pricing_source=_string(values, "pricing_source"),
                    rate_card_provider=rate_card_provider,
                    required=values["required"],
                )
            )
        return cls(
            schema_version=_integer(raw, "schema_version"),
            candidate_sha=_string(raw, "candidate_sha"),
            candidate_tree=_string(raw, "candidate_tree"),
            environment=_string(raw, "environment"),
            yibuapi_sync=(None if sync is None else MappingProxyType(dict(sync))),
            providers=tuple(parsed),
            artifact_digest=_string(raw, "artifact_digest"),
        )


def build_production_defaults_artifact(
    profile_path: Path,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    environment: str,
) -> ProductionDefaultsArtifact:
    """Load and normalize exact candidate defaults without secret values."""
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": image_tag,
            "ENV_CONFIG_VERSION": image_tag,
            "GIT_SHA": candidate_sha,
        },
        expected_environment=environment,
    )
    sync = _normalized_yibuapi_sync(profile)
    providers = tuple(
        sorted(
            (
                ProviderPricingDefault(
                    name=str(item["name"]),
                    pricing_source=str(item["pricing_source"]),
                    rate_card_provider=(
                        None
                        if item.get("rate_card_provider") is None
                        else str(item["rate_card_provider"])
                    ),
                    required=bool(item["required"]),
                )
                for item in profile.hosted_provider_pricing_defaults
            ),
            key=lambda item: item.name,
        )
    )
    base = {
        "schema_version": 1,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "environment": environment,
        "yibuapi_sync": sync,
        "providers": [provider.to_dict() for provider in providers],
    }
    return ProductionDefaultsArtifact(
        schema_version=1,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        environment=environment,
        yibuapi_sync=sync,
        providers=providers,
        artifact_digest=_hash_json(base),
    )


def _normalized_yibuapi_sync(profile: EnvironmentStateProfile) -> dict[str, str] | None:
    raw = profile.rate_card_sync.get("yibuapi")
    if raw is None:
        return None
    if not isinstance(raw, dict) or not set(raw).issubset({"enabled", "group", "source_url"}):
        raise ValueError("production defaults rate-card sync is invalid")
    if type(raw.get("enabled", False)) is not bool:
        raise ValueError("production defaults rate-card sync is invalid")
    if not raw.get("enabled", False):
        return None
    group = raw.get("group", "default")
    if not isinstance(group, str) or not group.strip():
        raise ValueError("production defaults rate-card sync is invalid")
    source_url = raw.get("source_url", DEFAULT_YIBUAPI_PRICING_URL)
    if not isinstance(source_url, str) or not source_url.strip():
        raise ValueError("production defaults rate-card sync is invalid")
    result = {"group": group.strip(), "source_url": source_url.strip()}
    return result


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _hash_json(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise ValueError(f"production defaults {key} is invalid")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if type(item) is not int:
        raise ValueError(f"production defaults {key} is invalid")
    return item


__all__ = [
    "ProductionDefaultsArtifact",
    "ProviderPricingDefault",
    "build_production_defaults_artifact",
]
