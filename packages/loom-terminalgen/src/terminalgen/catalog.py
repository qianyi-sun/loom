from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_CATALOG_FILENAME = "catalog.realistic.json"


@dataclass(frozen=True)
class DomainSpec:
    name: str
    summary: str
    description: str
    weight: float
    skills: tuple[str, ...]


def default_catalog_path() -> Path:
    return Path(__file__).with_name(DEFAULT_CATALOG_FILENAME)


def _normalize_catalog_path(config_path: str | Path | None) -> Path:
    if config_path is None:
        return default_catalog_path()
    return Path(config_path).expanduser().resolve()


def _load_json_with_comments(path: Path) -> Any:
    return json.loads(_strip_json_comments(path.read_text(encoding="utf-8")))


def _strip_json_comments(text: str) -> str:
    result: list[str] = []
    in_string = False
    escape = False
    index = 0

    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if in_string:
            result.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            index += 1
            continue

        if char == '"':
            in_string = True
            result.append(char)
            index += 1
            continue

        if char == "/" and next_char == "/":
            index += 2
            while index < len(text) and text[index] not in "\r\n":
                index += 1
            continue

        if char == "/" and next_char == "*":
            index += 2
            while index + 1 < len(text) and not (text[index] == "*" and text[index + 1] == "/"):
                index += 1
            index += 2
            continue

        result.append(char)
        index += 1

    return "".join(result)


def _as_tuple(payload: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(payload, list):
        raise ValueError(f"{field_name} must be a list of strings")
    values: list[str] = []
    for item in payload:
        if not isinstance(item, str):
            raise ValueError(f"{field_name} must be a list of strings")
        values.append(item)
    return tuple(values)


def _dedupe_ordered(values: list[str]) -> tuple[str, ...]:
    ordered: list[str] = []
    for value in values:
        if value not in ordered:
            ordered.append(value)
    return tuple(ordered)


def _parse_skills(payload: Any, *, field_name: str) -> tuple[str, ...]:
    skills = _as_tuple(payload, field_name)
    if not skills:
        raise ValueError(f"{field_name} must define at least one skill")
    return _dedupe_ordered(list(skills))


def _parse_weight(payload: Any, *, field_name: str) -> float:
    if payload is None:
        return 1.0
    if isinstance(payload, bool) or not isinstance(payload, (int, float)):
        raise ValueError(f"{field_name} must be a non-negative number")
    weight = float(payload)
    if math.isnan(weight) or math.isinf(weight) or weight < 0:
        raise ValueError(f"{field_name} must be a non-negative finite number")
    return weight


def _parse_domain_spec(payload: dict[str, Any]) -> DomainSpec:
    if not isinstance(payload, dict):
        raise ValueError("domain payload must be an object")
    name = payload.get("name")
    summary = payload.get("summary")
    description = payload.get("description", summary)
    if not isinstance(name, str) or not name:
        raise ValueError("domain must have a non-empty name")
    if not isinstance(summary, str) or not summary:
        raise ValueError(f"domain {name!r} must have a non-empty summary")
    if not isinstance(description, str) or not description:
        raise ValueError(f"domain {name!r} must have a non-empty description")
    merged_skills = _parse_skills(payload.get("skills"), field_name="skills")

    return DomainSpec(
        name=name,
        summary=summary,
        description=description,
        weight=_parse_weight(payload.get("weight"), field_name="weight"),
        skills=merged_skills,
    )


@lru_cache(maxsize=None)
def load_catalog(config_path: str | Path | None = None) -> dict[str, DomainSpec]:
    path = _normalize_catalog_path(config_path)
    if not path.exists():
        raise ValueError(f"catalog config not found: {path}")

    payload = _load_json_with_comments(path)
    if not isinstance(payload, dict):
        raise ValueError(f"catalog config must be a JSON object: {path}")

    domains_payload = payload.get("domains")
    if not isinstance(domains_payload, list) or not domains_payload:
        raise ValueError("catalog config must define a non-empty domains list")

    domain_specs_list = [_parse_domain_spec(domain_payload) for domain_payload in domains_payload]
    domain_names = [spec.name for spec in domain_specs_list]
    if len(set(domain_names)) != len(domain_names):
        raise ValueError("catalog config contains duplicate domain names")

    return {spec.name: spec for spec in domain_specs_list}


def get_domain_specs(
    domains: list[str] | None = None,
    *,
    config_path: str | Path | None = None,
) -> list[DomainSpec]:
    domain_specs = load_catalog(config_path)
    if not domains:
        return list(domain_specs.values())

    missing = [domain for domain in domains if domain not in domain_specs]
    if missing:
        raise ValueError(f"unknown domains: {', '.join(sorted(set(missing)))}")

    ordered_unique: list[str] = []
    for domain in domains:
        if domain not in ordered_unique:
            ordered_unique.append(domain)
    return [domain_specs[domain] for domain in ordered_unique]


def get_domain_spec(domain: str, *, config_path: str | Path | None = None) -> DomainSpec:
    domain_specs = load_catalog(config_path)
    if domain not in domain_specs:
        raise ValueError(f"unknown domain: {domain}")
    return domain_specs[domain]
