from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_AGENT_SKILL_PLANS_PATH = Path(__file__).with_name("agent_skill_plans.jsonl")

AGENT_DOMAIN_HINT_MAP = {
    "software-engineering": "software_engineering",
    "frontend": "frontend_engineering",
    "testing": "software_engineering",
    "security": "security",
    "database": "data_processing",
    "code-quality": "software_maintenance",
    "debugging": "debugging",
    "code-refactoring": "software_maintenance",
    "optimization": "debugging",
    "code-analysis": "software_maintenance",
    "data-processing": "data_processing",
    "build-release-automation": "software_engineering",
    "code-migration": "software_maintenance",
    "file-operations": "file_operations",
    "machine-learning": "machine_learning",
    "devops": "system_administration",
    "workflow-orchestration": "software_engineering",
    "code-review": "software_maintenance",
    "bioinformatics": "bioinformatics",
    "data-engineering": "data_processing",
    "scientific-computing": "scientific_computing",
    "data-science": "data_science",
    "system-administration": "system_administration",
    "deployment": "system_administration",
    "version-control": "version_control",
    "multimodal-processing": "multimodal_processing",
    "model-training": "machine_learning",
    "mathematics": "algorithms_logic_puzzles",
}

AGENT_DOMAIN_FALLBACK_SOURCES = {
    "reverse_engineering": ("security", "debugging", "systems_programming"),
    "systems_programming": ("software_engineering", "debugging", "file_operations"),
    "programming_languages": ("software_engineering", "algorithms_logic_puzzles", "debugging"),
}

_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class AgentSkillCatalog:
    skills_by_domain: dict[str, tuple[str, ...]]

    def skills_for_domain(self, domain: str) -> tuple[str, ...]:
        try:
            return self.skills_by_domain[domain]
        except KeyError as exc:
            raise ValueError(f"no agent skills mapped to domain {domain!r}") from exc


def load_agent_skill_catalog(
    path: str | Path | None = None,
    *,
    min_skills_per_domain: int = 3,
    max_skills_per_domain: int | None = None,
) -> AgentSkillCatalog:
    normalized_path = _normalize_agent_skill_path(path)
    return _load_agent_skill_catalog_cached(
        str(normalized_path),
        min_skills_per_domain,
        max_skills_per_domain,
    )


def _normalize_agent_skill_path(path: str | Path | None) -> Path:
    if path is None:
        return DEFAULT_AGENT_SKILL_PLANS_PATH
    return Path(path).expanduser().resolve()


@lru_cache(maxsize=None)
def _load_agent_skill_catalog_cached(
    path: str,
    min_skills_per_domain: int,
    max_skills_per_domain: int | None,
) -> AgentSkillCatalog:
    if min_skills_per_domain <= 0:
        raise ValueError("min_skills_per_domain must be > 0")
    if max_skills_per_domain is not None and max_skills_per_domain < min_skills_per_domain:
        raise ValueError("max_skills_per_domain must be >= min_skills_per_domain")

    skill_path = Path(path)
    if not skill_path.exists():
        raise ValueError(f"agent skill plans file not found: {skill_path}")

    raw_skills_by_domain: dict[str, list[str]] = {}
    with skill_path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid agent skill JSON on line {line_number}: {skill_path}"
                ) from exc

            domain = _map_agent_domain(payload)
            if domain is None:
                continue
            skill_text = _format_agent_skill(payload)
            if not skill_text:
                continue
            raw_skills_by_domain.setdefault(domain, []).append(skill_text)

    skills_by_domain: dict[str, tuple[str, ...]] = {}
    for domain, raw_skills in raw_skills_by_domain.items():
        skills = _dedupe_ordered(raw_skills)
        if max_skills_per_domain is not None:
            skills = skills[:max_skills_per_domain]
        if len(skills) >= min_skills_per_domain:
            skills_by_domain[domain] = tuple(skills)
    _fill_fallback_domains(skills_by_domain, max_skills_per_domain)

    if not skills_by_domain:
        raise ValueError(f"agent skill plans file did not yield mapped skills: {skill_path}")
    return AgentSkillCatalog(skills_by_domain=skills_by_domain)


def _map_agent_domain(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return None
    domain_hint = metadata.get("domain_hint")
    if not isinstance(domain_hint, str):
        return None
    return AGENT_DOMAIN_HINT_MAP.get(domain_hint.strip())


def _format_agent_skill(payload: dict[str, Any]) -> str:
    title = _clean_text(payload.get("title") or payload.get("slug") or payload.get("skill_id"))
    plan = payload.get("plan")
    if not isinstance(plan, dict):
        return title
    user_goal = _clean_text(plan.get("user_goal"))
    if title and user_goal:
        return f"{title}: {user_goal}"
    return title or user_goal


def _clean_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip()


def _dedupe_ordered(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        ordered.append(value)
        seen.add(value)
    return ordered


def _fill_fallback_domains(
    skills_by_domain: dict[str, tuple[str, ...]],
    max_skills_per_domain: int | None,
) -> None:
    for target_domain, source_domains in AGENT_DOMAIN_FALLBACK_SOURCES.items():
        if target_domain in skills_by_domain:
            continue
        fallback_skills: list[str] = []
        for source_domain in source_domains:
            fallback_skills.extend(skills_by_domain.get(source_domain, ()))
        fallback_skills = _dedupe_ordered(fallback_skills)
        if max_skills_per_domain is not None:
            fallback_skills = fallback_skills[:max_skills_per_domain]
        if fallback_skills:
            skills_by_domain[target_domain] = tuple(fallback_skills)
