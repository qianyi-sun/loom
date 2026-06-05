"""SkillRef + SkillSource — declarations for domain-knowledge bundles injected
into the sandbox (spec §4.2). v1 only supports local sources; git/registry are
parsed but not resolved by the foundation library."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict


class SkillSource(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    kind: Literal["local", "git", "registry"]
    path: PurePosixPath | None = None    # local
    repo: str | None = None              # git
    ref: str | None = None               # git
    id: str | None = None                # registry


class SkillRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    version: str | None = None
    source: SkillSource
