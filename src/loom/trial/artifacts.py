"""ArtifactCollector — downloads matched files out of the sandbox and uploads
to MinIO under s3://{bucket}/{team}/{trial}/{step}/{rel_path}.

Spec §3.9. POSIX globs anchored at the workspace root; safe quoting via shlex.
"""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path, PurePosixPath

from loom.driver.base import Driver
from loom.security.redaction import ShareStatus, contains_secret_like_content
from loom.trajectory.storage import ObjectStore


@dataclass(frozen=True)
class CollectedArtifact:
    bucket: str
    key: str
    size: int
    content_hash: str
    share_status: ShareStatus
    blocked_reason: str | None = None


@dataclass(frozen=True)
class ArtifactCollection:
    prefix: str
    artifacts: list[CollectedArtifact]
    missing_required: list[str] = field(default_factory=list)


@dataclass
class ArtifactCollector:
    store: ObjectStore
    bucket: str
    team_id: str
    trial_id: str
    step_name: str
    local_root: Path
    workspace_root: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )

    @property
    def prefix(self) -> str:
        return f"s3://{self.bucket}/{self.team_id}/{self.trial_id}/{self.step_name}/"

    async def collect(
        self,
        *,
        env: Driver,
        patterns: Sequence[str],
        required_patterns: Sequence[str] = (),
        platform_patterns: Sequence[str] = (),
    ) -> ArtifactCollection:
        self.local_root.mkdir(parents=True, exist_ok=True)
        artifacts: list[CollectedArtifact] = []
        missing_required: list[str] = []
        required = set(required_patterns)
        seen: set[str] = set()
        pattern_specs = [
            (pattern, False)
            for pattern in _ordered_unique((*patterns, *required_patterns))
        ]
        pattern_specs.extend(
            (pattern, True) for pattern in _ordered_unique(platform_patterns)
        )
        for pattern, allow_reserved_verifier in pattern_specs:
            if allow_reserved_verifier and not _is_exact_verifier_pattern(pattern):
                continue
            anchored = f"{self.workspace_root.as_posix()}/{pattern}"
            cmd = (
                f"find {shlex.quote(self.workspace_root.as_posix())} "
                f"-path {shlex.quote(anchored)} -type f -print0"
            )
            result = await env.exec(cmd, user="root")
            if result.return_code != 0 or not result.stdout:
                if pattern in required:
                    missing_required.append(pattern)
                continue
            matched = False
            for sandbox_path in result.stdout.split(b"\x00"):
                if not sandbox_path:
                    continue
                p = PurePosixPath(sandbox_path.decode())
                rel = p.relative_to(self.workspace_root)
                if allow_reserved_verifier and rel.as_posix() != pattern:
                    continue
                if _is_reserved_verifier_path(rel) and not allow_reserved_verifier:
                    continue
                matched = True
                if rel.as_posix() in seen:
                    continue
                seen.add(rel.as_posix())
                local_target = self.local_root / rel
                local_target.parent.mkdir(parents=True, exist_ok=True)
                await env.download(p, local_target)
                key = (
                    f"{self.team_id}/{self.trial_id}/{self.step_name}/"
                    f"{rel.as_posix()}"
                )
                body = local_target.read_bytes()
                share_decision = contains_secret_like_content(body)
                await self.store.put_object(
                    bucket=self.bucket, key=key, body=body,
                )
                artifacts.append(
                    CollectedArtifact(
                        bucket=self.bucket,
                        key=key,
                        size=len(body),
                        content_hash=f"sha256:{sha256(body).hexdigest()}",
                        share_status=share_decision.status,
                        blocked_reason=share_decision.reason,
                    ),
                )
            if pattern in required and not matched:
                missing_required.append(pattern)
        return ArtifactCollection(
            prefix=self.prefix,
            artifacts=artifacts,
            missing_required=missing_required,
        )


def _ordered_unique(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _is_reserved_verifier_path(path: PurePosixPath) -> bool:
    parts = path.parts
    return len(parts) >= 2 and parts[:2] == (".loom", "verifier")


def _is_exact_verifier_pattern(pattern: str) -> bool:
    path = PurePosixPath(pattern)
    return (
        path.as_posix() == pattern
        and _is_reserved_verifier_path(path)
        and not any(char in pattern for char in "*?[")
    )
