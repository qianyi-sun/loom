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
        self, *, env: Driver, patterns: Sequence[str],
    ) -> ArtifactCollection:
        self.local_root.mkdir(parents=True, exist_ok=True)
        artifacts: list[CollectedArtifact] = []
        for pattern in patterns:
            anchored = f"{self.workspace_root.as_posix()}/{pattern}"
            cmd = (
                f"find {shlex.quote(self.workspace_root.as_posix())} "
                f"-path {shlex.quote(anchored)} -type f -print0"
            )
            result = await env.exec(cmd, user="root")
            if result.return_code != 0 or not result.stdout:
                continue
            for sandbox_path in result.stdout.split(b"\x00"):
                if not sandbox_path:
                    continue
                p = PurePosixPath(sandbox_path.decode())
                rel = p.relative_to(self.workspace_root)
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
        return ArtifactCollection(prefix=self.prefix, artifacts=artifacts)
