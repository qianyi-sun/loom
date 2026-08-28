"""Admit one exact historical rollout runtime under a newer installed runner."""

from __future__ import annotations

import hashlib
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

from loom_cli.rollout.credential_authority import read_trusted_file

from .candidate import GitRunner, verify_resume_runtime_candidate
from .config import OperatorConfig, candidate_sha_from_runner_repo, environment_authority

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_CONFIG_BYTES = 1024 * 1024

RuntimeVerifier = Callable[[OperatorConfig, str, str | None], None]
AncestryVerifier = Callable[[Path, str, str], bool]
ClusterConfigReader = Callable[[Path], bytes]


class ResumeRuntimeUpgradeError(ValueError):
    """Raised when a failed request cannot reuse its historical runtime binding."""


@dataclass(frozen=True, slots=True)
class ResumeRuntimeUpgradeAuthority:
    """Resolve a whole-config-equivalent historical merged-dev runtime."""

    current_config_payload: bytes
    verify_runtime: RuntimeVerifier
    prove_ancestor: AncestryVerifier
    read_cluster_config: ClusterConfigReader

    def __post_init__(self) -> None:
        if (
            not self.current_config_payload
            or len(self.current_config_payload) > _MAX_CONFIG_BYTES
            or not callable(self.verify_runtime)
            or not callable(self.prove_ancestor)
            or not callable(self.read_cluster_config)
        ):
            raise ValueError("resume runtime upgrade authority is invalid")

    def resolve(
        self,
        config: OperatorConfig,
        *,
        candidate_sha: str,
        candidate_tree: str | None,
        runner_config_sha256: str,
        cluster_config_path: str,
    ) -> OperatorConfig:
        """Return the exact historical config or fail before any launch."""
        if (
            config.source_mode != "merged-dev"
            or config.source_commit_sha is not None
            or config.source_tree_sha is not None
            or config.source_base_sha is not None
            or _SHA_RE.fullmatch(candidate_sha) is None
            or candidate_tree is None
            or _SHA_RE.fullmatch(candidate_tree) is None
            or _SHA256_RE.fullmatch(runner_config_sha256) is None
            or hashlib.sha256(self.current_config_payload).hexdigest()
            != config.config_sha256
        ):
            raise ResumeRuntimeUpgradeError("resume runtime upgrade binding is invalid")
        authority = environment_authority(config.short_name)
        current_sha = candidate_sha_from_runner_repo(config.runner_repo, authority=authority)
        historical_repo = authority.candidate_runtime_root / candidate_sha / "repo"
        historical_cluster_config = historical_repo / authority.candidate_cluster_config
        if (
            candidate_sha == current_sha
            or cluster_config_path != str(historical_cluster_config)
            or self.current_config_payload.count(str(config.runner_repo).encode()) != 2
        ):
            raise ResumeRuntimeUpgradeError("resume runtime upgrade path binding drifted")
        historical_payload = self.current_config_payload.replace(
            str(config.runner_repo).encode(),
            str(historical_repo).encode(),
        )
        if hashlib.sha256(historical_payload).hexdigest() != runner_config_sha256:
            raise ResumeRuntimeUpgradeError("resume runtime upgrade config binding drifted")
        current_cluster_payload = self.read_cluster_config(config.cluster_config_path)
        historical_cluster_payload = self.read_cluster_config(historical_cluster_config)
        if (
            not current_cluster_payload
            or len(current_cluster_payload) > _MAX_CONFIG_BYTES
            or current_cluster_payload != historical_cluster_payload
        ):
            raise ResumeRuntimeUpgradeError("resume runtime upgrade cluster config drifted")
        effective = replace(
            config,
            runner_repo=historical_repo,
            cluster_config_path=historical_cluster_config,
            config_sha256=runner_config_sha256,
        )
        try:
            self.verify_runtime(config, current_sha, None)
            self.verify_runtime(effective, candidate_sha, candidate_tree)
            ancestor = self.prove_ancestor(config.runner_repo, candidate_sha, current_sha)
        except Exception as exc:
            raise ResumeRuntimeUpgradeError(
                "resume runtime upgrade candidate evidence is invalid"
            ) from exc
        if not ancestor:
            raise ResumeRuntimeUpgradeError("resume runtime upgrade is not a forward upgrade")
        return effective


def _git_ancestry_argv(repo: Path, historical_sha: str, current_sha: str) -> list[str]:
    return [
        "git",
        "--no-pager",
        "--no-replace-objects",
        "--no-optional-locks",
        "-c",
        f"safe.directory={repo}",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "submodule.recurse=false",
        "-C",
        str(repo),
        "merge-base",
        "--is-ancestor",
        historical_sha,
        current_sha,
    ]


def build_installed_resume_runtime_upgrade_authority(
    config: OperatorConfig,
    *,
    service_uid: int,
    run: GitRunner,
) -> ResumeRuntimeUpgradeAuthority:
    """Compose the installed, read-only evidence sources for upgrade recovery."""
    if service_uid < 1 or not callable(run):
        raise ValueError("installed resume runtime upgrade authority is invalid")
    current_config = read_trusted_file(
        config.config_path,
        service_uid=service_uid,
        private=True,
        max_bytes=_MAX_CONFIG_BYTES,
        require_nonempty=True,
    ).payload

    def verify_runtime(active: OperatorConfig, sha: str, tree: str | None) -> None:
        verify_resume_runtime_candidate(active, sha, tree, run=run)

    def prove_ancestor(repo: Path, historical_sha: str, current_sha: str) -> bool:
        try:
            result = run(_git_ancestry_argv(repo, historical_sha, current_sha))
        except (OSError, subprocess.TimeoutExpired):
            return False
        return bool(
            type(result.returncode) is int
            and result.returncode == 0
            and result.stdout == ""
            and result.stderr == ""
        )

    def read_cluster_config(path: Path) -> bytes:
        return read_trusted_file(
            path,
            service_uid=service_uid,
            private=False,
            max_bytes=_MAX_CONFIG_BYTES,
            require_nonempty=True,
        ).payload

    return ResumeRuntimeUpgradeAuthority(
        current_config_payload=current_config,
        verify_runtime=verify_runtime,
        prove_ancestor=prove_ancestor,
        read_cluster_config=read_cluster_config,
    )


__all__ = [
    "ResumeRuntimeUpgradeAuthority",
    "ResumeRuntimeUpgradeError",
    "build_installed_resume_runtime_upgrade_authority",
]
