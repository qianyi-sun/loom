"""TB2.1 rev-6 catalog audit and atomic public-profile activation."""

from __future__ import annotations

import json
import tempfile
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom_benchmark_terminal_bench_2.upstream import TB21_TASK_COUNT, load_tb21_lock
from loom_benchmarks.util import sha256_of_dir
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Benchmark, BenchmarkAlias
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.manifest import tb21_workspace_policy_isolated

TB21_PROFILE_ID = "terminal-bench-2@tb2.1-r6"
TB21_PUBLIC_ALIAS = "terminal-bench-2"
TB21_VERIFIER_IDENTITY = "tb21-native-reward-file-v1"


class ProfileActivationError(ValueError):
    """Raised when a profile lacks the evidence required for public use."""


@dataclass(frozen=True)
class AuditResult:
    profile: str
    verified_bundles: int
    private_workspace_isolation: bool
    issues: tuple[str, ...] = ()
    architecture_diagnostics: tuple[str, ...] = ()

    def require_exact_profile(self, profile: str, *, task_count: int) -> None:
        if self.profile != profile:
            raise ProfileActivationError(
                f"audit profile mismatch: expected {profile!r}, got {self.profile!r}",
            )
        if self.verified_bundles != task_count:
            raise ProfileActivationError(
                f"activation requires exactly {task_count} verified bundles; "
                f"got {self.verified_bundles}",
            )
        if not self.private_workspace_isolation:
            raise ProfileActivationError(
                "activation requires verified private workspace isolation",
            )
        if self.issues:
            raise ProfileActivationError(
                "activation requires a clean audit: " + "; ".join(self.issues),
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "verified_bundles": self.verified_bundles,
            "private_workspace_isolation": self.private_workspace_isolation,
            "issues": list(self.issues),
            "architecture_diagnostics": list(self.architecture_diagnostics),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> AuditResult:
        profile = value.get("profile")
        verified = value.get("verified_bundles")
        isolated = value.get("private_workspace_isolation")
        issues = value.get("issues", [])
        diagnostics = value.get("architecture_diagnostics", [])
        if not isinstance(profile, str) or not isinstance(verified, int):
            raise ValueError("audit JSON requires string profile and integer verified_bundles")
        if not isinstance(isolated, bool):
            raise ValueError("audit JSON requires boolean private_workspace_isolation")
        if not isinstance(issues, list) or not all(isinstance(item, str) for item in issues):
            raise ValueError("audit JSON issues must be strings")
        if not isinstance(diagnostics, list) or not all(
            isinstance(item, str) for item in diagnostics
        ):
            raise ValueError("audit JSON architecture_diagnostics must be strings")
        return cls(
            profile=profile,
            verified_bundles=verified,
            private_workspace_isolation=isolated,
            issues=tuple(issues),
            architecture_diagnostics=tuple(diagnostics),
        )

    @classmethod
    def from_json_file(cls, path: Path) -> AuditResult:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("audit JSON must contain an object")
        return cls.from_dict(raw)


async def activate_tb21_alias(session: AsyncSession, audit: AuditResult) -> None:
    """Atomically point the public alias at the fully audited rev-6 profile."""
    audit.require_exact_profile(TB21_PROFILE_ID, task_count=TB21_TASK_COUNT)
    await session.execute(
        pg_insert(BenchmarkAlias)
        .values(alias=TB21_PUBLIC_ALIAS, benchmark_id=TB21_PROFILE_ID)
        .on_conflict_do_update(
            index_elements=["alias"],
            set_={"benchmark_id": TB21_PROFILE_ID},
        ),
    )
    await session.commit()


async def audit_tb21_profile(
    session: AsyncSession,
    *,
    object_store: ObjectStore,
) -> AuditResult:
    """Verify persisted TB2.1 provenance and each mirrored bundle's bytes."""
    lock = load_tb21_lock()
    issues: list[str] = []
    architecture_diagnostics: list[str] = []
    benchmark = await session.get(Benchmark, TB21_PROFILE_ID)
    profile_provenance: dict[str, Any] = (
        dict(benchmark.profile_provenance or {}) if benchmark is not None else {}
    )
    if benchmark is None:
        issues.append("physical TB2.1 profile is not registered")
    else:
        _require_equal(
            issues,
            "profile physical id",
            profile_provenance.get("physical_profile"),
            TB21_PROFILE_ID,
        )
        _require_equal(issues, "profile upstream kind", benchmark.upstream_kind, "harbor-package")
        _require_equal(issues, "profile Hub dataset", benchmark.upstream_locator, lock.dataset)
        _require_equal(issues, "profile Hub revision", benchmark.upstream_revision, lock.revision)
        if benchmark.execution_state != "runnable":
            issues.append(f"physical profile execution_state is {benchmark.execution_state!r}")
        _require_equal(
            issues,
            "profile hub metadata version",
            profile_provenance.get("hub_metadata_version"),
            lock.hub_metadata_version,
        )
        _require_equal(
            issues,
            "profile source reference snapshot",
            profile_provenance.get("source_reference_snapshot"),
            lock.source_revision,
        )
        _require_equal(
            issues,
            "profile reviewed source divergence",
            profile_provenance.get("source_reference_divergences"),
            lock.source_manifest_divergences,
        )
        _require_equal(
            issues,
            "profile verifier identity",
            profile_provenance.get("verifier_identity"),
            TB21_VERIFIER_IDENTITY,
        )

    profile_isolated = tb21_workspace_policy_isolated(
        profile_provenance.get("workspace_staging_policy"),
    )
    if not profile_isolated:
        issues.append("profile lacks reviewed private workspace isolation policy")

    task_rows = list(
        (
            await session.scalars(
                select(TaskRow).where(TaskRow.benchmark_id == TB21_PROFILE_ID).order_by(TaskRow.id),
            )
        ).all()
    )
    expected_names = {task.name for task in lock.tasks}
    observed_names = {
        row.id.removeprefix(f"{TB21_PROFILE_ID}/")
        for row in task_rows
        if row.id.startswith(f"{TB21_PROFILE_ID}/")
    }
    if observed_names != {name.removeprefix("terminal-bench/") for name in expected_names}:
        issues.append(
            "profile task-set drift: "
            f"missing={sorted({name.removeprefix('terminal-bench/') for name in expected_names} - observed_names)}; "
            f"extra={sorted(observed_names - {name.removeprefix('terminal-bench/') for name in expected_names})}",
        )

    verified = 0
    task_isolated = True
    for row in task_rows:
        short_name = row.id.removeprefix(f"{TB21_PROFILE_ID}/")
        source_name = f"terminal-bench/{short_name}"
        row_issues: list[str] = []
        provenance = dict(row.source_provenance or {})
        try:
            expected_digest = lock.digest_for(source_name)
        except ValueError:
            row_issues.append("task is absent from the Hub lock")
            expected_digest = None
        if expected_digest is not None:
            _require_equal(
                row_issues,
                "Harbor package digest",
                provenance.get("harbor_package_digest"),
                expected_digest,
            )
            _require_equal(
                row_issues,
                "source reference",
                provenance.get("source_reference"),
                lock.source_reference_for(source_name),
            )
        _require_equal(
            row_issues,
            "Harbor metadata version",
            provenance.get("harbor_metadata_version"),
            lock.hub_metadata_version,
        )
        _require_equal(
            row_issues,
            "verifier identity",
            provenance.get("verifier_identity"),
            TB21_VERIFIER_IDENTITY,
        )
        isolated = tb21_workspace_policy_isolated(
            provenance.get("workspace_staging_policy"),
        )
        task_isolated = task_isolated and isolated
        if not isolated:
            row_issues.append("task lacks reviewed private workspace isolation policy")

        config = _validated_config(row.config, row_issues)
        if config is not None:
            if config.task.id != row.id:
                row_issues.append("normalized task config id does not match task row")
            expected_image = _image_provenance(config)
            _require_equal(
                row_issues,
                "image provenance",
                provenance.get("image_provenance"),
                expected_image,
            )
            architecture_diagnostics.append(
                f"{row.id}: cpu_arch={config.environment.cpu_arch}",
            )

        try:
            await _verify_bundle_bytes(row, object_store=object_store, expected_config=config)
        except Exception as exc:
            row_issues.append(f"bundle bytes: {exc}")

        if row_issues:
            issues.extend(f"{row.id}: {issue}" for issue in row_issues)
        else:
            verified += 1

    return AuditResult(
        profile=TB21_PROFILE_ID,
        verified_bundles=verified,
        private_workspace_isolation=profile_isolated and task_isolated,
        issues=tuple(issues),
        architecture_diagnostics=tuple(architecture_diagnostics),
    )


async def _verify_bundle_bytes(
    row: TaskRow,
    *,
    object_store: ObjectStore,
    expected_config: TaskConfig | None,
) -> None:
    source = row.source
    if not isinstance(source, str) or not source.startswith("s3://"):
        raise ValueError("requires an internal s3:// source")
    bucket, separator, prefix = source.removeprefix("s3://").partition("/")
    if not bucket or not separator or not prefix:
        raise ValueError("has invalid internal source")
    with tempfile.TemporaryDirectory(prefix="loom-tb21-audit-") as temp:
        bundle_dir = Path(temp)
        # ObjectStore implementations validate object-key traversal while
        # materializing; invoke the same contract used by workers.
        await object_store.download_prefix(
            bucket=bucket,
            prefix=prefix,
            out_dir=bundle_dir,
        )
        actual = sha256_of_dir(bundle_dir)
        if not _same_sha256(row.checksum, actual):
            raise ValueError(f"checksum mismatch expected={row.checksum} actual={actual}")
        task_toml = bundle_dir / "task.toml"
        if not task_toml.is_file():
            raise ValueError("missing task.toml")
        bundle_config = TaskConfig.model_validate(tomllib.loads(task_toml.read_text()))
        if expected_config is not None and bundle_config != expected_config:
            raise ValueError("normalized config differs from persisted task config")


def _validated_config(value: object, issues: list[str]) -> TaskConfig | None:
    try:
        return TaskConfig.model_validate(value)
    except Exception as exc:
        issues.append(f"normalized config invalid: {exc}")
        return None


def _image_provenance(config: TaskConfig) -> dict[str, object]:
    environment = config.environment
    return {
        "docker_image": environment.docker_image,
        "dockerfile": (
            environment.dockerfile.as_posix() if environment.dockerfile is not None else None
        ),
        "docker_build_context": (
            environment.docker_build_context.as_posix()
            if environment.docker_build_context is not None
            else None
        ),
        "cpu_arch": environment.cpu_arch,
    }


def _require_equal(issues: list[str], label: str, actual: object, expected: object) -> None:
    if actual != expected:
        issues.append(f"{label} mismatch")


def _same_sha256(expected: str, actual: str) -> bool:
    return expected.removeprefix("sha256:") == actual.removeprefix("sha256:")


__all__ = [
    "TB21_PROFILE_ID",
    "AuditResult",
    "ProfileActivationError",
    "activate_tb21_alias",
    "audit_tb21_profile",
]
