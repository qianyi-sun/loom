"""TB2.1 rev-6 provenance and private-workspace policy contracts."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import pytest
import tomli_w
from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock
from loom_benchmarks.util import sha256_of_dir

from loom.trial.workspace import WorkspaceStagingPolicy, materialize_workspace
from loom_benchmark_tool.audit_cmd import (
    AuditResult,
    ProfileActivationError,
    audit_tb21_profile,
)
from loom_benchmark_tool.manifest import (
    MANIFEST_SCHEMA_VERSION,
    TB21_AGENT_WORKSPACE_POLICY,
)


class _RecordingDriver:
    def __init__(self) -> None:
        self.uploaded: list[str] = []

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self.uploaded.append(dst.as_posix())


def test_tb21_manifest_schema_requires_a_private_agent_workspace_policy() -> None:
    assert MANIFEST_SCHEMA_VERSION == 4
    assert TB21_AGENT_WORKSPACE_POLICY == {
        "schema_version": 1,
        "agent_excluded_paths": [
            "solution/**",
            "tests/**",
            "verifier/**",
            "upstream-task.toml",
        ],
        "verifier_only_paths": [
            "solution/**",
            "tests/**",
            "verifier/**",
            "upstream-task.toml",
        ],
    }


@pytest.mark.asyncio
async def test_tb21_agent_staging_excludes_private_bundle_content(
    tmp_path: Path,
) -> None:
    (tmp_path / "instruction.md").write_text("Solve the task.\n")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "solve.sh").write_text("reference answer\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test.sh").write_text("private verifier\n")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier" / "run.sh").write_text("private bridge\n")
    (tmp_path / "upstream-task.toml").write_text("native private fields\n")

    policy = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    driver = _RecordingDriver()

    await materialize_workspace(
        driver=driver,
        task_dir=tmp_path,
        dst=PurePosixPath("/workspace"),
        policy=policy,
        phase="agent",
    )

    assert driver.uploaded == ["/workspace/instruction.md"]

    await materialize_workspace(
        driver=driver,
        task_dir=tmp_path,
        dst=PurePosixPath("/workspace"),
        policy=policy,
        phase="verifier",
    )

    assert set(driver.uploaded) == {
        "/workspace/instruction.md",
        "/workspace/solution/solve.sh",
        "/workspace/tests/test.sh",
        "/workspace/verifier/run.sh",
        "/workspace/upstream-task.toml",
    }


def test_activation_rejects_partial_or_unisolated_audit() -> None:
    with pytest.raises(ProfileActivationError, match="89 verified bundles"):
        AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=88,
            private_workspace_isolation=True,
        ).require_exact_profile("terminal-bench-2@tb2.1-r6", task_count=89)


@pytest.mark.asyncio
async def test_audit_verifies_locked_provenance_config_and_bundle_bytes(
    tmp_path: Path,
) -> None:
    lock = load_tb21_lock()
    profile = "terminal-bench-2@tb2.1-r6"
    payloads: dict[str, bytes] = {}
    task_rows: list[object] = []
    for task in lock.tasks:
        short_name = task.name.removeprefix("terminal-bench/")
        task_id = f"{profile}/{short_name}"
        config = {
            "schema_version": "1",
            "task": {"id": task_id, "name": short_name},
            "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
            "agent": {"name": "oracle"},
            "verifier": {"name": "script", "args": {"script_path": "/workspace/verifier/run.sh"}},
            "steps": [{"name": "main"}],
        }
        bundle = tmp_path / short_name
        bundle.mkdir()
        (bundle / "task.toml").write_text(tomli_w.dumps(config))
        checksum = sha256_of_dir(bundle)
        payloads[f"{short_name}/"] = (bundle / "task.toml").read_bytes()
        task_rows.append(
            SimpleNamespace(
                id=task_id,
                checksum=checksum,
                config=config,
                source=f"s3://loom-benchmarks/{short_name}/",
                source_provenance={
                    "harbor_package_digest": task.digest,
                    "harbor_metadata_version": lock.hub_metadata_version,
                    "source_reference": lock.source_reference_for(task.name),
                    "verifier_identity": "tb21-native-reward-file-v1",
                    "image_provenance": {
                        "docker_image": "python:3.12-slim",
                        "dockerfile": None,
                        "docker_build_context": None,
                        "cpu_arch": "x86_64",
                    },
                    "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
                },
            )
        )

    class _Session:
        async def get(self, _model: object, _key: object) -> object:
            return SimpleNamespace(
                execution_state="runnable",
                upstream_kind="harbor-package",
                upstream_locator=lock.dataset,
                upstream_revision=lock.revision,
                profile_provenance={
                    "physical_profile": profile,
                    "hub_metadata_version": lock.hub_metadata_version,
                    "source_reference_snapshot": lock.source_revision,
                    "source_reference_divergences": lock.source_manifest_divergences,
                    "verifier_identity": "tb21-native-reward-file-v1",
                    "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
                },
            )

        async def scalars(self, _statement: object) -> object:
            return SimpleNamespace(all=lambda: task_rows)

    class _ObjectStore:
        async def download_prefix(
            self,
            *,
            bucket: str,
            prefix: str,
            out_dir: Path,
        ) -> int:
            assert bucket == "loom-benchmarks"
            (out_dir / "task.toml").write_bytes(payloads[prefix])
            return 1

    result = await audit_tb21_profile(_Session(), object_store=_ObjectStore())

    assert result.verified_bundles == 89
    assert result.private_workspace_isolation is True
    assert result.issues == ()

    with pytest.raises(ProfileActivationError, match="private workspace isolation"):
        AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=89,
            private_workspace_isolation=False,
        ).require_exact_profile("terminal-bench-2@tb2.1-r6", task_count=89)
