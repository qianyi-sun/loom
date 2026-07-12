"""Harbor-native Terminal-Bench 2.1 revision-6 adapter."""

from __future__ import annotations

import shutil
import tomllib
from collections.abc import Iterator
from importlib.resources import files
from pathlib import Path

import tomli_w
from loom_benchmarks.base import BenchmarkInstance, ConvertedTask, UpstreamSource
from loom_benchmarks.util import sha256_of_dir

from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml
from loom.trial.workspace import TB21_AGENT_WORKSPACE_POLICY
from loom_benchmark_terminal_bench_2 import upstream


class TerminalBench2Adapter:
    """Convert the locked Harbor-native task directories without legacy fallbacks."""

    name = "terminal-bench-2@tb2.1-r6"
    display_name = "Terminal-Bench 2.1 (Harbor rev 6)"
    series = "tool-use"
    upstream_source: UpstreamSource = upstream.TB21_HARBOR_SOURCE
    license_spdx = "Apache-2.0"
    license_url = "https://github.com/harbor-framework/terminal-bench-2-1/blob/dde3cd95b80ff25af5abd99a80b6513a018ad3b4/LICENSE"
    splits = ("test",)
    task_count = upstream.TB21_TASK_COUNT

    def profile_provenance(self) -> dict[str, object]:
        """Immutable profile identity persisted with the published manifest."""
        lock = upstream.load_tb21_lock()
        return {
            "physical_profile": self.name,
            "hub_dataset": lock.dataset,
            "hub_revision": lock.revision,
            "hub_metadata_version": lock.hub_metadata_version,
            "source_reference_snapshot": lock.source_revision,
            "source_reference_divergences": lock.source_manifest_divergences,
            "verifier_identity": "tb21-native-reward-file-v1",
            "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
        }

    def task_source_provenance(
        self,
        *,
        instance: BenchmarkInstance,
        bundle_dir: Path,
        task_config: dict[str, object],
        checksum: str,
    ) -> dict[str, object]:
        """Evidence tying one Loom bundle to its rev-6 source package."""
        del bundle_dir, checksum  # The publish layer records the bundle checksum.
        source_name = f"terminal-bench/{instance.instance_id}"
        lock = upstream.load_tb21_lock()
        environment = task_config.get("environment")
        env = environment if isinstance(environment, dict) else {}
        return {
            "harbor_package_digest": lock.digest_for(source_name),
            "harbor_metadata_version": lock.hub_metadata_version,
            "source_reference": lock.source_reference_for(source_name),
            "verifier_identity": "tb21-native-reward-file-v1",
            "image_provenance": {
                "docker_image": env.get("docker_image"),
                "dockerfile": env.get("dockerfile"),
                "docker_build_context": env.get("docker_build_context"),
                "cpu_arch": env.get("cpu_arch"),
            },
            "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
        }

    def list_instances(
        self,
        *,
        source_dir: Path,
        split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Yield precisely the native directories admitted by the Task-3 lock."""
        upstream.verify_tb21_materialization(source_dir)
        lock = upstream.load_tb21_lock()
        for task in lock.tasks:
            instance_id = task.name.rsplit("/", 1)[-1]
            task_dir = source_dir / "tasks" / instance_id
            self._require_native_layout(task_dir, task_name=task.name)
            yield BenchmarkInstance(
                instance_id=instance_id,
                split=split,
                raw={"source_path": str(task_dir)},
                tags={
                    "oracle_eligible": (
                        "true" if self._has_reference_solution(task_dir) else "false"
                    ),
                },
            )

    def convert_instance(
        self,
        instance: BenchmarkInstance,
        *,
        out_dir: Path,
    ) -> ConvertedTask:
        """Copy one verified native directory and re-stamp only Loom's task id."""
        source_path = instance.raw.get("source_path")
        if not isinstance(source_path, str) or not source_path:
            raise upstream.TB21LockError(
                "native TB2.1 instance is missing its verified source_path",
            )
        source_dir = Path(source_path)
        materialization_root = self._materialization_root(source_dir)
        upstream.verify_tb21_materialization(materialization_root)
        source_name = f"terminal-bench/{instance.instance_id}"
        lock = upstream.load_tb21_lock()
        if source_name not in lock.package_digests:
            raise upstream.TB21LockError(
                f"native TB2.1 instance is not admitted by the lock: {source_name!r}",
            )
        expected_source_dir = materialization_root / "tasks" / instance.instance_id
        if source_dir != expected_source_dir:
            raise upstream.TB21LockError(
                "native TB2.1 instance source_path does not match its locked task directory",
            )
        self._require_native_layout(
            source_dir,
            task_name=source_name,
        )
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        source_task_toml = source_dir / "task.toml"
        upstream_task_toml = out_dir / "upstream-task.toml"
        shutil.copy2(source_task_toml, upstream_task_toml)
        try:
            native_config = tomllib.loads(source_task_toml.read_text())
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise upstream.TB21LockError(
                f"native TB2.1 task TOML is invalid: {source_task_toml}",
            ) from exc
        normalized = normalize_terminal_bench_task_toml(native_config)
        task = normalized.get("task")
        if not isinstance(task, dict):
            raise upstream.TB21LockError(
                f"native TB2.1 task has no normalizable [task] section: {source_task_toml}",
            )
        task["id"] = task_id
        (out_dir / "task.toml").write_text(tomli_w.dumps(normalized))

        shutil.copy2(source_dir / "instruction.md", out_dir / "instruction.md")
        for directory in ("environment", "tests", "solution"):
            shutil.copytree(
                source_dir / directory,
                out_dir / directory,
                copy_function=shutil.copy2,
                symlinks=True,
            )
        self._write_verifier_shim(out_dir)

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )

    @staticmethod
    def _has_reference_solution(task_dir: Path) -> bool:
        solution = task_dir / "solution" / "solve.sh"
        return solution.is_file() and not solution.is_symlink() and solution.stat().st_size > 0

    @staticmethod
    def _materialization_root(task_dir: Path) -> Path:
        if task_dir.parent.name != "tasks":
            raise upstream.TB21LockError(
                "native TB2.1 source_path must be a direct tasks/<task> directory",
            )
        return task_dir.parent.parent

    @staticmethod
    def _require_native_layout(task_dir: Path, *, task_name: str) -> None:
        required = (
            "task.toml",
            "instruction.md",
            "environment",
            "tests/test.sh",
            "solution/solve.sh",
        )
        missing = [relative for relative in required if not (task_dir / relative).exists()]
        if missing:
            raise upstream.TB21LockError(
                f"native TB2.1 task {task_name!r} lacks required paths: {missing}",
            )

    @staticmethod
    def _write_verifier_shim(out_dir: Path) -> None:
        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        run_sh = verifier_dir / "run.sh"
        run_sh.write_bytes(
            files("loom_benchmark_terminal_bench_2").joinpath("verifier_shim.sh").read_bytes(),
        )
        run_sh.chmod(0o755)
