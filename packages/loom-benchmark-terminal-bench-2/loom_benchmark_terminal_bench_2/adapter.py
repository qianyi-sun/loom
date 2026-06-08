"""TerminalBench2Adapter — implements loom_benchmarks.BenchmarkAdapter
for terminal-bench-core v0.1.1.

Spec: docs/specs/2026-06-08-loom-harbor-parity-arc-design.md
      §3 Plan 25 deliverable 1.
Probe: docs/notes/2026-06-08-tb2-upstream-probe.md.

`list_instances` walks the cloned upstream `tasks/<slug>/` tree and
yields one BenchmarkInstance per task dir, with the parsed task.yaml in
`.raw` plus a `__source_path` key pointing back to the on-disk dir so
`convert_instance` can copy auxiliary files (Dockerfile, tests/...)
without re-walking. `convert_instance` is implemented in Tasks 5-7.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml
from loom_benchmarks.base import (
    BenchmarkInstance,
    ConvertedTask,
    UpstreamSource,
)

from loom_benchmark_terminal_bench_2.upstream import (
    TASK_SUBDIR,
    UPSTREAM_SOURCE,
)


class TerminalBench2Adapter:
    name = "terminal-bench-2"
    display_name = "Terminal-Bench-2.0 (core v0.1.1)"
    upstream_source: UpstreamSource = UPSTREAM_SOURCE
    license_spdx = "Apache-2.0"
    license_url = "https://github.com/laude-institute/terminal-bench/blob/main/LICENSE"
    splits = ("test",)

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        tasks_root = source_dir / TASK_SUBDIR
        if not tasks_root.is_dir():
            return
        for child in sorted(tasks_root.iterdir()):
            if not child.is_dir():
                continue
            task_yaml = child / "task.yaml"
            if not task_yaml.is_file():
                continue
            parsed: dict[str, Any] = yaml.safe_load(task_yaml.read_text()) or {}
            parsed["__source_path"] = str(child)
            yield BenchmarkInstance(
                instance_id=child.name, split=split, raw=parsed,
            )

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        from textwrap import dedent

        from loom_benchmarks.util import sha256_of_dir, toml_string

        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        instruction = str(r.get("instruction", "")).rstrip() + "\n"
        (out_dir / "instruction.md").write_text(instruction)

        docker_image, warnings = self._resolve_docker_image(r)
        agent_timeout = float(r.get("max_agent_timeout_sec", 360.0))
        verifier_timeout = float(r.get("max_test_timeout_sec", 60.0))

        toml_id = toml_string(task_id)
        toml_name = toml_string(f"{self.display_name} — {instance.instance_id}")
        toml_image = toml_string(docker_image)
        (out_dir / "task.toml").write_text(dedent(f"""
            schema_version = "1"

            [task]
            id = {toml_id}
            name = {toml_name}

            [environment]
            os = "linux"
            docker_image = {toml_image}

            [agent]
            name = "oracle"
            timeout_sec = {agent_timeout}

            [verifier]
            name = "script"
            timeout_sec = {verifier_timeout}

            [verifier.args]
            script_path = "/loom/verifier/run.sh"

            [[steps]]
            name = "main"
            artifacts = ["tb2-verifier.json"]
        """).strip() + "\n")

        self._copy_tests(r, out_dir)
        self._write_verifier_shim(out_dir)

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=tuple(warnings),
        )

    def _resolve_docker_image(
        self, raw: dict[str, Any],
    ) -> tuple[str, list[str]]:
        """Return (image_ref, warnings).

        TB-2 tasks may declare a multi-service docker-compose.yaml.
        Loom's environment model boots a single image per trial; for
        compose topologies with more than the `client` service we warn
        and use the client Dockerfile alone. Pure single-image tasks
        pass through cleanly.
        """
        warnings: list[str] = []
        src = Path(raw["__source_path"])
        compose = src / "docker-compose.yaml"
        if compose.is_file():
            services = self._compose_service_names(compose)
            non_client = [s for s in services if s != "client"]
            if non_client:
                warnings.append(
                    f"docker-compose declares non-client services "
                    f"{sorted(non_client)!r}; running single-image client "
                    "only — some tasks may not grade faithfully",
                )

        dockerfile = src / "Dockerfile"
        if dockerfile.is_file():
            for line in dockerfile.read_text().splitlines():
                stripped = line.strip()
                if stripped.upper().startswith("FROM "):
                    return stripped[len("FROM "):].strip(), warnings
        warnings.append(
            "no Dockerfile FROM found; defaulted to TB-2 base image",
        )
        return "ghcr.io/laude-institute/t-bench/python-3-13:latest", warnings

    @staticmethod
    def _compose_service_names(compose_path: Path) -> list[str]:
        """Yield the top-level service names from a docker-compose.yaml.
        Tolerates malformed YAML by returning an empty list — the caller
        treats absence-of-services the same as absence-of-file.
        """
        try:
            doc = yaml.safe_load(compose_path.read_text()) or {}
        except yaml.YAMLError:
            return []
        services = doc.get("services") if isinstance(doc, dict) else None
        if not isinstance(services, dict):
            return []
        return list(services.keys())

    def _copy_tests(self, raw: dict[str, Any], out_dir: Path) -> None:
        """Stage TB-2's tests/ subtree + run-tests.sh under
        environment/tb2-tests/ so the prepare phase can mount it into
        the container at /tb2-tests (the path the shim points TEST_DIR
        at). Missing pieces are silently skipped — some TB-2 tasks have
        no auxiliary test scripts.

        Symlinks are skipped to prevent a malicious upstream task at a
        future SHA from smuggling host file contents (e.g.
        `tests/passwd -> /etc/passwd`) into the converted task via
        shutil.copy2's default follow-symlinks behavior.
        """
        import shutil

        src = Path(raw["__source_path"])
        staged = out_dir / "environment" / "tb2-tests"
        staged.mkdir(parents=True, exist_ok=True)

        tests_src = src / "tests"
        if tests_src.is_dir():
            for child in tests_src.rglob("*"):
                if child.is_dir() or child.is_symlink():
                    continue
                dst = staged / child.relative_to(tests_src)
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(child, dst)

        run_tests = src / "run-tests.sh"
        if run_tests.is_file() and not run_tests.is_symlink():
            shutil.copy2(run_tests, staged / "run-tests.sh")

    def _write_verifier_shim(self, out_dir: Path) -> None:
        """Install the bundled verifier_shim.sh as verifier/run.sh —
        the path our generated task.toml points ScriptVerifier at."""
        from importlib.resources import files

        verifier_dir = out_dir / "verifier"
        verifier_dir.mkdir(parents=True, exist_ok=True)
        shim_src = (
            files("loom_benchmark_terminal_bench_2")
            .joinpath("verifier_shim.sh")
            .read_bytes()
        )
        run_sh = verifier_dir / "run.sh"
        run_sh.write_bytes(shim_src)
        run_sh.chmod(0o755)
