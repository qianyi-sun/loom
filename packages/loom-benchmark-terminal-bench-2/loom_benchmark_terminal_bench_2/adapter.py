"""TerminalBench2Adapter — implements loom_benchmarks.BenchmarkAdapter
for terminal-bench-core v0.1.1.

`list_instances` walks the cloned upstream `tasks/<slug>/` tree and
yields one BenchmarkInstance per task dir, with the parsed task.yaml in
`.raw` plus a `__source_path` key pointing back to the on-disk dir so
`convert_instance` can copy auxiliary files (Dockerfile, tests/...)
without re-walking.
"""

from __future__ import annotations

import shlex
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import tomli_w
import yaml  # type: ignore[import-untyped]
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
    series = "tool-use"
    upstream_source: UpstreamSource = UPSTREAM_SOURCE
    license_spdx = "Apache-2.0"
    license_url = "https://github.com/laude-institute/terminal-bench/blob/main/LICENSE"
    splits = ("test",)
    # Pinned upstream `terminal-bench-core` v0.1.1 ships exactly 86 official
    # tasks. `src/loom_cli/builtin.py:load_builtin_entries` surfaces this on
    # `loom datasets list` so users see real metadata instead of `-`. Bump
    # when the pin moves; `test_upstream_pin.py` will force co-review.
    task_count = 86

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        tasks_root = self._resolve_tasks_root(source_dir)
        if tasks_root is None:
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
                instance_id=child.name,
                split=split,
                raw=parsed,
                tags={
                    "oracle_eligible": (
                        "true" if self._has_reference_solution(child) else "false"
                    ),
                },
            )

    @staticmethod
    def _has_reference_solution(task_dir: Path) -> bool:
        """A TB-2 task is oracle-eligible iff upstream ships either
        `solution.sh` or `solution.yaml`. Matches `_copy_solution`'s
        precondition — without one of these the adapter cannot stage
        `solution/solve.sh` and the oracle agent fails at runtime."""
        for name in ("solution.sh", "solution.yaml"):
            candidate = task_dir / name
            if candidate.is_file() and not candidate.is_symlink():
                return True
        return False

    @staticmethod
    def _resolve_tasks_root(source_dir: Path) -> Path | None:
        for candidate in (source_dir / TASK_SUBDIR, source_dir / "repo" / TASK_SUBDIR):
            if candidate.is_dir():
                return candidate
        return None

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        from loom_benchmarks.util import sha256_of_dir

        r = instance.raw
        task_id = f"{self.name}/{instance.instance_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        instruction = str(r.get("instruction", "")).rstrip() + "\n"
        (out_dir / "instruction.md").write_text(instruction)

        environment, warnings = self._stage_environment(r, out_dir)
        agent_timeout = float(r.get("max_agent_timeout_sec", 360.0))
        verifier_timeout = float(r.get("max_test_timeout_sec", 60.0))

        task_toml = {
            "schema_version": "1",
            "task": {
                "id": task_id,
                "name": f"{self.display_name} — {instance.instance_id}",
            },
            "environment": environment,
            "agent": {"name": "oracle", "timeout_sec": agent_timeout},
            "verifier": {
                "name": "script",
                "timeout_sec": verifier_timeout,
                "args": {"script_path": "/app/verifier/run.sh"},
            },
            "steps": [{"name": "main", "artifacts": ["tb2-verifier.json"]}],
        }
        (out_dir / "task.toml").write_text(tomli_w.dumps(task_toml))

        self._copy_tests(r, out_dir)
        self._copy_solution(r, out_dir)
        self._write_verifier_shim(out_dir)

        return ConvertedTask(
            task_id=task_id,
            checksum=sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=tuple(warnings),
        )

    def _stage_environment(
        self, raw: dict[str, Any], out_dir: Path,
    ) -> tuple[dict[str, Any], list[str]]:
        warnings: list[str] = []
        src = Path(raw["__source_path"])
        compose_doc = self._compose_doc(src / "docker-compose.yaml")
        services = compose_doc.get("services") if compose_doc else None
        services = services if isinstance(services, dict) else {}
        client_service = services.get("client") if services else None
        client_build = self._resolve_service_build(
            src=src,
            service=client_service if isinstance(client_service, dict) else None,
            default_context=src,
        )
        client_context = out_dir / ".loom-build" / "client"
        self._copy_tree_without_symlinks(client_build.context, client_context)
        rel_client_dockerfile = client_build.dockerfile.relative_to(
            client_build.context,
        )
        self._rewrite_dockerfile_copy_heredocs(
            client_context / rel_client_dockerfile,
        )

        environment: dict[str, Any] = {
            "os": "linux",
            "dockerfile": (
                Path(".loom-build") / "client" / rel_client_dockerfile
            ).as_posix(),
            "docker_build_context": ".loom-build/client",
            "workdir": "/app",
        }
        client_environment = self._parse_environment(
            client_service.get("environment")
            if isinstance(client_service, dict) else None,
        )
        if client_environment:
            environment["environment"] = {
                key: self._expand_tb2_env_value(value)
                for key, value in client_environment.items()
            }
        environment.setdefault("environment", {})["TEST_DIR"] = (
            "/app/environment/tb2-tests"
        )
        if isinstance(client_service, dict):
            dns = self._parse_string_list(client_service.get("dns"))
            if dns:
                environment["dns"] = dns
            extra_hosts = self._parse_extra_hosts(
                client_service.get("extra_hosts"),
            )
            if extra_hosts:
                environment["extra_hosts"] = extra_hosts
            tmpfs = self._parse_string_list(client_service.get("tmpfs"))
            if tmpfs:
                environment["tmpfs"] = tmpfs

        sidecars: list[dict[str, Any]] = []
        for name, service in services.items():
            if name == "client":
                continue
            if not isinstance(service, dict):
                warnings.append(f"ignored malformed compose service {name!r}")
                continue
            sidecar = self._sidecar_from_compose_service(
                src=src,
                out_dir=out_dir,
                name=str(name),
                service=service,
            )
            sidecars.append(sidecar)
        if sidecars:
            environment["sidecars"] = sidecars
        return environment, warnings

    @staticmethod
    def _compose_doc(compose_path: Path) -> dict[str, Any]:
        if not compose_path.is_file():
            return {}
        try:
            doc = yaml.safe_load(compose_path.read_text()) or {}
        except yaml.YAMLError:
            return {}
        return doc if isinstance(doc, dict) else {}

    class _ServiceBuild:
        def __init__(self, *, context: Path, dockerfile: Path) -> None:
            self.context = context
            self.dockerfile = dockerfile

    def _resolve_service_build(
        self,
        *,
        src: Path,
        service: dict[str, Any] | None,
        default_context: Path,
    ) -> _ServiceBuild:
        build = (service or {}).get("build")
        context = default_context
        dockerfile_name = "Dockerfile"
        if isinstance(build, str):
            context = src / build
        elif isinstance(build, dict):
            raw_context = build.get("context") or "."
            raw_dockerfile = build.get("dockerfile") or "Dockerfile"
            if isinstance(raw_context, str):
                context = src / raw_context
            if isinstance(raw_dockerfile, str):
                dockerfile_name = raw_dockerfile
        dockerfile = context / dockerfile_name
        if not dockerfile.is_file():
            fallback = src / "Dockerfile"
            if fallback.is_file():
                context = src
                dockerfile = fallback
        return self._ServiceBuild(context=context, dockerfile=dockerfile)

    def _sidecar_from_compose_service(
        self,
        *,
        src: Path,
        out_dir: Path,
        name: str,
        service: dict[str, Any],
    ) -> dict[str, Any]:
        sidecar: dict[str, Any] = {"name": name}
        image = service.get("image")
        build = service.get("build")
        if isinstance(build, (str, dict)):
            service_build = self._resolve_service_build(
                src=src,
                service=service,
                default_context=src,
            )
            sidecar_context = out_dir / ".loom-build" / "sidecars" / name
            self._copy_tree_without_symlinks(
                service_build.context,
                sidecar_context,
            )
            rel_dockerfile = service_build.dockerfile.relative_to(
                service_build.context,
            )
            self._rewrite_dockerfile_copy_heredocs(
                sidecar_context / rel_dockerfile,
            )
            sidecar["dockerfile"] = (
                Path(".loom-build") / "sidecars" / name / rel_dockerfile
            ).as_posix()
            sidecar["docker_build_context"] = (
                Path(".loom-build") / "sidecars" / name
            ).as_posix()
        elif isinstance(image, str):
            sidecar["docker_image"] = image

        command = service.get("command")
        if isinstance(command, (str, list)):
            sidecar["command"] = command
        environment = self._parse_environment(service.get("environment"))
        if environment:
            sidecar["environment"] = environment
        hostname = service.get("hostname")
        if isinstance(hostname, str):
            sidecar["hostname"] = hostname
        depends_on = self._parse_depends_on(service.get("depends_on"))
        if depends_on:
            sidecar["depends_on"] = depends_on
        healthcheck = self._parse_healthcheck(service.get("healthcheck"))
        if healthcheck:
            sidecar["healthcheck"] = healthcheck
        return sidecar

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

    @staticmethod
    def _parse_environment(raw: object) -> dict[str, str]:
        if isinstance(raw, dict):
            return {str(k): str(v) for k, v in raw.items()}
        if not isinstance(raw, list):
            return {}
        parsed: dict[str, str] = {}
        for item in raw:
            if not isinstance(item, str) or "=" not in item:
                continue
            key, value = item.split("=", 1)
            parsed[key] = value
        return parsed

    @staticmethod
    def _parse_string_list(raw: object) -> list[str]:
        if isinstance(raw, str):
            value = raw.strip()
            return [value] if value else []
        if not isinstance(raw, list):
            return []
        parsed: list[str] = []
        for item in raw:
            if not isinstance(item, str):
                continue
            value = item.strip()
            if value:
                parsed.append(value)
        return parsed

    @staticmethod
    def _parse_extra_hosts(raw: object) -> dict[str, str]:
        if isinstance(raw, dict):
            return {
                str(host).strip(): str(ip).strip()
                for host, ip in raw.items()
                if str(host).strip() and str(ip).strip()
            }
        parsed: dict[str, str] = {}
        for item in TerminalBench2Adapter._parse_string_list(raw):
            separator = "=" if "=" in item else ":"
            if separator not in item:
                continue
            host, ip = item.split(separator, 1)
            host = host.strip()
            ip = ip.strip()
            if host and ip:
                parsed[host] = ip
        return parsed

    @staticmethod
    def _expand_tb2_env_value(value: str) -> str:
        return value.replace("${T_BENCH_TEST_DIR}", "/app/environment/tb2-tests")

    @staticmethod
    def _parse_depends_on(raw: object) -> list[str]:
        if isinstance(raw, dict):
            return [str(k) for k in raw]
        if isinstance(raw, list):
            return [str(v) for v in raw]
        return []

    @staticmethod
    def _parse_healthcheck(raw: object) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None
        test = raw.get("test")
        command: str | None = None
        if isinstance(test, str):
            command = test
        elif isinstance(test, list) and test:
            mode = test[0]
            parts = [str(p) for p in test[1:]]
            if mode == "CMD-SHELL" and parts:
                command = parts[0]
            elif mode == "CMD" and parts:
                command = shlex.join(parts)
        if not command:
            return None
        healthcheck: dict[str, Any] = {"command": command}
        interval = TerminalBench2Adapter._parse_duration_sec(raw.get("interval"))
        timeout = TerminalBench2Adapter._parse_duration_sec(raw.get("timeout"))
        start_period = TerminalBench2Adapter._parse_duration_sec(
            raw.get("start_period"),
        )
        if interval is not None:
            healthcheck["interval_sec"] = interval
        if timeout is not None:
            healthcheck["timeout_sec"] = timeout
        if start_period is not None:
            healthcheck["start_period_sec"] = start_period
        retries = raw.get("retries")
        if isinstance(retries, int):
            healthcheck["retries"] = retries
        return healthcheck

    @staticmethod
    def _parse_duration_sec(raw: object) -> float | None:
        if isinstance(raw, (int, float)):
            return float(raw)
        if not isinstance(raw, str) or not raw:
            return None
        unit = raw[-1]
        try:
            value = float(raw[:-1])
        except ValueError:
            return None
        if unit == "s":
            return value
        if unit == "m":
            return value * 60.0
        if unit == "h":
            return value * 3600.0
        return None

    @staticmethod
    def _copy_tree_without_symlinks(src: Path, dst: Path) -> None:
        import shutil

        if not src.is_dir():
            return
        for child in src.rglob("*"):
            if child.is_dir() or child.is_symlink():
                continue
            target = dst / child.relative_to(src)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(child, target)

    @staticmethod
    def _rewrite_dockerfile_copy_heredocs(dockerfile: Path) -> None:
        """Materialize BuildKit COPY heredocs for docker-py legacy builds."""
        if not dockerfile.is_file():
            return
        lines = dockerfile.read_text().splitlines(keepends=True)
        rewritten: list[str] = []
        heredoc_dir = dockerfile.parent / ".loom-heredocs"
        changed = False
        index = 0
        i = 0
        while i < len(lines):
            line = lines[i]
            parsed = TerminalBench2Adapter._parse_copy_heredoc_start(line)
            if parsed is None:
                rewritten.append(line)
                i += 1
                continue

            marker, destination, flags = parsed
            content: list[str] = []
            i += 1
            while i < len(lines) and lines[i].strip() != marker:
                content.append(lines[i])
                i += 1
            if i >= len(lines):
                rewritten.append(line)
                rewritten.extend(content)
                break

            index += 1
            changed = True
            heredoc_dir.mkdir(parents=True, exist_ok=True)
            filename = (
                f"{index:03d}-"
                f"{TerminalBench2Adapter._safe_heredoc_filename(destination)}"
            )
            (heredoc_dir / filename).write_text("".join(content))
            replacement_parts = [
                "COPY",
                *flags,
                f".loom-heredocs/{filename}",
                destination,
            ]
            rewritten.append(" ".join(replacement_parts) + "\n")
            i += 1

        if changed:
            dockerfile.write_text("".join(rewritten))

    @staticmethod
    def _parse_copy_heredoc_start(
        line: str,
    ) -> tuple[str, str, list[str]] | None:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            return None
        parts = stripped.split()
        if len(parts) < 3 or parts[0].upper() != "COPY":
            return None
        marker_index = next(
            (i for i, part in enumerate(parts[1:], start=1) if part.startswith("<<")),
            None,
        )
        if marker_index is None or marker_index + 1 >= len(parts):
            return None
        marker = parts[marker_index][2:]
        if marker.startswith("-"):
            marker = marker[1:]
        marker = marker.strip("\"'")
        if not marker:
            return None
        flags = parts[1:marker_index]
        destination = parts[marker_index + 1]
        return marker, destination, flags

    @staticmethod
    def _safe_heredoc_filename(destination: str) -> str:
        safe = "".join(
            char if char.isalnum() else "-"
            for char in destination.strip("/").lower()
        ).strip("-")
        return safe or "payload"

    def _copy_tests(self, raw: dict[str, Any], out_dir: Path) -> None:
        """Stage TB-2's tests/ subtree + run-tests.sh under
        environment/tb2-tests/ so the shared workspace materializer uploads
        it into the sandbox at /app/environment/tb2-tests, which is the
        shim default TEST_DIR. Missing pieces are silently skipped — some
        TB-2 tasks have no auxiliary test scripts.

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

    def _copy_solution(self, raw: dict[str, Any], out_dir: Path) -> None:
        """Stage TB-2's reference solution when upstream ships one.

        Loom's generic oracle treats a non-zero `solve.sh` exit as an
        agent failure. TB-2's oracle agent instead sends reference commands
        into a terminal and lets the verifier decide the reward. The wrapper
        here preserves that benchmark contract by best-effort executing the
        reference solution and always handing control to the verifier.
        """
        import shutil

        src_dir = Path(raw["__source_path"])
        sh_src = src_dir / "solution.sh"
        yaml_src = src_dir / "solution.yaml"
        if (
            (not sh_src.is_file() or sh_src.is_symlink())
            and (not yaml_src.is_file() or yaml_src.is_symlink())
        ):
            return

        solution_dir = out_dir / "solution"
        solution_dir.mkdir(parents=True, exist_ok=True)

        if sh_src.is_file() and not sh_src.is_symlink():
            reference = solution_dir / "reference.sh"
            shutil.copy2(sh_src, reference)
            reference.chmod(reference.stat().st_mode | 0o755)
            body = self._render_reference_shell_wrapper()
        else:
            shutil.copy2(yaml_src, solution_dir / "reference.yaml")
            body = self._render_solution_yaml_script(yaml_src)

        dst = solution_dir / "solve.sh"
        dst.write_text(body)
        dst.chmod(dst.stat().st_mode | 0o755)

    @staticmethod
    def _render_reference_shell_wrapper() -> str:
        return "\n".join((
            "#!/usr/bin/env bash",
            "set +e",
            'task_root="${LOOM_TASK_ROOT:-$(pwd)}"',
            'bash "$task_root/solution/reference.sh"',
            "exit 0",
            "",
        ))

    @staticmethod
    def _render_solution_yaml_script(solution_yaml: Path) -> str:
        data = yaml.safe_load(solution_yaml.read_text()) or []
        lines = [
            "#!/usr/bin/env bash",
            "set +e",
            "# Generated from Terminal-Bench solution.yaml.",
            "# Reference commands are best-effort; verifier output is the score.",
            "",
        ]
        if not isinstance(data, list):
            data = []
        index = 0
        while index < len(data):
            raw_command = data[index]
            index += 1
            if not isinstance(raw_command, dict):
                continue
            command = raw_command.get("command")
            if not isinstance(command, str) or not command.strip():
                continue
            if TerminalBench2Adapter._is_python_repl_command(command):
                repl_lines: list[str] = []
                while index < len(data):
                    next_command = data[index]
                    index += 1
                    if not isinstance(next_command, dict):
                        continue
                    repl_command = next_command.get("command")
                    if (
                        not isinstance(repl_command, str)
                        or not repl_command.strip()
                    ):
                        continue
                    if TerminalBench2Adapter._is_python_repl_exit(
                        repl_command,
                    ):
                        break
                    repl_lines.append(repl_command.rstrip())
                lines.append(f"# command {index}")
                lines.append(f"{command.strip()} <<'PYTHON_REPL'")
                lines.extend(repl_lines)
                lines.append("PYTHON_REPL")
                lines.append("")
                continue
            lines.append(f"# command {index}")
            lines.append(command.rstrip())
            min_timeout = TerminalBench2Adapter._coerce_sleep_seconds(
                raw_command.get("min_timeout_sec"),
            )
            if raw_command.get("block") is not True and min_timeout > 0:
                lines.append(f"sleep {min_timeout:g}")
            lines.append("")
        lines.append("exit 0")
        lines.append("")
        return "\n".join(lines)

    @staticmethod
    def _is_python_repl_command(command: str) -> bool:
        parts = shlex.split(command.strip())
        if not parts:
            return False
        executable = Path(parts[0]).name
        if executable not in {"python", "python3"}:
            return False
        return len(parts) == 1 or parts[1:] == ["-i"]

    @staticmethod
    def _is_python_repl_exit(command: str) -> bool:
        return command.strip() in {"quit()", "exit()"}

    @staticmethod
    def _coerce_sleep_seconds(value: object) -> float:
        if isinstance(value, bool):
            return 0.0
        if isinstance(value, (int, float)):
            return max(float(value), 0.0)
        return 0.0

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
