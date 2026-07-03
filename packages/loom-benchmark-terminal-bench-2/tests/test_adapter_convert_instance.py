"""convert_instance writes Loom's canonical task layout for a TB-2 task."""

from __future__ import annotations

import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest
from loom_benchmark_terminal_bench_2.adapter import TerminalBench2Adapter
from loom_benchmarks.base import BenchmarkInstance

from loom.models.task import TaskConfig
from loom.models.verifier import VerifierResult


@pytest.fixture
def hello_world_instance(
    fixtures_dir: Path, tmp_path: Path,
) -> BenchmarkInstance:
    """Stage the vendored fixture as a source-tree task so the adapter
    can read its on-disk auxiliaries through `__source_path`."""
    staged = tmp_path / "tasks" / "hello-world"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    adapter = TerminalBench2Adapter()
    (only,) = list(adapter.list_instances(source_dir=tmp_path, split="test"))
    return only


def test_convert_writes_instruction_md(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    text = (out / "instruction.md").read_text()
    assert text.startswith("Create a file called hello.txt")
    assert text.endswith("\n")


def test_convert_writes_task_toml_with_required_fields(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.task.id == "terminal-bench-2/hello-world"
    assert cfg.task.name.endswith("hello-world")
    assert cfg.environment.os == "linux"
    assert cfg.environment.docker_image is None
    assert cfg.environment.dockerfile.as_posix() == (
        ".loom-build/client/Dockerfile"
    )
    assert cfg.environment.docker_build_context.as_posix() == (
        ".loom-build/client"
    )
    assert str(cfg.environment.workdir) == "/app"
    assert cfg.verifier.name == "script"
    assert cfg.verifier.args["script_path"] == "/app/verifier/run.sh"
    assert cfg.agent.name == "oracle"
    # Fixture Dockerfile does NOT use a runtime-fallback base, so the
    # adapter must not fabricate a cpu_arch override (#342).
    assert cfg.environment.cpu_arch == "x86_64"


def test_convert_marks_cpu_arch_any_for_runtime_fallback_base(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """Terminal-Bench tasks whose client Dockerfile uses the amd64-only
    ``mictern2/terminus2-full:latest`` base can safely run on GB10
    arm64 workers because the worker materializes an arm64 substitute
    at trial start. The adapter emits ``cpu_arch = any`` so the
    scheduler routes those trials to arm64 pools too. #342."""
    staged = tmp_path / "tasks" / "terminus2-hello"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "Dockerfile").write_text(
        "FROM mictern2/terminus2-full:latest\n"
        "RUN echo ready\n",
    )
    adapter = TerminalBench2Adapter()
    (only,) = list(adapter.list_instances(source_dir=tmp_path, split="test"))

    out = tmp_path / "out"
    adapter.convert_instance(only, out_dir=out)

    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.environment.cpu_arch == "any"


def test_convert_stages_build_context_without_exposing_it_to_workspace(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "protected-copy"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "Dockerfile").write_text(
        "FROM ghcr.io/laude-institute/t-bench/python-3-13:latest\n"
        "COPY ./protected/answer.txt /protected/answer.txt\n"
    )
    (staged / "protected").mkdir()
    (staged / "protected" / "answer.txt").write_text("hidden\n")
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    assert (out / ".loom-build" / "client" / "Dockerfile").exists()
    assert (
        out / ".loom-build" / "client" / "protected" / "answer.txt"
    ).read_text() == "hidden\n"
    assert not (out / "protected").exists()


def test_convert_rewrites_dockerfile_heredoc_copy_for_legacy_builder(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "heredoc-copy"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "Dockerfile").write_text(
        "FROM ghcr.io/laude-institute/t-bench/python-3-13:latest\n"
        "WORKDIR /app\n"
        "COPY <<EOT /app/data.json\n"
        '{"answer": 42}\n'
        "EOT\n"
        "RUN test -f /app/data.json\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    staged_context = out / ".loom-build" / "client"
    dockerfile = (staged_context / "Dockerfile").read_text()
    assert "COPY <<EOT" not in dockerfile
    assert "COPY .loom-heredocs/" in dockerfile
    assert "/app/data.json" in dockerfile
    heredocs = list((staged_context / ".loom-heredocs").iterdir())
    assert len(heredocs) == 1
    assert heredocs[0].read_text() == '{"answer": 42}\n'


def test_convert_rewrites_sidecar_dockerfile_heredoc_copy(
    tmp_path: Path,
) -> None:
    """`src/loom_worker/task_image.py:185` uses `docker-py`'s
    `client.images.build` — the legacy builder — for sidecar images
    just as for the client image. A sidecar Dockerfile with a BuildKit
    `COPY <<EOF` heredoc would fail at build time on the public-beta
    worker; the adapter must rewrite sidecar Dockerfiles into ordinary
    `COPY` form too. Existing tests only cover the client Dockerfile,
    so this case is the explicit guard for the sidecar build path."""
    staged = tmp_path / "tasks" / "heredoc-sidecar"
    (staged / "client").mkdir(parents=True)
    (staged / "api").mkdir()
    (staged / "client" / "Dockerfile").write_text("FROM python:3.11-slim\n")
    (staged / "api" / "Dockerfile").write_text(
        "FROM python:3.11-slim\n"
        "WORKDIR /srv\n"
        "COPY <<EOT /srv/config.json\n"
        '{"mode": "sidecar"}\n'
        "EOT\n",
    )
    (staged / "task.yaml").write_text(
        "instruction: hit the api\n"
        "parser_name: pytest\n"
        "max_agent_timeout_sec: 10\n"
        "max_test_timeout_sec: 10\n",
    )
    (staged / "docker-compose.yaml").write_text(
        "services:\n"
        "  client:\n"
        "    build:\n"
        "      context: client\n"
        "      dockerfile: Dockerfile\n"
        "  api:\n"
        "    build: ./api\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    sidecar_context = out / ".loom-build" / "sidecars" / "api"
    sidecar_dockerfile = (sidecar_context / "Dockerfile").read_text()
    assert "COPY <<EOT" not in sidecar_dockerfile
    assert "COPY .loom-heredocs/" in sidecar_dockerfile
    heredocs = list((sidecar_context / ".loom-heredocs").iterdir())
    assert len(heredocs) == 1
    assert heredocs[0].read_text() == '{"mode": "sidecar"}\n'


def test_no_materialized_dockerfile_carries_buildkit_heredoc(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """Contract test for the public-beta image-build path: every
    Dockerfile materialized under `.loom-build/` must be consumable by
    `client.images.build` (docker-py legacy builder). Any `COPY <<` or
    `RUN <<` BuildKit heredoc that slips through `_rewrite_dockerfile_*`
    would cause production task-image builds to fail with parse errors,
    so we assert the whole materialized tree is heredoc-free.

    This guards against future refactors that add a new build context
    surface (additional sidecar, init-container, etc.) without wiring
    the rewriter through."""
    staged = tmp_path / "tasks" / "heredoc-everywhere"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "Dockerfile").write_text(
        "FROM ghcr.io/laude-institute/t-bench/python-3-13:latest\n"
        "WORKDIR /app\n"
        "COPY <<EOT /app/answer.txt\n"
        "42\n"
        "EOT\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    build_root = out / ".loom-build"
    assert build_root.is_dir()
    offenders: list[str] = []
    for path in build_root.rglob("Dockerfile*"):
        if not path.is_file():
            continue
        text = path.read_text()
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split()
            if parts[0].upper() not in {"COPY", "RUN", "ADD"}:
                continue
            if any(part.startswith("<<") for part in parts[1:]):
                offenders.append(
                    f"{path.relative_to(out)}: {stripped}",
                )
    assert offenders == [], (
        "materialized Dockerfile retains BuildKit heredoc syntax "
        "incompatible with docker-py legacy builder: "
        + ", ".join(offenders)
    )


def test_convert_task_toml_id_escapes_special_chars(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """toml_string-escaped instance_ids cannot break the TOML document."""
    inst = BenchmarkInstance(
        instance_id='weird"name',
        split="test",
        raw={
            "instruction": "hi",
            "parser_name": "pytest",
            "max_agent_timeout_sec": 1.0,
            "max_test_timeout_sec": 1.0,
            "__source_path": str(fixtures_dir / "tb2-task-hello-world"),
        },
    )
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(inst, out_dir=out)
    parsed = tomllib.loads((out / "task.toml").read_text())
    assert parsed["task"]["id"] == 'terminal-bench-2/weird"name'


def test_convert_copies_tb2_test_tree(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    tb2 = out / "environment" / "tb2-tests"
    assert (tb2 / "test_outputs.py").read_text().startswith("from pathlib")
    assert (tb2 / "run-uv-pytest.sh").exists()
    assert (tb2 / "setup-uv-pytest.sh").exists()
    assert (tb2 / "run-tests.sh").read_text().startswith("#!/bin/bash")


def test_convert_copies_reference_solution_for_oracle_smoke(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    solve = out / "solution" / "solve.sh"
    assert solve.exists()
    assert solve.stat().st_mode & 0o111
    assert "reference.sh" in solve.read_text()
    assert "Hello, world!" in (out / "solution" / "reference.sh").read_text()


def test_convert_wraps_reference_solution_as_best_effort(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "nonzero-solution"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "solution.sh").write_text(
        "#!/usr/bin/env bash\n"
        "echo before-exit > reference-output.txt\n"
        "exit 7\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)
    completed = subprocess.run(
        ["bash", str(out / "solution" / "solve.sh")],
        cwd=out,
        check=True,
    )

    assert completed.returncode == 0
    assert (out / "reference-output.txt").read_text() == "before-exit\n"


def test_solve_sh_runs_reference_from_task_root_even_when_invoked_from_solution_dir(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """Regression for the cwd-mismatch bug observed against the
    public-beta cluster: `OracleAgent` invokes
    `<workdir>/solution/solve.sh` with cwd=`<workdir>/solution`, but the
    TB-2 verifier (`run-tests.sh`) checks files relative to the task
    workdir itself (e.g. hello-world expects `hello.txt` at the task
    root, not in `solution/`). The earlier wrapper anchored at
    `$(pwd)/solution/...` and produced the broken path
    `solution/solution/reference.sh`; `exit 0` masked the bash error
    and trials shipped as state=succeeded with reward=0.

    This test reproduces oracle's exact invocation pattern (cwd =
    `<out>/solution`) and asserts the reference solution's output lands
    at the TASK ROOT (`<out>/hello.txt`), not under
    `solution/solution/`.
    """
    staged = tmp_path / "tasks" / "cwd-regression"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    completed = subprocess.run(
        ["bash", str(out / "solution" / "solve.sh")],
        cwd=str(out / "solution"),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert (out / "hello.txt").exists(), (
        f"reference solution did not write hello.txt at the task root; "
        f"stderr={completed.stderr!r} cwd_listing="
        f"{sorted(p.name for p in out.iterdir())}"
    )
    assert (out / "hello.txt").read_text() == "Hello, world!\n"
    # The pre-fix bug would have created `solution/solution/`, so guard
    # against the broken path coming back.
    assert not (out / "solution" / "solution").exists()
    assert not (out / "solution" / "hello.txt").exists()


def test_solve_yaml_runs_commands_from_task_root_when_invoked_from_solution_dir(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """Same regression as the .sh wrapper, for the solution.yaml path.
    OracleAgent invokes solve.sh from `solution/`; the yaml-generated
    commands must execute with cwd=task_root so files written with
    relative paths land where the verifier expects them."""
    staged = tmp_path / "tasks" / "yaml-cwd-regression"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "solution.sh").unlink()
    (staged / "solution.yaml").write_text(
        "- command: \"printf 'yaml\\\\n' > marker.txt\"\n"
        "  block: true\n"
        "  append_enter: true\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    completed = subprocess.run(
        ["bash", str(out / "solution" / "solve.sh")],
        cwd=str(out / "solution"),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert (out / "marker.txt").read_text() == "yaml\n"
    assert not (out / "solution" / "marker.txt").exists()


def test_convert_renders_solution_yaml_for_oracle_smoke(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "yaml-solution"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "solution.sh").unlink()
    (staged / "solution.yaml").write_text(
        "- command: \"printf 'alpha\\\\n' > yaml-output.txt\"\n"
        "  block: true\n"
        "  append_enter: true\n"
        "- command: \"printf 'beta\\\\n' >> yaml-output.txt\"\n"
        "  min_timeout_sec: 0.01\n"
        "  block: false\n"
        "  append_enter: true\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)
    completed = subprocess.run(
        ["bash", str(out / "solution" / "solve.sh")],
        cwd=out,
        check=True,
    )

    assert completed.returncode == 0
    assert (out / "yaml-output.txt").read_text() == "alpha\nbeta\n"


def test_convert_renders_python_repl_solution_yaml_for_oracle_smoke(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "python-repl-solution"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "solution.sh").unlink()
    (staged / "solution.yaml").write_text(
        "- command: python\n"
        "  block: false\n"
        "  min_timeout_sec: 0.01\n"
        "  append_enter: true\n"
        "- command: from pathlib import Path\n"
        "  block: false\n"
        "  min_timeout_sec: 0.01\n"
        "  append_enter: true\n"
        "- command: Path('yaml-repl-output.txt').write_text('ok' + chr(10))\n"
        "  block: false\n"
        "  min_timeout_sec: 0.01\n"
        "  append_enter: true\n"
        "- command: quit()\n"
        "  block: false\n"
        "  min_timeout_sec: 0.01\n"
        "  append_enter: true\n",
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)
    completed = subprocess.run(
        ["bash", str(out / "solution" / "solve.sh")],
        cwd=out,
        check=True,
    )

    assert completed.returncode == 0
    assert (out / "yaml-repl-output.txt").read_text() == "ok\n"


def test_convert_writes_verifier_shim(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    shim = out / "verifier" / "run.sh"
    assert shim.exists()
    assert shim.stat().st_mode & 0o111  # executable
    body = shim.read_text()
    assert "$LOOM_VERIFIER_OUTPUT" in body
    assert 'TEST_DIR="${TEST_DIR:-/app/environment/tb2-tests}"' in body
    assert "run-tests.sh" in body
    assert '"rewards":' in body


def test_generated_verifier_shim_emits_loom_verifier_result(
    hello_world_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        hello_world_instance, out_dir=out,
    )
    output = tmp_path / "verifier-output.json"
    test_dir = tmp_path / "tb2-tests"
    test_dir.mkdir()
    (test_dir / "setup.sh").write_text("export TB2_SHIM_OK=1\n")
    (test_dir / "run-tests.sh").write_text(
        "#!/bin/bash\nsource \"$TEST_DIR/setup.sh\"\n"
        "test \"$TB2_SHIM_OK\" = 1\n"
    )

    completed = subprocess.run(
        ["sh", str(out / "verifier" / "run.sh")],
        env={
            "LOOM_VERIFIER_OUTPUT": str(output),
            "TEST_DIR": str(test_dir),
        },
        check=True,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0
    result = VerifierResult.model_validate_json(output.read_text())
    assert result.rewards == {"resolved": 1.0}
    assert result.checks[0].name == "tb2_run_tests"
    assert result.checks[0].passed is True
    assert result.checks[0].message == "exit=0"


@pytest.fixture
def multiservice_instance(
    fixtures_dir: Path, tmp_path: Path,
) -> BenchmarkInstance:
    staged = tmp_path / "tasks" / "ssh-flag"
    shutil.copytree(fixtures_dir / "tb2-task-multiservice", staged)
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    return only


def test_multiservice_stages_sidecar_services_without_warning(
    multiservice_instance: BenchmarkInstance, tmp_path: Path,
) -> None:
    out = tmp_path / "out"
    result = TerminalBench2Adapter().convert_instance(
        multiservice_instance, out_dir=out,
    )
    assert result.warnings == ()
    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.environment.dockerfile.as_posix() == (
        ".loom-build/client/Dockerfile"
    )
    assert cfg.environment.docker_build_context.as_posix() == (
        ".loom-build/client"
    )
    assert len(cfg.environment.sidecars) == 1
    sidecar = cfg.environment.sidecars[0]
    assert sidecar.name == "server"
    assert sidecar.docker_image == "linuxserver/openssh-server:latest"
    assert sidecar.environment == {"PUID": "1000"}


def test_multiservice_stages_sidecar_build_contexts(
    tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "api-task"
    (staged / "client").mkdir(parents=True)
    (staged / "api").mkdir()
    (staged / "client" / "Dockerfile").write_text("FROM python:3.11-slim\n")
    (staged / "api" / "Dockerfile").write_text("FROM python:3.11-slim\n")
    (staged / "api" / "app.py").write_text("print('api')\n")
    (staged / "task.yaml").write_text(
        "instruction: call the api\n"
        "parser_name: pytest\n"
        "max_agent_timeout_sec: 10\n"
        "max_test_timeout_sec: 10\n"
    )
    (staged / "docker-compose.yaml").write_text(
        "services:\n"
        "  client:\n"
        "    build:\n"
        "      context: client\n"
        "      dockerfile: Dockerfile\n"
        "    environment:\n"
        "      - API_URL=http://api:8000\n"
        "    depends_on:\n"
        "      api:\n"
        "        condition: service_healthy\n"
        "  api:\n"
        "    build: ./api\n"
        "    command: [\"python\", \"app.py\"]\n"
        "    environment:\n"
        "      - DEBUG=1\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"python\", \"-c\", \"print('ok')\"]\n"
        "      interval: 5s\n"
        "      timeout: 5s\n"
        "      retries: 5\n"
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(
        only, out_dir=out,
    )

    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.environment.dockerfile.as_posix() == (
        ".loom-build/client/Dockerfile"
    )
    assert cfg.environment.sidecars[0].name == "api"
    assert cfg.environment.sidecars[0].dockerfile.as_posix() == (
        ".loom-build/sidecars/api/Dockerfile"
    )
    assert cfg.environment.sidecars[0].docker_build_context.as_posix() == (
        ".loom-build/sidecars/api"
    )
    assert cfg.environment.sidecars[0].command == ["python", "app.py"]
    assert cfg.environment.sidecars[0].environment == {"DEBUG": "1"}
    assert cfg.environment.sidecars[0].healthcheck is not None
    assert cfg.environment.environment == {
        "API_URL": "http://api:8000",
        "TEST_DIR": "/app/environment/tb2-tests",
    }
    assert (out / ".loom-build" / "sidecars" / "api" / "app.py").exists()


def test_client_compose_sandbox_options_are_preserved(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    staged = tmp_path / "tasks" / "broken-networking"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    (staged / "docker-compose.yaml").write_text(
        "services:\n"
        "  client:\n"
        "    build:\n"
        "      context: .\n"
        "      dockerfile: Dockerfile\n"
        "    command: [\"sh\", \"-c\", \"sleep infinity\"]\n"
        "    dns:\n"
        "      - 192.0.2.1\n"
        "    extra_hosts:\n"
        "      - example.com:131.25.18.2\n"
        "      - archive.ubuntu.com:162.242.195.82\n"
        "    tmpfs:\n"
        "      - /root:size=100M,mode=755\n"
    )
    (only,) = list(
        TerminalBench2Adapter().list_instances(
            source_dir=tmp_path, split="test",
        ),
    )
    out = tmp_path / "out"

    TerminalBench2Adapter().convert_instance(only, out_dir=out)

    cfg = TaskConfig.model_validate(
        tomllib.loads((out / "task.toml").read_text()),
    )
    assert cfg.environment.dns == ["192.0.2.1"]
    assert cfg.environment.extra_hosts == {
        "archive.ubuntu.com": "162.242.195.82",
        "example.com": "131.25.18.2",
    }
    assert cfg.environment.tmpfs == ["/root:size=100M,mode=755"]


def test_checksum_stable_across_runs(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """Two converts of the same source produce identical checksums."""
    staged = tmp_path / "tasks" / "chess-best-move"
    shutil.copytree(fixtures_dir / "tb2-task-chess-best-move", staged)
    adapter = TerminalBench2Adapter()
    (inst,) = list(adapter.list_instances(source_dir=tmp_path, split="test"))

    a = tmp_path / "out-a"
    b = tmp_path / "out-b"
    ca = adapter.convert_instance(inst, out_dir=a)
    cb = adapter.convert_instance(inst, out_dir=b)
    assert ca.checksum == cb.checksum
    assert ca.task_id == "terminal-bench-2/chess-best-move"
    assert ca.license_spdx == "Apache-2.0"


def test_convert_skips_symlinks_in_tests_dir(
    fixtures_dir: Path, tmp_path: Path,
) -> None:
    """A malicious upstream task at some future SHA could ship a symlink
    under tests/ pointing at a host file. _copy_tests must skip symlinks
    so we don't copy host bytes into the converted task."""
    import os

    staged = tmp_path / "tasks" / "evil"
    shutil.copytree(fixtures_dir / "tb2-task-hello-world", staged)
    sensitive = tmp_path / "host-secret.txt"
    sensitive.write_text("DO NOT LEAK")
    os.symlink(sensitive, staged / "tests" / "leaked.txt")
    (only,) = list(TerminalBench2Adapter().list_instances(
        source_dir=tmp_path, split="test",
    ))
    out = tmp_path / "out"
    TerminalBench2Adapter().convert_instance(only, out_dir=out)
    leaked = out / "environment" / "tb2-tests" / "leaked.txt"
    assert not leaked.exists(), "symlink under tests/ was copied through"
