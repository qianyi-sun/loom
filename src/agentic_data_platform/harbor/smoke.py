from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.harbor.ingestion import HarborResultIngestor
from agentic_data_platform.harbor.runner import HarborCliRunnerBackend, HarborRunSpec
from agentic_data_platform.sandbox.docker_terminal import CommandRunner


@dataclass(frozen=True)
class HarborCliSmokeConfig:
    workspace_root: Path
    run_id: str
    model_name: str = "smoke/noop"
    timeout_seconds: int = 600
    executable: str = "harbor"


def run_harbor_cli_smoke(
    config: HarborCliSmokeConfig,
    *,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    run_root = config.workspace_root / config.run_id
    task_dir = run_root / "task"
    jobs_dir = run_root / "jobs"
    artifacts_dir = run_root / "platform-artifacts"
    if run_root.exists():
        shutil.rmtree(run_root)
    task_dir.mkdir(parents=True)
    write_harbor_cli_smoke_task(task_dir)

    runner_result = HarborCliRunnerBackend(
        executable=config.executable,
        command_runner=command_runner,
    ).run(
        HarborRunSpec(
            run_id=config.run_id,
            task_instance_id="harbor-cli-smoke-task",
            task_path=task_dir,
            agent="oracle",
            model_name=config.model_name,
            environment="docker",
            jobs_dir=jobs_dir,
            timeout_seconds=config.timeout_seconds,
            extra_args=["--n-tasks", "1", "--quiet"],
        )
    )
    if runner_result.exit_code != 0:
        raise RuntimeError(
            "Harbor CLI smoke failed: "
            f"exit_code={runner_result.exit_code}, stderr={runner_result.stderr.strip()!r}"
        )

    artifact_persistence = ArtifactPersistence(LocalArtifactStore(artifacts_dir))
    ingested = HarborResultIngestor(artifact_persistence=artifact_persistence).ingest(
        run_id=config.run_id,
        task_instance_id="harbor-cli-smoke-task",
        jobs_dir=jobs_dir,
    )
    evaluator = ingested.evaluator_results[0] if ingested.evaluator_results else None
    if evaluator is None or evaluator.status != "completed" or evaluator.score is None or evaluator.score < 1.0:
        raise RuntimeError("Harbor CLI smoke did not produce a passing verifier result")

    return {
        "run_id": config.run_id,
        "status": "succeeded",
        "exit_code": runner_result.exit_code,
        "job_name": ingested.job_name,
        "trial_name": ingested.trial_name,
        "score": evaluator.score,
        "artifact_count": len(ingested.artifacts),
        "turn_count": len(ingested.turns),
        "jobs_dir": str(jobs_dir),
    }


def main() -> int:
    result = run_harbor_cli_smoke(_config_from_env())
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0


def _config_from_env(environ: Mapping[str, str] | None = None) -> HarborCliSmokeConfig:
    values = os.environ if environ is None else environ
    return HarborCliSmokeConfig(
        workspace_root=Path(_env(values, "HARBOR_SMOKE_WORKSPACE_ROOT", ".runtime/harbor-smoke")),
        run_id=_env(values, "HARBOR_SMOKE_RUN_ID", f"harbor_smoke_{uuid4().hex}"),
        model_name=_env(values, "HARBOR_SMOKE_MODEL_NAME", "smoke/noop"),
        timeout_seconds=int(_env(values, "HARBOR_SMOKE_TIMEOUT_SECONDS", "600")),
        executable=_env(values, "HARBOR_SMOKE_EXECUTABLE", "harbor"),
    )


def write_harbor_cli_smoke_task(task_dir: Path) -> None:
    (task_dir / "environment").mkdir(parents=True, exist_ok=True)
    (task_dir / "solution").mkdir(parents=True, exist_ok=True)
    (task_dir / "tests").mkdir(parents=True, exist_ok=True)
    (task_dir / "instruction.md").write_text(
        "Create `/app/smoke-output.txt` containing the text `harbor-smoke-ok`.\n",
        encoding="utf-8",
    )
    (task_dir / "task.toml").write_text(
        "\n".join(
            [
                'schema_version = "1.2"',
                'artifacts = ["/logs/artifacts/smoke-output.txt"]',
                "",
                "[task]",
                'name = "carinrc/harbor-cli-smoke"',
                'description = "Minimal shared dev Harbor CLI smoke task."',
                'authors = [{ name = "CARIN Research Center" }]',
                'keywords = ["smoke", "shared dev"]',
                "",
                "[verifier]",
                "timeout_sec = 120.0",
                "",
                "[agent]",
                "timeout_sec = 120.0",
                "",
                "[environment]",
                "build_timeout_sec = 120.0",
                'os = "linux"',
                "allow_internet = true",
                "mcp_servers = []",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (task_dir / "environment" / "Dockerfile").write_text(
        "FROM ubuntu:24.04\n\nWORKDIR /app\n",
        encoding="utf-8",
    )
    solve_path = task_dir / "solution" / "solve.sh"
    solve_path.write_text(
        "#!/bin/bash\nset -euo pipefail\nprintf 'harbor-smoke-ok\\n' > /app/smoke-output.txt\n",
        encoding="utf-8",
    )
    test_path = task_dir / "tests" / "test.sh"
    test_path.write_text(
        "\n".join(
            [
                "#!/bin/bash",
                "set -euo pipefail",
                "mkdir -p /logs/verifier /logs/artifacts",
                "if grep -q 'harbor-smoke-ok' /app/smoke-output.txt; then",
                "  cp /app/smoke-output.txt /logs/artifacts/smoke-output.txt",
                "  echo 1 > /logs/verifier/reward.txt",
                "else",
                "  echo 0 > /logs/verifier/reward.txt",
                "fi",
                "",
            ]
        ),
        encoding="utf-8",
    )
    solve_path.chmod(0o755)
    test_path.chmod(0o755)


_write_smoke_task = write_harbor_cli_smoke_task


def _env(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    return value.strip() if isinstance(value, str) and value.strip() else default


if __name__ == "__main__":
    raise SystemExit(main())
