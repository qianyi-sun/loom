"""Run the Oracle baseline for a converted task fixture, return pass/fail.

Used by `loom_benchmark_tool verify` to prove that an imported benchmark
bundle is actually executable end-to-end. Not used at trial-execution
time — that path goes through `loom_worker.trial_runner` which composes
Driver + Agent + Verifier and adds network policy, fence-aware state
PATCHes, trajectory writing, etc.

The runner shells out to the host's `docker` binary rather than going
through the docker SDK / `DockerDriver` so `verify` works in any env
where docker-cp + docker-exec work (matching what a human operator
would do at the terminal). Trade-off: bypasses the DockerDriver's
NetworkPolicy enforcement; verify is treated as a development/QA tool,
not a sandboxed runtime.
"""

from __future__ import annotations

import asyncio
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


@dataclass(frozen=True)
class OracleResult:
    task_id: str
    passed: bool
    return_code: int
    stdout_tail: str
    stderr_tail: str


def _copy_into_container(
    container_id: str, src: Path, dest: PurePosixPath,
) -> None:
    """`docker cp` via subprocess (sg-docker-friendly)."""
    with tempfile.NamedTemporaryFile(suffix=".tar") as tar_file:
        with tarfile.open(tar_file.name, "w") as tar:
            tar.add(src, arcname=src.name)
        subprocess.run(
            ["docker", "cp", tar_file.name,
             f"{container_id}:/tmp/loom_bundle.tar"],
            check=True,
        )
    subprocess.run(
        ["docker", "exec", container_id, "sh", "-c",
         f"mkdir -p {dest} && tar -xf /tmp/loom_bundle.tar -C {dest}"],
        check=True,
    )


async def run_oracle_for_task(
    *, task_id: str, task_dir: Path, image: str,
) -> OracleResult:
    """Spin up `image`, copy task_dir contents in, run the Oracle
    solution, invoke pytest (or the structured verifier), tear down.

    Synchronous logic wrapped in `to_thread` so callers can await
    alongside other tasks.
    """
    def _do() -> OracleResult:
        container_id = subprocess.check_output(
            ["docker", "run", "-d", "--rm", image, "sleep", "300"],
        ).decode().strip()
        try:
            _copy_into_container(
                container_id, task_dir, PurePosixPath("/workspace"),
            )

            # SWE-Bench-style bundles include solution/solve.sh.
            if (task_dir / "solution" / "solve.sh").exists():
                rc = subprocess.call(
                    ["docker", "exec", container_id, "sh",
                     f"/workspace/{task_dir.name}/solution/solve.sh"],
                )
                if rc != 0:
                    return OracleResult(
                        task_id=task_id, passed=False, return_code=rc,
                        stdout_tail="",
                        stderr_tail=f"solve.sh exited {rc}",
                    )

            tests_dir = task_dir / "tests"
            verifier_run = task_dir / "verifier" / "run.sh"
            if tests_dir.exists():
                proc = subprocess.run(
                    ["docker", "exec", "-w",
                     f"/workspace/{task_dir.name}",
                     container_id, "sh", "-c",
                     "pip install pytest >/dev/null 2>&1; "
                     "PYTHONPATH=solution:tests pytest tests -q"],
                    capture_output=True, text=True,
                )
            elif verifier_run.exists():
                proc = subprocess.run(
                    ["docker", "exec", "-w",
                     f"/workspace/{task_dir.name}",
                     container_id, "sh", "verifier/run.sh"],
                    capture_output=True, text=True,
                )
            else:
                return OracleResult(
                    task_id=task_id, passed=False, return_code=2,
                    stdout_tail="",
                    stderr_tail="no tests/ and no verifier/run.sh",
                )
            return OracleResult(
                task_id=task_id, passed=proc.returncode == 0,
                return_code=proc.returncode,
                stdout_tail=(proc.stdout or "")[-500:],
                stderr_tail=(proc.stderr or "")[-500:],
            )
        finally:
            subprocess.call(
                ["docker", "kill", container_id],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
    return await asyncio.to_thread(_do)
