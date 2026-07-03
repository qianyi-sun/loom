"""ScriptVerifier — runs an arbitrary script, reads $LOOM_VERIFIER_OUTPUT JSON.

Contract: the script writes a JSON object {rewards, checks, structured?,
confidence?} to the path in `LOOM_VERIFIER_OUTPUT`. We then download +
parse it. Missing file or invalid JSON surfaces as VerifierResult.error.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal

from loom.driver.base import Driver
from loom.models.exec import ExecResult
from loom.models.verifier import CheckResult, VerifierError, VerifierResult

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader

_OUTPUT_PATH = PurePosixPath("/loom/verifier/output.json")
_DIAGNOSTIC_TAIL_BYTES = 4096


@dataclass
class ScriptVerifier:
    script_path: PurePosixPath
    name: str = "script"
    user: str | int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.script_path, str):
            self.script_path = PurePosixPath(self.script_path)

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        await env.exec("mkdir -p /loom/verifier", user="root")
        cmd = f"sh {self.script_path.as_posix()}"
        exec_result = await env.exec(
            cmd,
            user=self.user,
            env={"LOOM_VERIFIER_OUTPUT": _OUTPUT_PATH.as_posix()},
        )
        diagnostic = _exec_diagnostic(
            exec_result=exec_result,
            output_path=_OUTPUT_PATH,
            script_path=self.script_path,
        )

        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "output.json"
            try:
                await env.download(_OUTPUT_PATH, local)
            except FileNotFoundError:
                kind: Literal["exec_failure", "missing_tests"] = (
                    "exec_failure" if exec_result.return_code != 0 else "missing_tests"
                )
                return VerifierResult(
                    rewards={},
                    error=VerifierError(
                        kind=kind,
                        message=f"script did not write {_OUTPUT_PATH}",
                        detail=diagnostic,
                    ),
                )
            try:
                data = json.loads(local.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                return VerifierResult(
                    rewards={},
                    error=VerifierError(
                        kind="parse_failure",
                        message=f"$LOOM_VERIFIER_OUTPUT json parse failed: {exc}",
                        detail=diagnostic,
                    ),
                )

        return VerifierResult(
            rewards=dict(data.get("rewards", {})),
            checks=[CheckResult(**c) for c in data.get("checks", [])],
            structured=data.get("structured"),
            confidence=data.get("confidence"),
        )


def _exec_diagnostic(
    *,
    exec_result: ExecResult,
    output_path: PurePosixPath,
    script_path: PurePosixPath,
) -> dict[str, object]:
    return {
        "return_code": exec_result.return_code,
        "stdout_tail": _decode_tail(exec_result.stdout),
        "stderr_tail": _decode_tail(exec_result.stderr),
        "truncated": exec_result.truncated,
        "duration_sec": exec_result.duration_sec,
        "output_path": output_path.as_posix(),
        "script_path": script_path.as_posix(),
    }


def _decode_tail(data: bytes) -> str:
    tail = data[-_DIAGNOSTIC_TAIL_BYTES:]
    return tail.decode("utf-8", errors="replace")
