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
from typing import TYPE_CHECKING

from loom.driver.base import Driver
from loom.models.verifier import CheckResult, VerifierError, VerifierResult

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader

_OUTPUT_PATH = PurePosixPath("/loom/verifier/output.json")


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
        cmd = (
            f"LOOM_VERIFIER_OUTPUT={_OUTPUT_PATH.as_posix()} "
            f"sh {self.script_path.as_posix()} || true"
        )
        await env.exec(cmd, user=self.user)

        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "output.json"
            try:
                await env.download(_OUTPUT_PATH, local)
            except FileNotFoundError:
                return VerifierResult(
                    rewards={},
                    error=VerifierError(
                        kind="missing_tests",
                        message=f"script did not write {_OUTPUT_PATH}",
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
                    ),
                )

        return VerifierResult(
            rewards=dict(data.get("rewards", {})),
            checks=[CheckResult(**c) for c in data.get("checks", [])],
            structured=data.get("structured"),
            confidence=data.get("confidence"),
        )
