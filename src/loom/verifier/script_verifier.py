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
                # #380: enrich the diagnostic with the post-mortem state
                # of the output directory so an operator can tell script
                # bug vs. permission bug vs. env-var bug apart without
                # rerunning. Runs a single non-mutating exec.
                diagnostic = {
                    **diagnostic,
                    **await _output_dir_post_mortem(env, _OUTPUT_PATH),
                }
                kind: Literal["exec_failure", "missing_output"] = (
                    "exec_failure"
                    if exec_result.return_code != 0
                    else "missing_output"
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


async def _output_dir_post_mortem(
    env: Driver, output_path: PurePosixPath,
) -> dict[str, object]:
    """Return a small dict describing the state of ``output_path.parent``.

    Used only in the missing-output failure path (#380) to help operators
    distinguish a script-side bug (wrote nothing) from a permission bug
    (dir wasn't writable) or an env-var bug (script wrote to a different
    path) without a repro run. Single, non-mutating exec.
    """
    output_dir = output_path.parent.as_posix()
    probe = (
        # `stat` prints one line per requested field; `ls -la` gives us
        # the sibling listing so the operator sees what DID land in the
        # dir (e.g. an output.json.tmp the script forgot to rename).
        f"echo -- MODE ; stat -c %a {output_dir!s} 2>/dev/null || echo MISSING ; "
        f"echo -- OWNER ; stat -c %U:%G {output_dir!s} 2>/dev/null || echo MISSING ; "
        f"echo -- LISTING ; ls -la {output_dir!s} 2>&1 || true"
    )
    try:
        probe_result = await env.exec(probe, user="root")
    except Exception as exc:  # pragma: no cover - defensive
        return {"output_dir_post_mortem_error": repr(exc)}
    text = probe_result.stdout.decode("utf-8", errors="replace")
    return {
        "output_dir": output_dir,
        "output_dir_probe": text,
        "output_dir_probe_return_code": probe_result.return_code,
    }


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
