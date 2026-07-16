"""ScriptVerifier — runs an arbitrary script, reads $LOOM_VERIFIER_OUTPUT JSON.

Contract: the script writes a JSON object {rewards, checks, structured?,
confidence?} to the path in `LOOM_VERIFIER_OUTPUT`. We then download +
parse it. Missing file or invalid JSON surfaces as VerifierResult.error.

#865: On every verify attempt, also persist a capped copy of the shim
stdout/stderr under ``{workdir}/.loom/verifier/`` so ArtifactCollector
and delivery export can retain auditable verifier output on success.
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
# Hard cap for retained verifier audit logs (#865 PR1).
MAX_VERIFIER_LOG_BYTES = 1_048_576
_VERIFIER_LOG_HEAD_BYTES = 360_000
_VERIFIER_LOG_TRUNCATION_MARKER = (
    b"\n...[truncated verifier log; preserved trailing output]...\n"
)
_VERIFIER_AUDIT_RELDIR = ".loom/verifier"
_VERIFIER_LOG_NAME = "script.log"
_VERIFIER_META_NAME = "script.log.meta.json"


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
        # Standard task-context env vars for verifier scripts:
        # - LOOM_TASK_DIR: where the bundle was materialized (DockerDriver
        #   defaults to /workspace; other drivers may differ). In normal
        #   trial execution this comes from the task's configured workdir;
        #   `artifacts_dir.parent` remains only a compatibility fallback
        #   for direct verifier calls that do not provide a task.
        # - LOOM_AGENT_OUTPUT: if the task's first step declares exactly
        #   one artifact that is a bare file path (no glob metachars),
        #   resolve it against the workspace. Scripts that grade a single
        #   agent-produced answer file (AIME, GAIA, etc.) can rely on
        #   this without inventing their own convention. Multi-artifact
        #   or glob-based tasks (HumanEval, SWE-Bench) get their pytest
        #   verifier and don't need this variable set. See #688.
        # - LOOM_VERIFIER_OUTPUT: the path the script must write its
        #   result JSON to (unchanged; existing contract).
        workspace = _workspace_path(task, artifacts_dir=artifacts_dir)
        script_env = {
            "LOOM_TASK_DIR": workspace,
            "LOOM_VERIFIER_OUTPUT": _OUTPUT_PATH.as_posix(),
        }
        agent_output = _agent_output_path(task, workspace=workspace)
        if agent_output is not None:
            script_env["LOOM_AGENT_OUTPUT"] = agent_output
        exec_result = await env.exec(
            cmd,
            user=self.user,
            env=script_env,
        )
        diagnostic = _exec_diagnostic(
            exec_result=exec_result,
            output_path=_OUTPUT_PATH,
            script_path=self.script_path,
        )
        # #865: always retain capped shim stdout/stderr under the workspace
        # so ArtifactCollector can upload them after verify returns.
        await _persist_verifier_audit_log(
            env,
            workspace=workspace,
            exec_result=exec_result,
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
                    "exec_failure" if exec_result.return_code != 0 else "missing_output"
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


def _agent_output_path(task: TaskConfig | None, *, workspace: str) -> str | None:
    """Resolve LOOM_AGENT_OUTPUT for the given task, or return None.

    Set only when the task's first step declares exactly one artifact
    that is a plain relative file path (no `*`, `?`, `[` globbing).
    Multi-artifact or glob-based tasks don't get a single output
    variable — those verifiers should walk `LOOM_TASK_DIR` themselves."""
    if task is None or not task.steps:
        return None
    artifacts = task.steps[0].artifacts
    if len(artifacts) != 1:
        return None
    artifact = artifacts[0]
    if any(c in artifact for c in "*?["):
        return None
    return f"{workspace.rstrip('/')}/{artifact.lstrip('/')}"


def _workspace_path(
    task: TaskConfig | None,
    *,
    artifacts_dir: PurePosixPath,
) -> str:
    if task is not None:
        return task.environment.workdir.as_posix()
    return artifacts_dir.parent.as_posix()


async def _persist_verifier_audit_log(
    env: Driver,
    *,
    workspace: str,
    exec_result: ExecResult,
    script_path: PurePosixPath,
) -> None:
    """Best-effort write of capped verifier stdout/stderr into the workspace.

    Failures here must not mask the scoring result — operators still get
    rewards/checks even if audit persistence fails.
    """
    combined = _combine_exec_streams(exec_result)
    kept, truncated, original_bytes = _cap_verifier_log(combined)
    meta = {
        "schema_version": "1",
        "truncated": truncated or bool(exec_result.truncated),
        "original_bytes": original_bytes,
        "kept_bytes": len(kept),
        "return_code": exec_result.return_code,
        "script_path": script_path.as_posix(),
        "duration_sec": exec_result.duration_sec,
        "driver_truncated": bool(exec_result.truncated),
        "log_path": f"{_VERIFIER_AUDIT_RELDIR}/{_VERIFIER_LOG_NAME}",
    }
    audit_dir = PurePosixPath(workspace.rstrip("/")) / _VERIFIER_AUDIT_RELDIR
    log_path = audit_dir / _VERIFIER_LOG_NAME
    meta_path = audit_dir / _VERIFIER_META_NAME
    try:
        await env.exec(f"mkdir -p {audit_dir.as_posix()}", user="root")
        with tempfile.TemporaryDirectory() as td:
            local_root = Path(td)
            local_log = local_root / _VERIFIER_LOG_NAME
            local_meta = local_root / _VERIFIER_META_NAME
            local_log.write_bytes(kept)
            local_meta.write_text(
                json.dumps(meta, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            await env.upload(local_log, log_path)
            await env.upload(local_meta, meta_path)
    except Exception:  # pragma: no cover - best-effort audit path
        return


def _combine_exec_streams(exec_result: ExecResult) -> bytes:
    parts: list[bytes] = []
    if exec_result.stdout:
        parts.append(b"--- stdout ---\n")
        parts.append(exec_result.stdout)
        if not exec_result.stdout.endswith(b"\n"):
            parts.append(b"\n")
    if exec_result.stderr:
        parts.append(b"--- stderr ---\n")
        parts.append(exec_result.stderr)
        if not exec_result.stderr.endswith(b"\n"):
            parts.append(b"\n")
    return b"".join(parts)


def _cap_verifier_log(data: bytes) -> tuple[bytes, bool, int]:
    """Return (kept_bytes, truncated, original_bytes) with head+tail retention."""
    original = len(data)
    if original <= MAX_VERIFIER_LOG_BYTES:
        return data, False, original
    marker = _VERIFIER_LOG_TRUNCATION_MARKER
    head_budget = min(
        _VERIFIER_LOG_HEAD_BYTES,
        MAX_VERIFIER_LOG_BYTES - len(marker) - 1,
    )
    if head_budget < 0:
        head_budget = 0
    head = data[:head_budget]
    tail_budget = MAX_VERIFIER_LOG_BYTES - len(head) - len(marker)
    if tail_budget < 0:
        tail_budget = 0
    tail = data[-tail_budget:] if tail_budget else b""
    kept = head + marker + tail
    if len(kept) > MAX_VERIFIER_LOG_BYTES:
        kept = kept[:MAX_VERIFIER_LOG_BYTES]
    return kept, True, original


async def _output_dir_post_mortem(
    env: Driver,
    output_path: PurePosixPath,
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
