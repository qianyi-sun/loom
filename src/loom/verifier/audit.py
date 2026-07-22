"""Shared verifier-artifact audit channel (#865 PR2 / #867).

Both ``ScriptVerifier`` and ``PytestVerifier`` retain capped stdout/stderr
under ``{workdir}/.loom/verifier/`` so ArtifactCollector + delivery export
can audit successful and failed verifies without dumping raw logs into
``VerifierResult.structured``.

The reserved structured namespace is ``loom_verifier_audit``: a bounded
redacted summary plus artifact path refs only.
"""

from __future__ import annotations

import json
import shlex
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from loom.driver.base import Driver
from loom.models.exec import ExecResult
from loom.security.redaction import redact_text

if TYPE_CHECKING:
    from loom.models.task import TaskConfig

# Hard cap for retained verifier audit logs (#865).
MAX_VERIFIER_LOG_BYTES = 1_048_576
MAX_VERIFIER_OUTPUT_BYTES = 1_048_576
MAX_VERIFIER_JUNIT_BYTES = 4_194_304
VERIFIER_LOG_HEAD_BYTES = 360_000
VERIFIER_LOG_TRUNCATION_MARKER = b"\n...[truncated verifier log; preserved trailing output]...\n"
VERIFIER_AUDIT_RELDIR = ".loom/verifier"
LOOM_VERIFIER_AUDIT_KEY = "loom_verifier_audit"
_SUMMARY_TAIL_CHARS = 512


@dataclass(frozen=True)
class VerifierAuditRecord:
    """Result of best-effort audit persistence for one verifier exec."""

    log_relpath: str
    meta_relpath: str
    truncated: bool
    original_bytes: int
    kept_bytes: int
    return_code: int | None
    duration_sec: float | None
    summary: str
    persisted: bool
    canonical_artifacts: tuple[tuple[str, str], ...] = ()

    def structured_payload(self) -> dict[str, Any]:
        """Bounded redacted summary + artifact refs for VerifierResult.structured."""
        artifacts: list[dict[str, Any]] = []
        if self.persisted:
            artifacts.extend(
                [
                    {
                        "path": self.log_relpath,
                        "kind": "stdout_stderr",
                        "truncated": self.truncated,
                    },
                    {
                        "path": self.meta_relpath,
                        "kind": "meta",
                    },
                ]
            )
        artifacts.extend(
            {"path": path, "kind": kind}
            for path, kind in self.canonical_artifacts
        )
        return {
            "schema_version": "1",
            "return_code": self.return_code,
            "truncated": self.truncated,
            "original_bytes": self.original_bytes,
            "kept_bytes": self.kept_bytes,
            "duration_sec": self.duration_sec,
            "persisted": self.persisted,
            "artifacts": artifacts,
            "summary": self.summary,
        }


def combine_exec_streams(exec_result: ExecResult) -> bytes:
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


def cap_verifier_log(data: bytes) -> tuple[bytes, bool, int]:
    """Return (kept_bytes, truncated, original_bytes) with head+tail retention."""
    original = len(data)
    if original <= MAX_VERIFIER_LOG_BYTES:
        return data, False, original
    marker = VERIFIER_LOG_TRUNCATION_MARKER
    head_budget = min(
        VERIFIER_LOG_HEAD_BYTES,
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


def sanitize_utf8(data: bytes) -> str:
    """Decode bytes for summary text; replace undecodable sequences."""
    return data.decode("utf-8", errors="replace")


def summary_from_log(kept: bytes) -> str:
    text = redact_text(sanitize_utf8(kept)).strip()
    if len(text) <= _SUMMARY_TAIL_CHARS:
        return text
    return text[-_SUMMARY_TAIL_CHARS:]


def merge_loom_verifier_audit(
    structured: dict[str, Any] | None,
    audit: VerifierAuditRecord | None,
) -> dict[str, Any] | None:
    """Merge reserved audit payload into an existing structured dict."""
    if audit is None:
        return structured
    base: dict[str, Any] = dict(structured) if structured else {}
    base[LOOM_VERIFIER_AUDIT_KEY] = audit.structured_payload()
    return base


def workspace_from_task(
    task: TaskConfig | None,
    *,
    artifacts_dir: PurePosixPath,
) -> str:
    if task is not None:
        return task.environment.workdir.as_posix()
    return artifacts_dir.parent.as_posix()


def add_canonical_artifact(
    audit: VerifierAuditRecord,
    *,
    relpath: str,
    kind: str,
) -> VerifierAuditRecord:
    """Return ``audit`` with one successfully persisted canonical file ref."""
    return replace(
        audit,
        canonical_artifacts=(*audit.canonical_artifacts, (relpath, kind)),
    )


async def persist_verifier_file(
    env: Driver,
    *,
    workspace: str,
    local_file: Path,
    name: str,
    max_bytes: int,
) -> bool:
    """Best-effort persist of one bounded canonical verifier file.

    The destination is a platform-owned exact filename under the canonical
    channel. Callers add a structured ref only after this upload succeeds.
    """
    if not _safe_leaf(name):
        return False
    audit_dir = PurePosixPath(workspace) / VERIFIER_AUDIT_RELDIR
    final = audit_dir / name
    try:
        # Clear a prior step's or agent-preplaced exact-name file before any
        # early return. Static artifact collection must never mistake stale
        # bytes for this verifier attempt's canonical output.
        if not await _prepare_audit_targets(env, audit_dir, final):
            return False
        if local_file.stat().st_size > max_bytes:
            return False
        await env.upload(local_file, final)
        return True
    except Exception:  # pragma: no cover - best-effort audit path
        await _cleanup(env, final)
        return False


async def persist_verifier_audit_log(
    env: Driver,
    *,
    workspace: str,
    exec_result: ExecResult,
    log_name: str,
    script_path: str,
) -> VerifierAuditRecord:
    """Best-effort write of capped verifier stdout/stderr into the workspace.

    Failures here must not mask the scoring result. A failed write returns a
    record with ``persisted=false`` and no artifact refs so structured output
    distinguishes audit I/O failure from legacy trials with no channel.
    """
    combined = combine_exec_streams(exec_result)
    kept, truncated, original_bytes = cap_verifier_log(combined)
    truncated = truncated or bool(exec_result.truncated)
    log_relpath = f"{VERIFIER_AUDIT_RELDIR}/{log_name}"
    meta_name = f"{log_name}.meta.json"
    meta_relpath = f"{VERIFIER_AUDIT_RELDIR}/{meta_name}"
    summary = summary_from_log(kept)
    record = VerifierAuditRecord(
        log_relpath=log_relpath,
        meta_relpath=meta_relpath,
        truncated=truncated,
        original_bytes=original_bytes,
        kept_bytes=len(kept),
        return_code=exec_result.return_code,
        duration_sec=exec_result.duration_sec,
        summary=summary,
        persisted=False,
    )
    meta = {
        "schema_version": "1",
        "truncated": truncated,
        "original_bytes": original_bytes,
        "kept_bytes": len(kept),
        "return_code": exec_result.return_code,
        "script_path": script_path,
        "duration_sec": exec_result.duration_sec,
        "driver_truncated": bool(exec_result.truncated),
        "log_path": log_relpath,
    }
    audit_dir = PurePosixPath(workspace) / VERIFIER_AUDIT_RELDIR
    log_path = audit_dir / log_name
    meta_path = audit_dir / meta_name
    try:
        if not _safe_leaf(log_name):
            return record
        # Remove exact-name leftovers before mkdir/upload so a failed current
        # attempt cannot publish a previous step's otherwise-valid pair.
        if not await _prepare_audit_targets(env, audit_dir, meta_path, log_path):
            return record
        with tempfile.TemporaryDirectory() as td:
            local_root = Path(td)
            local_log = local_root / log_name
            local_meta = local_root / meta_name
            local_log.write_bytes(kept)
            local_meta.write_text(
                json.dumps(meta, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            # Publish metadata first and the log second. Artifact collection
            # starts only after verify returns; if the second upload fails,
            # cleanup removes both names before the collector can see them.
            await env.upload(local_meta, meta_path)
            await env.upload(local_log, log_path)
    except Exception:  # pragma: no cover - best-effort audit path
        await _cleanup(env, meta_path, log_path)
        return record

    return replace(record, persisted=True)


def _safe_leaf(name: str) -> bool:
    return bool(name) and PurePosixPath(name).name == name and name not in {".", ".."}


async def _prepare_audit_targets(
    env: Driver,
    audit_dir: PurePosixPath,
    *paths: PurePosixPath,
) -> bool:
    """Create the audit dir and atomically gate publication on stale cleanup."""
    try:
        result = await env.exec(
            f"mkdir -p -- {shlex.quote(audit_dir.as_posix())} && rm -f -- "
            + " ".join(shlex.quote(path.as_posix()) for path in paths),
            user="root",
        )
        return result.return_code == 0
    except Exception:  # pragma: no cover - best-effort audit path
        return False


async def _cleanup(env: Driver, *paths: PurePosixPath) -> bool:
    try:
        result = await env.exec(
            "rm -f -- " + " ".join(shlex.quote(path.as_posix()) for path in paths),
            user="root",
        )
        return result.return_code == 0
    except Exception:  # pragma: no cover - best-effort cleanup
        return False
