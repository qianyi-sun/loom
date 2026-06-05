"""ExecResult — what `Driver.exec()` returns (spec §2.2).

Hard size caps on stdout/stderr are enforced by the driver, not this model
(MAX_EXEC_STREAM_BYTES default 10 MB). `truncated=True` signals a cap hit.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ExecResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    return_code: int
    stdout: bytes
    stderr: bytes
    truncated: bool = False
    duration_sec: float = Field(ge=0)
