"""Attempt-local trajectory fencing for agent-owned writes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from loom.models.trajectory import TrajectoryEvent
from loom.trajectory.writer import TrajectoryWriter


class AttemptTrajectoryFencedError(RuntimeError):
    """An agent tried to write after its attempt reached a terminal cause."""


@dataclass(slots=True)
class AttemptFence:
    """First-terminal-cause latch shared by a supervisor and writer guard."""

    _terminal_cause: str | None = None

    @property
    def terminal_cause(self) -> str | None:
        return self._terminal_cause

    @property
    def closed(self) -> bool:
        return self._terminal_cause is not None

    def latch(self, cause: str) -> bool:
        """Latch ``cause`` if no earlier terminal cause won."""

        if self._terminal_cause is not None:
            return False
        self._terminal_cause = cause
        return True


class AttemptTrajectoryGuard:
    """Expose agent write methods only while the owning attempt is active."""

    def __init__(self, writer: TrajectoryWriter, fence: AttemptFence) -> None:
        self._writer = writer
        self._fence = fence

    @property
    def local_path(self) -> Path:
        return self._writer.local_path

    @property
    def remote_uri(self) -> str:
        return self._writer.remote_uri

    @property
    def remote_committed(self) -> bool:
        return self._writer.remote_committed

    @property
    def remote_version_id(self) -> str | None:
        return self._writer.remote_version_id

    @property
    def llm_call_event_count(self) -> int:
        return self._writer.llm_call_event_count

    @property
    def _next_seq(self) -> int:
        """Compatibility read used by the Terminus2 checkpoint envelope."""

        return self._writer._next_seq

    async def append(self, event: TrajectoryEvent) -> None:
        self.require_attempt_active()
        await self._writer.append(event)

    async def write_raw_dict(self, data: dict[str, object]) -> None:
        self.require_attempt_active()
        await self._writer.write_raw_dict(data)

    def require_attempt_active(self) -> None:
        """Fail before any non-trajectory attempt-owned mutation starts."""
        if self._fence.closed:
            raise AttemptTrajectoryFencedError(
                "agent trajectory write rejected after attempt terminal cause "
                f"{self._fence.terminal_cause}",
            )
