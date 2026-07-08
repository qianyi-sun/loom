"""Worker pre-start helper: download family state + build bind-mount (#672).

Called by the worker's ``_spawn_trial`` when the claim response carries
a ``family_state_uri``. Downloads the tarball into a temp directory and
returns the ``(host_dir, container_dir, mode)`` volume tuple that the
LocalTrialRunner appends to its ``StartOptions.volumes``.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class FamilyStateMount:
    """Result of :func:`prepare_family_state_mount`.

    ``cleanup()`` removes the temp directory - call it in the worker's
    trial-teardown ``finally`` block.
    """

    host_dir: Path
    container_dir: str
    mode: str

    def as_volume_tuple(self) -> tuple[str, str, str]:
        return (str(self.host_dir), self.container_dir, self.mode)

    def cleanup(self) -> None:
        shutil.rmtree(self.host_dir, ignore_errors=True)


async def prepare_family_state_mount(
    *,
    trial_id: str,
    state_uri: str,
    mount_path: str,
    state_backend: Any,
    backend_params: dict[str, Any] | None = None,
    download_timeout_sec: float = 120.0,
) -> FamilyStateMount:
    """Download the family state tarball into a fresh temp dir.

    The caller is responsible for cleanup - use ``FamilyStateMount.cleanup()``
    in a ``finally`` block. Raises :class:`asyncio.TimeoutError` when the
    download exceeds ``download_timeout_sec``.
    """
    host_dir = Path(tempfile.mkdtemp(prefix=f"loom-family-state-{trial_id}-"))
    try:
        await asyncio.wait_for(
            state_backend.download(state_uri, host_dir, backend_params or {}),
            timeout=download_timeout_sec,
        )
    except BaseException:
        shutil.rmtree(host_dir, ignore_errors=True)
        raise
    return FamilyStateMount(
        host_dir=host_dir,
        container_dir=mount_path,
        mode="rw",
    )
