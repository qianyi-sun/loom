"""Evidence directory management for the rollout driver (#340).

Layout under ``<rollout-root>/rollouts/<rollout_id>/``:

* ``state.json`` — top-level RolloutState (see :mod:`state`)
* ``inputs.json`` — resolved CLI args + config shas (for --resume audit)
* ``logs/driver.log`` — top-level driver log
* ``NN-<step-name>/`` — per-step directory:
    * ``result.json`` — {state, inputs_hash, timestamps, exit_code}
    * ``stdout.log``, ``stderr.log`` — captured subprocess output
    * any step-specific artifacts (rendered.yaml, loaded-images.json, …)
* ``99-summary/summary.md`` — human-readable summary written at the end

The evidence root path model is inherited from #174: the parent
``<rollout-root>/rollouts/`` directory is created by
``loom cluster bootstrap-evidence-paths`` and owned by the operator user.
The driver assumes it can create the per-rollout subdirectory without
sudo — if it can't, the diagnostic points at the bootstrap subcommand.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def new_rollout_id(*, image_tag: str, now: datetime | None = None) -> str:
    """Return a deterministic rollout id: ``<utc-timestamp>-<image-tag-slug>``.

    The timestamp comes first so directory listings sort chronologically.
    The image tag gets normalised to RFC 1123 DNS-label shape so the id
    is safe to use in kubectl object names / paths.
    """
    now = now or datetime.now(UTC)
    ts = now.strftime("%Y%m%dt%H%M%Sz")
    slug = _slugify(image_tag)
    return f"{ts}-{slug}"


def _slugify(text: str) -> str:
    """Normalise to lowercase-alnum-dash form."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9-]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "unknown"


@dataclass(frozen=True, slots=True)
class StepDir:
    """A per-step subdirectory under the rollout evidence dir."""

    number: int
    name: str
    path: Path

    def result_path(self) -> Path:
        return self.path / "result.json"

    def stdout_path(self) -> Path:
        return self.path / "stdout.log"

    def stderr_path(self) -> Path:
        return self.path / "stderr.log"

    def artifact_path(self, name: str) -> Path:
        """Path for a step-specific artifact file (rendered.yaml, …)."""
        return self.path / name


class EvidenceDirectory:
    """Root of one rollout's evidence tree.

    Creates the tree on first use; safe to construct against an
    already-populated tree (resume path). All writes are string I/O;
    subprocess stdout/stderr should be piped to :meth:`stdout_path` /
    :meth:`stderr_path` by the caller — we don't own the subprocess
    lifecycle here so the pipe-writing is left to the step.
    """

    STATE_JSON = "state.json"
    INPUTS_JSON = "inputs.json"

    def __init__(self, root: Path, rollout_id: str) -> None:
        self.root = root
        self.rollout_id = rollout_id
        self.path = root / "rollouts" / rollout_id

    def ensure(self) -> None:
        """Create the rollout directory if missing. Idempotent."""
        self.path.mkdir(parents=True, exist_ok=True)
        (self.path / "logs").mkdir(exist_ok=True)

    def exists(self) -> bool:
        return self.path.is_dir()

    # -------- top-level artifacts --------

    def state_path(self) -> Path:
        return self.path / self.STATE_JSON

    def inputs_path(self) -> Path:
        return self.path / self.INPUTS_JSON

    def driver_log_path(self) -> Path:
        return self.path / "logs" / "driver.log"

    def write_inputs(self, inputs: dict[str, Any]) -> None:
        """Persist the resolved CLI args + config shas for --resume audit."""
        self.inputs_path().write_text(
            json.dumps(inputs, indent=2, sort_keys=True) + "\n",
        )

    def read_inputs(self) -> dict[str, Any]:
        result: dict[str, Any] = json.loads(self.inputs_path().read_text())
        return result

    # -------- per-step subdir --------

    def step_dir(self, number: int, name: str) -> StepDir:
        """Return the per-step subdirectory, creating it if missing."""
        slug = _slugify(name)
        subdir_name = f"{number:02d}-{slug}"
        subdir = self.path / subdir_name
        subdir.mkdir(parents=True, exist_ok=True)
        return StepDir(number=number, name=name, path=subdir)

    def existing_step_dir(self, number: int, name: str) -> StepDir | None:
        """Return the per-step subdirectory only if it already exists.

        Used by resume: distinguishing "step never started" (no dir)
        from "step was interrupted" (dir + partial result.json) is
        one of the recovery signals.
        """
        slug = _slugify(name)
        subdir = self.path / f"{number:02d}-{slug}"
        if subdir.is_dir():
            return StepDir(number=number, name=name, path=subdir)
        return None

    def write_step_result(self, step_dir: StepDir, result: dict[str, Any]) -> None:
        step_dir.result_path().write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
        )

    def read_step_result(self, step_dir: StepDir) -> dict[str, Any] | None:
        path = step_dir.result_path()
        if not path.is_file():
            return None
        result: dict[str, Any] = json.loads(path.read_text())
        return result

    # -------- discovery: find in-progress rollouts --------

    @classmethod
    def find_in_progress(
        cls,
        root: Path,
        image_tag: str,
    ) -> EvidenceDirectory | None:
        """Return the most-recent rollout dir under ``root`` whose id
        contains the slugified ``image_tag`` and whose state.json exists
        with ``status != done``.

        Used by --resume auto-detect: if the operator re-invokes the
        driver without --resume against an in-progress rollout, we
        surface the existing dir so they can pick up rather than
        accidentally start a fresh one.
        """
        rollouts_dir = root / "rollouts"
        if not rollouts_dir.is_dir():
            return None
        slug = _slugify(image_tag)
        candidates = sorted(
            (p for p in rollouts_dir.iterdir()
             if p.is_dir() and p.name.endswith(f"-{slug}")),
            reverse=True,
        )
        for candidate in candidates:
            state_path = candidate / cls.STATE_JSON
            if not state_path.is_file():
                continue
            try:
                state_doc = json.loads(state_path.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            if state_doc.get("status") != "done":
                return cls(root, candidate.name)
        return None
