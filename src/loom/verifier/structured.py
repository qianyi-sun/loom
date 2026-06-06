"""StructuredOutputVerifier — validates an artifact file against a JSON Schema."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

import jsonschema

from loom.driver.base import Driver
from loom.models.verifier import VerifierError, VerifierResult

if TYPE_CHECKING:
    from loom.models.task import TaskConfig
    from loom.trajectory.reader import TrajectoryReader


@dataclass
class StructuredOutputVerifier:
    artifact_path: PurePosixPath
    schema: dict[str, Any]
    name: str = "structured"

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        with tempfile.TemporaryDirectory() as td:
            local = Path(td) / "output.json"
            try:
                await env.download(self.artifact_path, local)
            except FileNotFoundError:
                return VerifierResult(
                    rewards={"valid": 0.0},
                    error=VerifierError(
                        kind="missing_tests",
                        message=f"artifact {self.artifact_path} not produced",
                    ),
                )
            try:
                payload = json.loads(local.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                return VerifierResult(
                    rewards={"valid": 0.0},
                    error=VerifierError(
                        kind="parse_failure",
                        message=f"artifact is not valid JSON: {exc}",
                    ),
                )

        try:
            jsonschema.validate(payload, self.schema)
        except jsonschema.ValidationError as exc:
            return VerifierResult(
                rewards={"valid": 0.0},
                error=VerifierError(
                    kind="exec_failure",
                    message=str(exc.message),
                    detail={"path": [str(p) for p in exc.path]},
                ),
            )

        return VerifierResult(
            rewards={"valid": 1.0},
            structured={"validated": True},
        )
