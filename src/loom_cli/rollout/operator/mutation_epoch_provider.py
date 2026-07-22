"""Read the protected staging mutation epoch through one fixed readonly query."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Protocol

from .config import OperatorConfig

_EPOCH_TIMEOUT_SECONDS = 30.0
_EPOCH_SQL = """
SELECT jsonb_build_object(
  'environment', environment,
  'namespace', namespace,
  'epoch', epoch
)::text
FROM staging_mutation_epochs
WHERE environment = 'staging' AND namespace = 'loom-staging';
""".strip()


class MutationEpochCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


class KubernetesMutationEpochProvider:
    """Return the exact current epoch without exposing database credentials."""

    def __init__(
        self,
        config: OperatorConfig,
        *,
        runner: MutationEpochCommandRunner,
        environment: Mapping[str, str],
    ) -> None:
        if config.environment != "staging" or config.namespace != "loom-staging":
            raise ValueError("mutation epoch authority is staging-only")
        self._config = config
        self._runner = runner
        self._environment = dict(environment)

    def __call__(self) -> int:
        payload = self._runner.capture_stdout(
            [
                "kubectl",
                "-n",
                self._config.namespace,
                "exec",
                "statefulset/loom-postgres",
                "--",
                "sh",
                "-ceu",
                'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX -v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                _EPOCH_SQL,
            ],
            env=self._environment,
            timeout_seconds=_EPOCH_TIMEOUT_SECONDS,
        )
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("mutation epoch query did not return valid JSON") from exc
        if (
            not isinstance(document, dict)
            or set(document) != {"environment", "namespace", "epoch"}
            or document["environment"] != self._config.environment
            or document["namespace"] != self._config.namespace
            or type(document["epoch"]) is not int
            or document["epoch"] < 0
        ):
            raise ValueError("mutation epoch query authority is incomplete or drifted")
        return document["epoch"]


__all__ = ["KubernetesMutationEpochProvider"]
