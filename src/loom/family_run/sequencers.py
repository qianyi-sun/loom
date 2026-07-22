"""Family sequencer plugins (#672).

The ranking-file sequencer reproduces SkillFlow's upstream ordering when
the bundle ships an ``ALL_TASK_DIFFICULTY_RANKING.json`` array of task
names. Alphabetical/submitted-order cover the "no upstream file" cases.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loom.family_run.protocols import TaskLike


@dataclass
class AlphabeticalSequencer:
    default_params: dict[str, Any] = field(default_factory=dict)

    def sequence(
        self,
        family_key: str,
        tasks: list[TaskLike],
        params: dict[str, Any],
    ) -> list[str]:
        return sorted(t.id for t in tasks)


@dataclass
class SubmittedOrderSequencer:
    default_params: dict[str, Any] = field(default_factory=dict)

    def sequence(
        self,
        family_key: str,
        tasks: list[TaskLike],
        params: dict[str, Any],
    ) -> list[str]:
        return [t.id for t in tasks]


@dataclass
class RankingFileSequencer:
    """Reads an ``ALL_TASK_DIFFICULTY_RANKING.json`` array of task names.

    Task IDs in the family are matched by the last path segment (task
    name) against the ranking entries; matched IDs come first in
    ranking order, unmatched IDs fall to the end alphabetically.
    """

    default_params: dict[str, Any] = field(default_factory=dict)

    def sequence(
        self,
        family_key: str,
        tasks: list[TaskLike],
        params: dict[str, Any],
    ) -> list[str]:
        ranked_from_tags = self._sequence_from_snapshot_tags(tasks)
        if ranked_from_tags is not None:
            return ranked_from_tags
        path_str = str(params.get("path") or self.default_params.get("path", ""))
        if not path_str:
            return sorted(t.id for t in tasks)
        path = Path(path_str)
        if not path.is_file():
            return sorted(t.id for t in tasks)
        try:
            ranking = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return sorted(t.id for t in tasks)
        if not isinstance(ranking, list) or any(
            not isinstance(name, str) for name in ranking
        ):
            raise ValueError(
                f"ranking file {path} is not a JSON array of task names",
            )

        by_leaf: dict[str, str] = {t.id.rsplit("/", 1)[-1]: t.id for t in tasks}
        ordered: list[str] = []
        seen: set[str] = set()
        for name in ranking:
            task_id = by_leaf.get(name)
            if task_id is not None and task_id not in seen:
                ordered.append(task_id)
                seen.add(task_id)
        remaining = sorted(t.id for t in tasks if t.id not in seen)
        ordered.extend(remaining)
        return ordered

    @staticmethod
    def _sequence_from_snapshot_tags(tasks: list[TaskLike]) -> list[str] | None:
        """Use seed-time ranking metadata when every task carries it.

        Dev ``fixture://`` bundles are mounted only inside workers, while
        sequencing happens in loom-service before a worker exists. The dev
        seeder therefore snapshots ``ALL_TASK_DIFFICULTY_RANKING.json`` into
        string tags. Partial or malformed metadata is ignored so normal
        published bundles retain the existing ranking-file/fallback contract.
        """
        ranked: list[tuple[int, str]] = []
        for task in tasks:
            tags = getattr(task, "tags", None)
            if (
                not isinstance(tags, dict)
                or tags.get("dev_fixture") != "true"
                or "family_run_rank" not in tags
            ):
                return None
            try:
                rank = int(tags["family_run_rank"])
            except (TypeError, ValueError):
                return None
            if rank < 0:
                return None
            ranked.append((rank, task.id))
        if len({rank for rank, _task_id in ranked}) != len(ranked):
            return None
        return [task_id for _rank, task_id in sorted(ranked)]
