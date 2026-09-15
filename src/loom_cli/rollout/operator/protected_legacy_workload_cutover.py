"""Retain and stop old staging workloads under the permanent cutover fence.

This separate operation has no restore path. The enclosing installed cutover
must retain the live mutation guard and probe the permanent policies on every
check. SQL/host retirement, successor deployment and final freeze publication
remain separate phases; a successful Pod census alone never authorizes execution.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

from .final_gate_plan import FinalGatePlan
from .protected_application_workload_runtime import ApplicationWorkloadRunner, _decode, _observe
from .protected_application_workloads import (
    APPLICATION_CRONJOB,
    ApplicationWorkload,
    _mapping,
    validate_workload_inventory,
)
from .protected_legacy_writer_fence_installation import LegacyWriterFenceJournal
from .staging_mutation_guard import MutationGuardEvidence

_BATCHES = 24


@dataclass(frozen=True, slots=True)
class LegacyWorkloadCutoverJournal(LegacyWriterFenceJournal):
    allowed_records: ClassVar[frozenset[str]] = frozenset({"retirement.intent.json", "retirement.terminal.json"} | {
        f"workloads-{index:02d}.intent.json" for index in range(_BATCHES)})

    @property
    def root(self) -> Path:
        return self.state_root / "protected-capacity" / "legacy-workload-cutover-journals" / self.request_id / str(self.attempt_number)


@dataclass(frozen=True, slots=True)
class LegacyWorkloadCutover:
    plan: FinalGatePlan
    journal: LegacyWorkloadCutoverJournal
    runner: ApplicationWorkloadRunner
    guard: MutationGuardEvidence
    guard_check: Callable[[], None]
    fence_check: Callable[[], str]

    def __post_init__(self) -> None:
        if (self.journal.request_id != self.plan.request_id or self.journal.attempt_number != self.plan.attempt_number
            or self.guard.request_id != self.plan.request_id or self.guard.candidate_sha != self.plan.candidate_sha
            or self.guard.candidate_tree != self.plan.candidate_tree or self.guard.state != "ready"
            or self.guard.mutation_epoch not in {self.plan.starting_mutation_epoch, self.plan.starting_mutation_epoch + 1}
            or not callable(self.guard_check) or not callable(self.fence_check)):
            raise ValueError("legacy workload cutover authority is invalid")

    def _check(self) -> str:
        self.guard_check()
        fence = self.fence_check()
        if not isinstance(fence, str) or re.fullmatch(r"[0-9a-f]{64}", fence) is None or fence == "0" * 64:
            raise ValueError("legacy workload cutover has no enforcing fence")
        return fence

    def _intent(self, fence: str) -> dict[str, object]:
        return {"schema_version": 1, "plan_digest": self.plan.plan_digest,
            "cronjob_uid": self.guard.cronjob_uid, "claimed_epoch": self.plan.starting_mutation_epoch + 1,
            "fence_digest": fence}

    def _saved(self) -> tuple[tuple[ApplicationWorkload, ...], int]:
        result: list[ApplicationWorkload] = []
        count = 0
        missing = False
        for index in range(_BATCHES):
            record = self.journal.read(f"workloads-{index:02d}.intent.json")
            if record is None:
                missing = True
                continue
            if missing or record.get("plan_digest") != self.plan.plan_digest or set(record) != {"plan_digest", "workloads"}:
                raise RuntimeError("legacy workload cutover inventory history drifted")
            rows = record["workloads"]
            if not isinstance(rows, list) or not rows:
                raise ValueError("legacy workload cutover inventory is invalid")
            result.extend(ApplicationWorkload.from_dict(_mapping(row)) for row in rows)
            count += 1
        return (validate_workload_inventory(tuple(result)) if result else (), count)

    def _patch(self, saved: ApplicationWorkload, current: Mapping[str, object], intent: dict[str, object]) -> bool:
        patch = saved.patch(current, recovering=False)
        if patch is None:
            return False
        if self._intent(self._check()) != intent:
            raise RuntimeError("legacy workload cutover authority changed")
        observed = _decode(self.runner.capture_stdout_with_input(
            ("kubectl", "--namespace", "loom-staging", "patch", saved.resource, saved.name,
                "--type=json", "--patch-file=/dev/stdin", "--output=json", "--request-timeout=30s"),
            env=self.runner.environment, input_payload=json.dumps(patch, separators=(",", ":")).encode(), timeout_seconds=30))
        if saved.patch(observed, recovering=False) is not None:
            raise RuntimeError("legacy workload cutover patch did not stop its workload")
        return True

    def _mark_lifecycle(self, document: dict[str, object], intent: dict[str, object]) -> bool:
        metadata = _mapping(document["metadata"])
        annotations = _mapping(metadata.get("annotations", {}))
        name = "loom.dev/legacy-writer-retirement"
        if name in annotations:
            if annotations[name] != self.plan.plan_digest:
                raise RuntimeError("legacy lifecycle retirement belongs to another cutover")
            return False
        if self._intent(self._check()) != intent:
            raise RuntimeError("legacy workload cutover authority changed")
        patch = [{"op": "test", "path": "/metadata/uid", "value": self.guard.cronjob_uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": metadata["resourceVersion"]},
            {"op": "test", "path": "/spec/suspend", "value": True},
            {"op": "add", "path": "/metadata/annotations", "value": {**annotations, name: self.plan.plan_digest}}]
        observed = _decode(self.runner.capture_stdout_with_input(
            ("kubectl", "--namespace", "loom-staging", "patch", "cronjobs.batch", APPLICATION_CRONJOB,
                "--type=json", "--patch-file=/dev/stdin", "--output=json", "--request-timeout=30s"),
            env=self.runner.environment, input_payload=json.dumps(patch, separators=(",", ":")).encode(), timeout_seconds=30))
        if (_mapping(observed.get("metadata")).get("uid") != self.guard.cronjob_uid
            or _mapping(_mapping(observed["metadata"]).get("annotations")).get(name) != self.plan.plan_digest):
            raise RuntimeError("legacy lifecycle retirement annotation was not retained")
        return True

    def retire(self) -> dict[str, object]:
        intent = self._intent(self._check())
        with self.journal.exclusive():
            self.journal.retain("retirement.intent.json", intent)
            terminal = self.journal.read("retirement.terminal.json")
            deadline = time.monotonic() + 180
            while True:
                if self._intent(self._check()) != intent:
                    raise RuntimeError("legacy workload cutover authority changed")
                saved, count = self._saved()
                objects, active = _observe(self.plan, self.runner, self.guard, saved)
                previous = {(item.kind, item.name) for item in saved}
                new = tuple(ApplicationWorkload.capture(document) for key, document in sorted(objects.items()) if key not in previous)
                if terminal is not None:
                    return self._verify_terminal(saved, objects, active, new, intent, terminal)
                if new:
                    if count >= _BATCHES:
                        raise RuntimeError("legacy workload cutover discovered too many late writers")
                    validate_workload_inventory(tuple(saved) + new)
                    self.journal.retain(f"workloads-{count:02d}.intent.json",
                        {"plan_digest": self.plan.plan_digest, "workloads": [item.to_dict() for item in new]})
                    saved, _ = self._saved()
                # Check all saved endpoints before changing any of this capture.
                for item in saved:
                    current = objects.get((item.kind, item.name))
                    if current is None and item.kind == "Job":
                        continue
                    if current is None:
                        raise RuntimeError("legacy workload cutover controller disappeared")
                    item.patch(current, recovering=False)
                changed = False
                for item in saved:
                    current = objects.get((item.kind, item.name))
                    if current is not None:
                        changed = self._patch(item, current, intent) or changed
                if not changed and not active:
                    cron = objects[("CronJob", APPLICATION_CRONJOB)]
                    if not self._mark_lifecycle(cron, intent):
                        terminal = {**intent, "workloads": [item.to_dict() for item in saved]}
                        # One fresh complete census after annotation/last patch.
                        if self._intent(self._check()) != intent:
                            raise RuntimeError("legacy workload cutover authority changed")
                        final, active = _observe(self.plan, self.runner, self.guard, saved)
                        self._verify_terminal(saved, final, active, (), intent, terminal)
                        self.journal.retain("retirement.terminal.json", terminal)
                        return terminal
                if time.monotonic() >= deadline:
                    raise RuntimeError("legacy workload cutover is still draining")
                time.sleep(0.25)

    def _verify_terminal(self, saved: tuple[ApplicationWorkload, ...], objects: dict[tuple[str, str], dict[str, object]],
                         active: bool, new: tuple[ApplicationWorkload, ...], intent: dict[str, object],
                         terminal: dict[str, object]) -> dict[str, object]:
        if (not saved or active or new or terminal != {**intent, "workloads": [item.to_dict() for item in saved]}
            or set(objects) - {(item.kind, item.name) for item in saved}):
            raise RuntimeError("retired workload inventory changed")
        for item in saved:
            current = objects.get((item.kind, item.name))
            if current is None and item.kind == "Job":
                continue
            if current is None or item.patch(current, recovering=False) is not None:
                raise RuntimeError("retired workload resumed or disappeared")
        cron = objects[("CronJob", APPLICATION_CRONJOB)]
        if _mapping(_mapping(cron["metadata"]).get("annotations", {})).get("loom.dev/legacy-writer-retirement") != self.plan.plan_digest:
            raise RuntimeError("retired workload lifecycle marker disappeared")
        if self._intent(self._check()) != intent:
            raise RuntimeError("legacy workload cutover authority changed")
        return terminal
