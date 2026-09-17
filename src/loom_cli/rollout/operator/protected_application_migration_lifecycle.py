"""Ordered migration effects under an active, retained component journal.

A resumed credential generation is retired before any new delivery. A process
loss never renews its credential or resumes arming it. The runtime supplies exact
SQL/cluster input admission, original-peer retirement and generation-bound
resources; this engine owns durable dispatch and cleanup ordering.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Protocol

from loom.application_database_admission import ApplicationDatabaseHandoffBackend

from .protected_application_admission_recovery import _backend, _integer, _string
from .protected_application_migration_journal import (
    ApplicationMigrationEvent,
    ApplicationMigrationJournal,
)
from .protected_application_migration_resources import (
    ProtectedApplicationMigrationResources,
    _digest,
)
from .staging_mutation_guard import MutationGuardEvidence


class ApplicationMigrationRuntime(Protocol):
    def checkpoint(self) -> None: ...
    def prepare_generation(self, ordinal: int) -> Mapping[str, object]: ...
    def create(self, generation: ApplicationMigrationEvent, persist: Callable[[int], None]) -> None: ...
    def arm(self, generation: ApplicationMigrationEvent, oid: int) -> None: ...
    def resources(self, generation: ApplicationMigrationEvent) -> ProtectedApplicationMigrationResources: ...
    def release_creator(self) -> None: ...
    def begin_retirement(self, generation: ApplicationMigrationEvent,
                         peers: Sequence[ApplicationDatabaseHandoffBackend]) -> ApplicationDatabaseHandoffBackend: ...
    def role_exists(self, generation: ApplicationMigrationEvent, oid: int | None) -> bool: ...
    def require_role_retired(self, generation: ApplicationMigrationEvent, oid: int | None) -> None: ...
    def seal(self, generation: ApplicationMigrationEvent, oid: int) -> None: ...
    def close(self, generation: ApplicationMigrationEvent, oid: int) -> None: ...
    def retire(self, generation: ApplicationMigrationEvent, oid: int) -> None: ...
    def reopen(self, generation: ApplicationMigrationEvent, oid: int) -> None: ...
    def read_revision(self) -> str: ...


@dataclass(frozen=True, slots=True)
class ApplicationMigrationLifecycle:
    migration: ApplicationMigrationJournal
    guard: MutationGuardEvidence
    runtime: ApplicationMigrationRuntime

    def _events(self) -> tuple[ApplicationMigrationEvent, ...]:
        self.runtime.checkpoint()
        self.migration.journal.require_application_guard_retained(self.migration.plan, guard=self.guard)
        return self.migration.read()

    def _append(self, phase: str, payload: Mapping[str, object]) -> ApplicationMigrationEvent:
        self.runtime.checkpoint()
        return self.migration.append(phase, payload, guard=self.guard)

    def run(self) -> None:
        events = self._events()
        if not events or events[0].phase != "authority":
            raise RuntimeError("application migration lacks original admitted authority")
        if events[-1].phase == "noop" or (events[-1].phase == "complete" and events[-1].payload["successful"]):
            self._require_retired_history(events)
            self._require_target()
            return
        if events[-1].phase not in {"authority", "abandoned", "complete"}:
            self.runtime.release_creator()
            self._retire(successful=False)
        while True:
            events = self._events()
            revision = self.runtime.read_revision()
            self.runtime.checkpoint()
            if revision == self.migration.target_revision:
                self._append("noop", {"revision": revision})
                return
            if revision != self.migration.source_revision:
                raise RuntimeError("application migration schema changed outside the admitted transaction")
            ordinal = sum(event.phase == "generation" for event in events) + 1
            if ordinal > 8:
                raise RuntimeError("application migration exhausted its bounded credential generations")
            generation = self._append("generation", self.runtime.prepare_generation(ordinal))
            try:
                self.runtime.checkpoint()
                def persist(oid: int) -> None:
                    self._append("role", {"oid": oid})
                self.runtime.create(generation, persist)
                role = self._events()[-1]
                if role.phase != "role":
                    raise RuntimeError("application migration creation lacks durable identity")
                self.runtime.arm(generation, _integer(role.payload, "oid"))
                resources = self.runtime.resources(generation)
                self._append("secret-dispatch", {"manifest_sha256": _digest(resources.secret)})
                secret = resources.ensure_secret(creation_dispatched=True)
                self._append("secret", {"uid": secret.uid})
                self._append("job-dispatch", {"manifest_sha256": _digest(resources.job)})
                job = resources.ensure_job(creation_dispatched=True, expected_secret_uid=secret.uid)
                self._append("job", {"uid": job.uid})
                deadline = time.monotonic() + 660
                while not resources.job_complete(expected_uid=job.uid):
                    self.runtime.checkpoint()
                    if time.monotonic() >= deadline:
                        raise RuntimeError("application migration Job did not complete within its deadline")
                    time.sleep(0.25)
            except Exception:
                # Even a failed delivery may have committed. Persist and complete
                # retirement before deciding whether another generation is needed.
                self.runtime.release_creator()
                self._retire(successful=False)
                continue
            self.runtime.release_creator()
            self._retire(successful=True)
            self._require_target()
            return

    def _require_retired_history(self, events: Sequence[ApplicationMigrationEvent]) -> None:
        generation = None
        oid = None
        for event in events:
            if event.phase == "generation":
                generation = event
                oid = self._original_capacity_oid(events)
            elif event.phase == "role":
                oid = _integer(event.payload, "oid")
            elif event.phase in {"abandoned", "complete"}:
                assert generation is not None
                self.runtime.resources(generation).require_retired()
                self.runtime.require_role_retired(generation, oid)
        self.runtime.checkpoint()

    def _require_target(self) -> None:
        if self.runtime.read_revision() != self.migration.target_revision:
            raise RuntimeError("application migration completed target revision changed")
        self.runtime.checkpoint()

    def _retire(self, *, successful: bool) -> None:
        events = self._events()
        generations = [event for event in events if event.phase == "generation"]
        if not generations:
            raise RuntimeError("application migration retirement has no generation")
        generation = generations[-1]
        current = [event for event in events if event.sequence > generation.sequence]
        by_phase = {event.phase: event for event in current}
        role = by_phase.get("role")
        oid = self._original_capacity_oid(events) if role is None else _integer(role.payload, "oid")
        previous_peers = [
            _backend(event.payload["maintenance_backend"] if event.phase == "retirement" else event.payload["backend"])
            for event in current if event.phase in {"retirement", "maintenance-peer"}
        ]
        peer = self.runtime.begin_retirement(generation, previous_peers)
        if "retirement" not in by_phase:
            self._append("retirement", {"successful": successful, "maintenance_backend": asdict(peer)})
        elif peer != previous_peers[-1]:
            self._append("maintenance-peer", {"backend": asdict(peer)})
        if not self.runtime.role_exists(generation, oid):
            if "secret-dispatch" not in by_phase and "role-retire" not in by_phase:
                self.runtime.resources(generation).require_retired()
                self._append("abandoned", {})
                return
            if "role-retire" not in by_phase:
                raise RuntimeError("application migration delivered role disappeared before retirement")
        elif "role-retire" not in by_phase:
            assert oid is not None
            self.runtime.checkpoint()
            self.runtime.seal(generation, oid)
        assert oid is not None
        resources = self.runtime.resources(generation)
        # A lost creation reply is read-only reconciliation, never ensure/create.
        # The enclosing runtime validates both manifests against the dispatches.
        for phase, document in (("secret-dispatch", resources.secret), ("job-dispatch", resources.job)):
            if phase in by_phase and by_phase[phase].payload["manifest_sha256"] != _digest(document):
                raise RuntimeError("application migration original dispatch manifest changed")
        if "job-stopped" not in by_phase:
            self.runtime.checkpoint()
            expected = by_phase.get("job")
            job = resources.observe_job(expected_uid=None if expected is None else _string(expected.payload, "uid"))
            if job is not None:
                if "job-dispatch" not in by_phase:
                    raise RuntimeError("application migration found an undispatched Job")
                resources.delete_job(expected_uid=job.uid)
            self._append("job-stopped", {})
        if "closed" not in by_phase:
            self.runtime.checkpoint()
            self.runtime.close(generation, oid)
            self._append("closed", {})
        if "role-retired" not in by_phase:
            if "role-retire" not in by_phase:
                self._append("role-retire", {})
            self.runtime.checkpoint()
            self.runtime.retire(generation, oid)
            self.runtime.require_role_retired(generation, oid)
            self._append("role-retired", {})
        if "secret-deleted" not in by_phase:
            self.runtime.checkpoint()
            expected = by_phase.get("secret")
            secret = resources.observe_secret(expected_uid=None if expected is None else _string(expected.payload, "uid"))
            if secret is not None:
                if "secret-dispatch" not in by_phase:
                    raise RuntimeError("application migration found an undispatched Secret")
                resources.delete_secret(expected_uid=secret.uid)
            resources.require_retired()
            self._append("secret-deleted", {})
        if "reopened" not in by_phase:
            if "reopen" not in by_phase:
                self._append("reopen", {})
            self.runtime.checkpoint()
            resources.require_retired()
            self.runtime.require_role_retired(generation, oid)
            self.runtime.reopen(generation, oid)
            self._append("reopened", {})
        resources.require_retired()
        events = self._events()
        retirement = next(event for event in events if event.sequence > generation.sequence and event.phase == "retirement")
        revision = self.runtime.read_revision()
        self._append("complete", {"successful": retirement.payload["successful"], "revision": revision})

    def _original_capacity_oid(self, events: Sequence[ApplicationMigrationEvent]) -> int | None:
        if not self.migration.capacity_bootstrap:
            return None
        # The permanent identity was durably admitted before the generation.
        # A loss immediately after generation publication cannot erase it.
        identity = events[0].payload["guard_migrator"]
        if not isinstance(identity, Mapping):
            raise RuntimeError("application capacity original migrator identity changed")
        return _integer(identity, "role_oid")
