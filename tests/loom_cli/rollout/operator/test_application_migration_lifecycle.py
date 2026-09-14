"""Real journal ordering across interrupted migration effects; no live resources."""

import os
from dataclasses import asdict, replace

import pytest

from loom_cli.rollout.operator.protected_application_guard_retention import application_guard_is_retained
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component, _handoff
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
from tests.loom_cli.rollout.operator.test_application_migration_journal import _authority, _generation
from tests.loom_cli.rollout.operator.test_application_migration_resources import _resources


class ProcessLost(BaseException):
    pass


@pytest.mark.parametrize("interrupt", [None, "create", "arm", "seal", "close", "retire", "reopen"])
def test_migration_lifecycle_recovers_and_retires_before_delivering_fresh_credentials(tmp_path, interrupt):
    from loom_cli.rollout.operator.protected_application_migration_journal import ApplicationMigrationJournal
    from loom_cli.rollout.operator.protected_application_migration_lifecycle import ApplicationMigrationLifecycle

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    effects = []
    failure = [interrupt]
    role = [None]
    closed = [False]
    revision = [plan.schema_revision]
    last_resources = [None]
    maintenance = [777]
    def done(name):
        effects.append(name)
        if failure[0] == name:
            failure[0] = None
            raise ProcessLost(name)
    class Runtime:
        def checkpoint(self):
            pass
        def prepare_generation(self, ordinal):
            assert role[0] is None and not closed[0]
            return _generation(ordinal)
        def create(self, generation, persist):
            assert role[0] is None
            persist(90 + generation.payload["ordinal"])
            role[0] = 90 + generation.payload["ordinal"]
            done("create")
        def arm(self, generation, oid):
            assert role[0] == oid and not closed[0]
            done("arm")
        def resources(self, generation):
            if last_resources[0] is None or last_resources[0][0] != generation.event_digest:
                resources, runner = _resources(tmp_path)
                runner.objects.clear()
                original = runner.run_checked
                def run(argv, **kwargs):
                    original(argv, **kwargs)
                    if "create" in argv and "Job" in runner.objects:
                        runner.objects["Job"]["status"] = {"conditions": [{"type": "Complete", "status": "True"}], "succeeded": 1}
                        revision[0] = plan.migration_target_revision
                runner.run_checked = run
                last_resources[0] = generation.event_digest, resources
            return last_resources[0][1]
        def release_creator(self):
            pass
        def begin_retirement(self, generation, peers):
            maintenance[0] += 1
            return replace(_handoff(), pid=maintenance[0], database_oid=5)
        def role_exists(self, generation, oid):
            assert role[0] is None or role[0] == oid
            return role[0] is not None
        def seal(self, generation, oid):
            assert role[0] == oid
            done("seal")
        def close(self, generation, oid):
            assert role[0] == oid
            closed[0] = True
            done("close")
        def retire(self, generation, oid):
            assert closed[0]
            role[0] = None
            done("retire")
        def reopen(self, generation, oid):
            assert role[0] is None
            last_resources[0][1].require_retired()
            closed[0] = False
            done("reopen")
        def read_revision(self):
            assert not closed[0]
            return revision[0]
    runtime = Runtime()
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=0)
        if not migration.read():
            migration.append("authority", _authority(plan, component, guard), guard=guard)
        ApplicationMigrationLifecycle(migration=migration, guard=guard, runtime=runtime).run()
        raise RuntimeError("end test")
    component = replace(_component(apply), component_id="database-migration")
    if interrupt is not None:
        with pytest.raises(ProcessLost, match=interrupt):
            journal.execute(plan, [component])
    with pytest.raises(RuntimeError, match="end test"):
        journal.execute(plan, [component])
    migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=0)
    assert migration.read()[-1].phase in {"noop", "complete"}
    assert revision[0] == plan.migration_target_revision and role[0] is None and not closed[0]
    last_resources[0][1].require_retired()
    assert effects.count("create") <= 2
    if effects.count("create") == 2:
        assert effects.index("retire") < len(effects) - 1 - effects[::-1].index("create")
    before = list(effects)
    with pytest.raises(RuntimeError, match="end test"):
        journal.execute(plan, [component])
    assert effects == before
