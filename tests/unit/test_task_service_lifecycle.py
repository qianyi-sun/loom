from pathlib import PurePosixPath
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.models.exec import ExecResult
from loom.models.task import TaskConfig
from loom.service_execution_materialization import (
    compile_service_execution_plan,
    runtime_profile_rejections,
)
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_sandbox_task import Sandbox
from tests.unit.test_service_execution_terminus_plan import _inputs


def inputs():
    task, trial, profile = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["service_lifecycle"] = {
        "startup_command": ["/entrypoint.sh", "/bin/true"],
        "readiness": {"command": "test -f /tmp/ready", "retries": 0},
    }
    return TaskConfig.model_validate(raw), trial, profile


def test_lifecycle_requires_runtime_readiness_and_declares_startup_evidence():
    task, trial, profile = inputs()
    assert "service_lifecycle_runtime_unavailable" in runtime_profile_rejections(task, trial, profile)
    with pytest.raises(ValueError, match=r"service_lifecycle.*ready"):
        compile_service_execution_plan(task=task, trial=trial, profile=profile,
            source_provenance=_provenance(), task_revision_sha256=_REVISION)
    profile = profile.model_copy(update={"service_lifecycle_ready": True})
    plan = compile_service_execution_plan(task=task, trial=trial, profile=profile,
        source_provenance=_provenance(), task_revision_sha256=_REVISION)
    assert any(output.relative_path == "diagnostics/service-startup.json" for output in plan.output_declarations)
    assert "service_lifecycle_ready" not in inputs()[2].model_dump(mode="json")


def test_undeclared_handoff_options_do_not_change_existing_task_serialization():
    task, _, _ = _inputs()
    assert "mutable_paths" not in task.environment.model_dump(mode="json")
    assert "service_lifecycle" not in task.environment.model_dump(mode="json")
    assert "execution_requirements" not in task.environment.model_dump(mode="json")


@pytest.mark.parametrize("startup", [[""], ["/bin/sh", "bad\x00value"]])
def test_invalid_startup_argv_is_rejected(startup):
    task, _, _ = inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["service_lifecycle"]["startup_command"] = startup
    with pytest.raises(ValidationError):
        TaskConfig.model_validate(raw)


def test_startup_only_readiness_requires_initializer_and_preserves_old_defaults():
    from loom.models.task import ServiceLifecycleConfig

    default = ServiceLifecycleConfig(readiness={"command": "true"})
    assert "readiness_scope" not in default.model_dump()
    declared = ServiceLifecycleConfig(
        startup_command=("/start-listener",), readiness={"command": "true"},
        readiness_scope="startup_only",
    )
    assert declared.readiness_scope == "startup_only"
    with pytest.raises(ValueError, match="startup_only.*startup_command"):
        ServiceLifecycleConfig(readiness={"command": "true"}, readiness_scope="startup_only")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "startup", "snapshot", "verifier"])
async def test_service_survives_snapshot_until_private_verifier_finishes(tmp_path, monkeypatch, failure):
    from loom import service_execution_sandbox_task as module

    task, trial, _ = inputs()
    (tmp_path / "instruction.md").write_text("use the service")
    events = []

    class ServiceSandbox(Sandbox):
        async def pause_processes(self):
            events.append("pause")
            self.quiesced = True

        async def resume_processes(self):
            events.append("resume")
            self.quiesced = False

        async def stop_processes(self):
            events.append("kill")
            await super().stop_processes()

        async def export_workspace_archive(self, src, dst):
            assert events[-1] == "pause"
            events.append("snapshot")
            if failure == "snapshot":
                raise RuntimeError("snapshot failed")
            await super().export_workspace_archive(src, dst)

        async def run_healthcheck(self, hc=None):
            events.append("ready")

    agent, verifier, cleanup = ServiceSandbox(), Sandbox(), ServiceSandbox()
    connections = iter((agent, cleanup))
    monkeypatch.setattr(module, "sandbox_driver", lambda role, _: next(connections) if role == "task-sandbox" else verifier)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        events.append("agent")
        assert "ready" in events
        agent.filesystem[PurePosixPath("/app/result")] = b"answer"

    def execute(cmd, user, cwd, env):
        if "/entrypoint.sh" in cmd:
            events.append("startup")
            return ExecResult(return_code=int(failure == "startup"), stdout=b"started", stderr=b"", duration_sec=0)
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    def verify(cmd, user, cwd, env):
        if env and "LOOM_VERIFIER_OUTPUT" in env:
            events.append("verify")
            assert "resume" in events and "kill" not in events
            if failure == "verifier":
                raise RuntimeError("verifier failed")
            verifier.filesystem[PurePosixPath(env["LOOM_VERIFIER_OUTPUT"])] = b'{"rewards":{"passed":0}}'
        return ExecResult(return_code=0, stdout=b"", stderr=b"", duration_sec=0)

    agent.exec_handler, verifier.exec_handler = execute, verify
    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    if failure in {"startup", "snapshot"}:
        with pytest.raises(RuntimeError):
            await module.run_agent(tmp_path, task, trial)
        assert events[-1] == "kill"
        assert "resume" not in events
        assert ("agent" in events) is (failure == "snapshot")
    else:
        await module.run_agent(tmp_path, task, trial)
        assert events[-1] == "resume"
        if failure:
            with pytest.raises(RuntimeError, match="verifier failed"):
                await module.run_verifier(tmp_path, task, trial)
        else:
            await module.run_verifier(tmp_path, task, trial)
        assert events[-1] == "kill"
        assert events.index("snapshot") < events.index("resume") < events.index("verify")
    assert (tmp_path / ".loom/service-startup.json").is_file()


@pytest.mark.asyncio
@pytest.mark.parametrize("timed_out", [False, True])
async def test_cancelled_service_handoff_cleans_up_and_timeout_can_finalize(tmp_path, monkeypatch, timed_out):
    import asyncio
    import time

    from loom import service_execution_sandbox_task as module

    task, trial, _ = inputs()
    (tmp_path / "instruction.md").write_text("start a service")
    events = []

    class ServiceSandbox(Sandbox):
        async def pause_processes(self):
            events.append("pause")
            self.quiesced = True

        async def resume_processes(self):
            events.append("resume")

        async def stop_processes(self):
            events.append("kill")
            await super().stop_processes()

    driver = ServiceSandbox()
    monkeypatch.setattr(module, "sandbox_driver", lambda *_: driver)
    monkeypatch.setenv("LOOM_GATEWAY_URL", "http://127.0.0.1:9999")
    monkeypatch.setenv("LOOM_TASK_ARTIFACTS_JSON", "[]")
    if timed_out:
        monkeypatch.setenv("LOOM_EXECUTION_PHASE_DEADLINE", str(time.time() + 0.03))
        monkeypatch.setenv("LOOM_EXECUTION_TERMINATION_GRACE_SECONDS", "1")

    async def identity(_):
        return uuid4(), uuid4()

    async def terminus(**kwargs):
        if timed_out:
            await asyncio.sleep(10)
        raise asyncio.CancelledError

    monkeypatch.setattr(module, "_execution_identity", identity)
    monkeypatch.setattr(module, "run_terminus2", terminus)
    with pytest.raises(module.AgentTimeoutFinalizedError if timed_out else asyncio.CancelledError):
        await module.run_agent(tmp_path, task, trial)
    if timed_out:
        assert events == ["pause", "resume"]
    else:
        assert "kill" in events and "resume" not in events
    assert driver.state == "stopped"
