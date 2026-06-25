from loom.models.networking import NoNetwork, Public
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    StepNetworkPlan,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from loom_control_plane.scheduler.requires_caps import derive_requires_caps


def _task(env: EnvironmentConfig) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=env,
        agent=AgentDefaults(name="x"),
        verifier=VerifierDefaults(name="pytest"),
        steps=[StepConfig(name="main")],
    )


def test_linux_no_gpu():
    env = EnvironmentConfig(os="linux", gpu_vendor="none")
    req = derive_requires_caps(_task(env))
    assert req.os == "linux"
    assert req.gpu_vendor == "none"
    assert "public" in req.network_policies


def test_gpu_required():
    env = EnvironmentConfig(os="linux", gpu_vendor="nvidia")
    req = derive_requires_caps(_task(env))
    assert req.gpu_vendor == "nvidia"


def test_cpu_architecture_required():
    env = EnvironmentConfig(os="linux", cpu_arch="arm64")
    req = derive_requires_caps(_task(env))
    assert req.cpu_arch == "arm64"


def test_cpu_architecture_defaults_to_x86_64():
    env = EnvironmentConfig(os="linux")
    req = derive_requires_caps(_task(env))
    assert req.cpu_arch == "x86_64"


def test_step_requires_no_network_adds_to_set():
    env = EnvironmentConfig(
        os="linux",
        network_policies_supported=frozenset({"public", "no-network"}),
    )
    task = _task(env).model_copy(update={"steps": [
        StepConfig(
            name="main",
            network=StepNetworkPlan(
                agent_phase=NoNetwork(), verifier_phase=Public(),
            ),
        ),
    ]})
    req = derive_requires_caps(task)
    assert "no-network" in req.network_policies
    assert "public" in req.network_policies
