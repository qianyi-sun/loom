from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
    normalize_steps,
)


def _config(steps: list[StepConfig]) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t", name="t"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="x"),
        verifier=VerifierDefaults(name="pytest"),
        steps=steps,
    )


def test_normalize_synthesises_main_step_when_empty():
    cfg = _config([])
    normalized = normalize_steps(cfg)
    assert len(normalized.steps) == 1
    assert normalized.steps[0].name == "main"


def test_normalize_preserves_existing_steps():
    cfg = _config([StepConfig(name="phase-1"), StepConfig(name="phase-2")])
    normalized = normalize_steps(cfg)
    assert [s.name for s in normalized.steps] == ["phase-1", "phase-2"]
