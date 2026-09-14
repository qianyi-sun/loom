from datetime import UTC, datetime

from tests.support.execution_image_admission import signed_image_admission_bundle

from loom.agent_runtime import AgentRuntimeReleaseV1


def release(version: str = "harbor-a", digit: str = "8") -> AgentRuntimeReleaseV1:
    image = "registry.example/harbor@sha256:" + digit * 64
    return AgentRuntimeReleaseV1(
        schema_version="loom.agent-runtime-release.v1",
        agent_name="terminus-2",
        agent_version=version,
        runtime_contract="loom.terminus-controller.v1",
        agent_image_ref=image,
        harbor_version="0.18.0",
        harbor_source_revision="a" * 40,
        loom_bridge_revision="1.0",
        publisher_source_revision="b" * 40,
        image_admission=signed_image_admission_bundle(
            (image,),
            now=datetime(2026, 1, 1, tzinfo=UTC),
        ).admissions[0],
    )
