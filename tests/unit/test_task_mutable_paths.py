from pathlib import PurePosixPath

import pytest
from pydantic import ValidationError

from loom.models.task import EnvironmentConfig, TaskConfig
from loom.service_execution_materialization import compile_service_execution_plan
from tests.unit.test_service_execution_materialization import _REVISION, _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


def test_declared_paths_round_trip_and_become_required_execution_outputs():
    task, trial, profile = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["mutable_paths"] = ["/data", "/home/agent"]
    task = TaskConfig.model_validate(raw)
    assert task.environment.mutable_paths == (PurePosixPath("/data"), PurePosixPath("/home/agent"))
    plan = compile_service_execution_plan(
        task=task, trial=trial, profile=profile, source_provenance=_provenance(),
        task_revision_sha256=_REVISION,
    )
    outputs = {item.relative_path: item for item in plan.output_declarations}
    assert outputs["artifacts/mutable-paths/0.tar"].required
    assert outputs["artifacts/mutable-paths/1.tar"].required
    assert outputs["artifacts/mutable-paths/manifest.json"].required


@pytest.mark.parametrize("paths", [
    ["/"], ["relative"], ["/data/../tests"], ["/data", "/data/sub"],
    ["/data", "/data"], ["/app"], ["/app/tests"], ["/loom"], ["/proc"],
    ["/run"], ["/var"], ["/tests"], ["/solution"], ["/tmp"],
    ["/opt"], ["/opt/verifier"], ["/opt/verifier-python"], ["/opt/verifier-assets"],
    ["/data\x00bad"], [f"/data{i}" for i in range(17)],
])
def test_unsafe_or_ambiguous_path_declarations_are_rejected(paths):
    with pytest.raises(ValidationError):
        EnvironmentConfig(os="linux", workdir="/app", mutable_paths=paths)


def test_original_path_classes_are_not_rewritten():
    paths = ["/media/sf_livro-de-asa", "/opt/git", "/data", "/root/Documents/projects/permutation",
             "/build", "/mnt", "/home/agent"]
    env = EnvironmentConfig(os="linux", workdir="/app", mutable_paths=paths)
    assert env.model_dump(mode="json")["mutable_paths"] == paths
