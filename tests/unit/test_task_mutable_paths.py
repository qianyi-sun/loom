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
    ["/opt/verifier-tools"], ["/opt/verifier-tools/pytest/lib"],
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


def test_exact_mutable_reference_files_round_trip_without_changing_defaults():
    env = EnvironmentConfig(os="linux", workdir="/app", mutable_paths=["/root/.cache/pypoetry"],
                            mutable_path_reference_files=["/usr/local/bin/python3.9"])
    assert env.mutable_path_reference_files == (PurePosixPath("/usr/local/bin/python3.9"),)
    assert env.model_dump(mode="json")["mutable_path_reference_files"] == ["/usr/local/bin/python3.9"]
    assert "mutable_path_reference_files" not in EnvironmentConfig(os="linux").model_dump(mode="json")


@pytest.mark.parametrize("references", [
    ["/"], ["relative"], ["/usr/../tests/private"], ["/app/tool"], ["/cache/tool"],
    ["/tests/private"], ["/loom/tool"], ["/opt/verifier/bin/python"], ["/run/tool"],
    ["/opt/verifier-tools"], ["/opt/verifier-tools/pytest/lib/python3.13/site-packages/_pytest/main.py"],
    ["/usr/bin/python", "/usr/bin/python"], [f"/usr/bin/ref{i}" for i in range(17)],
])
def test_reference_files_cannot_overlap_transferred_or_private_state(references):
    with pytest.raises(ValidationError):
        EnvironmentConfig(os="linux", workdir="/app", mutable_paths=["/cache"],
                          mutable_path_reference_files=references)


def test_reference_files_require_mutable_directory_declaration():
    with pytest.raises(ValidationError):
        EnvironmentConfig(os="linux", mutable_path_reference_files=["/usr/bin/python"])
