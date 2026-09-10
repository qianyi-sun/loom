"""Native plans require the complete admitted source, never a digest-shaped hint."""

from copy import deepcopy

import pytest

from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_image_build_plan import TaskImageBuildPlanV2, derive_task_image_build_plan
from loom.task_image_materialization import task_image_materialization_key
from tests.unit.test_task_bundle_registration import _bundle
from tests.unit.test_task_image_build_plan import _authorization, _row


def _source_row(tmp_path):
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(_bundle(tmp_path), task_id="benchmark/native-source"),
        bucket="task-sources",
    )
    row = _row(
        task_id=spec.catalog_task_id,
        task_checksum=spec.manifest.task_checksum,
        cpu_arch="x86_64",
        task_config=spec.task_config,
        task_source=spec.source_uri,
        task_source_provenance=spec.provenance,
        bundle_content_manifest_sha256=spec.manifest.digest,
        materialization_key=task_image_materialization_key(
            task_id=spec.catalog_task_id, task_checksum=spec.manifest.task_checksum,
            cpu_arch="x86_64", bundle_content_manifest_sha256=spec.manifest.digest,
        ),
    )
    return spec, row


def test_derives_v2_only_from_complete_admitted_source(tmp_path):
    spec, row = _source_row(tmp_path)
    # Non-source upstream provenance is retained but does not replace source facts.
    row.task_source_provenance["upstream_revision"] = "retained-extra"
    plan = derive_task_image_build_plan(
        row, _authorization(cpu_arch="x86_64"), admitted_source=spec,
    )
    assert type(plan) is TaskImageBuildPlanV2
    assert plan.content_manifest_digest == spec.manifest.digest
    assert plan.bundle_prefix == spec.data_prefix
    assert plan.task_checksum == spec.manifest.task_checksum
    assert plan.components[0].dockerfile_path == "Dockerfile"


@pytest.mark.parametrize("field", [
    "task_id", "task_checksum", "task_source", "task_config", "task_source_provenance",
    "bundle_content_manifest_sha256", "materialization_key", "missing_provenance",
])
def test_admitted_spec_cannot_cover_a_different_frozen_image(tmp_path, field):
    spec, row = _source_row(tmp_path)
    if field == "task_config":
        row.task_config = deepcopy(row.task_config)
        row.task_config["environment"]["build_timeout_sec"] += 1
    elif field == "task_source_provenance":
        row.task_source_provenance["bundle_task_identity"]["bundle_task_id"] = "different"
    elif field == "missing_provenance":
        del row.task_source_provenance["bundle_content_manifest_sha256"]
    else:
        setattr(row, field, "a" * 64)
    with pytest.raises(ValueError):
        derive_task_image_build_plan(row, _authorization(cpu_arch="x86_64"), admitted_source=spec)


def test_strong_plan_requires_source_admission_even_with_valid_provenance(tmp_path):
    _spec, row = _source_row(tmp_path)
    with pytest.raises(ValueError):
        derive_task_image_build_plan(row, _authorization(cpu_arch="x86_64"))


def test_manifest_column_without_provenance_cannot_be_downgraded_to_v1():
    row = _row(bundle_content_manifest_sha256="6" * 64)
    with pytest.raises(ValueError):
        derive_task_image_build_plan(row, _authorization())
