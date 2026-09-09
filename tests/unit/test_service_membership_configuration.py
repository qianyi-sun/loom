"""Active service configuration is explicit and cannot reinterpret legacy evidence."""

import pytest

from loom_service.config import LoomServiceSettings
from loom_service.personal_dev_lifecycle import build_personal_dev_capacity_runtime
from tests.unit.test_service_dev_instance_runtime import _settings


def test_active_membership_settings_are_separate_and_disabled_by_default():
    fields = LoomServiceSettings.model_fields
    assert fields["personal_dev_runtime_mode"].default == "shadow"
    assert fields["personal_dev_membership_binding_json"].default == "{}"
    assert fields["personal_dev_membership_plan_sha256"].default == ""
    assert fields["personal_dev_membership_observer_principal_id"].default == ""
    assert fields["personal_dev_capacity_observer_bearer_token_file"].default != fields[
        "personal_dev_capacity_lifecycle_bearer_token_file"
    ].default


@pytest.mark.parametrize("enabled", (True, False))
@pytest.mark.parametrize("binding", (
    {"personal_dev_membership_plan_sha256": "a" * 64},
    {"personal_dev_membership_successor_plan_file": "/protected/successors.json"},
    {"personal_dev_membership_successor_plan_sha256": "b" * 64},
))
def test_legacy_mode_rejects_active_binding_instead_of_ignoring_it(tmp_path, monkeypatch, enabled, binding):
    settings = _settings(tmp_path, **binding)
    if not enabled:
        settings = settings.model_copy(update={"dev_instances_enabled": False, "personal_dev_runtime_mode": "shadow"})

    def unexpected(*args, **kwargs):
        pytest.fail("mixed legacy/active configuration opened installation credentials")

    monkeypatch.setattr(
        "loom_service.personal_dev_lifecycle.build_personal_dev_capacity_installation", unexpected,
    )
    with pytest.raises(RuntimeError, match="membership"):
        build_personal_dev_capacity_runtime(settings)
