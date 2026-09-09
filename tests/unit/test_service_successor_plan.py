"""Only a paired owner-only plan file and digest may supply recovery bindings."""

import hashlib
from importlib import import_module
from types import SimpleNamespace

import pytest

from tests.unit.test_personal_dev_membership_successor_plan import _plan, _wire


@pytest.mark.parametrize("case", ("ready", "absent", "file_only", "digest_only", "mode", "permissions", "symlink", "digest"))
def test_service_loads_only_paired_protected_successor_plan(tmp_path, case):
    module = import_module("loom_service.personal_dev_membership")
    binding, document = _plan()
    payload = _wire(document)
    path = tmp_path / "reviewed-successors.json"
    path.write_bytes(payload)
    path.chmod(0o600)
    settings = SimpleNamespace(
        personal_dev_runtime_mode="membership-v1",
        personal_dev_membership_successor_plan_file=str(path),
        personal_dev_membership_successor_plan_sha256=hashlib.sha256(payload).hexdigest(),
    )
    if case in {"absent", "digest_only"}:
        settings.personal_dev_membership_successor_plan_file = ""
    if case in {"absent", "file_only"}:
        settings.personal_dev_membership_successor_plan_sha256 = ""
    if case == "mode":
        settings.personal_dev_runtime_mode = "shadow"
    if case == "permissions":
        path.chmod(0o644)
    if case == "symlink":
        link = tmp_path / "linked-plan"
        link.symlink_to(path)
        settings.personal_dev_membership_successor_plan_file = str(link)
    if case == "digest":
        settings.personal_dev_membership_successor_plan_sha256 = "f" * 64
    if case in {"ready", "absent"}:
        result = module.load_membership_successor_bindings(settings, binding.authority)
        assert len(result) == (2 if case == "ready" else 0)
    else:
        with pytest.raises((OSError, ValueError)):
            module.load_membership_successor_bindings(settings, binding.authority)
