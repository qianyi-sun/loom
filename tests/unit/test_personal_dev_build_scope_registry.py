"""Protected registry publication must not drop another owner or rotation."""

import os
from hashlib import sha256
from importlib import import_module

import pytest

from loom_capacity_manager.contracts import canonical_bytes
from loom_service.personal_dev_build_management import BuildManagementServiceConfigV1
from tests.unit.test_capacity_agent_client import _owner_file


def registry_type():
    return import_module("loom_capacity_build_guard.scope_registry").BuildScopeRegistry


def test_registry_locks_private_directory_and_preserves_exact_replay(tmp_path):
    registry = registry_type()
    directory = tmp_path / "registry"
    directory.mkdir(mode=0o700)
    with registry(directory) as first:
        assert first.current is None
        with pytest.raises((ValueError, BlockingIOError)):
            with registry(directory):
                pytest.fail("concurrent registry owner acquired the same directory")
    with registry(directory) as reopened:
        assert reopened.current is None


@pytest.mark.parametrize("boundary", ["mode", "symlink", "invalid-file", "file-symlink", "file-mode"])
def test_registry_rejects_unsafe_or_invalid_existing_state(tmp_path, boundary):
    registry = registry_type()
    directory = tmp_path / "registry"
    directory.mkdir(mode=0o700)
    if boundary == "mode":
        directory.chmod(0o755)
    elif boundary == "symlink":
        link = tmp_path / "link"
        link.symlink_to(directory, target_is_directory=True)
        directory = link
    else:
        path = _owner_file(directory / "management.json", b'{"schema_version":1}')
        if boundary == "file-mode":
            path.chmod(0o644)
        elif boundary == "file-symlink":
            path.rename(directory / "target.json")
            path.symlink_to(directory / "target.json")
    with pytest.raises((ValueError, OSError)):
        with registry(directory):
            pytest.fail("unsafe registry was accepted")


def test_registry_atomic_publication_and_stale_writer_fencing(tmp_path, monkeypatch):
    from tests.integration.test_personal_dev_build_management_service import inputs
    from tests.unit.test_personal_dev_build_admission import admission_input
    from loom_capacity_build_guard.installation_store import BuildGuardInstallationV1, RetainedBuildInstallation, _identity
    from loom.personal_dev_build_platform_requests import runtime_installation_digest

    # Static candidate facts suffice here; startup/installer separately validate
    # committed private DB evidence. This test covers only filesystem authority.
    values = admission_input(tmp_path)
    member, runtime = values["member"], values["runtime"]
    config = member.configuration
    document = BuildGuardInstallationV1(id=_identity(config.subject_id, config.subject_incarnation, config.deployment_generation),
        owner_user_id=member.owner_id, subject_id=config.subject_id, subject_incarnation=config.subject_incarnation,
        deployment_generation=config.deployment_generation, candidate_generation=config.candidate_generation,
        reporter_incarnation=config.demand_reporter_incarnation,
        protected_admission_sha256=member.acknowledgement.protected_admission_sha256,
        runtime_installation_sha256=runtime_installation_digest(runtime, member.acknowledgement.protected_admission_sha256), runtime=runtime)
    retained = RetainedBuildInstallation(document, canonical_bytes(document))
    _settings, management, _admission = inputs((None, None, retained), tmp_path)
    scope = management.scopes[0]
    directory = tmp_path / "registry"
    directory.mkdir(mode=0o700)
    registry = registry_type()
    with registry(directory) as writer:
        proposed = writer.propose(scope, expected_sha256=None)
        writer.publish(proposed)
    wire = (directory / "management.json").read_bytes()
    assert wire == canonical_bytes(management)
    assert os.stat(directory / "management.json").st_mode & 0o777 == 0o600
    with registry(directory) as writer:
        assert writer.current == management
        # Exact retry tolerates its successful publication changing the old hash.
        assert writer.propose(scope, expected_sha256=None) == management
        from uuid import uuid4
        successor_document = document.model_copy(update={"deployment_generation": 2, "reporter_incarnation": uuid4(),
            "id": _identity(document.subject_id, document.subject_incarnation, 2)})
        successor = scope.model_copy(update={"installation": successor_document,
            "reporter": scope.reporter.model_copy(update={"deployment_generation": 2, "reporter_incarnation": successor_document.reporter_incarnation})})
        with pytest.raises(ValueError, match="registry changed"):
            writer.propose(successor, expected_sha256=None)
        proposed = writer.propose(successor, expected_sha256=sha256(wire).hexdigest())
        assert proposed.scopes == (scope, successor)
        original = os.replace
        def fail(*args, **kwargs):
            raise OSError("test full disk")
        monkeypatch.setattr(os, "replace", fail)
        with pytest.raises(OSError):
            writer.publish(proposed)
        assert (directory / "management.json").read_bytes() == wire
        assert sorted(path.name for path in directory.iterdir()) == ["management.json"]
        monkeypatch.setattr(os, "replace", original)
        writer.publish(proposed)
    assert BuildManagementServiceConfigV1.model_validate_json((directory / "management.json").read_bytes()).scopes == (scope, successor)
