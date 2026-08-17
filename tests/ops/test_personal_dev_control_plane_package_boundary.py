from __future__ import annotations

from pathlib import Path

from tests.unit.test_dev_instance_manifest import _immutable_config
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_manifest import _config as _builder_config

from loom.dev_instance import derive_identity
from loom.dev_instance_manifest import personal_dev_preparation_manifest_documents
from loom.personal_dev_builder_manifest import personal_dev_builder_manifest_documents

_ROOT = Path(__file__).resolve().parents[2]


def test_shadow_package_is_pure_render_only_and_has_no_legacy_extension() -> None:
    source = (_ROOT / "src/loom/personal_dev_control_plane_render.py").read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert "import kubernetes" not in source
    assert "slurm" not in source.casefold()
    assert "def apply" not in source
    assert "def activate" not in source
    assert "loom-dev-shared" not in source
    assert "0.0.0.0/0" not in source


def test_dynamic_personal_and_builder_namespaces_bind_read_authority_locally() -> None:
    personal = personal_dev_preparation_manifest_documents(
        derive_identity("alice"),
        _immutable_config(),
    )
    builder = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=_builder_config(),
    )

    for documents in (personal, builder):
        binding = next(
            item
            for item in documents
            if item["kind"] == "RoleBinding"
            and item["metadata"]["name"] == "loom-personal-dev-management"
        )
        assert binding["roleRef"]["name"] == "loom-personal-dev-managed-namespace"
        assert binding["subjects"] == [
            {
                "kind": "ServiceAccount",
                "name": "loom-personal-dev-management",
                "namespace": "loom-dev",
            }
        ]


def test_dev_fleet_package_never_creates_a_second_shared_namespace() -> None:
    for path in (_ROOT / "deploy/dev-fleet").glob("**/*"):
        if path.is_file():
            assert "loom-dev-shared" not in path.read_text(encoding="utf-8", errors="ignore")
