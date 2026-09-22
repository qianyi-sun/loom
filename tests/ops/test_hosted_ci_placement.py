"""Hosted validation and publication must not depend on retired runner pools."""

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("workflow", "job"),
    [
        ("ci", "lint-and-static"),
        ("ci", "tests-root"),
        ("ci", "tests-packages"),
        ("ci", "runtime-payload"),
        ("ci", "go-checks"),
        ("ci", "web-checks"),
        ("ci", "integration-docker"),
        ("cluster-smoke", "cluster-contract"),
        ("staging-smoke", "system-smoke"),
    ],
)
def test_validation_has_direct_hosted_placement(workflow: str, job: str) -> None:
    document = yaml.safe_load((ROOT / f".github/workflows/{workflow}.yml").read_text())
    selected = document["jobs"][job]
    assert selected["runs-on"] == "ubuntu-24.04"
    assert all(not dependency.endswith("-route") for dependency in selected["needs"])


def test_image_builds_preserve_native_architecture_on_hosted_runners() -> None:
    document = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())
    build = document["jobs"]["build"]
    assert build["runs-on"] == "ubuntu-24.04"
    assert set(build["needs"]) == {"plan", "trivy-binary"}
