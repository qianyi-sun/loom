"""Cold fixture downloads have their own bounded phase, not test deadlines."""

import subprocess

import pytest

from tests.support import minio_images


@pytest.mark.parametrize("cached", [False, True])
def test_minio_preparation_inspects_exact_images_and_only_pulls_missing(monkeypatch, cached):
    observed = []

    def run(command, **kwargs):
        observed.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0 if cached or command[1] == "pull" else 1)

    monkeypatch.setattr(subprocess, "run", run)
    minio_images.prepare_test_images()
    images = (minio_images.MINIO_TESTCONTAINERS_IMAGE, minio_images.MINIO_TLS_IMAGE)
    assert [call[0] for call in observed] == [command for image in images for command in (
        [["docker", "image", "inspect", image]] if cached else
        [["docker", "image", "inspect", image], ["docker", "pull", image]])]
    for command, kwargs in observed:
        assert kwargs["timeout"] == (180 if command[1] == "pull" else 10)
        assert kwargs["check"] is (command[1] == "pull")


def test_minio_download_failure_is_not_retried_or_ignored(monkeypatch):
    observed = []

    def run(command, **kwargs):
        observed.append(command)
        if command[1] == "pull":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(subprocess.TimeoutExpired):
        minio_images.prepare_test_images()
    assert len(observed) == 2


def test_both_integration_jobs_prepare_images_before_pytest():
    from pathlib import Path

    import yaml

    workflow = yaml.safe_load((Path(__file__).resolve().parents[2] / ".github/workflows/ci.yml").read_text())
    for name in ("integration", "integration-docker"):
        steps = workflow["jobs"][name]["steps"]
        preparation = next(index for index, step in enumerate(steps)
            if "python -m tests.support.minio_images" in step.get("run", ""))
        execution = next(index for index, step in enumerate(steps) if "pytest " in step.get("run", ""))
        assert preparation < execution
