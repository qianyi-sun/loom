"""Cold fixture downloads have their own bounded phase, not test deadlines."""

import subprocess
from dataclasses import replace

import pytest

from tests.support import minio_images


@pytest.mark.parametrize("cached", [False, True])
def test_minio_preparation_inspects_exact_images_and_only_pulls_missing(monkeypatch, cached):
    observed = []

    def run(command, **kwargs):
        observed.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0 if cached or command[1] == "pull" else 1)

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(minio_images, "_source_fixture_cached", lambda spec: False)
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
    monkeypatch.setattr(minio_images, "_source_fixture_cached", lambda spec: False)
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


def test_unavailable_upstream_builds_the_same_release_from_pinned_source(monkeypatch):
    original = minio_images.MINIO_TESTCONTAINERS_IMAGE
    builds = []

    def run(command, **kwargs):
        if command[1] == "pull":
            raise subprocess.CalledProcessError(1, command, stderr="unauthorized")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="")

    def build(spec):
        builds.append(spec)
        return "local-source-fixture"

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(minio_images, "_build_source_fixture", build, raising=False)
    assert minio_images.prepare_test_image(original) == "local-source-fixture"
    assert len(builds) == 1
    assert builds[0].commit == "5655272f5a62f827e6baea6a4cb21a4c3f065c2c"
    assert builds[0].release == "RELEASE.2022-12-02T19-19-22Z"


def test_unknown_image_is_not_accepted_as_a_fixture():
    with pytest.raises(ValueError, match="fixture"):
        minio_images.prepare_test_image("example.invalid/unreviewed:latest")


def test_bad_source_digest_fails_before_building(monkeypatch, tmp_path):
    spec = replace(minio_images.SOURCE_FIXTURES[0], source_sha256="0" * 64)
    monkeypatch.setattr(minio_images, "_fetch_source", lambda url, path: path.write_bytes(b"tampered archive"), raising=False)
    with pytest.raises(ValueError, match="checksum"):
        minio_images._download_source(spec, tmp_path / "source.tar.gz")
    assert not (tmp_path / "source.tar.gz").exists()


def test_rebuilt_fixture_cache_with_wrong_source_label_is_rejected(monkeypatch):
    spec = minio_images.SOURCE_FIXTURES[0]
    monkeypatch.setattr(subprocess, "run", lambda command, **kwargs:
        subprocess.CompletedProcess(command, 0, stdout='{"io.loom.fixture.commit":"wrong"}'))
    assert not minio_images._source_fixture_cached(spec)


def test_source_build_failure_propagates(monkeypatch):
    def unavailable(command, **kwargs):
        if command[1] == "pull":
            raise subprocess.CalledProcessError(1, command, stderr="unauthorized")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="")

    def failed(spec):
        raise RuntimeError("source build failed")

    monkeypatch.setattr(subprocess, "run", unavailable)
    monkeypatch.setattr(minio_images, "_build_source_fixture", failed, raising=False)
    with pytest.raises(RuntimeError, match="source build failed"):
        minio_images.prepare_test_image(minio_images.MINIO_TLS_IMAGE)


def test_verified_source_cache_avoids_unavailable_registry(monkeypatch):
    import json

    spec = minio_images.SOURCE_FIXTURES[0]
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if "--format" in command:
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps(minio_images._labels(spec)))
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr(subprocess, "run", run)
    assert minio_images.prepare_test_image(spec.image) == minio_images._source_tag(spec)
    assert not any(command[1] in ("pull", "build") for command in commands)


@pytest.mark.parametrize("failure", ["version", "execution"])
def test_unverified_source_build_is_removed_from_cache(monkeypatch, failure):
    spec = minio_images.SOURCE_FIXTURES[0]
    commands = []
    monkeypatch.setattr(minio_images, "_download_source", lambda spec, path: path.write_bytes(b"checked"))

    def run(command, **kwargs):
        commands.append(command)
        if command[1] == "run" and failure == "execution":
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0, stdout="minio version WRONG")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises((ValueError, subprocess.CalledProcessError)):
        minio_images._build_source_fixture(spec)
    assert not any(command[1] == "tag" for command in commands)
    build = next(command for command in commands if command[1] == "build")
    provisional = build[build.index("--tag") + 1]
    assert provisional != minio_images._source_tag(spec)
    assert ["docker", "image", "rm", provisional] in commands


def test_source_fetch_has_a_total_wall_clock_deadline(monkeypatch, tmp_path):
    calls = []

    def expired(command, **kwargs):
        calls.append((command, kwargs))
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", expired)
    with pytest.raises(subprocess.TimeoutExpired):
        minio_images._fetch_source("https://example.invalid/source", tmp_path / "source.tar.gz")
    assert calls[0][1]["timeout"] == 120


def test_failed_source_download_removes_partial_archive(monkeypatch, tmp_path):
    destination = tmp_path / "source.tar.gz"

    def partial(url, path):
        path.write_bytes(b"partial")
        raise subprocess.TimeoutExpired("source-download", 120)

    monkeypatch.setattr(minio_images, "_fetch_source", partial, raising=False)
    with pytest.raises(subprocess.TimeoutExpired):
        minio_images._download_source(minio_images.SOURCE_FIXTURES[0], destination)
    assert not destination.exists()


def test_source_cache_is_published_only_after_version_validation(monkeypatch):
    spec = minio_images.SOURCE_FIXTURES[0]
    commands = []
    monkeypatch.setattr(minio_images, "_download_source", lambda spec, path: path.write_bytes(b"checked"))

    def run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0,
            stdout=f"minio version {spec.release} (commit-id={spec.commit})")

    monkeypatch.setattr(subprocess, "run", run)
    assert minio_images._build_source_fixture(spec) == minio_images._source_tag(spec)
    validate_index = next(i for i, command in enumerate(commands) if command[1] == "run")
    publish_index = next(i for i, command in enumerate(commands) if command[1] == "tag")
    assert validate_index < publish_index
    assert commands[publish_index][-1] == minio_images._source_tag(spec)
