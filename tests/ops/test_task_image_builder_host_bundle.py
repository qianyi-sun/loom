from __future__ import annotations

import json
import os
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_host_bundle as bundle
from tests.ops.test_task_image_builder_host_release import (
    BundleFixture,
    FixtureRunner,
    _bundle_fixture,
    _write,
)


@dataclass
class FixtureFetcher:
    responses: dict[str, bytes]
    requested: list[str]

    def fetch(self, url: str, destination: Path, maximum: int) -> None:
        self.requested.append(url)
        try:
            payload = self.responses[url]
        except KeyError as exc:
            raise bundle.BundleAssemblyError("fixture response is missing") from exc
        if len(payload) > maximum:
            raise bundle.BundleAssemblyError("fixture response exceeds its size limit")
        _write(destination, payload, mode=0o400)


@dataclass
class AssemblyFixture:
    release_path: Path
    runtime_path: Path
    keyring_path: Path
    fetcher: FixtureFetcher
    expected_urls: set[str]
    source: BundleFixture

    def install_runner(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(bundle, "SubprocessCommandRunner", lambda: FixtureRunner(self.source))

    def assemble(self, output: Path) -> str:
        return bundle.assemble_host_bundle(
            release_path=self.release_path,
            runtime_manifest_path=self.runtime_path,
            keyring_path=self.keyring_path,
            architecture="x86_64",
            output=output,
            fetcher=self.fetcher,
        )


def release_fixture(tmp_path: Path) -> AssemblyFixture:
    source = _bundle_fixture(tmp_path / "authority")
    release = json.loads(source.release_path.read_text(encoding="utf-8"))
    runtime = json.loads(source.runtime_path.read_text(encoding="utf-8"))
    repository = release["repositories"][source.debian_architecture]
    responses: dict[str, bytes] = {}

    for suite, index in repository["indexes"].items():
        responses[f"{repository['base_url']}/{index['inrelease_path']}"] = (
            source.bundle / "apt" / f"{suite}.InRelease"
        ).read_bytes()
        responses[f"{repository['base_url']}/{index['packages_path']}"] = (
            source.bundle / "apt" / f"{suite}.Packages.xz"
        ).read_bytes()
    for artifact in release["packages"][source.debian_architecture].values():
        responses[f"{repository['base_url']}/{artifact['filename']}"] = (
            source.bundle / "packages" / Path(artifact["filename"]).name
        ).read_bytes()
    for artifact in runtime["architectures"][source.architecture]["artifacts"]:
        responses[artifact["url"]] = (
            source.bundle / "runtime" / artifact["name"]
        ).read_bytes()

    return AssemblyFixture(
        release_path=source.release_path,
        runtime_path=source.runtime_path,
        keyring_path=source.bundle / "ubuntu-archive-keyring.gpg",
        fetcher=FixtureFetcher(responses, []),
        expected_urls=set(responses),
        source=source,
    )


def _temporary_outputs(parent: Path, output: Path) -> list[Path]:
    return list(parent.glob(f".{output.name}.loom-tmp-*"))


def test_assembler_fetches_exact_closure_verifies_and_publishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = release_fixture(tmp_path)
    fixture.install_runner(monkeypatch)
    output = tmp_path / "published-bundle"

    digest = fixture.assemble(output)

    assert len(digest) == 64
    assert set(fixture.fetcher.requested) == fixture.expected_urls
    assert len(fixture.fetcher.requested) == len(fixture.expected_urls) == 11
    assert output.is_dir()
    assert not _temporary_outputs(tmp_path, output)


def test_assembler_never_replaces_existing_output(tmp_path: Path) -> None:
    fixture = release_fixture(tmp_path)
    output = tmp_path / "published-bundle"
    output.mkdir()
    marker = output / "owned"
    marker.write_text("preserve", encoding="utf-8")

    with pytest.raises(bundle.BundleAssemblyError, match="already exists"):
        fixture.assemble(output)

    assert marker.read_text(encoding="utf-8") == "preserve"
    assert fixture.fetcher.requested == []
    assert not _temporary_outputs(tmp_path, output)


@pytest.mark.parametrize("failure", ["missing", "changed", "signature"])
def test_assembler_verification_failures_leave_no_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    fixture = release_fixture(tmp_path)
    fixture.install_runner(monkeypatch)
    output = tmp_path / "published-bundle"
    first_url = sorted(fixture.expected_urls)[0]
    if failure == "missing":
        del fixture.fetcher.responses[first_url]
    elif failure == "changed":
        fixture.fetcher.responses[first_url] += b"changed\n"
    else:
        fixture.source.signature_valid = False

    with pytest.raises(bundle.BundleAssemblyError):
        fixture.assemble(output)

    assert not os.path.lexists(output)
    assert not _temporary_outputs(tmp_path, output)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.invalid/runtime",
        "https://user:secret@example.invalid/runtime",
    ],
)
def test_assembler_rejects_unsafe_runtime_urls(
    tmp_path: Path,
    url: str,
) -> None:
    fixture = release_fixture(tmp_path)
    runtime = json.loads(fixture.runtime_path.read_text(encoding="utf-8"))
    runtime["architectures"]["x86_64"]["artifacts"][0]["url"] = url
    fixture.runtime_path.write_text(json.dumps(runtime), encoding="utf-8")
    output = tmp_path / "published-bundle"

    with pytest.raises(bundle.BundleAssemblyError, match="runtime URL"):
        fixture.assemble(output)

    assert not os.path.lexists(output)
    assert fixture.fetcher.requested == []


def test_assembler_rejects_non_snapshot_ubuntu_authority(tmp_path: Path) -> None:
    fixture = release_fixture(tmp_path)
    release = json.loads(fixture.release_path.read_text(encoding="utf-8"))
    release["repositories"]["amd64"]["base_url"] = "https://archive.ubuntu.com/ubuntu"
    fixture.release_path.write_text(json.dumps(release), encoding="utf-8")
    output = tmp_path / "published-bundle"

    with pytest.raises(bundle.BundleAssemblyError, match="repository base URL"):
        fixture.assemble(output)

    assert not os.path.lexists(output)
    assert fixture.fetcher.requested == []


def test_assembler_fsync_failure_cleans_only_its_temporary_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = release_fixture(tmp_path)
    fixture.install_runner(monkeypatch)
    sibling = tmp_path / "sibling"
    sibling.write_text("preserve", encoding="utf-8")
    output = tmp_path / "published-bundle"

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("fixture fsync failure")

    monkeypatch.setattr(bundle.os, "fsync", fail_fsync)
    with pytest.raises(bundle.BundleAssemblyError, match="fsync"):
        fixture.assemble(output)

    assert sibling.read_text(encoding="utf-8") == "preserve"
    assert not os.path.lexists(output)
    assert not _temporary_outputs(tmp_path, output)


def test_assembler_surfaces_cleanup_failure_after_attempting_all_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = release_fixture(tmp_path)
    fixture.install_runner(monkeypatch)
    output = tmp_path / "published-bundle"
    real_rmtree = bundle.shutil.rmtree
    cleanup_attempts: list[Path] = []

    def fail_cleanup(path: str | Path, *args: object, **kwargs: object) -> None:
        del args, kwargs
        target = Path(path)
        cleanup_attempts.append(target)
        if target.name.startswith("loom-host-bundle-snapshot-") or target.name.startswith(
            f".{output.name}.loom-tmp-"
        ):
            raise OSError("injected cleanup failure")
        real_rmtree(path)

    monkeypatch.setattr(bundle.shutil, "rmtree", fail_cleanup)
    try:
        with pytest.raises(bundle.BundleAssemblyError, match="bundle assembly cleanup failed") as exc:
            fixture.assemble(output)

        assert isinstance(exc.value.__cause__, bundle.host_release.HostReleaseError)
        assert isinstance(exc.value.__cause__.__cause__, OSError)
        assert isinstance(exc.value.__context__, bundle.BundleAssemblyError)
        assert sum(path.name.startswith("loom-host-bundle-snapshot-") for path in cleanup_attempts) >= 2
        assert any(path.name.startswith(f".{output.name}.loom-tmp-") for path in cleanup_attempts)
        assert _temporary_outputs(tmp_path, output)
    finally:
        monkeypatch.undo()
        snapshot_paths = {
            path
            for path in cleanup_attempts
            if path.name.startswith("loom-host-bundle-snapshot-")
        }
        for snapshot in snapshot_paths:
            if snapshot.parent != Path(tempfile.gettempdir()):
                raise AssertionError("fixture snapshot cleanup path is unsafe")
            if os.path.lexists(snapshot):
                real_rmtree(snapshot)
        for temporary in _temporary_outputs(tmp_path, output):
            real_rmtree(temporary)


def test_assembler_directory_fsync_failure_prevents_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = release_fixture(tmp_path)
    fixture.install_runner(monkeypatch)
    output = tmp_path / "published-bundle"

    def fail_directory_fsync(_path: Path) -> None:
        raise bundle.BundleAssemblyError("directory fsync failed")

    monkeypatch.setattr(bundle, "_fsync_directory", fail_directory_fsync)
    with pytest.raises(bundle.BundleAssemblyError, match="directory fsync"):
        fixture.assemble(output)

    assert not os.path.lexists(output)
    assert not _temporary_outputs(tmp_path, output)


def test_assembler_rename_collision_preserves_competing_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = release_fixture(tmp_path)
    fixture.install_runner(monkeypatch)
    output = tmp_path / "published-bundle"

    def collide(_source: Path, destination: Path) -> None:
        destination.mkdir()
        (destination / "competitor").write_text("preserve", encoding="utf-8")
        raise bundle.BundleAssemblyError("output already exists")

    monkeypatch.setattr(bundle, "_rename_noreplace", collide)
    with pytest.raises(bundle.BundleAssemblyError, match="already exists"):
        fixture.assemble(output)

    assert (output / "competitor").read_text(encoding="utf-8") == "preserve"
    assert not _temporary_outputs(tmp_path, output)


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, count: int) -> bytes:
        result = self.payload[self.offset : self.offset + count]
        self.offset += len(result)
        return result


class FakeOpener:
    def __init__(self, result: FakeResponse | BaseException) -> None:
        self.result = result

    def open(self, _request: urllib.request.Request, *, timeout: float) -> FakeResponse:
        assert 0 < timeout <= 60
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def test_https_fetcher_rejects_oversized_response_and_removes_partial_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opener = FakeOpener(FakeResponse(b"12345"))
    monkeypatch.setattr(bundle.urllib.request, "build_opener", lambda *_args: opener)
    destination = tmp_path / "artifact"

    with pytest.raises(bundle.BundleAssemblyError, match="size limit"):
        bundle.HttpsArtifactFetcher().fetch(
            "https://example.invalid/artifact",
            destination,
            4,
        )

    assert not os.path.lexists(destination)


def test_https_fetcher_rejects_non_https_redirect() -> None:
    handler = bundle._HttpsOnlyRedirectHandler()
    request = urllib.request.Request("https://example.invalid/artifact")

    with pytest.raises(bundle.BundleAssemblyError, match="non-HTTPS redirect"):
        handler.redirect_request(
            request,
            None,
            302,
            "Found",
            {},
            "http://example.invalid/artifact",
        )


def test_https_fetcher_wraps_missing_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opener = FakeOpener(urllib.error.URLError("missing"))
    monkeypatch.setattr(bundle.urllib.request, "build_opener", lambda *_args: opener)

    with pytest.raises(bundle.BundleAssemblyError, match="fetch failed"):
        bundle.HttpsArtifactFetcher().fetch(
            "https://example.invalid/missing",
            tmp_path / "artifact",
            100,
        )
