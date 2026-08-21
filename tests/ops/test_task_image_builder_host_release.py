from __future__ import annotations

import hashlib
import io
import json
import lzma
import os
import stat
import tarfile
from dataclasses import dataclass, replace
from pathlib import Path

import pytest
from scripts.ops import task_image_builder_host_release as host_release

SIGNER = "F6ECB3762474EDA9D21B7022871920D1991BC93C"


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _tar_gz(files: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, payload in files.items():
            info = tarfile.TarInfo(name)
            info.mode = 0o755
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return output.getvalue()


@dataclass
class BundleFixture:
    bundle: Path
    release_path: Path
    runtime_path: Path
    release: dict[str, object]
    package_payloads: dict[str, bytes]
    architecture: str
    debian_architecture: str
    signer: str = SIGNER
    signature_valid: bool = True
    dynamic_runtime: bool = False
    unexpected_setuid: bool = False

    def write_release(self) -> None:
        _write(self.release_path, json.dumps(self.release).encode())

    def append_signed_index_bytes(self, suite: str, suffix: bytes) -> None:
        path = self.bundle / "apt" / f"{suite}.Packages.xz"
        path.write_bytes(path.read_bytes() + suffix)
        _rewrite_index_pins_and_inrelease(self, suite)


class FixtureRunner:
    def __init__(self, fixture: BundleFixture) -> None:
        self.fixture = fixture

    def run(
        self,
        args: tuple[str, ...] | list[str],
        *,
        input_bytes: bytes | None = None,
    ) -> host_release.CommandResult:
        del input_bytes
        command = tuple(args)
        if command[0] == "/usr/bin/gpgv":
            if not self.fixture.signature_valid:
                return host_release.CommandResult(1, "", "BAD signature")
            return host_release.CommandResult(
                0,
                f"[GNUPG:] VALIDSIG {self.fixture.signer} 1787165159 0 4 0 1 10 01\n",
                "",
            )
        if command[:2] == ("/usr/bin/dpkg-deb", "--field"):
            package_path = Path(command[2])
            name = package_path.name
            if name.startswith("libsubid4_"):
                package, version = "libsubid4", "1:4.13+dfsg1-4ubuntu3.2"
            elif name.startswith("uidmap_"):
                package, version = "uidmap", "1:4.13+dfsg1-4ubuntu3.2"
            else:
                package, version = "quota", "4.06-1build6"
            values = {
                "Package": package,
                "Version": version,
                "Architecture": self.fixture.debian_architecture,
            }
            if len(command) != 4 or command[3] not in values:
                return host_release.CommandResult(2, "", "one field is required")
            return host_release.CommandResult(0, values[command[3]] + "\n", "")
        if command[:2] == ("/usr/bin/dpkg-deb", "--contents"):
            name = Path(command[2]).name
            lines = ["-rw-r--r-- root/root 1 2026-01-01 00:00 ./usr/share/doc/file"]
            if name.startswith("uidmap_"):
                lines.extend(
                    [
                        "-rwsr-xr-x root/root 1 2026-01-01 00:00 ./usr/bin/newuidmap",
                        "-rwsr-xr-x root/root 1 2026-01-01 00:00 ./usr/bin/newgidmap",
                    ]
                )
            if self.fixture.unexpected_setuid:
                lines.append("-rwsr-xr-x root/root 1 2026-01-01 00:00 ./usr/bin/extra-root")
            return host_release.CommandResult(0, "\n".join(lines) + "\n", "")
        if command[0] == "/usr/bin/readelf":
            if self.fixture.dynamic_runtime:
                return host_release.CommandResult(
                    0,
                    " 0x0000000000000001 (NEEDED) Shared library: [libc.so.6]\n",
                    "",
                )
            return host_release.CommandResult(0, "There is no dynamic section in this file.\n", "")
        raise AssertionError(f"unexpected command: {command!r}")


def _write(path: Path, payload: bytes, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)


def _xz_streams(payload: bytes, *, concatenate: bool = False, padding: int = 0) -> bytes:
    if not concatenate:
        return lzma.compress(payload)
    midpoint = len(payload) // 2
    return lzma.compress(payload[:midpoint]) + (b"\0" * padding) + lzma.compress(
        payload[midpoint:]
    )


def _inrelease(suite: str, packages_xz: bytes, debian_architecture: str) -> bytes:
    return (
        "-----BEGIN PGP SIGNED MESSAGE-----\n"
        "Hash: SHA512\n\n"
        f"Suite: {suite}\n"
        "SHA256:\n"
        f" {_sha256(packages_xz)} {len(packages_xz)} "
        f"main/binary-{debian_architecture}/Packages.xz\n"
        "-----BEGIN PGP SIGNATURE-----\nfixture\n-----END PGP SIGNATURE-----\n"
    ).encode()


def _release_indexes(fixture: BundleFixture) -> dict[str, dict[str, object]]:
    repositories = fixture.release["repositories"]
    assert isinstance(repositories, dict)
    repository = repositories[fixture.debian_architecture]
    assert isinstance(repository, dict)
    indexes = repository["indexes"]
    assert isinstance(indexes, dict)
    return indexes


def _rewrite_inrelease_pin(fixture: BundleFixture, suite: str) -> None:
    inrelease = (fixture.bundle / "apt" / f"{suite}.InRelease").read_bytes()
    index = _release_indexes(fixture)[suite]
    assert isinstance(index, dict)
    index["inrelease_size"] = len(inrelease)
    index["inrelease_sha256"] = _sha256(inrelease)
    fixture.write_release()


def _rewrite_index_pins_and_inrelease(fixture: BundleFixture, suite: str) -> None:
    packages = (fixture.bundle / "apt" / f"{suite}.Packages.xz").read_bytes()
    inrelease_path = fixture.bundle / "apt" / f"{suite}.InRelease"
    _write(inrelease_path, _inrelease(suite, packages, fixture.debian_architecture))
    index = _release_indexes(fixture)[suite]
    assert isinstance(index, dict)
    index["packages_size"] = len(packages)
    index["packages_sha256"] = _sha256(packages)
    index["inrelease_size"] = inrelease_path.stat().st_size
    index["inrelease_sha256"] = _sha256(inrelease_path.read_bytes())
    fixture.write_release()


def _bundle_fixture(
    tmp_path: Path,
    architecture: str = "x86_64",
    *,
    concatenate_base_index: bool = False,
    base_index_padding: int = 0,
) -> BundleFixture:
    debian_architecture, runtime_platform = {
        "x86_64": ("amd64", "linux-amd64"),
        "aarch64": ("arm64", "linux-arm64"),
    }[architecture]
    bundle = tmp_path / "bundle"
    release_path = tmp_path / "host-release.json"
    runtime_path = tmp_path / "runtime.json"
    keyring = b"fixture ubuntu archive keyring\n"

    package_payloads = {
        f"libsubid4_4.13+dfsg1-4ubuntu3.2_{debian_architecture}.deb": (
            b"fixture libsubid4 deb\n"
        ),
        f"uidmap_4.13+dfsg1-4ubuntu3.2_{debian_architecture}.deb": b"fixture uidmap deb\n",
        f"quota_4.06-1build6_{debian_architecture}.deb": b"fixture quota deb\n",
    }
    package_rows = {
        "libsubid4": ("1:4.13+dfsg1-4ubuntu3.2", next(iter(package_payloads))),
        "uidmap": (
            "1:4.13+dfsg1-4ubuntu3.2",
            f"uidmap_4.13+dfsg1-4ubuntu3.2_{debian_architecture}.deb",
        ),
        "quota": ("4.06-1build6", f"quota_4.06-1build6_{debian_architecture}.deb"),
    }
    package_stanzas: dict[str, list[str]] = {"noble": [], "noble-updates": []}
    release_packages: dict[str, dict[str, object]] = {}
    for package, (version, filename) in package_rows.items():
        payload = package_payloads[filename]
        archive_path = f"pool/main/fixture/{filename}"
        source_suite = "noble" if package == "quota" else "noble-updates"
        package_stanzas[source_suite].append(
            "\n".join(
                [
                    f"Package: {package}",
                    f"Version: {version}",
                    f"Architecture: {debian_architecture}",
                    f"Filename: {archive_path}",
                    f"Size: {len(payload)}",
                    f"SHA256: {_sha256(payload)}",
                ]
            )
        )
        release_packages[package] = {
            "package": package,
            "source_suite": source_suite,
            "version": version,
            "architecture": debian_architecture,
            "filename": archive_path,
            "size": len(payload),
            "sha256": _sha256(payload),
        }
        _write(bundle / "packages" / filename, payload)

    repository_indexes: dict[str, dict[str, object]] = {}
    for suite in ("noble", "noble-updates"):
        packages_xz = _xz_streams(
            ("\n\n".join(package_stanzas[suite]) + "\n").encode(),
            concatenate=suite == "noble" and concatenate_base_index,
            padding=base_index_padding if suite == "noble" else 0,
        )
        packages_bundle_path = bundle / "apt" / f"{suite}.Packages.xz"
        inrelease_bundle_path = bundle / "apt" / f"{suite}.InRelease"
        _write(packages_bundle_path, packages_xz)
        _write(inrelease_bundle_path, _inrelease(suite, packages_xz, debian_architecture))
        repository_indexes[suite] = {
            "inrelease_path": f"dists/{suite}/InRelease",
            "inrelease_size": inrelease_bundle_path.stat().st_size,
            "inrelease_sha256": _sha256(inrelease_bundle_path.read_bytes()),
            "packages_path": (
                f"dists/{suite}/main/binary-{debian_architecture}/Packages.xz"
            ),
            "packages_size": len(packages_xz),
            "packages_sha256": _sha256(packages_xz),
        }
    _write(bundle / "ubuntu-archive-keyring.gpg", keyring)

    buildkit_files = {
        "bin/buildkitd": b"static buildkitd\n",
        "bin/buildctl": b"static buildctl\n",
        "bin/buildkit-runc": b"static buildkit-runc\n",
    }
    rootless_files = {
        "rootlesskit": b"static rootlesskit\n",
        "rootlessctl": b"static rootlessctl\n",
    }
    buildkit_name = f"buildkit-v0.32.2.{runtime_platform}.tar.gz"
    rootlesskit_name = f"rootlesskit-{architecture}.tar.gz"
    slirp_name = f"slirp4netns-{architecture}"
    fuse_name = f"fuse-overlayfs-{architecture}"
    runtime_artifacts = {
        buildkit_name: _tar_gz(buildkit_files),
        rootlesskit_name: _tar_gz(rootless_files),
        slirp_name: b"static slirp4netns\n",
        fuse_name: b"static fuse-overlayfs\n",
    }
    for name, payload in runtime_artifacts.items():
        _write(bundle / "runtime" / name, payload)
    runtime_manifest = {
        "schema": "loom.task-image-builder-rootless-runtime/v1",
        "release": "rootless-runtime-v1",
        "architectures": {
            architecture: {
                "artifacts": [
                    {"name": name, "url": f"https://example.invalid/{name}", "sha256": _sha256(data)}
                    for name, data in runtime_artifacts.items()
                ],
                "binaries": {
                    "buildkitd": _sha256(buildkit_files["bin/buildkitd"]),
                    "buildctl": _sha256(buildkit_files["bin/buildctl"]),
                    "buildkit-runc": _sha256(buildkit_files["bin/buildkit-runc"]),
                    "rootlesskit": _sha256(rootless_files["rootlesskit"]),
                    "rootlessctl": _sha256(rootless_files["rootlessctl"]),
                    "slirp4netns": _sha256(runtime_artifacts[slirp_name]),
                    "fuse-overlayfs": _sha256(runtime_artifacts[fuse_name]),
                },
            }
        },
    }
    runtime_path.write_text(json.dumps(runtime_manifest), encoding="utf-8")
    runtime_path.chmod(0o644)

    release = {
        "schema": "loom.task-image-builder-host-release/v2",
        "release": "host-release-v2",
        "runtime_manifest": runtime_path.name,
        "ubuntu": {
            "os_id": "ubuntu",
            "version_id": "24.04",
            "snapshot": "20260820T000000Z",
            "component": "main",
            "signer_fingerprint": SIGNER,
            "keyring_name": "ubuntu-archive-keyring.gpg",
            "keyring_sha256": _sha256(keyring),
        },
        "architecture_map": {architecture: debian_architecture},
        "repositories": {
            debian_architecture: {
                "base_url": (
                    "https://snapshot.ubuntu.com/ubuntu/20260820T000000Z"
                ),
                "indexes": repository_indexes,
            }
        },
        "packages": {debian_architecture: release_packages},
    }
    release_path.write_text(json.dumps(release), encoding="utf-8")
    release_path.chmod(0o644)
    bundle.chmod(0o755)
    for directory in (bundle / "apt", bundle / "packages", bundle / "runtime"):
        directory.chmod(0o755)
    return BundleFixture(
        bundle,
        release_path,
        runtime_path,
        release,
        package_payloads,
        architecture,
        debian_architecture,
    )


def _verify(fixture: BundleFixture) -> host_release.VerifiedHostBundle:
    release = host_release.load_host_release(fixture.release_path)
    return host_release.verify_host_bundle(
        fixture.bundle,
        release,
        fixture.architecture,
        FixtureRunner(fixture),
        runtime_manifest_path=fixture.runtime_path,
        required_snapshot_owner=os.geteuid(),
    )


def _v2_release_document() -> dict[str, object]:
    indexes = {
        "noble": {
            "inrelease_path": "dists/noble/InRelease",
            "inrelease_size": 101,
            "inrelease_sha256": "1" * 64,
            "packages_path": "dists/noble/main/binary-amd64/Packages.xz",
            "packages_size": 202,
            "packages_sha256": "2" * 64,
        },
        "noble-updates": {
            "inrelease_path": "dists/noble-updates/InRelease",
            "inrelease_size": 303,
            "inrelease_sha256": "3" * 64,
            "packages_path": "dists/noble-updates/main/binary-amd64/Packages.xz",
            "packages_size": 404,
            "packages_sha256": "4" * 64,
        },
    }
    packages: dict[str, dict[str, object]] = {}
    for package, source_suite, version, digest in (
        ("libsubid4", "noble-updates", "1:4.13+dfsg1-4ubuntu3.2", "a" * 64),
        ("uidmap", "noble-updates", "1:4.13+dfsg1-4ubuntu3.2", "b" * 64),
        ("quota", "noble", "4.06-1build6", "c" * 64),
    ):
        filename = f"pool/main/fixture/{package}_{version}_amd64.deb"
        packages[package] = {
            "package": package,
            "source_suite": source_suite,
            "version": version,
            "architecture": "amd64",
            "filename": filename,
            "size": 100,
            "sha256": digest,
        }
    return {
        "schema": "loom.task-image-builder-host-release/v2",
        "release": "host-release-v2",
        "runtime_manifest": "rootless-runtime-v1.json",
        "ubuntu": {
            "os_id": "ubuntu",
            "version_id": "24.04",
            "snapshot": "20260820T000000Z",
            "component": "main",
            "signer_fingerprint": SIGNER,
            "keyring_name": "ubuntu-archive-keyring.gpg",
            "keyring_sha256": "5" * 64,
        },
        "architecture_map": {"x86_64": "amd64"},
        "repositories": {
            "amd64": {
                "base_url": (
                    "https://snapshot.ubuntu.com/ubuntu/20260820T000000Z"
                ),
                "indexes": indexes,
            }
        },
        "packages": {"amd64": packages},
    }


def test_v2_release_binds_snapshot_suites_and_package_sources(tmp_path: Path) -> None:
    release_path = tmp_path / "host-release-v2.json"
    _write(release_path, json.dumps(_v2_release_document()).encode())

    release = host_release.load_host_release(release_path)

    assert release.release == "host-release-v2"
    assert release.snapshot == "20260820T000000Z"
    assert set(release.repositories["amd64"].indexes) == {
        "noble",
        "noble-updates",
    }
    assert {
        package: artifact.source_suite
        for package, artifact in release.packages["amd64"].items()
    } == {
        "libsubid4": "noble-updates",
        "quota": "noble",
        "uidmap": "noble-updates",
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown-suite",
        "mutable-base-url",
        "wrong-snapshot",
        "missing-metadata-pin",
        "source-suite-drift",
        "duplicate-index-path",
    ],
)
def test_v2_release_rejects_unbound_repository_authority(
    tmp_path: Path,
    mutation: str,
) -> None:
    release = _v2_release_document()
    ubuntu = release["ubuntu"]
    repositories = release["repositories"]
    packages = release["packages"]
    assert isinstance(ubuntu, dict)
    assert isinstance(repositories, dict)
    assert isinstance(packages, dict)
    repository = repositories["amd64"]
    architecture_packages = packages["amd64"]
    assert isinstance(repository, dict)
    assert isinstance(architecture_packages, dict)
    indexes = repository["indexes"]
    assert isinstance(indexes, dict)
    noble = indexes["noble"]
    updates = indexes["noble-updates"]
    assert isinstance(noble, dict)
    assert isinstance(updates, dict)

    if mutation == "unknown-suite":
        indexes["noble-security"] = indexes.pop("noble-updates")
    elif mutation == "mutable-base-url":
        repository["base_url"] = "https://archive.ubuntu.com/ubuntu"
    elif mutation == "wrong-snapshot":
        ubuntu["snapshot"] = "20260819T000000Z"
    elif mutation == "missing-metadata-pin":
        del noble["packages_sha256"]
    elif mutation == "source-suite-drift":
        quota = architecture_packages["quota"]
        assert isinstance(quota, dict)
        quota["source_suite"] = "noble-updates"
    elif mutation == "duplicate-index-path":
        updates["packages_path"] = noble["packages_path"]
    else:
        raise AssertionError(f"unknown mutation: {mutation}")

    release_path = tmp_path / "host-release-v2.json"
    _write(release_path, json.dumps(release).encode())
    with pytest.raises(host_release.HostReleaseError):
        host_release.load_host_release(release_path)


def _mutate_runtime_manifest(fixture: BundleFixture, mutation: str) -> None:
    runtime = json.loads(fixture.runtime_path.read_text(encoding="utf-8"))
    if mutation == "release":
        runtime["release"] = "unreviewed-runtime"
    elif mutation == "binary":
        del runtime["architectures"][fixture.architecture]["binaries"]["rootlessctl"]
    else:
        raise AssertionError(f"unknown runtime mutation: {mutation}")
    fixture.runtime_path.write_text(json.dumps(runtime), encoding="utf-8")


def _add_package_description_continuation(fixture: BundleFixture) -> None:
    packages_path = fixture.bundle / "apt/noble-updates.Packages.xz"
    packages = lzma.decompress(packages_path.read_bytes()).decode("utf-8")
    packages = packages.replace(
        f"Architecture: {fixture.debian_architecture}\n",
        f"Architecture: {fixture.debian_architecture}\n"
        "Description: fixture package\n continued description\n",
        1,
    )
    packages_payload = lzma.compress(packages.encode("utf-8"))
    packages_path.write_bytes(packages_payload)
    _rewrite_index_pins_and_inrelease(fixture, "noble-updates")


def test_v1_topology_cannot_authenticate_quota_from_updates(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    release = host_release.load_host_release(fixture.release_path)
    architecture_packages = dict(release.packages[fixture.debian_architecture])
    architecture_packages["quota"] = replace(
        architecture_packages["quota"],
        source_suite="noble-updates",
    )
    release = replace(
        release,
        packages={fixture.debian_architecture: architecture_packages},
    )

    with pytest.raises(host_release.HostReleaseError, match="signed metadata"):
        host_release.verify_host_bundle(
            fixture.bundle,
            release,
            fixture.architecture,
            FixtureRunner(fixture),
            runtime_manifest_path=fixture.runtime_path,
            required_snapshot_owner=os.geteuid(),
        )


def test_valid_concatenated_xz_streams_are_accepted(tmp_path: Path) -> None:
    verified = _verify(_bundle_fixture(tmp_path, concatenate_base_index=True))
    verified.close()


def test_aligned_xz_stream_padding_is_accepted(tmp_path: Path) -> None:
    verified = _verify(
        _bundle_fixture(
            tmp_path,
            concatenate_base_index=True,
            base_index_padding=4,
        )
    )
    verified.close()


def test_leading_aligned_xz_padding_is_rejected() -> None:
    with pytest.raises(host_release.HostReleaseError, match="trailing"):
        host_release._decompress_packages_index(b"\0" * 4 + lzma.compress(b"fixture"))


@pytest.mark.parametrize(
    ("suffix", "error"),
    [(b"not-xz", "trailing"), (b"\0", "padding")],
)
def test_invalid_xz_trailing_bytes_are_rejected(
    tmp_path: Path,
    suffix: bytes,
    error: str,
) -> None:
    fixture = _bundle_fixture(tmp_path)
    fixture.append_signed_index_bytes("noble", suffix)

    with pytest.raises(host_release.HostReleaseError, match=error):
        _verify(fixture)


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (b"", "empty"),
        (lzma.compress(b"fixture")[:-1], "incomplete"),
    ],
)
def test_empty_or_truncated_xz_index_is_rejected(payload: bytes, error: str) -> None:
    with pytest.raises(host_release.HostReleaseError, match=error):
        host_release._decompress_packages_index(payload)


def test_xz_aggregate_expansion_over_64_mib_is_rejected() -> None:
    payload = _xz_streams(
        b"x" * (host_release.MAX_METADATA_BYTES + 1),
        concatenate=True,
    )

    with pytest.raises(host_release.HostReleaseError, match="expands beyond"):
        host_release._decompress_packages_index(payload)


def test_extra_apt_file_is_rejected(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    _write(fixture.bundle / "apt/extra", b"not part of release\n")

    with pytest.raises(host_release.HostReleaseError, match="bundle layout"):
        _verify(fixture)


def test_changed_pinned_index_bytes_are_rejected(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    inrelease_path = fixture.bundle / "apt/noble.InRelease"
    inrelease_path.write_bytes(inrelease_path.read_bytes() + b"changed\n")

    with pytest.raises(host_release.HostReleaseError, match="pinned metadata"):
        _verify(fixture)


def test_exact_signed_static_bundle_is_accepted(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)

    verified = _verify(fixture)

    assert verified.architecture == "x86_64"
    assert len(verified.bundle_digest) == 64
    assert [path.name for path in verified.package_paths] == [
        "libsubid4_4.13+dfsg1-4ubuntu3.2_amd64.deb",
        "uidmap_4.13+dfsg1-4ubuntu3.2_amd64.deb",
        "quota_4.06-1build6_amd64.deb",
    ]
    assert [path.name for path in verified.runtime_paths] == [
        "buildkit-v0.32.2.linux-amd64.tar.gz",
        "rootlesskit-x86_64.tar.gz",
        "slirp4netns-x86_64",
        "fuse-overlayfs-x86_64",
    ]


def test_exact_aarch64_bundle_is_accepted(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path, "aarch64")

    verified = _verify(fixture)

    assert verified.architecture == "aarch64"
    assert [path.name for path in verified.package_paths] == [
        "libsubid4_4.13+dfsg1-4ubuntu3.2_arm64.deb",
        "uidmap_4.13+dfsg1-4ubuntu3.2_arm64.deb",
        "quota_4.06-1build6_arm64.deb",
    ]
    assert [path.name for path in verified.runtime_paths] == [
        "buildkit-v0.32.2.linux-arm64.tar.gz",
        "rootlesskit-aarch64.tar.gz",
        "slirp4netns-aarch64",
        "fuse-overlayfs-aarch64",
    ]


def test_release_reader_handles_short_regular_file_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _bundle_fixture(tmp_path)
    real_read = os.read

    def short_read(descriptor: int, count: int) -> bytes:
        return real_read(descriptor, min(count, 7))

    monkeypatch.setattr(host_release.os, "read", short_read)

    release = host_release.load_host_release(fixture.release_path)

    assert release.release == "host-release-v2"


@pytest.mark.parametrize("mutation", ["release", "binary"])
def test_incomplete_or_unreviewed_runtime_manifest_is_rejected(
    tmp_path: Path,
    mutation: str,
) -> None:
    fixture = _bundle_fixture(tmp_path)
    _mutate_runtime_manifest(fixture, mutation)

    with pytest.raises(host_release.HostReleaseError, match="runtime"):
        _verify(fixture)


def test_unrelated_package_field_continuation_is_accepted(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    _add_package_description_continuation(fixture)

    verified = _verify(fixture)

    assert verified.architecture == "x86_64"


def test_signed_index_from_a_different_suite_is_rejected(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    inrelease_path = fixture.bundle / "apt/noble-updates.InRelease"
    inrelease = inrelease_path.read_text(encoding="utf-8").replace(
        "Suite: noble-updates",
        "Suite: noble-security",
    )
    inrelease_path.write_text(inrelease, encoding="utf-8")
    _rewrite_inrelease_pin(fixture, "noble-updates")

    with pytest.raises(host_release.HostReleaseError, match="suite"):
        _verify(fixture)


@pytest.mark.parametrize("mutation", ["signature", "signer", "dynamic", "setuid"])
def test_unsafe_command_verified_bundle_is_rejected(tmp_path: Path, mutation: str) -> None:
    fixture = _bundle_fixture(tmp_path)
    if mutation == "signature":
        fixture.signature_valid = False
    elif mutation == "signer":
        fixture.signer = "0" * 40
    elif mutation == "dynamic":
        fixture.dynamic_runtime = True
    else:
        fixture.unexpected_setuid = True

    with pytest.raises(host_release.HostReleaseError):
        _verify(fixture)


def test_changed_package_bytes_are_rejected(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    package = fixture.bundle / "packages/uidmap_4.13+dfsg1-4ubuntu3.2_amd64.deb"
    package.write_bytes(b"different uidmap bytes\n")

    with pytest.raises(host_release.HostReleaseError, match="package artifact"):
        _verify(fixture)


def test_verified_bundle_consumers_are_bound_to_private_snapshot_bytes(
    tmp_path: Path,
) -> None:
    fixture = _bundle_fixture(tmp_path)

    verified = _verify(fixture)
    snapshot_root = verified.snapshot_root
    try:
        source = fixture.bundle / "packages/uidmap_4.13+dfsg1-4ubuntu3.2_amd64.deb"
        expected = source.read_bytes()
        source.write_bytes(b"replacement after snapshot\n")

        consumed = next(path for path in verified.package_paths if path.name.startswith("uidmap_"))
        assert consumed.read_bytes() == expected
        assert snapshot_root != fixture.bundle
        assert snapshot_root.stat().st_uid == os.geteuid()
        assert stat.S_IMODE(snapshot_root.stat().st_mode) == 0o700
    finally:
        verified.close()

    assert not snapshot_root.exists()


def test_verified_bundle_snapshot_defaults_to_the_effective_user(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    release = host_release.load_host_release(fixture.release_path)

    verified = host_release.verify_host_bundle(
        fixture.bundle,
        release,
        fixture.architecture,
        FixtureRunner(fixture),
        runtime_manifest_path=fixture.runtime_path,
    )
    try:
        assert verified.snapshot_root.stat().st_uid == os.geteuid()
    finally:
        verified.close()


def test_bundle_snapshot_rejects_blocking_artifact_types(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    artifact = fixture.bundle / "packages/uidmap_4.13+dfsg1-4ubuntu3.2_amd64.deb"
    artifact.unlink()
    os.mkfifo(artifact, mode=0o400)

    with pytest.raises(host_release.HostReleaseError, match="metadata is unsafe"):
        _verify(fixture)


def test_extra_symlink_or_writable_input_is_rejected(tmp_path: Path) -> None:
    fixture = _bundle_fixture(tmp_path)
    extra = fixture.bundle / "runtime/extra"
    extra.symlink_to("slirp4netns-x86_64")

    with pytest.raises(host_release.HostReleaseError, match="bundle layout"):
        _verify(fixture)

    extra.unlink()
    writable = fixture.bundle / "runtime/slirp4netns-x86_64"
    writable.chmod(os.stat(writable).st_mode | 0o020)
    with pytest.raises(host_release.HostReleaseError, match="writable"):
        _verify(fixture)
