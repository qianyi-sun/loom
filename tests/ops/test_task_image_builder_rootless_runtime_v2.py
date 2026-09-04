from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "deploy/task-image-builder/Dockerfile.rootless-runtime-v2"
MANIFEST = ROOT / "deploy/task-image-builder/rootless-runtime-v2.json"

SYNTAX_DIGEST = "26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d"
GO_IMAGE_DIGEST = "b17af760035fc2f338eed92d448a6c67f2d45438844fc6c60678fa5f99e44b57"
BUILDKIT_BASE_DIGEST = "504731e577c20559c00f968f33219f30115e70be29ab96728d1d06e963fc494b"
BUILDKIT_COMMIT = "991535e0973488b6a429096d21fa13f81f2d89d8"
BUILDKIT_ARCHIVE_SHA256 = "ebc242057b1eee67eb14ead8def52c3770c6793c8c8ac0c53d41983b085360f4"
ROOTLESSKIT_COMMIT = "62d2101fbbe4f79bc845a337c4e868d27ff602c9"
ROOTLESSKIT_ARCHIVE_SHA256 = "51aa4e79847ce9ad48e76a7b824f13ab323b4b90bc13a692e9c8035b8da9340a"
SLIRP4NETNS_RELEASE_SHA256 = {
    "amd64": "e8d0440de8d8c87072138883bc27cfa02f8b0e8a504badbf335c41f794788cc2",
    "arm64": "fbd8a9cabc716dc53e7c5a00bc7b3e91dbe0eab6b40e6d606b1b34c2ce80cfc0",
}
FUSE_OVERLAYFS_RELEASE_SHA256 = {
    "amd64": "1684ef18c337702a0378a4e9942802770c83b11aed6a93c445d43e641a1f3c90",
    "arm64": "34c9995c929dd52f45cca985858d7e58d9a9626104bc2610db218aaa11115c23",
}

RUNTIME_MEMBERS = (
    "buildctl",
    "buildkitd",
    "buildkit-runc",
    "rootlesskit",
    "rootlessctl",
    "slirp4netns",
    "fuse-overlayfs",
)

EXPECTED_BINARY_SHA256 = {
    "amd64": {
        "buildctl": "b2dc3eb537b912717670347cf449ca501e769d5a6beeebaad0aab8abb7f495e1",
        "buildkitd": "0e01e2cac7df591ac129ee86543ac36bc6ef08b50aee8550f5675675208564e7",
        "buildkit-runc": "b886d74fee2529334f7dcdd75a0a7a9e4935efb5554f96d2cdd26a564aa91c8c",
        "rootlesskit": "b607adb41b2537c3b6335e0ecdd4a269d9531c9258ec9261ff36220c820801db",
        "rootlessctl": "5f04200c8a5167f73b04b790fe59ebfb7fbffb505521002ef8bdaf254e220a96",
        "slirp4netns": "e8d0440de8d8c87072138883bc27cfa02f8b0e8a504badbf335c41f794788cc2",
        "fuse-overlayfs": "1684ef18c337702a0378a4e9942802770c83b11aed6a93c445d43e641a1f3c90",
    },
    "arm64": {
        "buildctl": "8913f65a380be955abe4f5a155970ece8a2b3a26408c16431e806963e6acc97a",
        "buildkitd": "fded12c1a4ab70a21278dc860a1d000bca68421e9f45a9e0fcf1e1807ae19c03",
        "buildkit-runc": "1f04f37ef4b2fba6fbbcc13c910b0f94ca067902daa59727edbccf75b5d9d441",
        "rootlesskit": "5f002d6f6ce9ff5e3e0e7730ed5b2518bfebe65cd4a4d51c6ed23ca41832cbb2",
        "rootlessctl": "d415cfe3f60e4cd00a9fc8b20c18dc8b5df99b56a9e1a513715e56ef71e4bf94",
        "slirp4netns": "fbd8a9cabc716dc53e7c5a00bc7b3e91dbe0eab6b40e6d606b1b34c2ce80cfc0",
        "fuse-overlayfs": "34c9995c929dd52f45cca985858d7e58d9a9626104bc2610db218aaa11115c23",
    },
}


def _dockerfile() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


def _manifest() -> dict[str, Any]:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _decoded_patch_payloads(dockerfile: str) -> dict[str, str]:
    matches = re.finditer(
        r"COPY <<'PATCH_B64' (/tmp/(?:buildkit|rootlesskit)-runtime-v2\.patch)\.b64\n"
        r"(?P<payload>[A-Za-z0-9+/=\n]+?)\nPATCH_B64",
        dockerfile,
    )
    payloads: dict[str, str] = {}
    for match in matches:
        payload = "".join(match.group("payload").splitlines())
        payloads[match.group(1)] = base64.b64decode(payload, validate=True).decode(
            "utf-8"
        )
    return payloads


def _runtime_patch_text(dockerfile: str) -> str:
    payloads = _decoded_patch_payloads(dockerfile)
    assert set(payloads) == {
        "/tmp/buildkit-runtime-v2.patch",
        "/tmp/rootlesskit-runtime-v2.patch",
    }
    return "\n".join(payloads.values())


def test_runtime_v2_dockerfile_binds_toolchain_sources_and_reproducible_flags() -> None:
    dockerfile = _dockerfile()

    assert dockerfile.startswith(
        f"# syntax=docker/dockerfile:1.20@sha256:{SYNTAX_DIGEST}\n"
    )
    assert (
        f"FROM --platform=$BUILDPLATFORM golang:1.26-alpine3.23@sha256:{GO_IMAGE_DIGEST}"
        in dockerfile
    )
    assert f"FROM moby/buildkit:rootless@sha256:{BUILDKIT_BASE_DIGEST}" in dockerfile
    assert f"github.com/moby/buildkit/archive/{BUILDKIT_COMMIT}.tar.gz" in dockerfile
    assert f"ADD --checksum=sha256:{BUILDKIT_ARCHIVE_SHA256}" in dockerfile
    assert f"github.com/rootless-containers/rootlesskit/archive/{ROOTLESSKIT_COMMIT}.tar.gz" in dockerfile
    assert f"ADD --checksum=sha256:{ROOTLESSKIT_ARCHIVE_SHA256}" in dockerfile
    for digest in SLIRP4NETNS_RELEASE_SHA256.values():
        assert f"ADD --checksum=sha256:{digest}" in dockerfile
    for digest in FUSE_OVERLAYFS_RELEASE_SHA256.values():
        assert f"ADD --checksum=sha256:{digest}" in dockerfile
    assert dockerfile.count("go get golang.org/x/crypto@v0.55.0") == 2
    assert "go get " in dockerfile
    assert not re.search(r"go get (?!golang\.org/x/crypto@v0\.55\.0)", dockerfile)
    assert dockerfile.count("-trimpath -buildvcs=false") >= 4
    assert '-tags "osusergo netgo static_build seccomp"' in dockerfile
    assert "go version go1.26.7" in dockerfile
    assert 'required_version="v0.55.0"' in dockerfile
    assert "buildctl github.com/moby/buildkit v0.32.2-loom.1" in dockerfile
    assert "buildkitd github.com/moby/buildkit v0.32.2-loom.1" in dockerfile
    assert "rootlesskit version 3.1.0" in dockerfile


def test_runtime_v2_emits_only_the_seven_host_runtime_members() -> None:
    dockerfile = _dockerfile()

    for member in RUNTIME_MEMBERS:
        assert f"/out/{member}" in dockerfile
        assert f"/runtime/{member}" in dockerfile
    assert "buildkit-qemu-" not in dockerfile
    assert "ENTRYPOINT" not in dockerfile
    assert "CMD " not in dockerfile
    assert "docker.sock" not in dockerfile
    assert "containerd.sock" not in dockerfile


def test_runtime_v2_patches_transitive_helper_paths_without_path_authority() -> None:
    dockerfile = _dockerfile()
    patches = _runtime_patch_text(dockerfile)

    assert 'exec.Command("/usr/bin/newuidmap"' in patches
    assert 'exec.Command("/usr/bin/newgidmap"' in patches
    assert '"/usr/bin/nsenter"' in patches
    assert '"/usr/bin/ip", "tuntap"' in patches
    assert '"/usr/bin/ip", "link"' in patches
    assert "BUILDKIT_FUSE_OVERLAYFS_BINARY" in patches
    assert "mountWithDirectFUSE" in patches
    assert "direct FUSE helper path" in patches
    assert "must be absolute clean path" in patches
    assert "Preserve only supervisor-derived cgroup paths" in patches
    assert "rootless cgroup parent override forbidden" in patches
    assert "meta.CgroupParent != cgroupParent" in patches
    assert "spec.Linux.CgroupsPath = \"\"" in patches
    assert "go mod vendor" in dockerfile
    assert dockerfile.count("go build -mod=vendor") == 2


def test_runtime_v2_applies_embedded_patches_with_strict_git_apply() -> None:
    dockerfile = _dockerfile()
    patches = _runtime_patch_text(dockerfile)

    assert "patch -p1" not in dockerfile
    assert "apk add --no-cache ca-certificates git patch" not in dockerfile
    assert (
        "base64 -d /tmp/buildkit-runtime-v2.patch.b64 > /tmp/buildkit-runtime-v2.patch"
        in dockerfile
    )
    assert (
        "base64 -d /tmp/rootlesskit-runtime-v2.patch.b64 > /tmp/rootlesskit-runtime-v2.patch"
        in dockerfile
    )
    assert "git apply --check" in dockerfile
    assert re.search(r"\bgit apply /tmp/(buildkit|rootlesskit)-runtime-v2\.patch\b", dockerfile)
    assert "--unidiff-zero" not in dockerfile
    assert not re.search(r"@@ -\d+(?:,0)? \+\d+(?:,0)? @@", patches)


def test_runtime_v2_manifest_records_exact_amd64_and_arm64_member_hashes() -> None:
    manifest = _manifest()

    assert manifest["schema"] == "loom.task-image-builder-rootless-runtime/v2"
    assert manifest["release"] == "rootless-runtime-v2"
    assert manifest["source"]["buildkit"] == {
        "version": "v0.32.2",
        "signed_tag_commit": BUILDKIT_COMMIT,
        "archive_sha256": BUILDKIT_ARCHIVE_SHA256,
    }
    assert manifest["source"]["rootlesskit"] == {
        "version": "v3.1.0",
        "signed_tag_commit": ROOTLESSKIT_COMMIT,
        "archive_sha256": ROOTLESSKIT_ARCHIVE_SHA256,
    }
    assert manifest["toolchain"] == {
        "go": "go1.26.7",
        "image": "golang:1.26-alpine3.23",
        "image_sha256": GO_IMAGE_DIGEST,
        "x_crypto": "v0.55.0",
        "reproducible_flags": ["-trimpath", "-buildvcs=false"],
    }
    assert set(manifest["architectures"]) == set(EXPECTED_BINARY_SHA256)
    for arch, expected in EXPECTED_BINARY_SHA256.items():
        entry = manifest["architectures"][arch]
        assert entry["platform"] == f"linux/{arch}"
        assert entry["members"] == expected
        assert set(entry["members"]) == set(RUNTIME_MEMBERS)


def test_runtime_v2_uses_no_mutable_downloads_or_registry_publication() -> None:
    dockerfile = _dockerfile()

    forbidden_fragments = (
        "docker.io/",
        ":latest",
        "curl ",
        "wget ",
        "git clone",
        "go install",
        "docker push",
        "buildctl push",
        "cache-from",
        "cache-to",
    )
    for fragment in forbidden_fragments:
        assert fragment not in dockerfile
    assert dockerfile.count("https://github.com/") == 6
    assert dockerfile.count("ADD --checksum=sha256:") == 6
