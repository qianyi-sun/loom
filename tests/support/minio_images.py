"""Resolve disposable MinIO fixtures without silently changing their releases.

Use cached/upstream images first. If upstream publication is unavailable, build
the same commit with its original compiler from a checksum-verified archive.
These explicitly named local rebuilds are not byte-identical upstream images.
Nothing here contacts Docker or the network at import time.
"""

import hashlib
import json
import subprocess
import sys
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

MINIO_TESTCONTAINERS_IMAGE = (
    "quay.io/minio/minio:RELEASE.2022-12-02T19-19-22Z"
    "@sha256:031fd97adca056cdbfd416a26670898d93adb9a79a3cf126aac5a41ddc22c5a3"
)
MINIO_TLS_IMAGE = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"


@dataclass(frozen=True)
class SourceFixture:
    image: str
    commit: str
    release: str
    version: str
    source_sha256: str
    compiler: str


SOURCE_FIXTURES = (
    SourceFixture(
        MINIO_TESTCONTAINERS_IMAGE,
        "5655272f5a62f827e6baea6a4cb21a4c3f065c2c",
        "RELEASE.2022-12-02T19-19-22Z", "2022-12-02T19:19:22Z",
        "c68e134c19910b5a3f0c0b72f8182cc837a26a02df627633e10a0cf6034362ca",
        "golang:1.19.3@sha256:10e3c0f39f8e237baa5b66c5295c578cac42a99536cc9333d8505324a82407d9",
    ),
    SourceFixture(
        MINIO_TLS_IMAGE,
        "07c3a429bfed433e49018cb0f78a52145d4bedeb",
        "RELEASE.2025-09-07T16-13-09Z", "2025-09-07T16:13:09Z",
        "8819e3e7817e46b7b3798f8f200ead208562e571563c2e040352378031abe9f2",
        "golang:1.24.6@sha256:8d9e57c5a6f2bede16fe674c16149eee20db6907129e02c4ad91ce5a697a4012",
    ),
)


def _dockerfile(spec: SourceFixture) -> str:
    # curl supports the existing Compose healthcheck; coreutils preserves the
    # upstream entrypoint's optional chroot --userspec behavior.
    return f'''FROM {spec.compiler} AS build
ADD source.tar.gz /src/
WORKDIR /src/minio-{spec.commit}
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "-s -w \
-X github.com/minio/minio/cmd.Version={spec.version} \
-X github.com/minio/minio/cmd.ReleaseTag={spec.release} \
-X github.com/minio/minio/cmd.CopyrightYear={spec.version[:4]} \
-X github.com/minio/minio/cmd.CommitID={spec.commit} \
-X github.com/minio/minio/cmd.ShortCommitID={spec.commit[:12]}" -o /out/minio .
FROM alpine@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40
RUN apk add --no-cache curl=8.22.0-r0 ca-certificates=20260909-r0 coreutils=9.8-r1
COPY --from=build /out/minio /usr/bin/minio
COPY --from=build /src/minio-{spec.commit}/dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
RUN chmod 755 /usr/bin/docker-entrypoint.sh
EXPOSE 9000
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]
'''


def _labels(spec: SourceFixture) -> dict[str, str]:
    return {
        "io.loom.fixture.commit": spec.commit,
        "io.loom.fixture.release": spec.release,
        "io.loom.fixture.source-sha256": spec.source_sha256,
        "io.loom.fixture.recipe-sha256": hashlib.sha256(_dockerfile(spec).encode()).hexdigest(),
    }


def _source_tag(spec: SourceFixture) -> str:
    recipe = _labels(spec)["io.loom.fixture.recipe-sha256"]
    return f"loom-minio-test-source:{spec.commit[:12]}-{recipe[:12]}"


def _source_fixture_cached(spec: SourceFixture) -> bool:
    result = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{json .Config.Labels}}", _source_tag(spec)],
        check=False, timeout=10, capture_output=True, text=True,
    )
    if result.returncode != 0:
        return False
    try:
        labels = json.loads(result.stdout)
    except (ValueError, TypeError):
        return False
    return isinstance(labels, dict) and all(labels.get(key) == value for key, value in _labels(spec).items())


def _fetch_source(url: str, destination: Path) -> None:
    # Socket inactivity limits alone do not bound a slowly progressing response.
    # subprocess.run kills and reaps this child on the total wall-clock deadline.
    script = """import shutil, sys, urllib.request
with urllib.request.urlopen(sys.argv[1], timeout=30) as response, open(sys.argv[2], 'wb') as output:
    shutil.copyfileobj(response, output)
"""
    subprocess.run([sys.executable, "-I", "-c", script, url, str(destination)], check=True, timeout=120)


def _download_source(spec: SourceFixture, destination: Path) -> None:
    try:
        _fetch_source(f"https://codeload.github.com/minio/minio/tar.gz/{spec.commit}", destination)
        with destination.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256")
        if digest.hexdigest() != spec.source_sha256:
            raise ValueError("MinIO fixture source checksum mismatch")
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


def _build_source_fixture(spec: SourceFixture) -> str:
    tag = _source_tag(spec)
    provisional = f"{tag}-checking-{uuid4().hex}"
    try:
        with tempfile.TemporaryDirectory(prefix="loom-minio-fixture-") as temporary:
            root = Path(temporary)
            _download_source(spec, root / "source.tar.gz")
            (root / "Dockerfile").write_text(_dockerfile(spec))
            labels = [arg for key, value in _labels(spec).items() for arg in ("--label", f"{key}={value}")]
            subprocess.run(["docker", "build", "--tag", provisional, *labels, str(root)], check=True, timeout=900)
        result = subprocess.run(
            ["docker", "run", "--rm", "--network=none", provisional, "--version"],
            check=True, timeout=30, capture_output=True, text=True,
        )
        if f"version {spec.release} " not in result.stdout or f"commit-id={spec.commit}" not in result.stdout:
            raise ValueError("MinIO source fixture version mismatch")
        # Only a verified image becomes a reusable cache entry. Interrupted
        # builds and failed cleanup can leave no accepted provisional tag.
        subprocess.run(["docker", "tag", provisional, tag], check=True, timeout=10)
    finally:
        with suppress(OSError, subprocess.SubprocessError):
            subprocess.run(["docker", "image", "rm", provisional], check=False, timeout=10,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return tag


def prepare_test_image(image: str) -> str:
    """Lazily select a usable image; fail closed on unknown releases or bad builds."""
    spec = next((spec for spec in SOURCE_FIXTURES
                 if image in (spec.image, "quay.io/minio/minio@" + spec.image.split("@", 1)[1])), None)
    if spec is None:
        raise ValueError("Unknown MinIO test fixture image")
    cached = subprocess.run(["docker", "image", "inspect", image], check=False,
        timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if cached.returncode == 0:
        return image
    if _source_fixture_cached(spec):
        return _source_tag(spec)
    try:
        subprocess.run(["docker", "pull", image], check=True, timeout=180)
    except subprocess.CalledProcessError:
        return _build_source_fixture(spec)
    return image


def prepare_test_images() -> None:
    """CI setup only: cold downloads/builds run before individual test deadlines."""
    for image in (MINIO_TESTCONTAINERS_IMAGE, MINIO_TLS_IMAGE):
        prepare_test_image(image)


if __name__ == "__main__":
    prepare_test_images()
