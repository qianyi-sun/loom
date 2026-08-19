from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_IMAGE = "loom-personal-dev-scanner-cache:pytest"
_FILES = {
    "db/metadata.json": (
        b'{"DownloadedAt":"2026-08-18T18:40:00Z","NextUpdate":"2026-08-19T18:35:46Z",'
        b'"UpdatedAt":"2026-08-18T18:35:46Z","Version":2}'
    ),
    "db/trivy.db": b"fixture-vulnerability-db",
    "java-db/metadata.json": (
        b'{"DownloadedAt":"2026-08-18T18:40:00Z","NextUpdate":"2026-08-21T01:10:28Z",'
        b'"UpdatedAt":"2026-08-18T01:10:28Z","Version":1}'
    ),
    "java-db/trivy-java.db": b"fixture-java-db",
}
pytestmark = pytest.mark.docker


def _docker(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *arguments],
        cwd=_ROOT,
        text=True,
        capture_output=True,
        check=check,
    )


@pytest.fixture(scope="module")
def cache_image(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, Path]:
    version = _docker("version", "--format", "{{.Server.Version}}", check=False)
    if version.returncode != 0:
        pytest.skip("Docker daemon is unavailable")
    assets = tmp_path_factory.mktemp("personal-dev-scanner-cache-assets")
    for relative, payload in _FILES.items():
        path = assets / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    build = _docker(
        "buildx",
        "build",
        "--platform",
        "linux/amd64",
        "--load",
        "--build-context",
        f"personal-dev-scanner-cache={assets}",
        "--file",
        "deploy/Dockerfile.personal-dev-scanner-cache",
        "--tag",
        _IMAGE,
        ".",
        check=False,
    )
    assert build.returncode == 0, build.stderr
    try:
        yield _IMAGE, assets
    finally:
        _docker("image", "rm", _IMAGE, check=False)


def test_cache_image_has_exact_user_and_source_contract(
    cache_image: tuple[str, Path],
) -> None:
    image, _ = cache_image
    inspected = _docker("image", "inspect", image, "--format", "{{json .Config.User}}")
    assert json.loads(inspected.stdout) == "65531:65532"
    source_check = _docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--entrypoint",
        "python",
        image,
        "-c",
        (
            "from pathlib import Path; "
            "from loom.personal_dev_scanner_cache_init import _source_snapshot; "
            "snapshot=_source_snapshot(Path('/opt/loom-personal-dev-scanner-cache/assets')); "
            "snapshot.close()"
        ),
        check=False,
    )
    assert source_check.returncode == 0, source_check.stderr


def test_cache_image_contains_the_exact_fixture_bytes(
    cache_image: tuple[str, Path],
    tmp_path: Path,
) -> None:
    image, _ = cache_image
    container = "loom-scanner-cache-pytest-" + uuid.uuid4().hex
    created = _docker("create", "--name", container, image)
    assert created.stdout.strip()
    try:
        for relative, expected in _FILES.items():
            target = tmp_path / relative.replace("/", "-")
            copied = _docker(
                "cp",
                f"{container}:/opt/loom-personal-dev-scanner-cache/assets/{relative}",
                str(target),
                check=False,
            )
            assert copied.returncode == 0, copied.stderr
            assert target.read_bytes() == expected
            assert hashlib.sha256(target.read_bytes()).digest() == hashlib.sha256(
                expected
            ).digest()
    finally:
        _docker("rm", "--force", container, check=False)
