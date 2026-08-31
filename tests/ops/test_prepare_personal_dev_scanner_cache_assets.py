from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import scripts.prepare_personal_dev_scanner_cache_assets as scanner_cache_assets
from scripts.prepare_personal_dev_scanner_cache_assets import (
    prepare_personal_dev_scanner_cache_assets,
    verify_personal_dev_scanner_cache_assets,
)

from loom.personal_dev_scanner_cache import PersonalDevScannerCacheError

_ROOT = Path(__file__).resolve().parents[2]
_LOCK = _ROOT / "deploy/dev-fleet/personal-dev-scanner-cache-lock.json"
_DATABASE_BYTES = b"vulnerability-db"
_JAVA_DATABASE_BYTES = b"java-db"
_DATABASE_METADATA = (
    b'{"DownloadedAt":"2026-08-18T18:40:00Z","NextUpdate":"2026-08-19T18:35:46Z",'
    b'"UpdatedAt":"2026-08-18T18:35:46Z","Version":2}'
)
_JAVA_DATABASE_METADATA = (
    b'{"DownloadedAt":"2026-08-18T18:40:00Z","NextUpdate":"2026-08-21T01:10:28Z",'
    b'"UpdatedAt":"2026-08-18T01:10:28Z","Version":1}'
)
_TRIVY = b"""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with Path(os.environ["LOOM_TEST_TRIVY_LOG"]).open("a", encoding="utf-8") as stream:
    stream.write(json.dumps(args, separators=(",", ":")) + "\\n")
if os.environ.get("LOOM_TEST_FAIL") == "1":
    raise SystemExit(23)
if args[:2] == ["image", "--download-db-only"]:
    expected = os.environ["LOOM_TEST_DB_REF"]
    directory = "db"
    database = "trivy.db"
    database_bytes = b"vulnerability-db"
    version = 2
elif args[:2] == ["image", "--download-java-db-only"]:
    expected = os.environ["LOOM_TEST_JAVA_DB_REF"]
    directory = "java-db"
    database = "trivy-java.db"
    database_bytes = b"java-db"
    version = 1
else:
    raise SystemExit(24)
repository_flag = "--db-repository" if directory == "db" else "--java-db-repository"
cache = Path(args[args.index("--cache-dir") + 1])
if args != ["image", args[1], repository_flag, expected, "--cache-dir", str(cache), "--no-progress"]:
    raise SystemExit(25)
target = cache / directory
target.mkdir(parents=True, exist_ok=True)
metadata = {
    "DownloadedAt": "2026-08-18T18:40:00Z",
    "NextUpdate": (
        "2026-08-19T18:35:46Z" if directory == "db" else "2026-08-21T01:10:28Z"
    ),
    "UpdatedAt": (
        "2026-08-18T18:35:46Z" if directory == "db" else "2026-08-18T01:10:28Z"
    ),
    "Version": version,
}
(target / database).write_bytes(database_bytes)
(target / "metadata.json").write_text(
    json.dumps(metadata, sort_keys=True, separators=(",", ":")), encoding="ascii"
)
variant = os.environ.get("LOOM_TEST_VARIANT")
if variant == "extra" and directory == "java-db":
    (cache / "unexpected").write_bytes(b"x")
elif variant in {"symlink", "hardlink"} and directory == "db":
    (target / database).unlink()
    external = Path(os.environ["LOOM_TEST_EXTERNAL"])
    external.write_bytes(database_bytes)
    if variant == "symlink":
        (target / database).symlink_to(external)
    else:
        os.link(external, target / database)
elif variant == "fifo" and directory == "db":
    (target / database).unlink()
    os.mkfifo(target / database)
elif variant == "oversize" and directory == "db":
    (target / "metadata.json").write_bytes(b"x" * (64 * 1024 + 1))
elif variant == "malformed-metadata" and directory == "db":
    (target / "metadata.json").write_bytes(b"{}")
if os.environ.get("LOOM_TEST_MUTATE_TRIVY") == "1" and directory == "db":
    executable = Path(__file__)
    executable.chmod(0o755)
    with executable.open("ab") as stream:
        stream.write(b"# changed\\n")
    executable.chmod(0o555)
"""
_DOCKER = b"""#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

if sys.argv[1:5] != ["buildx", "imagetools", "inspect", "--raw"] or len(sys.argv) != 6:
    raise SystemExit(31)
reference = sys.argv[5]
with Path(os.environ["LOOM_TEST_DOCKER_LOG"]).open("a", encoding="utf-8") as stream:
    stream.write(reference + "\\n")
if os.environ.get("LOOM_TEST_OVERSIZED_MANIFEST") == "1":
    for _ in range(256):
        sys.stdout.buffer.write(b"x" * (64 * 1024))
        sys.stdout.buffer.flush()
    Path(os.environ["LOOM_TEST_OVERSIZED_MARKER"]).write_text("finished")
    raise SystemExit(0)
manifests = json.loads(Path(os.environ["LOOM_TEST_MANIFESTS"]).read_text(encoding="ascii"))
sys.stdout.write(json.dumps(manifests[reference], sort_keys=True, separators=(",", ":")))
"""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def _manifest(*, layer_digest: str, layer_media_type: str) -> dict[str, Any]:
    return {
        "artifactType": "application/vnd.aquasec.trivy.config.v1+json",
        "config": {
            "data": "e30=",
            "digest": (
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
            ),
            "mediaType": "application/vnd.oci.empty.v1+json",
            "size": 2,
        },
        "layers": [
            {
                "digest": "sha256:" + layer_digest,
                "mediaType": layer_media_type,
                "size": 123,
            }
        ],
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "schemaVersion": 2,
    }


@dataclass(frozen=True)
class _Case:
    lock: Path
    output: Path
    trivy: Path
    database_ref: str
    java_database_ref: str
    trivy_log: Path
    docker_log: Path


def _case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Case:
    tools = tmp_path / "tools"
    tools.mkdir()
    trivy = tools / "trivy"
    trivy.write_bytes(_TRIVY)
    trivy.chmod(0o555)
    docker = tools / "docker"
    docker.write_bytes(_DOCKER)
    docker.chmod(0o555)

    database_layer = hashlib.sha256(b"database-layer").hexdigest()
    java_database_layer = hashlib.sha256(b"java-database-layer").hexdigest()
    database_manifest = _manifest(
        layer_digest=database_layer,
        layer_media_type="application/vnd.aquasec.trivy.db.layer.v1.tar+gzip",
    )
    java_database_manifest = _manifest(
        layer_digest=java_database_layer,
        layer_media_type="application/vnd.aquasec.trivy.javadb.layer.v1.tar+gzip",
    )
    database_ref = (
        "ghcr.io/aquasecurity/trivy-db@sha256:"
        + hashlib.sha256(_canonical(database_manifest)).hexdigest()
    )
    java_database_ref = (
        "ghcr.io/aquasecurity/trivy-java-db@sha256:"
        + hashlib.sha256(_canonical(java_database_manifest)).hexdigest()
    )
    manifests = tmp_path / "manifests.json"
    manifests.write_bytes(
        _canonical(
            {
                database_ref: database_manifest,
                java_database_ref: java_database_manifest,
            }
        )
    )
    value = json.loads(_LOCK.read_bytes())
    trivy_sha256 = hashlib.sha256(_TRIVY).hexdigest()
    value["binary_sha256"] = {
        "linux/amd64": trivy_sha256,
        "linux/arm64": trivy_sha256,
    }
    value["database"] = {"image": database_ref, "layer_sha256": database_layer}
    value["java_database"] = {
        "image": java_database_ref,
        "layer_sha256": java_database_layer,
    }
    lock = tmp_path / "scanner-cache-lock.json"
    lock.write_bytes(_canonical(value))

    trivy_log = tmp_path / "trivy.log"
    docker_log = tmp_path / "docker.log"
    monkeypatch.setenv("PATH", f"{tools}:{os.environ['PATH']}")
    monkeypatch.setenv("LOOM_TEST_TRIVY_LOG", str(trivy_log))
    monkeypatch.setenv("LOOM_TEST_DOCKER_LOG", str(docker_log))
    monkeypatch.setenv("LOOM_TEST_MANIFESTS", str(manifests))
    monkeypatch.setenv("LOOM_TEST_DB_REF", database_ref)
    monkeypatch.setenv("LOOM_TEST_JAVA_DB_REF", java_database_ref)
    monkeypatch.setenv("LOOM_TEST_EXTERNAL", str(tmp_path / "external"))
    return _Case(
        lock=lock,
        output=tmp_path / "assets",
        trivy=trivy,
        database_ref=database_ref,
        java_database_ref=java_database_ref,
        trivy_log=trivy_log,
        docker_log=docker_log,
    )


def test_materializer_publishes_exact_verified_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch)

    files = prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert files.database_sha256 == hashlib.sha256(_DATABASE_BYTES).hexdigest()
    assert files.database_metadata_sha256 == hashlib.sha256(_DATABASE_METADATA).hexdigest()
    assert files.java_database_sha256 == hashlib.sha256(_JAVA_DATABASE_BYTES).hexdigest()
    assert files.java_database_metadata_sha256 == hashlib.sha256(
        _JAVA_DATABASE_METADATA
    ).hexdigest()
    assert {
        path.relative_to(case.output).as_posix() for path in case.output.rglob("*")
    } == {
        "db",
        "db/metadata.json",
        "db/trivy.db",
        "java-db",
        "java-db/metadata.json",
        "java-db/trivy-java.db",
        "scanner-cache-evidence.json",
    }
    assert stat.S_IMODE(case.output.stat().st_mode) == 0o555
    for directory in (case.output / "db", case.output / "java-db"):
        assert stat.S_IMODE(directory.stat().st_mode) == 0o555
    for path in case.output.rglob("*"):
        if path.is_file():
            assert stat.S_IMODE(path.stat().st_mode) == 0o444

    trivy_commands = [json.loads(line) for line in case.trivy_log.read_text().splitlines()]
    staging = trivy_commands[0][5]
    assert trivy_commands == [
        [
            "image",
            "--download-db-only",
            "--db-repository",
            case.database_ref,
            "--cache-dir",
            staging,
            "--no-progress",
        ],
        [
            "image",
            "--download-java-db-only",
            "--java-db-repository",
            case.java_database_ref,
            "--cache-dir",
            staging,
            "--no-progress",
        ],
    ]
    assert case.docker_log.read_text().splitlines() == [
        case.database_ref,
        case.java_database_ref,
    ]
    evidence = json.loads((case.output / "scanner-cache-evidence.json").read_bytes())
    assert evidence == {
        "binary_platform": "linux/amd64",
        "binary_sha256": hashlib.sha256(_TRIVY).hexdigest(),
        "database": {
            "image": case.database_ref,
            "layer_sha256": hashlib.sha256(b"database-layer").hexdigest(),
            "metadata_sha256": hashlib.sha256(_DATABASE_METADATA).hexdigest(),
            "sha256": hashlib.sha256(_DATABASE_BYTES).hexdigest(),
        },
        "java_database": {
            "image": case.java_database_ref,
            "layer_sha256": hashlib.sha256(b"java-database-layer").hexdigest(),
            "metadata_sha256": hashlib.sha256(_JAVA_DATABASE_METADATA).hexdigest(),
            "sha256": hashlib.sha256(_JAVA_DATABASE_BYTES).hexdigest(),
        },
        "lock_sha256": hashlib.sha256(case.lock.read_bytes()).hexdigest(),
        "schema_version": 1,
        "trivy_version": "v0.74.0",
    }
    assert (case.output / "scanner-cache-evidence.json").read_bytes() == (
        _canonical(evidence) + b"\n"
    )


def test_asset_verifier_revalidates_transport_without_downloading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch)
    prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)
    original_log = case.trivy_log.read_bytes()
    expected = hashlib.sha256(b"loom-scanner-cache-build-context-v1\0")
    for relative in (
        "db/metadata.json",
        "db/trivy.db",
        "java-db/metadata.json",
        "java-db/trivy-java.db",
    ):
        payload = (case.output / relative).read_bytes()
        expected.update(relative.encode("ascii") + b"\0")
        expected.update(len(payload).to_bytes(8, "big") + payload)
    expected.update((case.output / "scanner-cache-evidence.json").read_bytes())

    observed = verify_personal_dev_scanner_cache_assets(case.lock, case.output)

    assert observed == expected.hexdigest()
    assert case.trivy_log.read_bytes() == original_log


def test_asset_verifier_streams_database_fingerprints_with_bounded_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch)
    prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)
    database_bytes = b"d" * (8 * 1024 * 1024)
    java_database_bytes = b"j" * (8 * 1024 * 1024)
    database_path = case.output / "db/trivy.db"
    java_database_path = case.output / "java-db/trivy-java.db"
    evidence_path = case.output / "scanner-cache-evidence.json"
    for path, payload in (
        (database_path, database_bytes),
        (java_database_path, java_database_bytes),
    ):
        path.chmod(0o644)
        path.write_bytes(payload)
        path.chmod(0o444)
    evidence = json.loads(evidence_path.read_bytes())
    evidence["database"]["sha256"] = hashlib.sha256(database_bytes).hexdigest()
    evidence["java_database"]["sha256"] = hashlib.sha256(java_database_bytes).hexdigest()
    evidence_path.chmod(0o644)
    evidence_path.write_bytes(_canonical(evidence) + b"\n")
    evidence_path.chmod(0o444)

    tracemalloc.start()
    try:
        verify_personal_dev_scanner_cache_assets(case.lock, case.output)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak_bytes < 4 * 1024 * 1024


@pytest.mark.parametrize("problem", ["changed", "extra", "evidence"])
def test_asset_verifier_rejects_transport_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    problem: str,
) -> None:
    case = _case(tmp_path, monkeypatch)
    prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)
    if problem == "changed":
        target = case.output / "db/trivy.db"
        target.chmod(0o644)
        target.write_bytes(b"changed")
    elif problem == "extra":
        case.output.chmod(0o755)
        (case.output / "extra").write_bytes(b"x")
    else:
        target = case.output / "scanner-cache-evidence.json"
        target.chmod(0o644)
        target.write_bytes(target.read_bytes() + b"\n")

    with pytest.raises(PersonalDevScannerCacheError, match="verification failed"):
        verify_personal_dev_scanner_cache_assets(case.lock, case.output)


@pytest.mark.parametrize("existing", ["empty", "nonempty"])
def test_materializer_requires_absent_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    existing: str,
) -> None:
    case = _case(tmp_path, monkeypatch)
    case.output.mkdir()
    if existing == "nonempty":
        (case.output / "owned").write_bytes(b"do not replace")

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert case.output.is_dir()
    assert not case.trivy_log.exists()
    if existing == "nonempty":
        assert (case.output / "owned").read_bytes() == b"do not replace"


def test_materializer_rejects_fifo_trivy_without_blocking(tmp_path: Path) -> None:
    trivy = tmp_path / "trivy"
    os.mkfifo(trivy)
    output = tmp_path / "assets"
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path\n"
            "from scripts.prepare_personal_dev_scanner_cache_assets import (\n"
            "    prepare_personal_dev_scanner_cache_assets,\n"
            ")\n"
            "from loom.personal_dev_scanner_cache import PersonalDevScannerCacheError\n"
            "import sys\n"
            "try:\n"
            "    prepare_personal_dev_scanner_cache_assets(\n"
            "        Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]),\n"
            "    )\n"
            "except PersonalDevScannerCacheError:\n"
            "    raise SystemExit(0)\n"
            "raise SystemExit(1)\n"
        ),
        str(_LOCK),
        str(trivy),
        str(output),
    ]
    environment = {
        **os.environ,
        "PYTHONPATH": f"{_ROOT / 'src'}:{_ROOT}",
    }

    try:
        result = subprocess.run(command, env=environment, timeout=2, check=False)
    except subprocess.TimeoutExpired:
        pytest.fail("scanner cache Trivy FIFO blocked before type validation")

    assert result.returncode == 0
    assert not output.exists()


def test_materializer_leaves_no_partial_output_on_command_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch)
    monkeypatch.setenv("LOOM_TEST_FAIL", "1")

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert not case.output.exists()
    assert not list(tmp_path.glob(".assets.staging-*"))


def test_materializer_revalidates_trivy_binary_after_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch)
    monkeypatch.setenv("LOOM_TEST_MUTATE_TRIVY", "1")

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert not case.output.exists()


@pytest.mark.parametrize(
    "variant",
    ["extra", "symlink", "hardlink", "fifo", "oversize", "malformed-metadata"],
)
def test_materializer_rejects_unsafe_or_invalid_trivy_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
) -> None:
    case = _case(tmp_path, monkeypatch)
    monkeypatch.setenv("LOOM_TEST_VARIANT", variant)

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert not case.output.exists()
    assert not list(tmp_path.glob(".assets.staging-*"))


@pytest.mark.parametrize("drift", ["manifest", "layer-media-type", "extra-layer"])
def test_materializer_rejects_oci_manifest_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    case = _case(tmp_path, monkeypatch)
    manifests_path = Path(os.environ["LOOM_TEST_MANIFESTS"])
    manifests = json.loads(manifests_path.read_bytes())
    manifest = manifests[case.database_ref]
    if drift == "manifest":
        manifest["schemaVersion"] = 1
    elif drift == "layer-media-type":
        manifest["layers"][0]["mediaType"] = "application/octet-stream"
    else:
        manifest["layers"].append(dict(manifest["layers"][0]))
    new_reference = (
        "ghcr.io/aquasecurity/trivy-db@sha256:"
        + hashlib.sha256(_canonical(manifest)).hexdigest()
    )
    manifests[new_reference] = manifests.pop(case.database_ref)
    manifests_path.write_bytes(_canonical(manifests))
    lock_value = json.loads(case.lock.read_bytes())
    lock_value["database"]["image"] = new_reference
    case.lock.write_bytes(_canonical(lock_value))
    monkeypatch.setenv("LOOM_TEST_DB_REF", new_reference)

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert not case.output.exists()
    assert not case.trivy_log.exists()


def test_manifest_inspection_stops_an_oversized_producer_at_the_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _case(tmp_path, monkeypatch)
    marker = tmp_path / "oversized-producer-finished"
    monkeypatch.setenv("LOOM_TEST_OVERSIZED_MANIFEST", "1")
    monkeypatch.setenv("LOOM_TEST_OVERSIZED_MARKER", str(marker))

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        prepare_personal_dev_scanner_cache_assets(case.lock, case.trivy, case.output)

    assert not marker.exists()
    assert not case.output.exists()
    assert not case.trivy_log.exists()


def test_manifest_inspection_cleanup_does_not_mask_command_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def denied_process_group_kill(_process_group: int, _signal: int) -> None:
        raise PermissionError("process group is already outside this namespace")

    monkeypatch.setattr(scanner_cache_assets.os, "killpg", denied_process_group_kill)

    with pytest.raises(PersonalDevScannerCacheError, match="preparation failed"):
        scanner_cache_assets._bounded_command_output(
            [sys.executable, "-c", "raise SystemExit(23)"]
        )
