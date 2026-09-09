"""Build runtime identity comes from the existing pinned CI release evidence."""

import hashlib
from importlib import import_module

import pytest

from tests.unit.test_personal_dev_control_plane_config import _canonical, _release, _write_release


def _evidence(release):
    return {
        "schema_version": 4,
        "release": {"repository": "qianyi-sun/loom", "ref": "refs/heads/dev",
                    "commit": release["source_sha"], "tree": release["source_tree"],
                    "run_id": 71, "run_attempt": 1},
        "internal_images": {
            component: {"reference": release["images"][key], "platforms": {
                platform: {"subject_digest": "sha256:" + digest * 64,
                           "scan_report_sha256": "a" * 64, "build": {}}
                for platform, digest in (("linux/amd64", amd), ("linux/arm64", arm))
            }}
            for component, key, amd, arm in (
                ("personal-dev-builder", "personal_dev_builder", "d", "e"),
                ("personal-dev-native-builder-agent", "personal_dev_native_builder_agent", "f", "9"),
            )
        },
        "external_images": {}, "scanner": {},
    }


def _load(tmp_path, *, mutate=None, payload_transform=None):
    release = _release()
    evidence = _evidence(release)
    if mutate:
        mutate(evidence)
    payload = _canonical(evidence)
    release["release_evidence_sha256"] = hashlib.sha256(payload).hexdigest()
    path, digest = _write_release(tmp_path, release)
    return import_module("loom.personal_dev_build_runtime_publication").load_personal_build_runtime_publication(
        path, expected_release_sha256=digest,
        evidence_payload=payload_transform(payload) if payload_transform else payload,
    )


def test_platform_images_and_service_candidate_come_from_pinned_release(tmp_path):
    result = _load(tmp_path)
    assert result.candidate.algorithm == "git-sha1"
    assert result.candidate.identity == "1" * 40
    assert result.source_tree == "2" * 40
    assert result.candidate.publication_sha256 == hashlib.sha256((tmp_path / "trusted-release.json").read_bytes()).hexdigest()
    by_platform = {item.platform: item for item in result.platforms}
    assert set(by_platform) == {"linux/amd64", "linux/arm64"}
    assert by_platform["linux/amd64"].builder_image == "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "d" * 64
    assert by_platform["linux/arm64"].agent_image == "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "9" * 64


@pytest.mark.parametrize("field,value", (
    ("repository", "foreign/loom"), ("ref", "refs/heads/feature/private"),
    ("commit", "a" * 40), ("tree", "b" * 40),
    ("run_id", 0), ("run_attempt", True),
))
def test_mismatched_or_nonrelease_provenance_is_rejected(tmp_path, field, value):
    with pytest.raises(ValueError):
        _load(tmp_path, mutate=lambda evidence: evidence["release"].update({field: value}))


@pytest.mark.parametrize("component", ("personal-dev-builder", "personal-dev-native-builder-agent"))
@pytest.mark.parametrize("boundary", ("reference", "missing", "extra", "digest", "duplicate"))
def test_platform_or_index_substitution_is_rejected(tmp_path, component, boundary):
    def mutate(evidence):
        image = evidence["internal_images"][component]
        if boundary == "reference":
            image["reference"] = "ghcr.io/foreign/runtime@sha256:" + "a" * 64
        elif boundary == "missing":
            del image["platforms"]["linux/amd64"]
        elif boundary == "extra":
            image["platforms"]["linux/neutral"] = image["platforms"]["linux/amd64"]
        elif boundary == "digest":
            image["platforms"]["linux/amd64"]["subject_digest"] = "sha256:" + "0" * 64
        else:
            image["platforms"]["linux/arm64"] = image["platforms"]["linux/amd64"]
    with pytest.raises(ValueError):
        _load(tmp_path, mutate=mutate)


@pytest.mark.parametrize("transform", (
    lambda payload: payload + b"\n", lambda payload: payload.replace(b"refs/heads/dev", b"refs/heads/main"),
    lambda payload: b"x" * (8 * 1024 * 1024 + 1),
))
def test_evidence_must_match_pinned_exact_bytes(tmp_path, transform):
    with pytest.raises(ValueError):
        _load(tmp_path, payload_transform=transform)


def test_old_application_only_release_cannot_become_build_runtime(tmp_path):
    release = _release()
    release["schema_version"] = 3
    del release["images"]["personal_dev_native_builder_agent"]
    path, digest = _write_release(tmp_path, release)
    with pytest.raises(ValueError):
        import_module("loom.personal_dev_build_runtime_publication").load_personal_build_runtime_publication(
            path, expected_release_sha256=digest, evidence_payload=b"{}",
        )


@pytest.mark.parametrize("payload", (b"[]", b"{}", b"{", b"\xff", b"[" * 2000, b'{"a":NaN}', b"x" * (8 * 1024 * 1024 + 1)))
def test_even_digest_pinned_evidence_requires_bounded_valid_shape(tmp_path, payload):
    release = _release()
    release["release_evidence_sha256"] = hashlib.sha256(payload).hexdigest()
    path, digest = _write_release(tmp_path, release)
    with pytest.raises(ValueError):
        import_module("loom.personal_dev_build_runtime_publication").load_personal_build_runtime_publication(
            path, expected_release_sha256=digest, evidence_payload=payload,
        )


@pytest.mark.parametrize("version", (True, 4.0, "4", 3))
def test_evidence_wire_version_is_exact(tmp_path, version):
    with pytest.raises(ValueError):
        _load(tmp_path, mutate=lambda evidence: evidence.update(schema_version=version))


@pytest.mark.parametrize("duplicate", (False, True))
def test_digest_pinned_noncanonical_or_duplicate_key_evidence_is_rejected(tmp_path, duplicate):
    release = _release()
    payload = _canonical(_evidence(release))
    if duplicate:
        payload = payload.replace(b'"schema_version":4', b'"schema_version":4,"schema_version":4')
    else:
        payload += b"\n"
    release["release_evidence_sha256"] = hashlib.sha256(payload).hexdigest()
    path, digest = _write_release(tmp_path, release)
    with pytest.raises(ValueError, match="JSON"):
        import_module("loom.personal_dev_build_runtime_publication").load_personal_build_runtime_publication(
            path, expected_release_sha256=digest, evidence_payload=payload,
        )
