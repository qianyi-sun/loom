"""Resolve native builder images through existing digest-pinned CI evidence.

The expected release digest must come from independently approved operator
authority, not the source candidate or an HTTP caller. This loader checks that
chain; it does not query CI or grant runtime certification/executable admission.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from loom.personal_dev_candidate import PersonalDevPlatform
from loom.personal_dev_control_plane_config import load_personal_dev_trusted_release
from loom_capacity_manager.executable_contracts import CandidateBindingV2

_MAX_EVIDENCE_BYTES = 8 * 1024 * 1024
_SHA256 = re.compile(r"sha256:([0-9a-f]{64})")
_PLATFORMS = ("linux/amd64", "linux/arm64")


@dataclass(frozen=True, slots=True)
class PersonalDevBuildRuntimeImages:
    platform: PersonalDevPlatform
    builder_image: str
    agent_image: str


@dataclass(frozen=True, slots=True)
class PersonalDevBuildRuntimePublication:
    candidate: CandidateBindingV2
    source_tree: str
    release_evidence_sha256: str
    platforms: tuple[PersonalDevBuildRuntimeImages, ...]


def _object(value: object, *, keys: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(value, dict) or (keys is not None and set(value) != keys):
        raise ValueError("personal build runtime evidence structure is invalid")
    return value


def _platform_subjects(value: object, *, reference: str) -> dict[str, str]:
    image = _object(value, keys={"reference", "platforms"})
    if image["reference"] != reference:
        raise ValueError("personal build runtime index differs from trusted release")
    platforms = _object(image["platforms"], keys=set(_PLATFORMS))
    result = {}
    for platform in _PLATFORMS:
        record = _object(platforms[platform], keys={"subject_digest", "scan_report_sha256", "build"})
        digest = record["subject_digest"]
        if not isinstance(digest, str) or not _SHA256.fullmatch(digest) or digest == "sha256:" + "0" * 64:
            raise ValueError("personal build runtime platform digest is invalid")
        result[platform] = reference.partition("@")[0] + "@" + digest
    if len(set(result.values())) != len(_PLATFORMS):
        raise ValueError("personal build runtime platform subjects must be distinct")
    return result


def load_personal_build_runtime_publication(
    release_path: Path, *, expected_release_sha256: str, evidence_payload: bytes,
) -> PersonalDevBuildRuntimePublication:
    """Resolve only the existing release's exact native builder/agent subjects."""
    release = load_personal_dev_trusted_release(release_path, expected_release_sha256)
    if (
        release.schema_version != 4 or release.images.personal_dev_native_builder_agent is None
        or not isinstance(evidence_payload, bytes) or not 0 < len(evidence_payload) <= _MAX_EVIDENCE_BYTES
        or hashlib.sha256(evidence_payload).hexdigest() != release.release_evidence_sha256
    ):
        raise ValueError("personal build runtime evidence does not match trusted release")
    try:
        evidence = _object(json.loads(evidence_payload), keys={
            "schema_version", "release", "internal_images", "external_images", "scanner",
        })
        canonical = json.dumps(evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
        if canonical != evidence_payload:
            raise ValueError("personal build runtime evidence is not canonical")
    except (RecursionError, UnicodeError, ValueError):
        raise ValueError("personal build runtime evidence JSON is invalid") from None
    provenance = _object(evidence["release"], keys={"repository", "ref", "commit", "tree", "run_id", "run_attempt"})
    if (
        type(evidence["schema_version"]) is not int or evidence["schema_version"] != 4
        or provenance["repository"] != "qianyi-sun/loom"
        or provenance["ref"] not in ("refs/heads/dev", "refs/heads/main")
        or provenance["commit"] != release.source_sha or provenance["tree"] != release.source_tree
        or any(type(provenance[name]) is not int or not 0 < provenance[name] <= 2**63 - 1
               for name in ("run_id", "run_attempt"))
    ):
        raise ValueError("personal build runtime provenance differs from trusted release")
    images = _object(evidence["internal_images"])
    builder = _platform_subjects(images.get("personal-dev-builder"), reference=release.images.personal_dev_builder)
    agent = _platform_subjects(images.get("personal-dev-native-builder-agent"), reference=release.images.personal_dev_native_builder_agent)
    return PersonalDevBuildRuntimePublication(
        candidate=CandidateBindingV2(algorithm="git-sha1", identity=release.source_sha,
                                     publication_sha256=expected_release_sha256),
        source_tree=release.source_tree, release_evidence_sha256=release.release_evidence_sha256,
        platforms=tuple(PersonalDevBuildRuntimeImages(
            platform=cast(PersonalDevPlatform, platform), builder_image=builder[platform], agent_image=agent[platform],
        ) for platform in _PLATFORMS),
    )
