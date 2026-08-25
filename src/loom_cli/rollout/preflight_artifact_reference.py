"""Evidence-bound identity for one immutable preflight artifact publication."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from loom_cli.rollout.preflight_pipeline import PreflightAssessment

if TYPE_CHECKING:
    from loom_cli.rollout.preflight_artifact_store import PreflightArtifactPublication

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EVIDENCE_FIELDS = frozenset(
    {
        "bundle-digest",
        "image-artifact-digest",
        "manifest-artifact-digest",
        "rendered-manifest-digest",
        "migration-manifest-digest",
        "migration-artifact-digest",
        "production-defaults-digest",
    }
)


class PreflightArtifactReferenceError(ValueError):
    """Raised when immutable preflight evidence cannot select one publication."""


@dataclass(frozen=True, slots=True)
class PreflightArtifactReference:
    """Strict projection of the passing ``artifacts.publish`` evidence."""

    bundle_digest: str
    image_artifact_sha256: str
    manifest_artifact_sha256: str
    rendered_manifest_sha256: str
    migration_manifest_sha256: str
    migration_artifact_sha256: str
    production_defaults_sha256: str

    def __post_init__(self) -> None:
        if any(
            _SHA256_RE.fullmatch(value) is None
            for value in (
                self.bundle_digest,
                self.image_artifact_sha256,
                self.manifest_artifact_sha256,
                self.rendered_manifest_sha256,
                self.migration_manifest_sha256,
                self.migration_artifact_sha256,
                self.production_defaults_sha256,
            )
        ):
            raise PreflightArtifactReferenceError(
                "preflight artifact publication evidence is invalid"
            )

    @classmethod
    def from_assessment(
        cls,
        assessment: PreflightAssessment,
    ) -> PreflightArtifactReference:
        if not isinstance(assessment, PreflightAssessment):
            raise PreflightArtifactReferenceError("preflight assessment is invalid")
        matches = tuple(
            execution
            for execution in assessment.executions
            if execution.check_id == "artifacts.publish"
        )
        if len(matches) != 1:
            raise PreflightArtifactReferenceError(
                "preflight artifact publication evidence is missing or ambiguous"
            )
        execution = matches[0]
        if not execution.passed:
            raise PreflightArtifactReferenceError(
                "preflight artifact publication did not pass"
            )
        evidence = dict(execution.evidence)
        if set(evidence) != _EVIDENCE_FIELDS or not all(
            isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None
            for value in evidence.values()
        ):
            raise PreflightArtifactReferenceError(
                "preflight artifact publication evidence is invalid"
            )
        values = cast(dict[str, str], evidence)
        return cls(
            bundle_digest=values["bundle-digest"],
            image_artifact_sha256=values["image-artifact-digest"],
            manifest_artifact_sha256=values["manifest-artifact-digest"],
            rendered_manifest_sha256=values["rendered-manifest-digest"],
            migration_manifest_sha256=values["migration-manifest-digest"],
            migration_artifact_sha256=values["migration-artifact-digest"],
            production_defaults_sha256=values["production-defaults-digest"],
        )

    def require_publication(self, publication: PreflightArtifactPublication) -> None:
        if (
            publication.bundle_digest != self.bundle_digest
            or publication.image_artifact_sha256 != self.image_artifact_sha256
            or publication.manifest_artifact_sha256 != self.manifest_artifact_sha256
            or publication.rendered_manifest_sha256 != self.rendered_manifest_sha256
            or publication.migration_manifest_sha256 != self.migration_manifest_sha256
            or publication.migration_manifest_artifact_sha256
            != self.migration_artifact_sha256
            or publication.production_defaults_sha256 != self.production_defaults_sha256
        ):
            raise PreflightArtifactReferenceError(
                "preflight artifact publication drifted from assessment evidence"
            )

__all__ = ["PreflightArtifactReference", "PreflightArtifactReferenceError"]
