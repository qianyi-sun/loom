"""Durable synthetic terminal-task authoring contracts."""

from loom.integrations.terminalgen.artifacts import (
    TerminalGenCorpusArtifactV1,
    TerminalGenFinalAuditArtifactV1,
    TerminalTaskBundleArtifactV1,
    TerminalTaskValidationArtifactV1,
    validate_artifact_document,
)
from loom.integrations.terminalgen.contracts import (
    AtomicVariantBucket,
    AtomicWeaknessCardV1,
    AuthoringCatalogV1,
    AuthoringImageLockV1,
    AuthoringParametersV1,
    AuthoringPlanV1,
    CanonicalSourceLockV1,
    Difficulty,
    LicenseAuthorityV1,
    PartitionPlanV1,
    SlotSpecV1,
    SlotTerminalRecordV1,
)
from loom.integrations.terminalgen.planning import build_authoring_plan
from loom.integrations.terminalgen.recipe import (
    TerminalGenRendererLocksV1,
    build_terminalgen_authoring_graph,
)

__all__ = [
    "AtomicVariantBucket",
    "AtomicWeaknessCardV1",
    "AuthoringCatalogV1",
    "AuthoringImageLockV1",
    "AuthoringParametersV1",
    "AuthoringPlanV1",
    "CanonicalSourceLockV1",
    "Difficulty",
    "LicenseAuthorityV1",
    "PartitionPlanV1",
    "SlotSpecV1",
    "SlotTerminalRecordV1",
    "TerminalGenCorpusArtifactV1",
    "TerminalGenFinalAuditArtifactV1",
    "TerminalGenRendererLocksV1",
    "TerminalTaskBundleArtifactV1",
    "TerminalTaskValidationArtifactV1",
    "build_authoring_plan",
    "build_terminalgen_authoring_graph",
    "validate_artifact_document",
]
