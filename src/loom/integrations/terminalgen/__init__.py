"""Durable synthetic terminal-task authoring contracts."""

from __future__ import annotations

from importlib import import_module

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
from loom.integrations.terminalgen.validation_policy import (
    TERMINALGEN_VALIDATION_POLICY_DIGEST,
    terminalgen_validation_argv,
)

_AUTHORITY_EXPORTS = frozenset(
    {
        "TERMINALGEN_POOL_POLICIES",
        "TERMINALGEN_RECIPE_NAME",
        "TERMINALGEN_RECIPE_VERSION",
        "TERMINALGEN_RUNTIME_POLICY_DIGEST",
        "TerminalGenAuthorityError",
        "TerminalGenPoolPolicy",
        "build_terminalgen_authoring_grant",
        "build_terminal_task_validation_grant",
    }
)


def __getattr__(name: str) -> object:
    """Load runtime authority only for callers that explicitly request it.

    Artifact validation is imported by the closed Stage-1 simulator image,
    whose intentionally minimal runtime does not include SQLAlchemy.  Keeping
    authority out of package initialization preserves that image boundary
    while retaining the package-level exports for control-plane callers.
    """

    if name not in _AUTHORITY_EXPORTS:
        raise AttributeError(name)
    value = getattr(import_module("loom.integrations.terminalgen.authority"), name)
    globals()[name] = value
    return value

__all__ = [
    "TERMINALGEN_POOL_POLICIES",
    "TERMINALGEN_RECIPE_NAME",
    "TERMINALGEN_RECIPE_VERSION",
    "TERMINALGEN_RUNTIME_POLICY_DIGEST",
    "TERMINALGEN_VALIDATION_POLICY_DIGEST",
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
    "TerminalGenAuthorityError",
    "TerminalGenCorpusArtifactV1",
    "TerminalGenFinalAuditArtifactV1",
    "TerminalGenPoolPolicy",
    "TerminalGenRendererLocksV1",
    "TerminalTaskBundleArtifactV1",
    "TerminalTaskValidationArtifactV1",
    "build_authoring_plan",
    "build_terminal_task_validation_grant",
    "build_terminalgen_authoring_grant",
    "build_terminalgen_authoring_graph",
    "terminalgen_validation_argv",
    "validate_artifact_document",
]
