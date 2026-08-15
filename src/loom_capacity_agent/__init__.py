"""Candidate-independent, zero-executable environment capacity reporter."""

from loom_capacity_agent.admission import (
    PreparedAdmissionPlanV1,
    PreparedBootstrapBindingV1,
    PreparedPlacementAllowanceV1,
    PreparedProtectedReleaseV1,
    PreparedWorkerBindingV1,
    PreparedWorkerShapeV1,
    ProtectedReleasePublicationCheckpointV2,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.claim_guard import (
    ClaimGuard,
    ClaimGuardDecisionV1,
    ClaimProposalV1,
    DisabledClaimGuard,
    InertAttemptTransitionV1,
)
from loom_capacity_agent.claim_guard_store import DatabaseClaimGuard, DatabaseClaimGuardError
from loom_capacity_agent.client import (
    DemandPublishError,
    DemandPublishReceiptV1,
    DemandReporterClient,
    DemandReporterConnection,
    DemandReporterTLSFiles,
    ExecutableProtectedReleasePublishReceiptV2,
    ProtectedReleasePublishReceiptV1,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardDemandAttemptV1,
    GuardDemandObservationV1,
    GuardLifecycleDemandAttemptV2,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.executable_bootstrap import (
    ProtectedExecutableBootstrapCoordinator,
    ProtectedExecutableBootstrapError,
    ProtectedExecutableBootstrapWork,
)
from loom_capacity_agent.executable_release_reporter import (
    ExecutableProtectedReleaseReporterRuntime,
    stable_release_publication_key,
)
from loom_capacity_agent.legacy_fence import (
    LEGACY_MUTATION_INVENTORY_DIGEST,
    LEGACY_MUTATION_PATH_IDS,
    MAX_LEGACY_WRITER_CURSORS,
    LegacyCompatibilityFreezeV1,
    LegacyCompatibilityPreparationV1,
    LegacyWriterCursorV1,
    LegacyWriterFreezeCursorV1,
)
from loom_capacity_agent.legacy_fence_store import (
    LegacyCompatibilityFenceError,
    LegacyCompatibilityFenceStore,
)
from loom_capacity_agent.lifecycle_store import (
    CapacityAttemptLifecycleError,
    CapacityAttemptLifecycleStore,
)
from loom_capacity_agent.prepared_store import (
    CapacityPreparedAdmissionError,
    CapacityPreparedAdmissionStore,
)
from loom_capacity_agent.reporter import (
    DemandReportBlockedError,
    build_demand_snapshot,
    build_lifecycle_demand_snapshot,
)
from loom_capacity_agent.store import (
    CapacityAgentStore,
    CapacityAgentStoreError,
    acknowledge_executable_protected_release_publication,
    capture_demand_observation,
    capture_lifecycle_demand_observation,
    read_agent_lifecycle_demand_observation,
    read_agent_reporter_high_water,
    read_next_executable_protected_release,
)

__all__ = [
    "LEGACY_MUTATION_INVENTORY_DIGEST",
    "LEGACY_MUTATION_PATH_IDS",
    "MAX_LEGACY_WRITER_CURSORS",
    "AgentPoolCapabilityV1",
    "AgentRegistrationV1",
    "CapacityAgentStore",
    "CapacityAgentStoreError",
    "CapacityAttemptLifecycleError",
    "CapacityAttemptLifecycleStore",
    "CapacityPreparedAdmissionError",
    "CapacityPreparedAdmissionStore",
    "ClaimGuard",
    "ClaimGuardDecisionV1",
    "ClaimProposalV1",
    "DatabaseClaimGuard",
    "DatabaseClaimGuardError",
    "DemandPublishError",
    "DemandPublishReceiptV1",
    "DemandReportBlockedError",
    "DemandReporterClient",
    "DemandReporterConnection",
    "DemandReporterTLSFiles",
    "DisabledClaimGuard",
    "ExecutableProtectedReleasePublishReceiptV2",
    "ExecutableProtectedReleaseReporterRuntime",
    "GuardDemandAttemptV1",
    "GuardDemandObservationV1",
    "GuardLifecycleDemandAttemptV2",
    "GuardLifecycleDemandObservationV2",
    "InertAttemptTransitionV1",
    "LegacyCompatibilityFenceError",
    "LegacyCompatibilityFenceStore",
    "LegacyCompatibilityFreezeV1",
    "LegacyCompatibilityPreparationV1",
    "LegacyWriterCursorV1",
    "LegacyWriterFreezeCursorV1",
    "PreparedAdmissionPlanV1",
    "PreparedBootstrapBindingV1",
    "PreparedPlacementAllowanceV1",
    "PreparedProtectedReleaseV1",
    "PreparedWorkerBindingV1",
    "PreparedWorkerShapeV1",
    "ProtectedExecutableBootstrapCoordinator",
    "ProtectedExecutableBootstrapError",
    "ProtectedExecutableBootstrapWork",
    "ProtectedReleasePublicationCheckpointV2",
    "ProtectedReleasePublishReceiptV1",
    "PublishableExecutableProtectedReleaseV2",
    "ReporterConfigurationV1",
    "acknowledge_executable_protected_release_publication",
    "build_demand_snapshot",
    "build_lifecycle_demand_snapshot",
    "capture_demand_observation",
    "capture_lifecycle_demand_observation",
    "read_agent_lifecycle_demand_observation",
    "read_agent_reporter_high_water",
    "read_next_executable_protected_release",
    "stable_release_publication_key",
]
