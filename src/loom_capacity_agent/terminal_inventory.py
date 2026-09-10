"""Application-only structural fence for manager-authenticated cleanup evidence.

This is not signature verification or admission authority. The protected import
must still join the exact local attempt, worker and current registration.
"""

from loom_capacity_manager.executable_contracts import (
    ExecutableTerminalInventoryEvidenceV2,
    canonical_executable_bytes,
)
from loom_capacity_manager.typed_inventory_contracts import (
    ExecutableTerminalInventoryEvidenceV3,
    TerminalInventoryEvidence,
    parse_terminal_inventory_evidence,
)


def application_terminal_evidence(value: TerminalInventoryEvidence) -> TerminalInventoryEvidence:
    if type(value) not in (
        ExecutableTerminalInventoryEvidenceV2,
        ExecutableTerminalInventoryEvidenceV3,
    ):
        raise ValueError("application terminal inventory evidence contract is invalid")
    checked = parse_terminal_inventory_evidence(canonical_executable_bytes(value))
    if isinstance(checked, ExecutableTerminalInventoryEvidenceV3):
        proof = checked.record.ownership_proof
        if proof is None or proof.metadata.subject_authority.purpose != "application-worker":
            raise ValueError("application terminal recovery cannot import build-purpose evidence")
    return checked
