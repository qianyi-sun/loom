# Validation artifacts

This directory contains machine-readable schemas consumed by current
repository checks. It is not a narrative documentation or release-history
area.

The active schemas define non-exclusive Slurm acceptance, task-image builder
prerequisite conformance, and shared sandbox capacity evidence. The current
benchmark reward contract is documented under
[`../score-alignment/`](../score-alignment/README.md).

Task-image builder prerequisite conformance v1 records the dedicated
controller Unix identity, the current immutable legacy builder contract, and
the verified binding from each Slurm node alias to its local physical host.
These are prerequisite observations, not certification or activation
evidence. A valid Phase 1 envelope still has `certified_nodes=[]`, keeps
`production_certification_allowed=false`, and cannot activate a builder.

Generated run reports and dated narrative summaries belong in external release
artifacts or [`../../archive/docs/evidence/`](../../archive/docs/evidence/), not
in the authoritative documentation tree.
