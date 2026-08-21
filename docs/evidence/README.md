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
the verified binding from each Slurm node alias to its local physical host. Its
controller and node fragments also carry the canonical Slurm, host-convergence,
and maintenance receipts; signed package provenance; installed runtime and
dependency digests; Slurm/cgroup readback; dedicated mount and project-quota
readback; allocation-scoped smoke containment; terminal accounting; reservation
lifecycle; and cleanup facts.

Use `scripts/ops/task_image_builder_prerequisite_evidence.py` to
`collect-controller` or `collect-node` on the corresponding local authority,
then transport those owner-readable fragments to a controller and `assemble`
them. Every input path is explicit, outputs are created once with mode `0600`,
and assembly performs no SSH or live mutation. Use
`scripts/ops/task_image_builder_prerequisite_conformance.py verify` separately
to apply schema and semantic verification; `canonicalize` writes a verified
envelope once without becoming a collection authority.

These are prerequisite observations, not certification or activation
evidence. Even a complete, fresh OLDLAB-and-GB10 envelope has
`certified_nodes=[]`, keeps `production_certification_allowed=false`, and
retains `blockers=["phase2_guard_provider_release_missing"]`. It cannot activate
a builder, certify a production node, or authorize a task rerun.

Generated run reports and dated narrative summaries belong in external release
artifacts or [`../../archive/docs/evidence/`](../../archive/docs/evidence/), not
in the authoritative documentation tree.
