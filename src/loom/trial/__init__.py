"""Trial composition (spec §2.5 + §3.3).

Submodules (added incrementally as tasks complete):
- artifacts : ArtifactCollector — POSIX glob + MinIO upload
- network   : _phase_network async context manager
- context   : TrialContext + _finalize_trajectory
- trial     : Trial class + run() body
"""
