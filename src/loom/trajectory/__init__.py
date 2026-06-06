"""Trajectory storage surface — append-only event log to MinIO with deterministic
ATIF projection at finalize.

Submodules (added incrementally as tasks complete):
- storage  : ObjectStore Protocol + FakeObjectStore + boto3-backed MinioObjectStore
- writer   : TrajectoryWriter (local-first append + S3 multipart upload)
- reader   : TrajectoryReader (iter, tail, kind-filter, excerpt)
- excerpt  : ExcerptStrategy types
- atif     : ATIF v1.7 models + project_to_atif
"""
