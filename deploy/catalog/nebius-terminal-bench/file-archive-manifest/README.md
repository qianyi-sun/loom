# File archive manifest: Nebius preparation

`original/` contains the seven exact source objects read from the old staging
`terminal-bench-2-harbor-90/file-archive-manifest` task on September 10, 2026.
`source-provenance.json` records the original registered checksum and location.
The original objects supplied no file-mode metadata. Do not substitute this
source with the separately versioned Terminal-Bench 2.1 catalog.
The historical registered checksum and the current canonical checksum over the
retrieved objects differ; provenance records both. The historical normalization
has not been established, so this is not a claim of corrupted source. The
derived bundle receives its own identity through the normal materializer.

The original Dockerfile uses Ubuntu 24.04 and architecture-independent shell
commands to create the input tree. Its explicit ARM64 registration does not
reflect an architecture-specific dependency in this task. The derived Dockerfile
retains that fixture construction and adds build-time runtime dependencies.
Build it for `linux/amd64`; the same image is used for task and isolated verifier.

The original verifier bootstrap installs curl, uv 0.9.5, Python 3.13, pytest 8.4.1
and pytest-json-ctrf 0.3.5 during execution. The derived image installs these
before execution. `verifier/run.sh` invokes the unchanged test assertions with
the same pytest arguments offline and emits the same Loom reward/check JSON
shape. A failed assertion still produces numeric reward zero.

The image contains neither tests, verifier scripts nor the oracle solution.
Only the trusted runtime can populate the isolated verifier's private files.
UID/GID 65532 can write `/app`, `/tests`, `/logs/verifier` and `/loom/verifier`;
root and runtime package installation are unnecessary. `/app` retains the baked
`archive_src` fixtures and must not be obscured by an empty workspace mount.

See [the runbook](../../../../docs/runbooks/nebius-terminus2.md) for local
preparation and the boundary between local image checks and live acceptance.
