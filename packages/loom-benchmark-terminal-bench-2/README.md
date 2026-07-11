# loom-benchmark-terminal-bench-2

Harbor-native adapter for Terminal-Bench 2.1 revision 6.

The package keeps the `terminal-bench-2` entry-point key, while its physical
adapter profile is `terminal-bench-2@tb2.1-r6`. It converts exactly the 89
tasks admitted by the checked-in lock for Harbor Hub dataset
`terminal-bench/terminal-bench-2-1@6`.

## Source authority

Harbor Hub metadata version
`sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a`
and its 89 immutable package digests are the execution authority. Conversion
fails closed unless `harbor-materialization.json` and the independently staged
source-reference manifest agree with the lock.

The source-reference snapshot is
`dde3cd95b80ff25af5abd99a80b6513a018ad3b4`. Its sole reviewed package digest
divergence, `terminal-bench/sanitize-git-repo`, is provenance only; it never
changes the Hub execution package or introduces a fallback source.

## Native conversion

The adapter accepts only native directories in this shape:

```text
tasks/<task>/
  task.toml
  instruction.md
  environment/
  tests/test.sh
  solution/solve.sh
```

It preserves the source `task.toml` byte-for-byte as `upstream-task.toml` and
writes a runnable Loom schema-1 `task.toml`, changing the Loom task id to
`terminal-bench-2@tb2.1-r6/<task>`. Supported timeouts, image/build settings,
environment values, task metadata, and artifact paths are normalized into the
runnable config. Native resource, internet, architecture, verifier-env, and
solution metadata remains intact in `upstream-task.toml` for provenance and
preflight. Instructions, environment assets, tests, and solution files retain
their original bytes and modes.

Oracle eligibility is derived only from a non-empty, non-symlink native
`solution/solve.sh`; it is not inferred from a task-id prefix.

## Verifier and reports

`verifier/run.sh` runs the native `tests/test.sh`, copies its test tree to the
native `/tests` location, and reads `/logs/verifier/reward.txt`. A finite
numeric reward, including `0`, is a valid verifier result. Missing, empty,
malformed, non-finite, or timed-out reward evidence is a platform/verifier
failure and is never converted to zero. CTRF and verifier-log paths are kept
in the structured verifier result, and the runnable Loom step appends
`logs/verifier/**` to the native artifact patterns so CTRF and verifier
evidence are collected even when a source task declares `artifacts = []`.

TB2 reports keep legacy task-id stripping for read-only historical reports.
For rev-6 trials they additionally record the physical profile, Hub package
digest and metadata version, source-reference snapshot/divergence, Loom bundle
checksum, verifier identity, and separately supplied runtime provenance.

## License

Apache-2.0, following the Terminal-Bench 2.1 source repository.
