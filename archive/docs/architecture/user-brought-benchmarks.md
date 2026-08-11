# User-brought benchmarks

> Archived redirect. Current user-owned task collection behavior is documented
> in `docs/architecture/user-brought-tasksets.md`.

This design has moved to the current
[`user-brought-tasksets.md`](../../../docs/architecture/user-brought-tasksets.md).

The rename is intentional: native platform benchmarks remain first-class
evaluation objects, while user-uploaded task bundles use TaskSet semantics. A
user TaskSet can still become evaluation-ready when it includes verifier/scoring
configuration, but TaskSet management, API, CLI, and UI should not be framed as
"user benchmarks" by default.
