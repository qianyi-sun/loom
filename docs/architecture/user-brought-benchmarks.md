# User-brought benchmarks

This design has moved to [`user-brought-tasksets.md`](user-brought-tasksets.md).

The rename is intentional: native platform benchmarks remain first-class
evaluation objects, while user-uploaded task bundles use TaskSet semantics. A
user TaskSet can still become evaluation-ready when it includes verifier/scoring
configuration, but TaskSet management, API, CLI, and UI should not be framed as
"user benchmarks" by default.
