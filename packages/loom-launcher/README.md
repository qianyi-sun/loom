# loom-launcher

Pluggable agent harnesses + sandbox capture utilities for [Loom](https://github.com/carinrc/loom) — a team platform for running LLMs on customizable tasks.

Two consumers:
- **Workers** install this package to read adapter metadata (env vars, argv, capture pattern) and construct a `SubprocessAgent`.
- **Sandboxes** run the argv built by the adapter and emit data that the adapter capture helpers can parse.

The launcher package does not install third-party agent CLIs. Service-mode deployments must provision each adapter's executable or Python module in the trial sandbox image before marking that adapter ready in `GET /api/v1/agents`.

See `docs/architecture/agent-adapter.md` in the Loom repo for the framework reference.
