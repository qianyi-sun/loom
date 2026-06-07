# loom-launcher

Per-agent adapters + sandbox capture utilities for the [Loom](https://github.com/carin-research/loom) agent-evaluation runtime.

Two consumers:
- **Workers** install this package to read adapter metadata (env vars, argv, capture pattern) and construct a `SubprocessAgent`.
- **Sandboxes** install this package via the task's `environment/Dockerfile` so the agent CLI's output can be parsed in-place when the capture pattern needs file/HTTP access from inside the container.

See `docs/superpowers/specs/2026-06-06-loom-agent-integrations-design.md` in the Loom repo for the design.
