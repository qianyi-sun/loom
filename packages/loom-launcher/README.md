# loom-launcher

Pluggable agent harnesses + sandbox capture utilities for [Loom](https://github.com/carinrc/loom) — a team platform for running LLMs on customizable tasks.

Two consumers:
- **Workers** install this package to read adapter metadata (env vars, argv, capture pattern) and construct a `SubprocessAgent`.
- **Sandboxes** install this package via the task's `environment/Dockerfile` so the agent CLI's output can be parsed in-place when the capture pattern needs file/HTTP access from inside the container.

See `docs/architecture/agent-adapter.md` in the Loom repo for the framework reference.
