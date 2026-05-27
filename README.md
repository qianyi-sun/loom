# Agentic Data Platform

Private platform workspace for agentic data generation and evaluation.

This project is being built as an independent system inspired by the local
`coder-harbor-cloud` reference repository, but it should not copy that codebase
or inherit its cloud/runtime assumptions. Storage, sandbox execution,
evaluation runners, and deployment targets will be replaced behind explicit
interfaces.

## Initial Scope

- Agentic data generation workflows.
- Evaluation workflows for generated trajectories, artifacts, and task results.
- Pluggable storage backend abstraction.
- Pluggable sandbox/execution backend abstraction.
- Project management, CI, and deployment scaffolding through GitHub.

## Repository Layout

```text
docs/                    Product, architecture, and operations notes.
.github/                 Issue templates, PR template, CI, and deploy workflows.
```

## Current Status

This repository is initialized for planning and deployment setup. Application
code will be added after the architecture and MVP boundaries are written down.
