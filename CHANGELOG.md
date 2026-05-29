# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Added multi-evaluator run result semantics for Harbor verifier and platform
  LLM judge outputs while preserving the latest evaluator summary.
- Added a fixture-backed Harbor `jobs/` result ingestor that maps trajectories,
  verifier rewards, collected artifact manifests, and raw jobs archives into
  shared platform records.
- Added the first Harbor local Docker runner backend slice, including injectable
  `harbor run` execution, runner report artifacts, and worker attachment of
  ingested Harbor verifier results.

## 0.0.0 - 2026-05-27

- Initialized private repository for agentic data generation and evaluation platform planning.
- Added project brief, GitHub templates, CI placeholder, and deployment placeholder.
