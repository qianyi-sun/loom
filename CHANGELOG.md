# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

- Added suite-specific upstream evaluator report normalization for #22.
  SkillFlow JSON reports and SkillLearnBench `report.csv` outputs are summarized
  into `artifacts/evaluator-report.json`, normalized wrapper `metrics`, and an
  `evaluator_report` artifact ref.
- Added a reusable SkillFlow/SkillLearnBench wrapper smoke entrypoint for #22.
  `python -m agentic_data_platform.benchmark_wrappers.smoke` can run fixture
  dry-runs in CI or executable local-upstream checks when an upstream root is
  available.
- Added benchmark wrapper upstream config synthesis for #22. Original
  SkillFlow/SkillLearnBench wrapper runs now write a redacted
  `artifacts/upstream-config.json` runner config artifact from platform model
  metadata, and SkillFlow uses the generated config path instead of the
  committed upstream baseline config.
- Added first upstream output artifact normalization for #22. Executable
  wrapper runs now copy generated upstream output files into
  `artifacts/upstream-output/` and expose them as `upstream_output` artifacts.
- Added the pilot group native workflow architecture target for #103,
  including trial/retry/refinement attempts, verifier rewards, LLM-judge
  feedback, final workspaces, artifact bundles, and future skill object hooks.
- Added multi-evaluator run result semantics for Harbor verifier and platform
  LLM judge outputs while preserving the latest evaluator summary.
- Added a fixture-backed Harbor `jobs/` result ingestor that maps trajectories,
  verifier rewards, collected artifact manifests, and raw jobs archives into
  shared platform records.
- Added the first Harbor local Docker runner backend slice, including injectable
  `harbor run` execution, runner report artifacts, and worker attachment of
  ingested Harbor verifier results.
- Installed Harbor 0.9.0 in the dev image, aligned runner commands with the
  current `harbor run` CLI, and added a deploy-time real Harbor CLI local
  Docker smoke check.
- Fixed the generated Harbor CLI smoke task metadata so Harbor 0.9.0 recognizes
  it as a valid task instead of treating the path as an empty dataset.
- Added Harbor ingestor support for verifier rewards persisted in trial
  `result.json` when standalone reward files are absent.
- Installed the Docker Compose CLI plugin in the dev image so Harbor local
  Docker jobs can call `docker compose` from inside the worker container.
- Updated the frontend launch path and `frontend-smoke` to submit real
  `metadata.harbor_run`, materialize the generated Harbor smoke task in the
  worker, ingest Harbor verifier output, and validate artifact bundle download.

## 0.0.0 - 2026-05-27

- Initialized private repository for agentic data generation and evaluation platform planning.
- Added project brief, GitHub templates, CI placeholder, and deployment placeholder.
