# Original Benchmark Runner Wrapper Spike

Last updated: 2026-06-01

Related trackers:

- Fixture catalog: https://github.com/carinrc/agentic-data-platform/issues/32
- Original runner wrappers: https://github.com/carinrc/agentic-data-platform/issues/33
- SkillFlow and SkillLearnBench adapters: https://github.com/carinrc/agentic-data-platform/issues/22
- User runner and pipeline interface: https://github.com/carinrc/agentic-data-platform/issues/21

## Purpose

The pilot group MVP should first run SkillFlow and SkillLearnBench through
their original benchmark semantics, while the platform owns common run records,
terminal trajectories, workspace snapshots, evaluator feedback, artifact
storage, and dashboard visibility.

This spike records the upstream runner entrypoints and the minimal wrapper
contract the platform should implement before production benchmark execution.
The first concrete wrappers now live in
`agentic_data_platform.benchmark_wrappers.skillflow` and
`agentic_data_platform.benchmark_wrappers.skilllearnbench`.

## Upstream References

| Suite | Source | Version recorded here | Task layout |
| --- | --- | --- | --- |
| SkillFlow | `https://github.com/ZhangZi-a/SkillFlow` plus `https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task` | Runner commit `7b49ff5a7e26cd7706e959bfa0dba4746d18440d`; task dataset commit `ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc` | `test_tasks/<workflow-family>/<task-name>/{instruction.md,task.toml,environment,tests,solution}` |
| SkillLearnBench | `https://github.com/cxcscmu/SkillLearnBench` | Commit `638284f5982f6be085a955435d2ec7a5258f5513` | `tasks/<task-id>/<task-id>-<n>/{instruction.md,task.toml,environment,tests,solution}` |

The checked-in seed catalogs live under
`src/agentic_data_platform/benchmarks/catalogs/`. They are intentionally small:
two task families per suite and two instances per family. They are not full
benchmark mirrors and do not download source assets.

## SkillFlow Runner Shape

Observed upstream commands:

```bash
python family_job_runner.py \
  --config configs/baseline.yaml \
  --dataset-path test_tasks/<workflow-family> \
  --run-root-dir <output-dir> \
  --only-group <workflow-family>
```

```bash
python iterative_shared_skills_runner.py \
  --config configs/iter.yaml \
  --dataset-path test_tasks/<workflow-family> \
  --run-root-dir <output-dir> \
  --only-group <workflow-family>
```

For the platform's single-family wrapper path, `--dataset-path` points at the
workflow-family directory. Passing the broader `test_tasks` root causes the
pinned runner to scan workflow-family directories as if they were task
directories.

Useful options:

- `--dry-run`
- `--max-parallel-trials` for baseline family jobs
- `--max-parallel-groups` for iterative shared-skill jobs
- `--max-steps`, `--max-obs-chars`, and patch-generation limits for iterative
  shared-skill runs

Environment assumptions:

- Task assets are expected under a local `test_tasks/` directory, usually
  downloaded from the Hugging Face dataset.
- The README describes building `docker/harbor-cli-base` and optionally
  prebuilding task images with `utils/prebuild_task_images.py`.
- Config files carry model/provider settings. The platform wrapper should
  synthesize config from the platform run request rather than asking users to
  edit YAML by hand. Native terminal-agent runs now have an OpenAI-compatible
  `ModelProvider` path through `provider_config_id`; original upstream
  SkillFlow config synthesis still needs to map that same safe provider ref into
  the suite's expected YAML shape.

Generated outputs to normalize:

- Upstream run directory under `--run-root-dir`.
- Trial logs and terminal trajectories.
- Final task workspace files.
- Benchmark/evaluator reports if present.

## SkillLearnBench Runner Shape

Observed upstream commands:

```bash
python generate_skills.py \
  --tasks <task-id> \
  --methods <method-id> \
  --models <model-name>
```

```bash
python evaluate_skills.py <task-id> \
  --skill-path <skills-dir-or-none> \
  --trials-dir <output-dir>
```

Useful options:

- `--dry-run`
- `--max-workers`, with upstream guidance to keep this at or below 50 for API
  rate limits
- `--build-workers`
- `--subtask-range`
- `--repeats`
- `--max-steps`
- `--skip-metrics` and `--metrics-only`
- `--judge-model`, defaulting upstream to `gpt-5-mini`

Environment assumptions:

- Docker is required because each trial runs in a container.
- API keys are read from `.env` and provider environment variables. Native
  terminal-agent runs now resolve the model API key server-side from
  `provider_config_id`, but the SkillLearnBench wrapper still needs an explicit
  mapping from the platform provider registry to upstream variables such as
  `ANTHROPIC_API_KEY`.
- Some tasks require extra variables such as `GH_TOKEN`.
- The upstream task tree contains per-instance `instruction.md`, `task.toml`,
  `environment/`, `tests/`, and `solution/`.

Generated outputs to normalize:

- `output/evaluation_reports/<method>/<task>/report.csv`.
- Per-trial result files unless `--no-record` is used.
- Trial workspaces and logs under the configured output/trials directory.
- Skill generation outputs under `output/skill_generation_results/` for
  generation workflows.

## Platform Wrapper Contract

The platform should expose a single wrapper shape for original benchmark
runners, even though SkillFlow and SkillLearnBench call different scripts.

Input:

```text
/input/task.json
/input/files/*
```

`task.json` should contain:

```json
{
  "run_id": "run_123",
  "suite_name": "SkillLearnBench",
  "benchmark_version": "git:cxcscmu/SkillLearnBench@638284f5982f6be085a955435d2ec7a5258f5513",
  "task_family": "financial-analysis",
  "instance_id": "financial-analysis-1",
  "instruction_ref": "tasks/financial-analysis/financial-analysis-1/instruction.md",
  "input_files": ["tasks/financial-analysis/financial-analysis-1/environment/"],
  "model": {
    "provider": "api-provider",
    "model_name": "api-model"
  },
  "output_dir": "/output",
  "artifacts_dir": "/output/artifacts"
}
```

Execution command:

```bash
python -m agentic_data_platform.benchmark_wrappers.<suite> \
  --task-manifest /input/task.json \
  --workspace /workspace \
  --output /output/result.json \
  --artifacts-dir /output/artifacts \
  --upstream-root /opt/upstream/<suite> \
  --timeout-seconds 3600
```

The same command supports a no-execution smoke mode:

```bash
python -m agentic_data_platform.benchmark_wrappers.<suite> \
  --task-manifest /input/task.json \
  --workspace /workspace \
  --output /output/result.json \
  --artifacts-dir /output/artifacts \
  --dry-run
```

The reusable wrapper smoke entrypoint builds the fixture-derived manifest and
invokes the same wrapper envelope:

```bash
python -m agentic_data_platform.benchmark_wrappers.smoke \
  --suite SkillFlow \
  --dry-run
```

When a local upstream checkout is available, use `--execute` with
`--upstream-root`:

```bash
python -m agentic_data_platform.benchmark_wrappers.smoke \
  --suite SkillLearnBench \
  --upstream-root /opt/upstream/SkillLearnBench \
  --execute
```

For a real-upstream smoke that prepares the pinned source first, use:

```bash
python -m agentic_data_platform.benchmark_wrappers.real_upstream_smoke
```

The default command targets SkillFlow. It materializes the pinned runner from
the fixture catalog metadata, applies the tracked Harbor API source patch,
downloads the selected Hugging Face task-family subset under the materialized
root, and then calls the same executable wrapper smoke. The Compose service
`benchmark-real-upstream-smoke` runs this command with cache and workspace
paths under `SANDBOX_HOST_WORKSPACE_ROOT`, which keeps Harbor child-container
bind mounts visible to the host Docker daemon.

Required output:

```json
{
  "status": "completed",
  "metrics": {},
  "artifacts": [],
  "trajectory_ref": "artifacts/trajectory.jsonl",
  "workspace_ref": "artifacts/workspace.tar.zst",
  "evaluator_report_ref": "artifacts/evaluator-report.json",
  "failure_reason": null
}
```

The wrapper should also preserve raw stdout/stderr logs and any upstream report
files as artifacts. The platform run record remains the source of truth for
status, model config, source version, task identity, and evaluator visibility.

Implemented wrapper behavior:

- SkillFlow writes a normalized `result.json` and a
  `artifacts/planned-command.json` file for the upstream
  `family_job_runner.py` command. It synthesizes
  `artifacts/upstream-config.json` from platform model metadata and passes that
  generated path to `--config` instead of relying on a committed upstream
  baseline config. The executable planned command passes
  `--dataset-path test_tasks/<task_family>` for the selected workflow family.
  In executable mode it requires `--upstream-root`, invokes the upstream script
  there, and writes `artifacts/stdout.log` and `artifacts/stderr.log`.
- SkillLearnBench writes the same normalized output for
  `evaluate_skills.py <task>`; when an instance id ends in a numeric suffix,
  the wrapper emits `--subtask-range N-N` so the planned command is
  instance-scoped.
- Both wrappers validate the suite name in `/input/task.json`, write the
  redacted `artifacts/upstream-config.json` runner config artifact, expose
  `ADP_*` environment variables to the upstream process, preserve stdout/stderr
  logs, copy generated upstream output files into
  `artifacts/upstream-output/` as `upstream_output` artifacts, summarize
  SkillFlow JSON reports and SkillLearnBench `report.csv` files into
  `artifacts/evaluator-report.json` plus normalized wrapper `metrics`, and map
  non-zero upstream exits or upstream timeouts to failed wrapper results.
- `--dry-run` still writes planned-command artifacts without invoking upstream
  code or requiring model API keys.
- `agentic_data_platform.benchmark_wrappers.smoke` creates a temporary
  fixture-derived task manifest, runs the selected wrapper, and returns a JSON
  summary with status, planned command, stdout/stderr, and artifact paths. It is
  suitable for CI dry-runs and for local real-upstream checks when upstream
  roots are mounted.
- `agentic_data_platform.benchmark_wrappers.real_upstream_smoke` adds the
  #114 evidence path above that materializes the source root before invoking the
  wrapper smoke. It is opt-in because it can download benchmark assets and run
  upstream Docker jobs.

The generated upstream manifest import path lives in
`agentic_data_platform.benchmarks.manifests`. It accepts generated repository or
dataset path lists, or scans a local upstream tree, groups paths into benchmark
task families and task instances, verifies each instance has `instruction.md`
and `task.toml`, and returns the same `BenchmarkFixtureCatalog` shape used by
checked-in seed fixtures.

The upstream source materializer also owns source-level compatibility patches.
For the pinned SkillFlow runner commit
`7b49ff5a7e26cd7706e959bfa0dba4746d18440d`, it applies
`skillflow-harbor-api-compat-20260601`, records the patch id and SHA-256 in
`adp-upstream-source-lock.json`, and updates SkillFlow's runner code to use the
current Harbor validation and async job-creation APIs. This keeps the
compatibility fix at the upstream source boundary rather than hiding it in a
runtime wrapper monkeypatch.

SkillFlow task assets are materialized through the same source boundary, but
with a separate lock because they come from Hugging Face rather than the runner
Git repo. The checked-in catalog pins `zhang-ziao/SkillFlow-Task` to dataset
commit `ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc`. The real-upstream smoke
hydrates `test_tasks/<task_family>/**` into the materialized runner root and
writes `adp-skillflow-task-assets-lock.json` with the dataset repo, revision,
allow patterns, local materialization directory, and hydrated file count.

## Platform-Control And Sandbox Split

Platform control plane owns:

- Run id, project/team ownership, status transitions, retries, and cancellation.
- Model provider configuration and API-only model policy.
- Task catalog metadata and source version identity.
- Artifact persistence keys and dashboard projections.
- Evaluator adapter invocation and verbal feedback storage.

Sandbox or wrapper owns:

- Materializing pinned upstream benchmark repos or local mirrors into a stable
  source cache before wrapper execution.
- Materializing upstream benchmark files into a workspace.
- Running original benchmark scripts and their Docker child containers.
- Capturing raw output directories, logs, and upstream report files.
- Reporting normalized wrapper status and artifact paths back to the platform.

The sandbox should not own platform lifecycle decisions or dashboard schemas.
The platform should not require researchers to modify upstream benchmark code
just to run a supported suite.

## Follow-Ups

- Generate a full SkillFlow manifest from the pinned Hugging Face dataset
  commit once the v0 executable task subset expands beyond the checked-in seed
  catalog.
- Extend upstream checkout/download management if SkillFlow later needs
  additional Hugging Face private assets or Git LFS credential handling.
- Expand benchmark-specific config synthesis beyond the current redacted
  `artifacts/upstream-config.json` contract so upstream runners can consume the
  same safe platform provider refs through suite-native YAML or environment
  files.
- Continue refining suite-specific report parsing against real upstream output
  samples as new stable shapes appear. The first real SkillFlow Harbor
  `result.json` parser now extracts trial counts, evaluator error count, and
  mean score.
- Run the opt-in `benchmark-real-upstream-smoke` service on shared dev, where
  Docker is installed, and record whether SkillFlow completes or now blocks on
  model/provider configuration rather than local Docker availability.
- Add a user-facing custom runner contract under #21 using the same input and
  output envelope.
- Decide how task-specific secrets such as `GH_TOKEN` are requested, scoped, and
  redacted before dashboard exposure.
