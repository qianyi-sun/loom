# Original Benchmark Runner Wrapper Spike

Last updated: 2026-05-28

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
| SkillFlow | `https://github.com/ZhangZi-a/SkillFlow` plus `https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task` | Runner commit `7b49ff5a7e26cd7706e959bfa0dba4746d18440d`; dataset placeholder `main-2026-05-28-placeholder` | `test_tasks/<workflow-family>/<task-name>/{instruction.md,task.toml,environment,tests,solution}` |
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
  --dataset-path test_tasks \
  --run-root-dir <output-dir> \
  --only-group <workflow-family>
```

```bash
python iterative_shared_skills_runner.py \
  --config configs/iter.yaml \
  --dataset-path test_tasks \
  --run-root-dir <output-dir> \
  --only-group <workflow-family>
```

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
  edit YAML by hand.

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
- API keys are read from `.env` and provider environment variables.
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
  baseline config. In executable mode it requires `--upstream-root`, invokes the
  upstream script there, and writes `artifacts/stdout.log` and
  `artifacts/stderr.log`.
- SkillLearnBench writes the same normalized output for
  `evaluate_skills.py <task>`; when an instance id ends in a numeric suffix,
  the wrapper emits `--subtask-range N-N` so the planned command is
  instance-scoped.
- Both wrappers validate the suite name in `/input/task.json`, write the
  redacted `artifacts/upstream-config.json` runner config artifact, expose
  `ADP_*` environment variables to the upstream process, preserve stdout/stderr
  logs, copy generated upstream output files into
  `artifacts/upstream-output/` as `upstream_output` artifacts, and map non-zero
  upstream exits or upstream timeouts to failed wrapper results.
- `--dry-run` still writes planned-command artifacts without invoking upstream
  code or requiring model API keys.

The generated upstream manifest import path lives in
`agentic_data_platform.benchmarks.manifests`. It accepts generated repository or
dataset path lists, or scans a local upstream tree, groups paths into benchmark
task families and task instances, verifies each instance has `instruction.md`
and `task.toml`, and returns the same `BenchmarkFixtureCatalog` shape used by
checked-in seed fixtures.

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

- Replace the SkillFlow dataset version placeholder with a pinned Hugging Face
  dataset snapshot or generated manifest hash.
- Extend upstream checkout/download management beyond the current local-tree and
  git cache/lock boundary if SkillFlow needs additional Hugging Face dataset or
  Git LFS handling.
- Expand benchmark-specific config synthesis beyond the current redacted
  `artifacts/upstream-config.json` contract if upstream runners require
  suite-native YAML or environment files.
- Deepen suite-specific parsing of upstream report directories once real
  upstream output shapes are stable; the current wrapper already preserves
  generated files as first-class `upstream_output` artifacts.
- Add optional smoke tests that run against real upstream dry-run commands when
  the upstream repos are available locally.
- Add a user-facing custom runner contract under #21 using the same input and
  output envelope.
- Decide how task-specific secrets such as `GH_TOKEN` are requested, scoped, and
  redacted before dashboard exposure.
