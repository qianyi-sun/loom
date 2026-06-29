# loom-benchmarks

Benchmark adapters for the [Loom](../../README.md) evaluation runtime.
Each adapter pairs a `BenchmarkAdapter` Protocol implementation with the
upstream-source kind (`huggingface`, `git`, `https-tarball`) so the
`loom_benchmark_tool` CLI can fetch, convert, and ingest benchmark
instances into the Loom Postgres + MinIO state.

See `docs/architecture/benchmark-adapter.md` for the framework
reference (Protocol, canonical task layout, fetchers, how to add a
new adapter). 21 adapters ship today.

## #307 full benchmark wave

These adapters pin full selected official task sets rather than smoke or
sample subsets:

| Slug | Upstream pin | Count | Notes |
|---|---:|---:|---|
| `gpqa` | `idavidrein/gpqa` @ `56686c0` | 546 | GPQA Extended CSV from the password-protected official archive; final A-D answer verifier. |
| `math-500` | `HuggingFaceH4/MATH-500` @ `6e4ed1a` | 500 | Public 500-problem MATH subset; final boxed/exact answer verifier. |
| `hendrycks-math` | `HuggingFaceTB/MATH` @ `140a673` | 5000 | Full `all` config test split; final boxed/exact answer verifier. |
| `mmlu-pro` | `TIGER-Lab/MMLU-Pro` @ `b189ec7` | 12032 | Full test split; supports variable A-J option counts. |
| `tau2-bench` | `HuggingFaceH4/tau2-bench-data` @ `60e37c7` | 278 | Default leaderboard domains: airline, retail, telecom. Bundles domain assets and grades structured `agent_output.json` actions/messages. |
| `browsecomp` | `openai/simple-evals` @ `652c89d` plus CSV ETag `0x8DD785A972BF8A0` | 1266 | Requires network/browsing-capable agents; final `Exact Answer:` verifier. |

Publish/register still follows the normal schema-v3 path. After publication,
`loom datasets register <slug>` must store valid `TaskConfig` rows before the
SPA marks the benchmark ready. Protected shared environments should use
`register --mirror-to-object-store` so runtime task sources point at internal
object storage while HF repo/revision/checksum provenance remains on the task
rows.

## SkillFlow and SkillLearnBench task bundles

`skillflow` points at the public Hugging Face git dataset
`zhang-ziao/SkillFlow-Task` rather than the SkillFlow code repository. The
code repository contains runners and examples, but the benchmark instances are
published as task bundles under `test_tasks/`.

`skilllearnbench` points at `github.com/cxcscmu/SkillLearnBench` pinned at
revision `2d714f2` and uses the upstream task-bundle shape under
`tasks/<family>/<task>/`. Both adapters still support the legacy
per-instance `manifest.json` fixture format used by older Loom tests, but real
publication should consume the official bundle directories.

SkillLearnBench tasks `COPY skills /root/.<agent>/skills`, where `skills/`
lives at the upstream repo root (not in the bundle), as
`skills/<method>/<family>/<skill-name>/`. The chosen method IS the system
under test — the agent reads those copied skills at runtime and the score
reflects skill quality. The adapter materializes the chosen
`skills/<skill_method>/<family>/` into each converted bundle before checksum.
`skill_method` is sourced from the catalog entry's `params.skill_method`
(default `human_authored`); additional methods (e.g.
`b1-one-shot-claude-sonnet-4-6`) are added as sibling catalog rows sharing
the same upstream. The adapter also emits an `oracle_eligible` task tag
derived from upstream `solution/solve.sh` suitability; 58 of the 100 upstream
SLB tasks form the deterministic oracle slate and 42 tag
`oracle_eligible=false`. The excluded tasks are agent-only, require external
oracle secrets, or ship known-bad upstream oracle solutions whose `solve.sh`
does not match the task/tests. The `human_authored` SkillLearnBench catalog row
also declares `params.cpu_arch=any`; converted task configs therefore carry an
explicit portable CPU-architecture requirement and can be scheduled on either
x86_64 or ARM64 workers. Other bundle-backed adapters keep the default
`x86_64` task requirement until their own dual-architecture evidence is added
to catalog metadata.

For official bundles, the adapter copies each task directory, preserves the
upstream `environment/Dockerfile`, writes a Loom-owned `task.toml`, and adds a
script-verifier shim at `verifier/run.sh`. Upstream SkillFlow task Dockerfiles
currently reference an unpublished Harbor CLI image; the adapter normalizes
that base to `skillflow/harbor-cli-base:ubuntu24.04`, matching the SkillFlow
code repository's documented local base image. Operators must prebuild that
base image on every Docker host that may build SkillFlow task images, or push
it to a registry reachable under the same tag. The adapter also chooses the
Docker build context from the Dockerfile's `COPY`/`ADD` sources: ordinary
SkillFlow bundles build from `environment/`, while bundles that copy `skills`
or root-level data assets such as `DATA/`, `data/`, spreadsheets, PDFs, or
archives build from the bundle root. When a root build context is required,
the adapter mirrors Dockerfile sources found under `environment/` into the
bundle root so mixed upstream Dockerfiles can still resolve local assets such
as spreadsheets or `data/`. SkillFlow solution scripts that reference files
under absolute `/solution/...` are normalized at conversion time so oracle
smoke runs can execute the materialized `solution/solve.sh` from the solution
directory. SkillLearnBench conversion also rewrites unsupported classic-Docker
heredoc `RUN <<EOF` forms into copied shell scripts and normalizes
Python-to-Scala oracle output back to the task root when the upstream verifier
expects root-level artifacts. The shim runs the upstream
`tests/test.sh` from the task root, reads `/logs/verifier/reward.txt`, and
converts that reward into Loom's structured `VerifierResult` JSON. When the
upstream test writes `/logs/verifier/output.log`, the shim includes the log
tail in `structured.output_log_tail` so reward-0 verifier failures remain
diagnosable after the sandbox is removed. Instance ids are derived from the
relative bundle path and sanitized so spaces or shell-significant characters
in upstream folder names cannot create invalid catalog task ids.

## LiveCodeBench coverage

The LiveCodeBench adapter targets the official Hugging Face dataset
`livecodebench/code_generation_lite` pinned at revision
`0fe84c3912ea0c4d4a78037083943e8f0c4dd505`. That pinned split currently
contains 1055 tasks. The adapter decodes both public and private test cases:
2762 public cases plus 25492 compressed private cases, for 28254 total cases.

Generated bundles support both upstream test modes. `stdin` cases run the
agent's `solution/solution.py` in a subprocess and compare stdout exactly.
`functional` cases import `Solution`, call `metadata.func_name`, parse
single-argument JSON inputs or multi-line positional JSON arguments, and
compare the returned Python value to the expected JSON value. The benchmark is
licensed `CC-BY-NC-4.0`, so teams must explicitly allow non-commercial
execution before submitting runs.

## SWE-Bench Verified coverage

The SWE-Bench Verified adapter targets the official Hugging Face dataset
`princeton-nlp/SWE-bench_Verified` pinned at revision
`c104f840cc67f8b6eec6f759ebc8b2693d585d4a`. That pinned test split contains
500 tasks.

Each converted bundle writes the issue body to `instruction.md`, the reference
patch application script to `solution/solve.sh`, and a pytest verifier that runs
the upstream `FAIL_TO_PASS` plus `PASS_TO_PASS` node ids inside the per-instance
SWE-Bench eval image. The generated `task.toml` points at
`swebench/sweb.eval.x86_64.<instance>:latest`, so service-mode workers must be
able to pull those per-instance images before executing the benchmark.
Some SWE-Bench Multimodal rows ship empty upstream test-id lists; those bundles
now emit a self-contained script verifier with an explicit diagnostic check,
producing numeric reward `0` instead of depending on image-local pytest.

Republish this benchmark with the schema-v3 manifest path before treating it as
ready. Older registered rows without `task_config` remain explicit legacy
placeholders and should audit as `Needs republish`, not runnable tasks.

## UI-agent adapters

OSWorld and WebArena adapters can convert upstream task metadata into Loom task
bundles, but those rows are intentionally marked `Not supported yet` by the
service readiness layer. They require a UI benchmark runtime rather than the
normal single Docker image contract: OSWorld needs desktop VM/DesktopEnv
support, and WebArena needs browser-agent control plus self-hosted site/auth
reset and URL/HTML evaluators. Keep them visible in the catalog, but do not
treat converted rows as supported runnable tasks until the runtime follow-ups
land.

## GAIA adapter

GAIA remains a catalog adapter but is intentionally marked `Deferred` by the
service readiness layer until operators can publish the gated dataset through a
GAIA-authorized Hugging Face access path. Keep it visible in the catalog, but
do not treat placeholder or unpublished rows as supported runnable tasks.

## BFCL output contract

The BFCL adapter targets the upstream v4 task layout under
`berkeley-function-call-leaderboard/bfcl_eval/data`, pinned at Gorilla commit
`6ea57973c7a6097fd7c5915698c54c17c5b1b6c8`. It publishes every
JSONL task row with either the matching `possible_answer/` ground truth or
the official relevance/irrelevance call-presence objective. Current upstream
v4 coverage is 4696 tasks.

BFCL tasks ask the selected agent to write `agent_output.json` rather than
free-form prose. Single-turn tasks use:

```json
{"calls": [{"name": "function_name", "arguments": {"arg": "value"}}]}
```

If no function should be called, write `{"calls": []}`. Multi-turn tasks use
`{"turns": [[...turn 1 calls...], [...turn 2 calls...]]}`. The bundled
script verifier is self-contained in each task bundle and scores ordinary
function-call matches, relevance/irrelevance call presence, and multi-turn
call sequences without requiring `/opt/bfcl/evaluator.py` in the sandbox
image.
