# Loom — Benchmark Integrations Design

**Status:** DRAFT — awaiting user review.
**Date:** 2026-06-06
**Owner:** Hongjian + Claude.
**Scope:** Ingestion adapters for 12 public benchmark suites + the shared infrastructure (CLI tool, MinIO bundle store, license tracking, content-addressed import) that makes Loom usable for benchmark-driven research.

---

## 1. Goal

v0.7 leaves benchmark integration as an exercise to the operator — they hand-write `task.toml` files from scratch. To match Harbor's reach (SkillFlow + SkillLearnBench) and attract external stakeholder groups, Loom needs first-class adapters for the public benchmarks researchers already use. This spec covers the 12 benchmarks the team selected:

| Benchmark               | Domain              | Verifier shape    | Upstream source                                | v1 ready?    |
|-------------------------|---------------------|-------------------|------------------------------------------------|--------------|
| SWE-Bench Verified      | Software engineering| pytest            | HF: princeton-nlp/SWE-bench_Verified            | ✓            |
| SWE-Bench (full)        | Software engineering| pytest            | HF: princeton-nlp/SWE-bench                     | ✓            |
| SWE-Bench Multimodal    | Software engineering| pytest            | HF: princeton-nlp/SWE-bench_Multimodal          | ✓            |
| OSWorld                 | OS interaction      | structured (script)| GitHub: xlang-ai/OSWorld                       | **deferred** |
| WebArena                | Web tasks           | structured (script)| GitHub: web-arena-x/webarena                   | **deferred** |
| HumanEval               | Code generation     | pytest            | HF: openai_humaneval                            | ✓            |
| MBPP                    | Code generation     | pytest            | HF: mbpp                                        | ✓            |
| BFCL                    | Function calling    | structured        | GitHub: ShishirPatil/gorilla                   | ✓            |
| GAIA                    | General AI Assistant| llm-judge         | HF: gaia-benchmark/GAIA                         | ✓ (gated CC-BY-4.0) |
| LiveCodeBench           | Code (contamination-resistant)| pytest    | HF: livecodebench/code_generation_lite          | ✓            |
| AIME                    | Math reasoning      | structured        | HF: AI-MO/aimo-validation-aime                  | ✓ (gated MAA terms) |
| SkillFlow + SkillLearnBench | Skill learning  | pytest + structured| CARIN-internal (Harbor parity)                  | ✓            |

**OSWorld + WebArena deferred to v1.5.** OSWorld requires a desktop OS
VM (VMware / VirtualBox / KVM) with a running GUI — Loom v0.7's
`DockerDriver` cannot host it. A new VM-based Driver (e.g.,
`FirecrackerDriver` or `LibvirtDriver`) is a prerequisite. WebArena
requires per-task multi-service stacks (a GitLab + Reddit + Map server
docker-compose per instance) that don't fit Loom's single-container
sandbox model; a new "multi-container task" abstraction is the
prerequisite. Both deferrals are documented in §15 (out of scope) and
the v1 spec covers the 10 remaining benchmarks.

After v1 ships, importing a benchmark is one command:

```
$ loom-benchmark-tool import swe-bench-verified --db-url $LOOM_CP_DB_URL
✓ Resolved upstream: princeton-nlp/SWE-bench_Verified @ HF
✓ Cached 500 instances to MinIO under benchmarks/swe-bench-verified/
✓ Registered 500 tasks in tasks table (skipped 0 existing)
✓ License: MIT (compatible with team policy)
```

## 2. Architecture

Three additive components:

```
PyPI: loom-benchmarks (NEW package, separate from loom-launcher)
  ├─ BenchmarkAdapter Protocol           ← contract
  ├─ 12 adapter instances                ← per-benchmark conversion logic
  └─ Shared conversion utilities         ← HF dataset loaders, pytest scaffolding, etc.

scripts/loom_benchmark_tool/             ← CLI wrapping loom-benchmarks
  ├─ __main__.py                         ← `python -m loom_benchmark_tool`
  └─ import_cmd.py                       ← the import workflow

src/loom/db/schema.py
  ├─ tasks.license            (NEW)      ← per-task SPDX-style license tag
  ├─ tasks.benchmark_id       (NEW)      ← parent benchmark, nullable for hand-authored
  └─ benchmarks               (NEW)      ← one row per registered benchmark
```

The pipeline runs in three stages: **fetch** (pull the upstream), **convert** (BenchmarkAdapter emits Task fixtures into a local tempdir), **publish** (CLI uploads fixtures to MinIO under the canonical prefix and inserts tasks rows). Each stage is independent; partial failure leaves the system in a consistent state.

## 3. BenchmarkAdapter Protocol

```python
# loom_benchmarks/base.py
@runtime_checkable
class BenchmarkAdapter(Protocol):
    name: str                          # "swe-bench-verified"
    display_name: str                  # "SWE-Bench Verified"
    upstream_source: UpstreamSource    # see §4
    license_spdx: str                  # "MIT", "Apache-2.0", "CC-BY-4.0", "BSD-3-Clause", etc.
    license_url: str                   # canonical link
    splits: tuple[str, ...]            # ("test",) or ("train", "validation", "test")

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Yield BenchmarkInstance records from the cached upstream source.
        Pure: same source_dir + split → same iteration order."""

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        """Write a complete Loom task fixture (task.toml + instruction.md
        + solution/ + tests/ + environment/) into out_dir. Returns a
        ConvertedTask record with task_id, checksum, and any warnings."""
```

Two value types:

```python
@dataclass(frozen=True)
class UpstreamSource:
    kind: Literal["huggingface", "git", "https-tarball"]
    locator: str                       # HF dataset path, git URL, tarball URL
    revision: str | None = None        # HF revision / git commit sha / tarball checksum
    subset: str | None = None          # HF subset, e.g. "test" for HumanEval

@dataclass(frozen=True)
class BenchmarkInstance:
    instance_id: str                   # canonical upstream id, e.g. "django__django-12345"
    split: str                         # which split this came from
    raw: dict                          # the raw upstream record, unparsed
```

`ConvertedTask` carries the Loom-facing facts:

```python
@dataclass(frozen=True)
class ConvertedTask:
    task_id: str                       # e.g. "swe-bench-verified/django__django-12345"
    checksum: str                      # sha256 of task.toml
    license_spdx: str
    warnings: tuple[str, ...]          # missing solution, no test, etc. — non-fatal
```

## 4. Upstream fetching

The CLI handles fetching uniformly by `UpstreamSource.kind`:

- **`huggingface`** — `datasets.load_dataset(locator, name=subset, revision=revision)` (HF's API names the subset positionally or via `name=`, not `subset=`). Cache lives at `$LOOM_BENCHMARK_CACHE_DIR/huggingface/{locator}/{revision}/`.
- **`git`** — `git clone --depth 1 locator` at `revision`. For benchmarks like OSWorld and WebArena where the dataset is in a repo's structure rather than a flat HF dataset.
- **`https-tarball`** — `httpx.get` + tarfile extract. For one-off cases.

`revision` pins the upstream version. v1 hardcodes a revision per adapter; updating it is an adapter version bump, not silent drift.

All fetches go through a cache layer with content-hash validation: cached path = `cache_root / {kind} / {sha256(locator + revision)}`. Re-running `import` reuses the cache; passing `--refresh` blows it away.

## 5. Conversion: from upstream to Loom Task fixture

`convert_instance` is the heart of each adapter — it's where benchmark-specific knowledge lives. The output is always a complete Loom fixture, identical in shape to a hand-authored one (`tests/fixtures/tasks/hello-world/` is the reference).

### 5.1 Layout an adapter produces

```
{out_dir}/
├── task.toml                # required
├── instruction.md           # required, derived from the upstream's "problem statement"
├── solution/                # optional, present if the upstream ships a gold solution
│   └── solve.sh             # the canonical fix as an Oracle baseline
├── environment/             # optional
│   └── Dockerfile           # custom image if needed (most benchmarks)
└── tests/                   # for pytest verifier; alternative shapes for structured/llm-judge
    └── test_*.py
```

### 5.2 Per-benchmark conversion rules (concrete)

| Benchmark              | instruction.md from | solution from | tests/ shape | Image base | Verifier |
|------------------------|----------------------|---------------|--------------|------------|----------|
| SWE-Bench Verified     | `problem_statement`  | `patch` applied to base repo | `FAIL_TO_PASS` + `PASS_TO_PASS` test names | `swebench/sweb.eval.x86_64.<sanitized_instance_id>:latest` (upstream-published; sanitized = `instance_id.lower().replace("__", "_1776_")`) | pytest |
| SWE-Bench full         | `problem_statement`  | `patch`       | same as Verified | upstream image (same naming) | pytest |
| SWE-Bench Multimodal   | `problem_statement` + screenshots saved to `tests/fixtures/images/{instance_id}/<n>.png` and linked from instruction.md | `patch` | same | upstream image | pytest |
| ~~OSWorld~~ (deferred to v1.5) | ~~`instruction`~~ | ~~`expected_outcome`~~ | ~~structured~~ | requires VM-based driver | ~~structured~~ |
| ~~WebArena~~ (deferred to v1.5) | ~~`intent`~~  | ~~none~~      | ~~structured~~ | requires multi-container task model | ~~structured~~ |
| HumanEval              | `prompt`             | `canonical_solution` | one test file per instance, runs `check(candidate)` from upstream | `python:3.11-slim` | pytest |
| MBPP                   | `text` + `test_setup_code` | `code` | upstream's `test_list` translated to pytest cases | `python:3.11-slim` | pytest |
| BFCL                   | `question`           | `ground_truth` function call format | structured verifier matches the agent's tool call format against ground truth | `python:3.11-slim` | structured |
| GAIA                   | `Question` + `file_name` (if present) | `Final answer` | n/a — verifier compares agent's last assistant message to `Final answer` via llm-judge | `python:3.11-slim` + curl for file download | llm-judge |
| LiveCodeBench          | `question_content`   | `code` (in some splits) | upstream's `public_test_cases` + `private_test_cases` → pytest | `python:3.11-slim` | pytest |
| AIME                   | `problem`            | `solution`    | structured verifier parses agent's final-line integer answer, compares to `answer` | `python:3.11-slim` | structured |
| SkillFlow              | `instruction.md` (already shipped in this form) | `solution/` (already shipped) | upstream's pytest tests | `python:3.11-alpine` | pytest |
| SkillLearnBench        | `instruction.md` (already shipped) | `solution/` (already shipped) | upstream's pytest tests | `python:3.11-alpine` | pytest |

### 5.3 Shared conversion utilities

`loom_benchmarks.util` ships helpers that adapters compose:

- `download_files_from_record(record, out_dir, fields=("file_url",))` — pulls referenced attachments (e.g., GAIA's `file_name`)
- `pytest_from_test_strings(test_strings: list[str], out_dir)` — writes one `test_*.py` per case, with each test calling the upstream's check function
- `pytest_from_unittest(unittest_class_source, out_dir)` — wraps a unittest.TestCase as pytest
- `structured_verifier_script(script_body: str, out_dir)` — writes `verifier/run.sh` + a small Python script that reads stdin, writes JSON to `$LOOM_VERIFIER_OUTPUT`
- `embed_base64_image(image_bytes, alt_text) -> str` — for instruction.md image inlining (used by SWE-Bench Multimodal)

Each adapter is 80-200 LOC of glue: tell the helpers what to extract from the upstream record + where to write it.

## 6. MinIO bundle store + worker fetch

This spec closes Loom v0.7's documented limitation that workers leave `task_dir` empty (`tempfile.mkdtemp` with nothing in it). The benchmark CLI uploads each task's fixture content to MinIO; the worker fetches it via the bundle's `source` URL.

### 6.1 MinIO layout

```
loom-benchmarks (bucket)
├── swe-bench-verified/
│   ├── django__django-12345/
│   │   ├── task.toml
│   │   ├── instruction.md
│   │   ├── solution/solve.sh
│   │   └── tests/test_*.py
│   ├── django__django-67890/
│   │   └── ...
│   └── ... (×500 instances)
├── osworld/
│   └── ... (×369 instances)
└── ... (×12 benchmarks)
```

Each task's fixture is uploaded as individual objects (not a tarball) so workers can do range-reads if they want to inspect a subset. v0.7's existing `artifacts` bucket pattern is reused — different bucket, same boto3 client.

### 6.2 Task row `source` field

For benchmark-imported tasks, `tasks.source` becomes:
```
s3://loom-benchmarks/swe-bench-verified/django__django-12345/
```

### 6.3 Worker fetch path

`loom_worker.main_loop._spawn_trial` grows a helper that resolves `bundle["source"]`:

```python
async def _materialize_task_dir(
    bundle: dict, object_store: MinioObjectStore,
) -> Path:
    task_dir = Path(tempfile.mkdtemp(prefix="loom-trial-"))
    source = bundle["source"]
    if source and source.startswith("s3://"):
        # Pull every object under the prefix into task_dir
        await object_store.download_prefix(
            bucket="loom-benchmarks",
            prefix=_parse_prefix(source),
            out_dir=task_dir,
        )
    # else: hand-authored task with source=None or git+...; left empty for ops
    return task_dir
```

`MinioObjectStore.download_prefix(bucket, prefix, out_dir)` is a new method that lists keys and streams each to disk. Additive to Plan 2's `ObjectStore` Protocol.

Hand-authored tasks (`source=None` or `git+...`) still get the empty tempdir — operator's responsibility, as documented in the v0.7 runbook.

## 7. License tracking

The `tasks` table grows two new columns:

```sql
ALTER TABLE tasks
  ADD COLUMN license      text,
  ADD COLUMN benchmark_id text;       -- FK to benchmarks.id (nullable)

CREATE TABLE benchmarks (
    id            text PRIMARY KEY,   -- "swe-bench-verified"
    display_name  text NOT NULL,
    upstream_kind text NOT NULL,      -- huggingface / git / https-tarball
    upstream_locator text NOT NULL,
    upstream_revision text NOT NULL,
    license_spdx  text NOT NULL,
    license_url   text NOT NULL,
    splits        text[] NOT NULL,    -- ("train", "validation", "test")
    imported_at   timestamptz NOT NULL DEFAULT now(),
    imported_by   text                -- admin who ran the import
);
```

At import time the CLI does `INSERT ... ON CONFLICT (id) DO UPDATE SET upstream_revision = EXCLUDED.upstream_revision, imported_at = now(), imported_by = EXCLUDED.imported_by` so re-importing the same benchmark idempotently updates the row. Each task row references the benchmarks row by `benchmark_id`.

**License policy.** Each `teams` row gains `license_allowlist text[]` (default `ARRAY['MIT', 'Apache-2.0', 'BSD-3-Clause', 'CC-BY-4.0']`). At trial submit, the Control Plane checks the trial's task's license against the team's allowlist and 403s if incompatible.

Per-benchmark license facts:

| Benchmark        | License            | Default allowlist?         | Notes                                      |
|------------------|--------------------|----------------------------|--------------------------------------------|
| SWE-Bench family | MIT                | ✓                          |                                            |
| HumanEval        | MIT                | ✓                          |                                            |
| MBPP             | CC-BY-4.0          | ✓ (in expanded default)    |                                            |
| BFCL             | Apache-2.0         | ✓                          |                                            |
| GAIA             | CC-BY-4.0          | ✓ (in expanded default)    | Also requires HF gated-access acceptance — `loom_benchmark_tool` surfaces the HF login prompt. |
| LiveCodeBench    | CC                 | partial — see notes        | Specific clause is CC-BY-NC; v1 marks LiveCodeBench tasks as `license="CC-BY-NC-4.0"` and operators must explicitly add it to the allowlist (non-commercial use only). |
| AIME             | MAA terms          | ✗ — not in default         | The AIME problem text is owned by the Mathematical Association of America. Loom v1 marks AIME tasks as `license="proprietary-MAA"` and importing is gated behind a `--accept-maa-terms` flag on `loom_benchmark_tool` that logs the operator's token prefix to `benchmarks.imported_by`. |
| SkillFlow + SkillLearnBench | CARIN-internal | added per-team manually | Not in any public allowlist; operator runs `UPDATE teams SET license_allowlist = array_append(license_allowlist, 'carin-internal')`. |

`loom_benchmark_tool import` refuses to register a task whose license isn't in `benchmarks.license_spdx`'s spec-published mapping — the import-time check is a guardrail; the trial-submit check is the enforcement.

## 8. CLI: `loom_benchmark_tool`

A `scripts/loom_benchmark_tool/` Python module shipped in the loom repo (not a separate package — it imports `loom_benchmarks` directly and writes to MinIO + Postgres). Three subcommands:

```
$ python -m loom_benchmark_tool list
swe-bench-verified           SWE-Bench Verified           MIT          500 instances
swe-bench                    SWE-Bench (full)             MIT          2294 instances
swe-bench-multimodal         SWE-Bench Multimodal         MIT          619 instances
osworld                      OSWorld                      Apache-2.0   369 instances
...

$ python -m loom_benchmark_tool import swe-bench-verified \
      --db-url $LOOM_CP_DB_URL \
      --minio-endpoint $LOOM_CP_MINIO_ENDPOINT \
      --minio-access-key $... --minio-secret-key $... \
      [--splits test] \
      [--limit 50] \           # for smoke-import a subset
      [--refresh]              # blow away cache + re-pull
✓ Fetched upstream …
✓ Converted 500 tasks (3 warnings)
✓ Uploaded 500 task fixtures to s3://loom-benchmarks/swe-bench-verified/
✓ Registered 500 tasks in DB

$ python -m loom_benchmark_tool verify swe-bench-verified
# Spot-check 10 random instances: pull each from MinIO, validate task.toml
# against TaskConfig, run pytest in a throwaway container, assert
# Oracle baseline solve succeeds.
```

`list` is offline (queries the adapter registry). `import` requires network + DB + MinIO. `verify` requires Docker + MinIO + DB.

## 9. Per-adapter inventory (concrete contracts)

One file per adapter under `loom_benchmarks/adapters/`. Each is small — most of the work is in shared util.

### Example: SWE-Bench Verified

```python
# loom_benchmarks/adapters/swe_bench_verified.py
@dataclass(frozen=True)
class SWEBenchVerifiedAdapter(BenchmarkAdapter):
    name = "swe-bench-verified"
    display_name = "SWE-Bench Verified"
    upstream_source = UpstreamSource(
        kind="huggingface",
        locator="princeton-nlp/SWE-bench_Verified",
        revision="<commit-pinned-at-spec-time>",
    )
    license_spdx = "MIT"
    license_url = "https://github.com/princeton-nlp/SWE-bench/blob/main/LICENSE"
    splits = ("test",)

    def list_instances(self, *, source_dir, split):
        ds = datasets.load_from_disk(source_dir)[split]
        for record in ds:
            yield BenchmarkInstance(
                instance_id=record["instance_id"],
                split=split,
                raw=record,
            )

    def convert_instance(self, instance, *, out_dir):
        r = instance.raw
        # 1. instruction.md
        (out_dir / "instruction.md").write_text(r["problem_statement"])
        # 2. environment uses the upstream-published Docker image
        (out_dir / "task.toml").write_text(textwrap.dedent(f"""
            schema_version = "1"

            [task]
            id = "{self.name}/{instance.instance_id}"
            name = "SWE-Bench Verified — {instance.instance_id}"

            [environment]
            os = "linux"
            docker_image = "swebench/sweb.eval.x86_64.{instance.instance_id.lower().replace('__', '_1776_')}:latest"

            [agent]
            name = "oracle"  # overridable per trial config

            [verifier]
            name = "pytest"

            [[steps]]
            name = "main"
            artifacts = ["patch.diff"]
        """).strip())
        # 3. solution: apply the gold patch
        sol_dir = out_dir / "solution"
        sol_dir.mkdir()
        (sol_dir / "solve.sh").write_text(f"#!/bin/sh\ncd /testbed && cat <<'PATCH' | git apply --3way\n{r['patch']}\nPATCH\n")
        (sol_dir / "solve.sh").chmod(0o755)
        # 4. tests: a pytest file that runs FAIL_TO_PASS + PASS_TO_PASS
        tests_dir = out_dir / "tests"
        tests_dir.mkdir()
        (tests_dir / "test_swebench.py").write_text(
            util.pytest_from_test_strings(
                fail_to_pass=r["FAIL_TO_PASS"],
                pass_to_pass=r["PASS_TO_PASS"],
            )
        )
        return ConvertedTask(
            task_id=f"{self.name}/{instance.instance_id}",
            checksum=util.sha256_of_dir(out_dir),
            license_spdx=self.license_spdx,
            warnings=(),
        )
```

Similar files for the other 11. Each is reviewable independently.

## 10. Verifier mapping per benchmark family

The 4 verifier families Loom already ships (pytest / script / structured / llm-judge) cover all 12 benchmarks. Decisions:

- **pytest verifier** for everything code-like (SWE-Bench family, HumanEval, MBPP, LiveCodeBench, SkillFlow, SkillLearnBench). Verifier mode `shared` (v1 has no separate-env support).
- **structured verifier** for benchmarks where the agent's output is a specific format that an upstream eval script grades (OSWorld, WebArena, BFCL, AIME). Verifier mode `script` invokes the upstream's eval, captures its JSON output.
- **llm-judge verifier** for open-ended answers (GAIA). The judge prompt template is in `loom_benchmarks.judges.gaia` — a single rubric + reference answer comparison.

No new verifier shapes are needed.

## 11. Multi-step vs single-step

Most public benchmarks are single-step (one instruction → one solve). The adapter emits a single `[[steps]]` block in `task.toml`. OSWorld and WebArena could be multi-step (sub-tasks within an "instruction") but upstream doesn't expose that structure — v1 treats each as single-step. SkillFlow and SkillLearnBench retain their existing multi-step layouts (Harbor parity).

## 12. Splits handling

Benchmarks with train/validation/test splits get one task per (instance, split). Task IDs are `{benchmark}/{instance_id}` for single-split or `{benchmark}/{split}/{instance_id}` for multi-split. The `benchmarks.splits` column carries the canonical list so the dashboard can surface them.

## 13. Dedup + re-import

Re-running `import` is idempotent at the (benchmark, instance_id) level:
- Adapter computes `checksum = sha256_of_dir(out_dir)` after conversion.
- CLI compares to existing `tasks.checksum`. Equal → skip. Different → log + INSERT...ON CONFLICT DO UPDATE.
- `--force` overrides skip and re-uploads everything.

This means the spec deliberately does NOT make task content content-addressed in the path (i.e., we don't put the checksum in the MinIO key). Re-importing an updated benchmark version overwrites the same key. Trade-off: trial trajectories that ran against the old version still reference `s3://...django__django-12345/` — they'll see the new content if re-played. The `tasks.checksum` column records the version that was active at trial submit time, so audit is possible; rollback to a prior version requires re-importing the older revision.

## 14. Testing strategy

- **Unit tests** (in `loom-benchmarks` repo): per-adapter, exercise `list_instances` and `convert_instance` against a tiny fixture (3-5 instances pre-downloaded into `tests/fixtures/<benchmark>/`). Assert the emitted Task fixture validates against `TaskConfig.model_validate`.
- **Integration tests** (in `loom-benchmarks` repo): per-adapter, full HF/git fetch + convert + upload to a testcontainers MinIO + assert MinIO contains expected keys. Marked slow; opt-in.
- **End-to-end smoke** (in Loom repo): for SWE-Bench Verified and HumanEval (the two highest-value benchmarks), spin up the full Loom stack via `docker-compose.test.yml`, import 3 instances via the CLI, submit trials with the Oracle agent, assert all succeed. Lives in `tests/system/test_benchmark_swe_bench_smoke.py` and `test_benchmark_humaneval_smoke.py`.

## 15. Out of scope

- **Benchmark suite orchestration.** Treating "a benchmark" as a first-class entity that you can submit-all-trials-against in one API call. Today operators submit per-instance. v1.5 ships a `POST /benchmarks/{id}/run` campaign endpoint.
- **Custom verifier types per benchmark.** All 12 fit existing pytest/structured/llm-judge. If a future benchmark needs a new shape, that's a Verifier change, not a Benchmark change.
- **Live benchmark updates.** v1 pins each adapter at one revision. Upgrading is an adapter version bump.
- **Reverse direction (Loom Task → published HF dataset).** Out of scope; export is a separate spec.
- **License audit at trial-run time.** v1 enforces license-allowlist at submit only. A team that changes its policy mid-running-trial gets a stale-policy window until the trial finishes.

## 16. Implementation sequencing

Four plans:

1. **Plan 13 — Schema + bundle store.** `tasks.license` + `tasks.benchmark_id` columns, `benchmarks` table, Alembic migration. `MinioObjectStore.download_prefix()`. Worker's `_materialize_task_dir` resolving `bundle["source"]`. Closes v0.7 task_dir limitation. ~3 days. No `loom-benchmarks` dependency yet.
2. **Plan 14 — `loom-benchmarks` core.** New PyPI package: `BenchmarkAdapter` Protocol, value types, shared util module, CLI scaffold (`list` works, `import` works for ONE reference adapter — HumanEval). ~4 days. Depends on Plan 13.
3. **Plan 15 — 9 remaining adapters.** SWE-Bench family (3 adapters), MBPP, BFCL, GAIA (with HF gated-access prompt), LiveCodeBench, AIME (with `--accept-maa-terms` flag), SkillFlow + SkillLearnBench. OSWorld + WebArena deferred — see §1 v1-ready column. Each ~1 day with parallel review. ~6 days sequential, ~3 days with two reviewers.
4. **Plan 16 — Verify + smoke.** `verify` CLI subcommand + 2 system smoke tests (SWE-Bench Verified, HumanEval) + license allowlist enforcement at trial submit. ~3 days. Closes the loop.

Total: ~17 working days. Plans 13 and 14 can run in parallel; 15 needs 14; 16 needs 15.

## 17. Open questions

None at spec write-time.
