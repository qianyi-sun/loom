# CI Trust Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Loom's four auto-merge contexts event-specific, fail-closed, and protected by a narrow human-reviewed trust root while preserving zero-review routine PRs.

**Architecture:** Retain the planner and four stable aggregate job IDs. Tighten path selection, validate string outputs inside the actual gates, give manual runs distinct check names, and narrow CODEOWNERS before enabling targeted code-owner review remotely.

**Tech Stack:** Python 3.11, pytest, GitHub Actions YAML, Bash, GitHub CODEOWNERS and branch-protection APIs.

## Global Constraints

- Scope is the first P0 slice of #787; #788-#791, #757, and #773 remain independent.
- PR/merge-group/push names stay exactly `repository-checks`, `images-gate`, `cluster-smoke-gate`, and `staging-smoke-gate`.
- Manual runs emit only corresponding `*-manual` aggregate names.
- Only exact lowercase `true` and `false` planner outputs are valid.
- Labels are additive overrides, never the validation authority.
- Unknown non-document paths select all heavy validation until #788 assigns an owner.
- Routine source paths must not acquire CODEOWNERS approval requirements.
- Do not enable live `dev` code-owner review until narrowed CODEOWNERS is merged and error-free.

---

### Task 1: Specify and implement planner fail-safe behavior

**Files:**
- Modify: `tests/ops/test_plan_ci_validations.py`
- Modify: `scripts/plan_ci_validations.py`

**Interfaces:**
- Consumes: `plan_validations(changed_paths, labels, event_name)`.
- Produces: exact booleans for docs-only and heavy lane selection.

- [ ] **Step 1: Add failing planner contracts**

Import `pytest`, `Path`, and `HEAVY_CHECKS`. Add a parameterized test for:

```python
@pytest.mark.parametrize(
    "path",
    [
        "deploy/catalog/gb10-smoke/tasks/gb10-oracle-hello-world/instruction.md",
        "docs/architecture/cluster-deploy-spikes/01-sandbox-bridge.sh",
        "unowned-runtime/new-input.bin",
    ],
)
def test_runtime_inputs_fail_safe_to_every_heavy_check(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")
    assert plan.docs_only is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
```

Add a migration assertion for `integration`, `images`, and `staging_smoke`.
Enumerate `tests/integration/**/*.py` containing `pytest.mark.docker`; every
discovered path must return `integration_docker=True`.

- [ ] **Step 2: Run the planner tests and verify RED**

```bash
uv run pytest tests/ops/test_plan_ci_validations.py -q
```

Expected: runtime Markdown/executable docs, unknown full selection, migration
staging, and currently missed Docker-marker cases fail.

- [ ] **Step 3: Implement the explicit docs and unknown-path contract**

In `scripts/plan_ci_validations.py`:

- define exact static metadata without `.github/CODEOWNERS`;
- accept declared static suffixes only under `docs/`;
- keep `.github/ISSUE_TEMPLATE/**` static;
- add `migrations/` to staging prefixes;
- make `tests/integration/` a Docker-selection prefix;
- track whether a non-doc path matched any owner and select every heavy check
  with reason `unowned-runtime-path:<path>` when none matched.

- [ ] **Step 4: Run the planner tests and verify GREEN**

```bash
uv run pytest tests/ops/test_plan_ci_validations.py -q
uv run ruff check scripts/plan_ci_validations.py tests/ops/test_plan_ci_validations.py
```

Expected: all planner contracts pass; Ruff emits no diagnostics.

### Task 2: Specify and implement protected-context identity

**Files:**
- Modify: `tests/ops/test_ci_throughput_workflows.py`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/images.yml`
- Modify: `.github/workflows/cluster-smoke.yml`
- Modify: `.github/workflows/staging-smoke.yml`

**Interfaces:**
- Consumes: planner results and string outputs from `needs.*`.
- Produces: protected PR contexts or distinct manual contexts.

- [ ] **Step 1: Add failing workflow contracts**

Add tests that:

- assert `edited` is in `pull_request.types` for all four workflows;
- assert each aggregate name contains its protected literal and `*-manual`
  literal controlled by `github.event_name == 'workflow_dispatch'`;
- extract and execute each aggregate `run` block with `bash`;
- prove optional gates fail for `REQUIRED=""` and `REQUIRED="invalid"`;
- prove `repository-checks` fails when any of `DOCS_ONLY`,
  `INTEGRATION_SELECTED`, `DOCKER_SELECTED`, or `COVERAGE_SELECTED` is empty or
  invalid;
- syntax-check all four aggregate scripts.

Use `subprocess.run(["bash"], input=script, env={...}, check=False)` and set all
unrelated results to their expected success/skipped values.

- [ ] **Step 2: Run workflow contracts and verify RED**

```bash
uv run pytest tests/ops/test_ci_throughput_workflows.py -q
```

Expected: missing `edited`, static manual names, and malformed booleans fail.

- [ ] **Step 3: Implement event-specific names and boolean validation**

Add `edited` to each `pull_request.types`. Change only each aggregate display
name, for example:

```yaml
name: ${{ github.event_name == 'workflow_dispatch' && 'images-gate-manual' || 'images-gate' }}
```

In each optional aggregate, validate before branching:

```bash
case "$REQUIRED" in
  true|false) ;;
  *) echo "FAIL: invalid planner boolean required=${REQUIRED@Q}" >&2; exit 1 ;;
esac
```

In `repository-checks`, add `DOCS_ONLY` and this helper:

```bash
require_boolean() {
  local name="$1" value="$2"
  case "$value" in
    true|false) ;;
    *) echo "FAIL: invalid planner boolean ${name}=${value@Q}" >&2; exit 1 ;;
  esac
}
```

Call it for every planner boolean before evaluating results.

- [ ] **Step 4: Run workflow contracts and actionlint**

```bash
uv run pytest tests/ops/test_ci_throughput_workflows.py -q
actionlint .github/workflows/ci.yml .github/workflows/images.yml \
  .github/workflows/cluster-smoke.yml .github/workflows/staging-smoke.yml
```

Expected: tests pass and actionlint emits no diagnostics.

### Task 3: Narrow and test CODEOWNERS

**Files:**
- Modify: `.github/CODEOWNERS`
- Modify: `tests/unit/test_repo_governance_templates.py`

**Interfaces:**
- Produces: ownership only for governance/CI/release authority.

- [ ] **Step 1: Add a failing governance contract**

Read `.github/CODEOWNERS` and assert:

```python
assert "* @qianyi-sun @Hongjian-Gu" not in codeowners.splitlines()
for path in (
    "/.github/",
    "/scripts/plan_ci_validations.py",
    "/tests/ops/test_plan_ci_validations.py",
    "/tests/ops/test_ci_throughput_workflows.py",
    "/pyproject.toml",
    "/scripts/ops/release_gate.py",
    "/scripts/ops/verify_production_release_gate.sh",
):
    assert f"{path} @qianyi-sun @Hongjian-Gu" in codeowners
```

Also assert broad `/src/`, `/packages/`, `/web/`, `/deploy/`, and `/docs/`
entries are absent.

- [ ] **Step 2: Run the governance test and verify RED**

```bash
uv run pytest tests/unit/test_repo_governance_templates.py -q
```

Expected: failure identifies the catch-all and broad runtime entries.

- [ ] **Step 3: Replace CODEOWNERS with narrow ownership**

Keep `.github/`, governance documents, planner and contracts, pytest policy,
and release verification/deploy scripts owned by Qianyi and Hongjian. Do not add
a default owner or routine source/deployment/docs directory owner.

- [ ] **Step 4: Verify locally and after push**

```bash
uv run pytest tests/unit/test_repo_governance_templates.py -q
gh api 'repos/qianyi-sun/loom/codeowners/errors?ref=codex/ci-gate-hardening' \
  --jq '.errors'
```

Expected after push: local test passes and GitHub returns `[]`.

### Task 4: Document the operating contract

**Files:**
- Modify: `CONTRIBUTING.md`
- Modify: `docs/contributing/contributor-quickstart.md`
- Modify: `docs/architecture/v1-release-ready-program.md`

- [ ] **Step 1: Update contributor guidance**

State that labels are additive, the planner is authoritative, unknown runtime
paths fail safe, manual checks have `*-manual` names, and base edits rerun CI.

- [ ] **Step 2: Update the release program**

Record the targeted trust-root review exception, bootstrap sequence, and the
longer-term external-app/required-workflow boundary. Do not claim that stronger
boundary exists today.

- [ ] **Step 3: Scan Markdown for contradictions**

```bash
rg -n "workflow_dispatch|required context|docs.only|docs-only|CODEOWNERS|CI label|ci:integration" \
  --glob '*.md' .
```

Update only statements contradicted by this slice.

### Task 5: Verify, commit, and open the bootstrap PR

**Files:** all Task 1-4 files plus this spec and plan.

- [ ] **Step 1: Run the targeted suite**

```bash
uv run pytest tests/ops/test_plan_ci_validations.py \
  tests/ops/test_ci_throughput_workflows.py \
  tests/unit/test_repo_governance_templates.py -q
uv run ruff check scripts/plan_ci_validations.py \
  tests/ops/test_plan_ci_validations.py \
  tests/ops/test_ci_throughput_workflows.py \
  tests/unit/test_repo_governance_templates.py
actionlint .github/workflows/ci.yml .github/workflows/images.yml \
  .github/workflows/cluster-smoke.yml .github/workflows/staging-smoke.yml
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 2: Commit in reviewable units**

```bash
git add -f docs/superpowers/specs/2026-07-10-ci-trust-boundary-design.md \
  docs/superpowers/plans/2026-07-10-ci-trust-boundary.md
git commit -m "docs(ci): define trusted auto-merge boundary (#787)"

git add .github scripts/plan_ci_validations.py tests CONTRIBUTING.md docs
git commit -m "fix(ci): fail closed at the auto-merge boundary (#787)"
```

- [ ] **Step 3: Push and open a `dev` PR**

Push `codex/ci-gate-hardening`, open a PR targeting `dev`, add every planner
label selected by the trust-root change, and enable squash auto-merge. Keep #787
open because this PR completes only its first slice.

- [ ] **Step 4: Perform post-merge live verification**

Require `codeowners/errors?ref=dev` to return `[]`, then PATCH dev review policy
to code-owner review enabled with approval count zero. Dispatch each protected
workflow and prove no manual check name equals a protected context. Confirm a
trust-root PR is review-blocked and an unmatched routine PR is not.
