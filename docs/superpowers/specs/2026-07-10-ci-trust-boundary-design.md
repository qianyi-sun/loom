# CI Trust Boundary Design

> **Superseded governance note (2026-07-14):** This document preserves the
> original targeted-CODEOWNER design as history. The active policy uses only
> the four strict, app-bound current-head checks as `dev` merge authority and
> requires no human approval, no CODEOWNER approval, and no conversation
> resolution. CODEOWNERS is advisory on `dev`; `main` promotion is reviewed and
> manually squash-merged by Qianyi. The implementation text below must not be
> treated as current branch-protection instructions.

## Status and scope

This design is the first implementation slice of #787. Loom intentionally lets
routine `dev` pull requests auto-merge without human approval, so the required
CI contexts are the merge authority. This slice prevents a pull request from
silently weakening that authority and makes the existing planner/gates fail
closed.

It covers trust-root ownership and bootstrap, protected versus manual check
identities, pull-request base changes, strict planner outputs, documentation
classification, migrations, and Docker-marked integration selection.
Component manifests, frontend checks, credential workflows, reproducible
dependencies, attestations, and candidate-bound production remain independent
deliveries in #788-#791 and #757/#773.

## Root cause

The branch rule binds four context strings to the GitHub Actions app. It cannot
bind a context to a trusted workflow file. A pull request can edit the workflow
or planner that emits the same context. The four workflows also expose
`workflow_dispatch` while retaining the protected job names, so a manual run
can emit an indistinguishable app/context pair.

Two fail-open contracts amplify that trust problem:

1. Gates interpret any planner value other than `true`, including empty or
   malformed values, as false/unselected.
2. The planner treats every Markdown file and every path under `docs/` as
   documentation, including runtime catalog inputs and executable proofs.

The durable direction is an external GitHub App or organization required
workflow whose identity cannot be changed by the PR. The personal repository
does not currently provide that boundary. The immediate safe boundary is
targeted CODEOWNERS approval for CI authority files while ordinary source PRs
keep zero required reviews.

## Chosen design

### Narrow, review-protected trust root

Remove catch-all and broad runtime ownership entries from CODEOWNERS. Keep
ownership only for files that can redefine merge or release authority:

- `.github/**`, including workflows and CODEOWNERS;
- `scripts/plan_ci_validations.py` and its planner/workflow contract tests;
- `pyproject.toml`, which owns test discovery and coverage policy;
- production release/deploy verification scripts;
- `CONTRIBUTING.md`, `SECURITY.md`, and `LICENSE` governance policy.

Owners are `@qianyi-sun` and `@Hongjian-Gu`. After this change is merged and
the live CODEOWNERS error API returns no errors, enable
`require_code_owner_reviews=true` on `dev` with
`required_approving_review_count=0`. GitHub then requires a code-owner approval
only for matched files; routine unmatched code remains zero-review.

The bootstrap PR necessarily lands under the old policy. It must not bundle
unrelated workflow or governance changes.

### Event-specific protected check identity

Keep manual validation, but make every aggregate display name conditional:

```yaml
name: ${{ github.event_name == 'workflow_dispatch' && 'repository-checks-manual' || 'repository-checks' }}
```

The other manual names are `images-gate-manual`,
`cluster-smoke-gate-manual`, and `staging-smoke-gate-manual`. PR,
merge-group, and push events retain the protected names. Changing only the
workflow name or `run-name` is insufficient because branch protection consumes
the aggregate job/check name.

Every pull-request trigger also includes `edited`. A base retarget keeps the
same head SHA, so relying on `synchronize` can preserve a plan calculated
against the old base. The existing three-dot diff is rerun with the current
`pull_request.base.sha`.

### Strict output contract

Every aggregate validates each planner boolean before evaluating results. Only
the exact lowercase strings `true` and `false` are accepted. Empty, missing,
uppercase, or arbitrary values fail the aggregate.

For a selected check, only `success` is accepted. For an unselected check,
`skipped` or `success` is accepted because GitHub may report a conditionally
skipped dependency either way. Planner failure always fails the aggregate.
`repository-checks` validates `docs_only`, `integration`,
`integration_docker`, and `coverage_summary`; each optional workflow validates
its `required` value.

### Explicit documentation boundary and fail-safe unknowns

Documentation is a static location-and-format contract, not a repository-wide
extension rule.

- Exact root governance documents and inert repository metadata are static.
- `.github/ISSUE_TEMPLATE/**` is static.
- Under `docs/`, only declared non-executable documentation/data suffixes are
  static: `.md`, `.mdx`, `.rst`, `.txt`, `.json`, `.jsonl`, `.csv`, `.png`,
  `.jpg`, `.jpeg`, `.gif`, and `.svg`.
- `.github/CODEOWNERS` is not documentation because it changes merge authority.
- Markdown outside those locations, including
  `deploy/catalog/**/instruction.md`, is runtime input.
- Executables such as `docs/**/*.sh` are runtime input.

Known paths select their mapped lanes. Any non-document path with no owner in
the current map selects every heavy lane. This makes new runtime surfaces
expensive but safe; #788 can later assign narrower owners through one manifest.

Migration changes select integration, images, and staging because staging
actually applies Alembic migrations. Any `tests/integration/**` change selects
the Docker tier until #788 provides generated marker ownership. A
filesystem-backed contract enumerates all current `pytest.mark.docker` modules
so future drift is visible.

## Data flow

```text
PR event (opened/synchronize/reopened/labeled/edited)
  -> checkout current head and current base
  -> planner emits exact lowercase booleans
  -> selected jobs run
  -> event-specific aggregate validates booleans and results
  -> protected PR context, or distinct *-manual context
  -> branch protection permits or blocks auto-merge
```

CODEOWNERS and branch protection protect the files that define this flow; they
do not replace test signal for ordinary code.

## Rejected shortcuts

- Requiring review on every PR contradicts Loom's operating model and hides
  rather than fixes the trust boundary.
- A job-level condition that skips the aggregate on manual runs is unsafe;
  skipped required jobs can be treated as successful.
- Labels remain additive only; absent or stale labels cannot be authoritative.
- `set -u` is not an output validator because a missing expression becomes an
  empty environment value.
- A docs extension allowlist without a location boundary still misclassifies
  runtime Markdown.

## Verification and rollout

Repository tests prove runtime Markdown/executables are non-doc, unknown paths
select all heavy lanes, migrations select staging, every Docker-marked test
selects Docker CI, malformed booleans fail the actual aggregate shell, the four
workflows include `edited`, manual names differ, and CODEOWNERS contains only
the declared trust root.

After merge:

1. verify `codeowners/errors?ref=dev` is empty;
2. enable targeted code-owner reviews with approval count zero;
3. read back all other branch protections unchanged;
4. dispatch every workflow and prove only `*-manual` contexts appear;
5. prove a trust-root PR is review-blocked and an unmatched routine PR is not.

Rollback disables `require_code_owner_reviews` before restoring the old
CODEOWNERS file. Protected PR context names are never renamed.
