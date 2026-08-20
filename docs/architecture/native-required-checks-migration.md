# Native required-check migration

## Decision

Loom uses the four final GitHub Actions jobs already emitted by its validation
workflows as pull-request merge authority:

- `repository-checks`
- `images-gate`
- `cluster-smoke-gate`
- `staging-smoke-gate`

The repository remains owned by a personal GitHub account. All collaborators
with write access are trusted to change code and CI, so no separate GitHub App
or CODEOWNER approval boundary is required.

The `dev protected admission` ruleset requires these four checks from the
GitHub Actions app and sets strict required-status-check evaluation to `true`.
A base update may therefore rerun pull-request validation before merge.

## Target topology

Each source workflow plans its selected validation lanes and exposes one final,
fail-closed aggregate job under its stable required name. GitHub Actions owns
the CheckRun directly. There is no cross-workflow publisher, custom CheckRun,
same-name commit status, retired failure, or merge controller.

Push-triggered image publication and deployment remain separate from
pull-request admission. A publication failure makes the merged commit
unreleasable until repaired; it does not rewrite the pull request's admission
result.

Eligible pull requests use GitHub's native squash auto-merge. A developer or
maintainer enables it in the pull request after the current head is ready; no
workflow has `contents: write` merely to enable auto-merge.

## Protected cutover

The migration preserves merge protection throughout:

1. Remove the authoritative publisher workflow and custom auto-merge
   controller. The migration pull request itself still uses the trusted
   default-branch copies for its final compatibility run.
2. Verify that the migration pull request merges only after the four existing
   required contexts succeed on its exact head.
3. On the first post-merge test pull request, verify the four source aggregate
   jobs directly own the stable required names and no `authoritative-gate-retired`
   CheckRuns or same-name commit statuses are created.
4. Set `strict_required_status_checks_policy=true` without changing the four
   app-bound required contexts, preserve the empty bypass actor list, and read
   the ruleset back.
5. Remove publisher-only scripts, metrics, tests, source-workflow generation
   detection, and `*-attempt` naming branches. Verify success, genuine failure,
   cancellation/replacement, head update, and base update on fresh pull
   requests.

Before step 4, rollback restores the two deleted workflows while the source
generation bridge still exists. After step 4, restore the workflows and the
previous ruleset policy before removing any native aggregate. No step may leave
the ruleset without all four functioning required checks.
