# Native admission migration

## Decision

Loom will use one GitHub-Actions-owned required check named `admission` for
pull-request merge authority. The repository remains owned by a personal
GitHub account, all collaborators with write access are trusted to change code
and CI, and no separate GitHub App or CODEOWNER approval boundary is required.

The `dev protected admission` ruleset will require `admission` from the GitHub
Actions app and set strict required-status-check evaluation to `true`. A base
update may therefore rerun pull-request validation before merge.

## Target topology

`.github/workflows/ci.yml` is the only pull-request admission entrypoint. It
runs the repository validation jobs directly and calls the image,
cluster-smoke, and staging-smoke workflows as reusable workflows. Its final
`admission` job runs with `if: always()` and fails unless the repository
aggregate and every called validation workflow succeeds.

The source workflows retain their existing path planner and selected-lane
aggregates. Workflow-level path filters are not used, so the final required
check cannot disappear because a lane was not selected.

Push-triggered image publication and deployment remain separate from
pull-request admission. A publication failure makes the merged commit
unreleasable until repaired; it does not rewrite the pull request's admission
result.

## Protected cutover

The migration preserves merge protection throughout:

1. Merge the reusable-workflow and native `admission` topology while the four
   existing required contexts remain active. During this compatibility phase,
   source workflows run both directly and through `admission`.
2. Verify one exact pull-request head where all four called workflows and the
   final native `admission` job complete successfully. Also verify a disposable
   failing head remains unmergeable.
3. Update the `dev protected admission` ruleset atomically: require only the
   GitHub-Actions-app-bound `admission` check and set
   `strict_required_status_checks_policy=true`. Keep the bypass actor list
   empty and read the ruleset back after the update.
4. Remove the direct pull-request triggers from the three reusable workflows,
   `.github/workflows/authoritative-gates.yml`,
   `scripts/ops/authoritative_gate.py`, its publisher-only metrics and tests,
   and `.github/workflows/auto-merge.yml`. Remove publisher generation and
   `*-attempt` naming branches from the source planners.
5. Verify a fresh success, failure, cancellation/replacement, head update, and
   base update through the single required `admission` check. Enable GitHub's
   native squash auto-merge on eligible pull requests.

Before step 3, rollback is deletion of the new admission workflow. After step
3, rollback first restores the old ruleset requirements while the compatibility
triggers still exist; only then may `admission` be removed. No step may leave
the ruleset without at least one functioning required-check authority.
