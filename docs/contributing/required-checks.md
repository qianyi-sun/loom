# Required checks and merge authority

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

## Check ownership

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

The checked-in contract is not a readback of live GitHub settings. Verify the
rulesets and current-head check ownership before integration; preserve all
required checks and the empty bypass list. See [contribution policy](../../CONTRIBUTING.md).
