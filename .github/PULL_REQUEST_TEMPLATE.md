## Summary

- 

## Linked Issue

- Refs #
- Use a closing keyword only when this PR fully satisfies the issue.

## Contributor Path

- [ ] I commented on or was assigned the linked issue before starting substantial work.
- [ ] This PR is from a trusted maintainer branch.
- [ ] This PR is from a fork or external contributor branch; pull request code must not rely on protected secrets.

## Target Branch

- [ ] This PR targets `dev` for normal development.
- [ ] For this normal `dev` PR, squash auto-merge and the coordinator-owned
      `ci:merge-ready` label are enabled only while it is the queue head.
      GitHub keeps that selected candidate queued until
      `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
      `staging-smoke-gate` succeed on the current head SHA. These four strict,
      app-bound checks are the only merge authority: `dev` requires no human
      approval, no CODEOWNER approval, and no conversation resolution.
      Non-head PRs run distinct preflight checks and cannot satisfy these
      protected contexts.
- [ ] This PR targets `main` only for a production release promotion from `dev`;
      Qianyi (`@qianyi-sun`) reviews the fixed candidate and its evidence and
      performs the manual squash merge. Never enable auto-merge for this PR.
      Repository-wide `allow_auto_merge` cannot disable it on `main` alone, so
      this prohibition is an operator-enforced release rule.

## Scope

- [ ] Product or requirements
- [ ] Backend or platform
- [ ] Infrastructure or deployment
- [ ] Evaluation, benchmark, agent, or data workflow
- [ ] Documentation or governance
- [ ] Developer environment

## Verification

- 

## Validation Selection

- Changed paths automatically select the minimum required validation work.
- Labels may add validation but cannot remove path-inferred validation.
- Static docs use a location-and-format fast path; unknown runtime paths select
  every heavy lane until they have an explicit owner.
- Manual dispatches report `*-manual` contexts and never replace the protected
  PR contexts. Record any manually dispatched additional validation above.
- CODEOWNERS supplies advisory ownership routing on `dev`; it is deliberately
  not a merge gate there. CI/release-authority changes still select full CI.

## Documentation

- [ ] Project docs were updated for every code, workflow, deployment, or contract change.
- [ ] Docker/dev-environment docs were updated if dependencies or services changed.
- [ ] Affected Markdown docs were scanned for stale instructions.
- [ ] No owner-local `AGENTS.md` or `MEMORY.md` content is included in this PR.

## Risk

- [ ] No credential, endpoint, or sensitive data added
- [ ] No production deployment change
- [ ] PR workflows do not require protected publish/deploy/provider secrets
- [ ] Rollback path is clear

## Release Promotion

Complete this section only for PRs targeting `main`.

GitHub evaluates the target branch's CODEOWNERS. For the first promotion that
introduces the Qianyi-only catch-all, the current `main` file still contains
invalid `@carinrc` owners, so only `main`'s generic one-approval rule plus
Qianyi's manual release process protect that bootstrap. Prefer a non-Qianyi
identity (or a future restricted bot) as PR author: GitHub users cannot approve
their own pull requests. Once the catch-all lands, Qianyi approval is required
for subsequent promotions.

- Candidate SHA:
- Immutable prod tag (`vX.Y.Z`; never move after publication):
- Staging URL:
- Image digests:
- Release gate workflow run:
- Gate evidence artifact:
- Frontend route evidence:
- Worker isolation evidence:
- Raw-delivery/export requirement status:
- Rollback notes:
- Previous production image digest:
- Rendered production manifest:
- DB recovery point:
- `release_owner_approval` evidence URL (candidate/evidence decision):
- Qianyi PR approval URL (when GitHub can record it):
- Qianyi manual squash operator:
- Production Environment approver (deployment-secret release):
- I confirm this PR targets `main` only for release promotion from validated `dev`.
- I confirm Qianyi (`@qianyi-sun`) reviewed the fixed candidate and evidence,
  auto-merge was never enabled, and Qianyi will perform the manual squash merge.
- I confirm `release_owner_approval`, GitHub PR review, and Production
  Environment approval are distinct controls and are not interchangeable.
- I confirm the prod tag is new, immutable, and will not be force-moved.

## Deployment Notes

- 
