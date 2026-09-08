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

- [ ] This full-Nebius series PR targets `codex/nebius-main` and follows
      `docs/architecture/nebius-primary-platform.md`.
- [ ] This unrelated normal-development PR targets `dev`.
- [ ] Every non-draft Nebius PR uses GitHub-native squash auto-merge. The sole
      required `repository-checks` result must succeed on the current head SHA
      and current integration base. It aggregates all selected validation.
      Eligibility is independent of author or reviewer identity: no human approval,
      no CODEOWNER approval, and no conversation resolution gate.
- [ ] For a normal `dev` PR, its existing four checks remain the only merge authority:
      `repository-checks`, `images-gate`, `cluster-smoke-gate`, `staging-smoke-gate`.
- [ ] This PR targets `main` only for production promotion from `dev`, under
      its separate current promotion policy.
- [ ] This PR does not promote `codex/nebius-main` into `dev` before pure
      Nebius end-to-end acceptance and the subsequent owner integration decision.

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
- Ownership routing is advisory, not a merge gate. CI/release-authority changes
  still select full CI.

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

`main` accepts only a same-repository current `dev` candidate that passed the
real staging release gate. The PR author and reviewer identities do not affect
merge eligibility.

- Candidate SHA:
- Immutable prod tag (`vX.Y.Z`; never move after publication):
- Staging URL:
- Image digests:
- Release gate workflow run and run ID:
- Gate evidence artifact:
- Main promotion gate workflow run:
- Frontend route evidence:
- Worker isolation evidence:
- Raw-delivery/export requirement status:
- Rollback notes:
- Previous production image digest:
- Rendered production manifest:
- DB recovery point:
- `release_owner_approval` evidence URL (candidate/evidence decision):
- Production Environment approver (deployment-secret release):
- I confirm this PR targets `main` only for release promotion from validated `dev`.
- I confirm GitHub native squash auto-merge is enabled and the protected
  `main-promotion-gate` is the only merge authority.
- I confirm `release_owner_approval` evidence and Production Environment
  approval are distinct controls and are not interchangeable with CI.
- I confirm the prod tag is new, immutable, and will not be force-moved.

## Deployment Notes

- 
