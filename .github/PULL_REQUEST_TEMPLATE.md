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
- [ ] For this Codex-authored normal `dev` PR, Codex enabled GitHub auto-merge
      with squash immediately after opening it. GitHub keeps it queued until
      `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
      `staging-smoke-gate` succeed on the current head SHA and any applicable
      repository protection passes.
- [ ] This PR targets `main` only for a production release promotion from `dev`;
      it remains explicitly owner-managed and does not use routine `dev`
      auto-merge.

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
- Changes to the CI/release trust root require a declared CODEOWNER even though
  routine `dev` source changes have no repository-wide approval count.

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
- Production deploy approver:
- I confirm this PR targets `main` only for release promotion from validated `dev`.
- I confirm the prod tag is new, immutable, and will not be force-moved.

## Deployment Notes

- 
