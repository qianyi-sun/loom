# Repository Migration

Active development has moved from `carinrc/loom` to
[`qianyi-sun/loom`](https://github.com/qianyi-sun/loom).

The move was required because the `carinrc` organization displayed an expired
Enterprise trial warning and required GitHub Actions could fail before runner
startup with an account billing lock. `qianyi-sun/loom` is a public personal
repository so normal public-repo GitHub Actions can run for active
development.

## Canonical Repository

- Canonical code repository: <https://github.com/qianyi-sun/loom>
- Default development branch: `dev`
- Release branch: `main`
- Normal PR target: `dev`
- Merge policy: squash merge only, delete branch after merge

## Migrated Repository Settings

The new repository has these settings recreated from `carinrc/loom`:

- public visibility;
- Issues enabled;
- Projects enabled;
- Wiki disabled;
- GitHub Actions enabled with selected Actions only;
- selected Actions allowlist: GitHub-owned Actions and `astral-sh/setup-uv@*`;
- `dev` branch protection requiring strict `repository-checks`;
- `main` branch protection requiring strict `repository-checks`, code-owner
  review, one approval, conversation resolution, and linear history;
- repo labels copied from `carinrc/loom`;
- repo milestones copied from `carinrc/loom`.

`Hongjian-Gu` has been invited to the new repository with write access. GitHub
reports effective read access until the invitation is accepted.

## Issue Tracker State

GitHub's `transferIssue` API currently rejects moving issues from
`carinrc/loom` to `qianyi-sun/loom` because the destination repository has a
different owner:

```text
New repository must have the same owner as the current repository
```

Because GitHub blocks cross-owner issue transfer, the 37 open issues were
manually recreated in `qianyi-sun/loom` on 2026-06-26. Each recreated issue
has a migration header pointing to the original `carinrc/loom` issue and has
the original labels, milestone, assignees, and comments copied over.

The old tracker remains the historical source for original issue numbers,
closed issues, old pull requests, and immutable source comments:

- Open issues: <https://github.com/carinrc/loom/issues?q=is%3Aissue%20state%3Aopen>
- v1.0 milestone: <https://github.com/carinrc/loom/milestone/10>
- Old roadmap project: <https://github.com/orgs/carinrc/projects/3>

The active recreated issue set is tracked in:

- New open issues: <https://github.com/qianyi-sun/loom/issues?q=is%3Aissue%20state%3Aopen>
- New roadmap project: <https://github.com/users/qianyi-sun/projects/3>

The temporary migration index
[`qianyi-sun/loom#24`](https://github.com/qianyi-sun/loom/issues/24) is closed
and superseded by the individual recreated issues.

## Validation PR

The old `carinrc/loom#587` branch was mirrored and reopened as
[`qianyi-sun/loom#1`](https://github.com/qianyi-sun/loom/pull/1) to validate
that required GitHub Actions start in the new canonical public repo.

## Developer Remote Update

Existing clones should add or switch `origin` to the new canonical repository:

```bash
git remote set-url origin https://github.com/qianyi-sun/loom.git
git fetch origin
git switch dev
git pull --ff-only
```

Keep any old `carinrc/loom` remote under a non-default name only when you need
to read historical issue, PR, or release links.
