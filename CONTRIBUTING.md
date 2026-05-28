# Contributing

## Workflow

1. Create a feature branch from `dev`.
2. Link work to a GitHub issue.
3. Keep changes scoped to one concern.
4. Open a pull request using the repository template.
5. Wait for CI and required review before merge.

Normal development pull requests target `dev`. The `main` branch is reserved for
production release promotion pull requests from `dev`.

## Branch Naming

- `feature/<short-name>` for product or platform features.
- `infra/<short-name>` for deployment and infrastructure work.
- `docs/<short-name>` for documentation-only changes.
- `fix/<short-name>` for defects.
- `research/<short-name>` for exploratory prototypes.

## Commit Style

Use concise imperative commit messages:

- `docs: define MVP platform scope`
- `infra: add dev deployment workflow`
- `feat: add run lifecycle API`
- `fix: handle evaluator timeout`

## Definition Of Done

- The linked issue has clear acceptance criteria.
- CI passes.
- New behavior has a verification path.
- No credentials, private endpoints, or large runtime artifacts are committed.
- Documentation is updated when contracts, operations, or workflows change.
- Docker development files and docs are updated when dependencies, services, or
  runtime commands change.

## Release Flow

Use `dev` as the integration branch. After dev validation passes for a planned
release, open a release issue, update `VERSION` and `CHANGELOG.md`, then open a
pull request from `dev` to `main`.

Tag the merged `main` commit as `vX.Y.Z` to create the GitHub release.

## Owner-Local Files

`AGENTS.md` and `MEMORY.md` are owner-local context files. They are ignored and
should not be included in pull requests.
