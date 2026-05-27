# Contributing

## Workflow

1. Create a feature branch from `main`.
2. Link work to a GitHub issue.
3. Keep changes scoped to one concern.
4. Open a pull request using the repository template.
5. Wait for CI and required review before merge.

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
