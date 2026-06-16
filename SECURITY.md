# Security

## Reporting

Report security issues privately to the repository owner or platform owner. Do
not open public GitHub issues for credentials, access control bugs, sandbox
escape concerns, private endpoint leaks, or sensitive data exposure.

If GitHub private vulnerability reporting is enabled, use it for reports that
include exploit details or sensitive reproduction steps. Otherwise contact a
maintainer directly and share only the minimum detail needed to triage the
issue.

## Repository Rules

- Do not commit secrets, API keys, cloud credentials, SSH keys, database dumps,
  private endpoint inventories, or model provider tokens.
- Keep environment-specific values in secret stores or local ignored files.
- Use `.env.example` for required variable names only.
- Treat sandbox execution, artifact storage, and evaluator outputs as potentially
  sensitive until data classification rules are defined.
- Pull request workflows must run with read-only default `GITHUB_TOKEN`
  permissions and must not receive deployment, publishing, model-provider, or
  infrastructure secrets.
- Workflows with side effects, such as benchmark publishing or deployment, must
  use protected GitHub Environments with branch restrictions and maintainer
  approval before secrets are exposed.

## Public Repository Operations

For public repository operation:

- Run full Git-history secret scans before visibility or release-boundary
  changes and rotate any credential that ever appears in history, even if it has
  since been removed.
- Keep GitHub Secret Scanning and Push Protection enabled.
- Keep non-GitHub Actions explicitly allowed by policy instead of relying on
  unrestricted workflow sources.
- Scope environment secrets such as `HF_TOKEN` to protected environments, not
  broad repository secrets.
- Keep external pull request workflows untrusted: no deployment, publishing,
  model-provider, or infrastructure secrets may be exposed to PR code.

## Platform Security Topics To Resolve

- Team and project-level access model; see
  [`docs/architecture/auth-threat-model.md`](docs/architecture/auth-threat-model.md)
  for the #10 admin/team-registration baseline and
  [`docs/architecture/auth-registration-spec.md`](docs/architecture/auth-registration-spec.md)
  for the target implementation contract.
- Secret injection into sandboxed jobs.
- Network egress policy for execution environments.
- Artifact retention and deletion rules.
- Audit log requirements for PM, infra, and research users. The first #10
  backend audit surface now covers team-registration approve/reject and
  `loom_service` token admin mint/revoke; broader admin mutation coverage and
  SPA audit review remain follow-up work.
