# Security

## Reporting

Report security issues privately to the repository owner or platform owner. Do
not open public GitHub issues for credentials, access control bugs, sandbox
escape concerns, private endpoint leaks, or sensitive data exposure.

## Repository Rules

- Do not commit secrets, API keys, cloud credentials, SSH keys, database dumps,
  private endpoint inventories, or model provider tokens.
- Keep environment-specific values in secret stores or local ignored files.
- Use `.env.example` for required variable names only.
- Treat sandbox execution, artifact storage, and evaluator outputs as potentially
  sensitive until data classification rules are defined.

## Platform Security Topics To Resolve

- Team and project-level access model.
- Secret injection into sandboxed jobs.
- Network egress policy for execution environments.
- Artifact retention and deletion rules.
- Audit log requirements for PM, infra, and research users.
