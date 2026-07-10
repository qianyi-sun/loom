# Loom Domain Model

This glossary defines release concepts whose meanings must remain consistent
between the product, operator runbooks, release manifests, and GitHub release
matrix.

## Release Candidate

An immutable commit SHA selected from `dev` together with its rendered
configuration hash, image identities, migration head, and secret-safe evidence
inputs. A candidate is not production authority and is not a release merely
because a staging rollout succeeded.

## Ready-to-Promote

The state in which every pre-promotion row in the executable v1 acceptance
matrix has candidate-bound evidence, merge/release gates are green, and no
release blocker remains. It authorizes preparing an explicit `dev` to `main`
promotion decision; it does not by itself merge `main`, create a tag, or deploy
production.

## Release

The owner-authorized promotion of a Ready-to-Promote candidate from `dev` to
`main`, with an immutable release tag and the production deployment and
post-deploy acceptance work recorded separately. A release remains distinct
from staging validation and from a GitHub pull request.

## Workload Trust Mode

The machine-enforced declaration of which workload capabilities a release
candidate may claim. V1 accepts only `internal_trusted` with TaskSet transforms,
transform network isolation, and untrusted-workload isolation all disabled.
The exact release contract is defined in
[`v1-workload-trust-contract.md`](../architecture/adr/v1-workload-trust-contract.md).

For v1, a TaskSet manifest that declares `transform` is rejected with
`transform_unavailable_in_internal_trusted` before any transform/source/verifier
blob fetch or runner invocation. A legacy subprocess helper or a best-effort
`os.unshare` result is not untrusted-workload isolation. Post-v1 #758 owns the
separate design for actual arbitrary-code isolation.
