# CI runner placement

Loom development CI runs on GitHub-hosted runners. The OLDLAB KVM pool,
placement leases, route controller, custom route CheckRun publisher and local
uv mirror integration have been removed from the repository. CI does not need
Nebius worker nodes or access to a shared development host.

The four protected source checks remain `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate`. GitHub-native squash auto-merge
still requires the current PR head on the current base; there is no bypass or
replacement gate publisher.

Python root, package and integration tests run without coverage instrumentation
by default. Use the existing `ci:coverage-summary` selector or dispatch CI with
`coverage_summary=true` for the full coverage report and 70% fast-tier floor.
The selector adds work and never suppresses relevant tests.

PR image builds validate amd64 server images. Existing signed legacy publication
consumers still receive their declared architecture manifests; migrating that
contract is separate from removing redundant PR architecture builds. macOS CLI
compatibility remains a separate developer-host check.

This repository change does not stop existing runner services or cancel running
jobs. Retire residual host services separately after in-flight jobs drain.

For a PR that edits only unreferenced Python test modules, each test lane runs
only the edited modules assigned to it. A test module referenced elsewhere is
potentially a shared fixture and retains full validation. Runtime, migration,
configuration, fixture, deleted-file and unknown changes also retain full lanes.
A selector label or manual CI dispatch requests full regression. Empty selected
shards finish without invoking pytest; failures while selecting files still fail
the job. Tests are assigned to shards before filtering so ownership stays stable.

The Go/Python socket/publication flow has one owner, `go-checks`, and is not
repeated in the ordinary integration shards. Ordinary coverage collection is
optional, but test failures and all four protected source checks remain required.
