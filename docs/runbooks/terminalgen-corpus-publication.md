# TerminalGen corpus publication

TerminalGen publication is a server-owned terminal projection of the
`terminalgen-authoring@1` Pipeline. It does not execute inside an authoring
container and it does not accept caller-supplied object keys.

## Publication contract

Publication begins only after the singleton `global_finalize`,
`package_authoring`, `package_runtime`, and `publish_boundary` stages have
succeeded and committed their final-output Artifacts. The orchestrator verifies
the committed root manifests and markers before reading any semantic document.
It then requires:

- a complete final audit with exact requested and dynamically validated counts;
- one restricted authoring corpus and one solution-free team runtime corpus;
- identical ordered task lineage across both corpora;
- exact source-task and dynamic-validation Artifact lineage for every task; and
- exact file inventory, byte digest, size, mode, and canonical Loom `task.toml`
  identity inside every task tar.

The first requested runtime tasks, up to 500, are projected into a deterministic
TaskSet tar. Tar headers have zero timestamps and ownership, entries are
bytewise ordered, and only modes `0644` and `0755` are accepted. The archive and
manifest are written under content-addressed server-owned keys and read back
before the database publication transaction begins.

That fenced transaction creates one immutable corpus version, one searchable
task lineage row per task, one terminal publication receipt, and switches the
team-scoped alias generation. Replaying the same request returns the stored
canonical receipt. Reusing an identity with different bytes or an incorrect
expected previous version fails closed.

## Read boundary

A team token with `read:own` can read its current alias at:

```text
GET /api/v1/terminalgen-corpora/{alias}
GET /api/v1/terminalgen-corpora/{alias}/taskset-smoke/archive
GET /api/v1/terminalgen-corpora/{alias}/taskset-smoke/manifest
```

The response exposes download routes, not storage locators. Each download
revalidates the published size and SHA-256 before streaming. Cross-team alias
reads return `404`. The restricted authoring corpus and reference solutions are
not exposed by these routes.

## Failure handling

An invalid committed Artifact set, marker, semantic document, corpus inventory,
task archive, lineage reference, alias fence, or object readback terminalizes
the Pipeline run with a bounded `publication_*` reason. A valid candidate that
fails publication also receives an immutable failed publication row. Operators
must not repair a failed row in place; submit a new run with a new request
identity and the correct expected previous version digest.

## Acceptance

Run the focused publication suite:

```bash
uv run --no-sync pytest -q \
  tests/unit/pipeline/test_terminalgen_publication.py \
  tests/unit/test_terminalgen_corpus_routes.py \
  tests/integration/test_migration_terminalgen_corpus_publication.py \
  tests/integration/test_terminalgen_corpus_publisher.py \
  tests/integration/test_terminalgen_corpus_routes.py
```

The imported source acceptance remains the four-source Aug19 replay. It must
reproduce all 20 selected task trees byte-for-byte, including file modes:

```bash
uv run --no-sync python \
  packages/loom-terminalgen/scripts/verify_aug19_acceptance.py \
  --reference-root /path/to/hq_delivery_aug19
```
