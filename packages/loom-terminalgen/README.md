# loom-terminalgen

This workspace package contains the TerminalGen Python source imported into the
Loom monorepo for issue #1432. The repository owner explicitly authorized the
direct source inclusion on 2026-08-20. The imported source is covered by the
Loom repository's Apache-2.0 license.

The source package is not itself production execution authority. Official Loom
runs continue to require the code-owned Pipeline recipe, attempt-scoped provider
and validation grants, digest-pinned images, durable budgets, artifact commits,
and cancellation/cleanup fences.

## Included

- the `terminalgen` Python modules;
- the small realistic-domain catalog;
- the 18 atomic weakness cards used to construct the 9,000 durable slots;
- a deterministic source manifest and dependency SBOM.

## Deliberately external

- `agent_skill_plans.jsonl`;
- the Terminal-Bench snapshot and its solutions;
- generated task bundles and corpora;
- historical run logs, PID files, archives, and validation evidence.

Those inputs must be supplied to Loom as immutable digest-addressed artifacts.
The legacy standalone loader fails closed when an external agent-skill plan path
does not exist.

The standalone `terminalgen` command is retained for source compatibility and
development inspection. It is not the official Loom execution path and must not
be used to bypass Pipeline admission or worker sandboxing.

## Aug19 acceptance replay

The four-source acceptance gate replays the recorded model boundary through the
imported sampler, prompt builder, acceptance logic, and TB2 exporter. It checks
the exact provider prompts and every generated task file byte-for-byte without
making a provider call or starting an unrestricted authoring subprocess:

```bash
uv run --no-sync python packages/loom-terminalgen/scripts/verify_aug19_acceptance.py \
  --reference-root /path/to/hq_delivery_aug19
```

The canonical set is QEMU, FEAL, financial-document processing, and HTML filter
bypass, with five selected tasks per source. QEMU, financial-document processing,
and HTML use the v2 catalogs and seeds 9201, 9203, and 9204; FEAL uses the v3
catalog and seed 9302. A pass requires all 20 regenerated bundle trees, including
file modes, to match the Aug19 final selection exactly.

The checked acceptance result and its 20 bundle-tree digests are recorded in
`acceptance/AUG19_ACCEPTANCE.json`. The delivered source adds one empty line to
the provider prompt relative to the archived Aug19 call records; all 21 prompts
match after duplicate-empty-line normalization, while task bundles remain under
strict byte-and-mode equality.
