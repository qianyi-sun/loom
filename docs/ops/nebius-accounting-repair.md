# Correct a previously committed Terminus accounting export

Use this only for an existing terminal, current-attempt Terminus-2 Trial whose canonical export omitted Gateway requests (#1921). New materializations already use the Gateway ledger. This operation performs no model calls, creates no Trial or worker, and changes no result, reward, attempt, source commit, or retention deadline.

Run the deployed module inside the Control Plane environment, using its existing database and object-storage configuration. Supply the exact lease UUID and owning team UUID. Start with a read-only projection:

```sh
python -m loom_control_plane.service_execution_accounting_repair \
  --lease-id LEASE_UUID --team-id TEAM_UUID
```

Check the reported request count and token totals against the selected lease's Gateway ledger. Publish that correction with:

```sh
python -m loom_control_plane.service_execution_accounting_repair \
  --lease-id LEASE_UUID --team-id TEAM_UUID --apply
```

The helper selects the source output generation, writes new objects under a unique `accounting-v2/<UUID>` revision, and atomically changes the canonical bundle and trajectory pointers with the materializer-owned event rows. Original objects and the lease’s original materialization ACK digests remain unchanged; corrected digests belong to the new canonical pointers. Objects remain registered under their existing lifecycle authority. Original usage and native trace are included as `source/accounting/usage.json` and `source/trajectory/events.jsonl`; corrected totals and the safe Gateway ledger are under `files/accounting/`. Native Harbor turns and commands retain their meaning.

The helper refuses another team's lease, a nonterminal or superseded attempt, foreign event producers, corrupt input, or changed projection inputs. It removes its unpublished objects on a known precommit failure. A lost database connection during commit leaves the uniquely named revision intact because commit may have succeeded; inspect the canonical pointer before cleanup. Repeating a successful invocation returns `already_corrected` without another write.

After `corrected`, download the ordinary Trial bundle and ATIF through the public API. Confirm complete request/token totals, unchanged verifier reward and native turn count, and that the original source files are still available. Deployment alone does not prove an older export was corrected.
