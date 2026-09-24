# Recover a legacy failed verifier archive

Use this operation only for a deleted, finalized current-attempt execution whose
committed source failed canonical publication with `verifier_reward_drift` because
the old runtime read `diagnostics/verifier-exception.json` before
`verifier/output.json`. Deploy the compatible Control Plane and apply migration
`0157` through the normal guarded rollout first. Inspect the original source,
lease/team identity, runtime failure and retention state before requesting recovery.

Run the deployed module inside the Control Plane environment, using its existing
database and source-spool configuration:

```sh
python -m loom_control_plane.service_execution_archival_recovery \
  --lease-id LEASE_UUID --team-id TEAM_UUID --apply
```

`requeued` means the single archival retry was durably queued. It does not mean the
bundle is ready. `not_eligible` makes no change; this includes another team,
an advanced attempt, another integrity error, an active execution, a previously
requested recovery or an already-published archive. The command makes no model
calls and does not run or modify the deleted execution.

The ordinary materializer authenticates every source object and publishes through
its persisted claim. Transient storage failures retain the queue across restart;
another integrity failure returns the archive to `unavailable` and consumes the
one-use recovery. The new archive error is retained on the lease, while the Trial's
original state, result, reward, attempt, failure and finish time remain unchanged.
Do not clear the audit timestamp or patch a terminal state to repeat the operation.

After the archive becomes `committed`, download its ordinary Trial bundle and ATIF.
Verify the unchanged original runtime result, the independent verifier score,
source manifests, native trace, accounting and Trial outcome. A separate verifier
score does not change the original null Trial reward or turn the failed attempt
into a success. Source cleanup remains forbidden until canonical acknowledgement;
the normal configured retention window starts at that acknowledgement.

Migration downgrade restores the original guards only while no recovery timestamp
has been used. Once history exists it refuses to discard the audit; roll application
images back only with the repository's forward-schema compatibility checks.
