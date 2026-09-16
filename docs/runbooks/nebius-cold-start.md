# Native Nebius cold-start diagnosis

Use this procedure when execution capacity grows again while nodes from an
initial scale-up are still becoming schedulable. Native Managed Kubernetes
Cluster Autoscaler remains the only component that changes physical capacity.
Loom's placement calculation authorizes resource demand; it does not resize the
node group.

First compare the effective **rendered Pod requests**, including controller,
restartable init sidecars, ordinary init peaks and overhead, with allocatable
node resources after DaemonSets. Task limits and nominal VM memory are not the
scheduler's packing inputs. Preserve task-specific limits and explicit requests;
a calibrated request policy only applies to its approved task revisions.

## Capture before reproducing

Use the existing protected kubeconfig and read-only Nebius profile. This command
only reads the node group, Pods, Nodes, scheduling Events and native autoscaler
status. It never creates workloads or edits configuration. It creates a new
mode-0600 JSONL file, refusing to overwrite existing evidence.

```sh
python scripts/ops/capture_nebius_cold_start.py \
  --kubeconfig /protected/kubeconfig \
  --node-group-id "$LOOM_EXECUTION_NODE_GROUP_ID" \
  --node-selector 'loom.nebius/node-role=integration-execution' \
  --namespace loom-nebius-platform-execution \
  --nebius-profile "$LOOM_OBSERVER_PROFILE" \
  --output /protected/cold-start.jsonl \
  --samples 120 --interval 10
```

Each source retains its own read time; samples are not atomic. The interval is a
pause **after** collection and API latency increases the total duration. Stop
with Ctrl-C when sufficient evidence is collected. Errors remain `unavailable`,
never zero capacity or a healthy status. Credentials, Pod commands/environment,
cloud-init, provider network details and arbitrary workload error messages are
excluded. Keep the output private: node and workload identities remain useful
for correlating provider diagnostics. With an existing SSH operator transport,
import `capture()` and inject the same read-only Kubernetes and native-node-group
JSON adapters instead of creating another credential or connection route.

Capture from zero through startup, workload deletion and final reclamation. In
particular retain:

- Native target/actual size and `cluster-autoscaler-status` upcoming/unregistered,
  not-started and ready counts at every scale-up.
- Each Node's creation time, Ready transition, startup taint keys/effects and
  deletion/unschedulable state. Ready alone does not prove schedulability.
- Pending Pod requests, selectors, tolerations and scheduling reason categories;
  UID-bound `TriggeredScaleUp` Events correlate the second expansion with demand.
- Actual task cleanup separately from pool cleanup: deleted Pods/Jobs and released
  execution reservations do not imply the provider has already deleted VMs.

## Bounded no-model packing acceptance

Prepare this independently of a paid agent batch. A live disposable workload
still consumes cloud resources and needs the existing operator's bounded run
scope; the read-only capture command does not authorize its creation.

1. Read provider quota, current workloads and node-group configuration. Begin at
   zero execution nodes only when the pool is otherwise idle. Do not delete
   other users' work to obtain that state.
2. Render representative Pods through the changed task-resource path. Build a
   small disposable Job using the same aggregate requests, selectors and exact
   execution tolerations. Use an already admitted CPU image with a bounded idle
   command, no credentials/model configuration, no host access, a finite
   `activeDeadlineSeconds`, `backoffLimit: 0`, and an ownership label unique to
   this run. Rendering is preparation; review the request count and expected
   packing before applying through the established operator path.
3. Start capture before applying the approved Jobs. Exercise one cold node and
   then a multi-node packing case. Compute expected packing from real
   allocatable CPU, RAM, storage and Pod slots minus DaemonSet reservations;
   do not assume a fixed task count per worker.
4. Compare native scale-up decisions against remaining unschedulable demand and
   upcoming capacity at those timestamps. A larger target needs a demonstrated
   constraint; do not count startup Nodes as usable merely to make the estimate
   look smaller. A second scale-up during readiness transitions alone does not
   identify which controller assumption caused it.
5. Remove only the run-owned Jobs through the operator path and verify their
   Pods disappear. Continue observing native actual and target counts until both
   return to the previous idle baseline. Document the measured delay; if the
   bounded observation ends earlier, report reclamation pending with its last
   status instead of claiming a leak or successful scale-zero.

This validates scheduler/autoscaler behavior without model calls. It does not
replace the separate real execution, output integrity or resource-usage checks.
Do not add a competing VM autoscaler, lower the node ceiling to hide excess
expansion, or tolerate CNI/startup taints before networking is ready.

## Managed-controller evidence boundary

The installed Nebius CLI exposes native node-group autoscaling minimum and
maximum; it does not expose Cluster Autoscaler startup-taint handling or its
provisioning/scale-down timer flags. The managed controller need not appear as a
Pod in the tenant's `kube-system`, even when its status ConfigMap is readable.
Consult the [native autoscaling documentation](https://docs.nebius.com/kubernetes/node-groups/autoscaling)
and the applicable API version before changing provider configuration.

If upcoming-node treatment remains unexplained, preserve UTC timestamps,
node-group identity and the sanitized capture for the provider operator. Obtain
managed autoscaler version, startup-taint configuration and scale-up decision
logs for that interval. Do not infer an exact timer or provider defect from
upstream defaults. Kubernetes Events expire, and a later healthy status cannot
reconstruct an earlier scheduling decision.

For #1985, the 2026-09-16 17:46:58 UTC `5 -> 9` event and initial taints establish
startup expansion, but not the controller's internal reason. The 19:35 UTC
read-only follow-up found all ten incident leases deleted with cleanup complete,
all ten reservations released, no execution Nodes and native actual/target both
zero. This rules out a persistent leak in that observation; it does not establish
when reclamation completed or prove the overshoot fixed. Keep the cold-start
acceptance open until correlated evidence supports the resulting change.
