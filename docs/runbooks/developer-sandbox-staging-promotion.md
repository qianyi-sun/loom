# Developer sandbox staging promotion authority

This authority closes the post-merge boundary of issue #1023. It produces the
only staging promotion document accepted by
`developer_sandbox_live_acceptance.py`:

`/var/lib/loom-staging-rollout/acceptance/promotion.json`

The document is not an operator assertion. The root-installed producer accepts
only one broker request ID and derives every other field from fixed staging
authority paths.

## Trust and scope

The installed command is:

`/usr/local/libexec/loom-developer-sandbox-staging-promotion`

It must run on `trt-eai-oldlab-1`. `produce` has no flags for candidate SHA,
tree, rollout ID, source path, result, timestamp, or source host. It reads:

- the immutable `request.json` and highest broker attempt `envelope.json` below
  `/var/lib/loom-staging-rollout/requests/<request-id>`;
- the complete broker-owned rollout state and every canonical successful step
  result below `/data/loom-staging/rollouts/<rollout-id>`;
- the fixed candidate-bound staging browser report from step 16;
- the installed candidate repository at
  `/opt/loom-staging-runner/candidates/<sha>/repo`.

The request and envelope must be a `start` request for
`https://github.com/qianyi-sun/loom.git`, `origin/dev`, the staging namespace,
and merged-dev source. Request, envelope, completed state, all 18 ordered result
records, the step-16 envelope digest, and the browser report must agree on the
same request, attempt, rollout, commit, and observed deployment. The installed
repository must have the exact clean `HEAD`, commit object, tree object, and
origin URL. A caller-created JSON file is never an input.

Source files are opened without following links, must be single-link private
regular files owned by root or `loom-rollout`, and are identity-checked again
immediately before publication. The public receipt and all internal authority
state are root-owned under a mode-`0700` directory with mode-`0600` files.

## Durable transaction

The consumer-facing receipt has the closed schema:

```json
{
  "schema_version": 1,
  "kind": "loom.staging-rollout.acceptance",
  "source_host": "trt-eai-oldlab-1",
  "rollout_id": "<broker rollout id>",
  "candidate_sha": "<40-hex merged dev commit>",
  "candidate_tree": "<40-hex git tree>",
  "result": "pass",
  "observed_at": "<step-16 completion UTC>"
}
```

The authority also maintains:

- `state.json`: the monotonic high-water record;
- `receipts/`: immutable, digest-addressed audit records binding the fixed
  request, attempt, all source digests, predecessor, and public receipt;
- `pending.json`: a crash-recovery transaction that can only finish the exact
  already-validated publication;
- `.lock`: the single-writer lock.

A repeated invocation for the same unchanged request is idempotent. Reusing a
request with changed evidence, publishing an older completion, rolling back the
public receipt, breaking the audit chain, exceeding the bounded journal, or
substituting a symlink/hardlink fails closed. A crash after the pending record,
immutable audit record, public receipt, or high-water update is recovered to the
same transaction on the next `produce` or `check`.

## Installation

Installation is a direct-root bootstrap from the reviewed exact candidate. It
does not modify or bypass the staging broker:

```bash
sudo python3 scripts/ops/developer_sandbox_staging_promotion.py \
  install --execute

sudo /usr/local/libexec/loom-developer-sandbox-staging-promotion check
```

Installation persists the executable, the root-owned acceptance hierarchy, and
the closed sudo policy from
`deploy/staging-rollout/loom-developer-sandbox-staging-promotion.sudoers`.
Members of `loom-staging-operators` may only run fixed `produce --request-id
req-* --execute` and `check`; the parser rejects extra or abbreviated flags.

## Pre-merge and post-merge boundary

The registry-selected developer-environment candidates and their live overlap
session are pre-merge evidence. Keep that session in `running` state after its
`11 * N` ordered checkpoints and `2 * N` trusted overlap receipts are
complete, where `N` is the exact accepted cohort size. Do not create a
promotion receipt from a PR head: before squash merge there is no merged-dev
promotion candidate.

After the aggregate PR is squash-merged to `dev`:

1. create a new normal staging broker request so its immutable candidate is the
   exact merged `origin/dev` commit and tree;
2. let that one broker rollout complete all steps, including release gate,
   smoke, and candidate-bound browser acceptance;
3. publish from its fixed request identity:

   ```bash
   sudo /usr/local/libexec/loom-developer-sandbox-staging-promotion \
     produce --request-id req-0123456789abcdef --execute
   ```

4. import the fixed root receipt into the still-running #1023 acceptance
   session with `session-record-promotion`;
5. set the final evidence promotion candidate and staging regression window to
   that exact squash-merged SHA/tree and broker rollout interval, then run
   `session-finalize`.

Finalization must therefore happen post-merge. The pre-merge session is not
weakened or recreated, and a pre-merge candidate cannot be substituted for the
independent squash-merged staging candidate.

## Stop conditions

Stop without publishing when any of these holds:

- the host is not `trt-eai-oldlab-1`;
- the request is not an exact merged `origin/dev` staging request;
- request, attempt, state, result, browser, or installed repository bindings
  differ;
- any step is not canonical `done` success, or the browser report is not a full
  candidate-bound pass;
- the candidate repository is dirty or its `HEAD`, commit tree, or origin
  differs;
- a source or authority path is linked, mutable, non-private, foreign-owned, or
  changes during the transaction;
- the completion timestamp does not advance the durable high-water record;
- crash recovery, current receipt, state, or immutable audit chain disagrees.

Do not hand-edit `promotion.json`, `state.json`, `pending.json`, or
`receipts/`. Recovery and idempotence are owned only by the installed authority.
