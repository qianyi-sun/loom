# Pipeline Stage 1 live preview

The BEHAVIOR Stage 1 live preview is an optional, ephemeral progress signal. It is never an
Artifact, acceptance result, retry input, lineage edge, billing record, or domain-outcome
authority. Only the committed `behavior_rollout_bundle.v1` Artifact is final evidence.

## Trust boundary

- The Stage container remains on `network=none` and may write only atomic JPEG/record pairs in
  its Attempt-private `/scratch/live-preview` directory.
- The pinned Stage 1 simulator driver may opt into the additive
  `run_episode_with_live_preview` boundary and offer only an already composed 672x448 JPEG plus
  step index. The Loom sink owns cadence, sequence allocation, validation, and atomic spooling;
  any sink failure permanently disables only preview and falls back to the unchanged authoritative
  `run_episode` path.
- The Worker validates owner, mode, link count, inode stability, canonical JCS+LF metadata, digest,
  size, dimensions, baseline RGB encoding, and absence of JPEG metadata before publishing.
- Publish uses the existing authenticated Worker channel and exact Attempt claim ID, lease epoch,
  lease token, lifecycle, and server-derived Run/Stage/team identity.
- Browser reads use the same-origin session. Wrong-team, wrong-resource, expired, cancelled,
  terminal-failed, worker-lost, and replaced-claim generations return 404.
- The relational preview backend is bounded to 64 frames and 32 MiB per Attempt, with independent
  team/global admission limits and a 300-second TTL. It stores no locator or credential.

## Lifecycle

The server creates a generation only for a frozen `rollout` execution contract whose required
container output is exactly `rollout: behavior_rollout_bundle.v1`. The generation UUID equals the
ExecutionAttempt UUID, so retries cannot share frame state. Frames are contiguous, step indices
are nondecreasing uint64 values below the Worker-held signed episode bound, and both Worker and
server enforce a maximum 2 Hz cadence.

Cancellation, failure, worker loss, claim replacement, lease expiry, and TTL reconciliation purge
all JPEG bytes idempotently. A successful output commit changes the preview to `handoff`; the Web
surface refetches the Stage and replaces the live image with the immutable Artifact viewer. Handoff
bytes remain eligible for TTL cleanup and are not copied into object storage.

## Operations

Prometheus counters cover bounded producer/publisher outcomes, accepted bytes, HTTP reads,
rejections, and purge reasons. Gauges cover active generations and oldest current-frame age. Metric
labels contain only closed result/reason values. A preview failure is deliberately isolated from
the Stage process result and Artifact commit.

Repository acceptance does not prove a live Stage 1 execution. Issue #1362 owns the authorized
live-to-committed run, screenshot, and zero-residue evidence; #1232 repeats it on the sealed
candidate.
