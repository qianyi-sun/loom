<!--
Derived and modified from OmniGibson agentic_sweep.
Copyright (c) 2023 Stanford Vision and Learning Group. SPDX-License-Identifier: MIT.
The Loom port removes ambient path, environment, and runtime-card discovery.
-->
# How to look — sampling, cameras, tools

## Sampling

Use two passes. First sweep the whole episode coarsely to find phase boundaries and spans where
progress stops. Then inspect manipulation moments and suspected failures densely. Do not sample
uniformly. A drop can complete in fewer than 20 frames, so tighten the sample when the event
itself matters.

Use the head, left-wrist, and right-wrist cameras at the same frame indices. The head view is
often occluded; grasp, insert, and lid-close evidence normally requires a wrist view. Video
tools speak seconds, while the judgement speaks frame indices. Convert using 30 fps and cite
frames, never timestamps.

## Locked mosaic tool

The provider tool writes one labelled JPEG with three camera rows at the requested frames. Its
only data selector is the direct signed video root. Examples:

```bash
python3 /opt/behavior/provider-assets/behavior_offline_judge/tools/mosaic.py \
  --video-root /inputs/rollout/payload/videos/task-0010 \
  --episode episode_00100010 --frames 500,900 --out /scratch/look.jpg \
  --cache-dir /scratch/mosaic-cache

python3 /opt/behavior/provider-assets/behavior_offline_judge/tools/mosaic.py \
  --video-root /inputs/dataset/payload/videos/task-0010 \
  --episode episode_00100010 --frames 500,900 --out /scratch/demo.jpg \
  --cache-dir /scratch/demo-mosaic-cache
```

The tool prints the authoritative column-to-frame mapping. Trust that mapping rather than tiny
labels in a resized image. Tool output and cache paths must remain below `/scratch`.

## MCP video tools

`mcp__video__*` serves the rollout and `mcp__video_demo__*` serves the demonstrations. Both are
stdio servers already configured for the signed task roots. There is no port or daemon to
discover or start. Use `list_videos` first, then an overview for the coarse pass. Use sections
or frame comparisons for a boundary and a precise frame only when fine detail decides the
verdict. An inline image is transient, while a mosaic under `/scratch` can be checked again.

Read the immutable task card and BDDL transition document named in the run parameters directly.
Do not search sibling directories, discover alternate roots, consult environment variables,
build another task card, or fetch any external source.
