<!--
Derived and modified from OmniGibson agentic_sweep.
Copyright (c) 2023 Stanford Vision and Learning Group. SPDX-License-Identifier: MIT.
This version uses the current Loom grasp-history shape and closed output contract.
-->
# Inspect a rollout

Judge one recorded rollout against its immutable task card. Read the task card first.

## Inputs

The run parameters name two direct video roots. Each has the three camera directories
`observation.images.rgb.{head,left_wrist,right_wrist}` and files named
`<episode_id>.mp4` at 30 fps.

The run parameters also name the exact BDDL transition document. Its `transitions` list is the
complete source-ordered record of state changes. Its `grasp_history` is the `IsGrasping` view
with exact entries `{step_idx,arm,old_value,new_value}`; `old_value` and `new_value` are nullable
scene object names and must differ. It also supplies `total_steps`, identity, and success.
Frame index equals simulator step index.

The log cannot show an attempt that changed nothing, but it cannot omit a state change. Video
is required to establish attempts, intent, and stalls. Do not read outcome labels, simulator
state, unrelated metadata, or undeclared files.

## Method

1. Coarsely sweep the whole episode and look for pauses before reading predicate data.
2. Read the exact transition document as changes, not attempted actions.
3. Inspect every grasp/release, insert, lid close, suspected stall, and final stop in progress.
4. Form a hypothesis, build the view that could refute it, and inspect that evidence.

Rows tile the episode. A row is one attempt at one objective, including its approach. The first
row starts at 0, every later row starts at the previous row's last frame plus one, and the final
row ends at `n_steps-1`.

## Verdicts

- `success`: the intended action achieved its objective and was in order.
- `execution`: the action should have run but failed to achieve or retain its objective.
- `ordering`: the action should not have run then, whether or not its execution was clean.
- `no progress`: nothing was attempted and nothing later resumed before the episode ended.

For fixed-order tasks, departure from the card sequence is ordering failure at the first frame
of the departure. For flexible-order tasks, use the card's must-precede constraints and whether
the action foreclosed required later work. Do not invent a planning constraint.

## Learn and seed

Progress is the world changing toward the current objective, not merely robot motion. On a
successful row, `learn` is the productive inclusive frame range, with at most two ordered,
disjoint ranges when an interior stall is removed. On every other row, `seed` is where a
corrected attempt begins. An execution seed is the in-row frame where progress stopped. An
ordering or no-progress seed is exactly the row's first frame.

No successful row after the first ordering failure may carry learn data. Prefer an earlier
defensible seed over a later state already damaged by the failure.

## Output

Write exactly `report.md` and `seed.json` in the declared output directory. The Timeline has
this exact nine-cell Markdown row shape; the validator treats the first six cells as the
machine judgement contract and cross-checks learn/seed cells with `seed.json`:

```markdown
# <task>/<instance>/<episode>   n_steps=<N>  fps=30

## Timeline

| first | last | primitive | object -> target | arm | verdict | learn | seed | why |
|---|---|---|---|---|---|---|---|---|
| 0 | 812 | `pick up from` | cap_188 -> countertop | right | success | 0-812 | — | fingers closed at f780 and the cap was clear by f800 |
| 813 | 1904 | `place in` | cap_188 -> washer | right | execution | — | 1121 | the cap stopped approaching at f1121 and fell outside at f1890 |
| 1905 | 4299 | — | — | — | no progress | — | 1905 | both grippers remained empty from f1905 through the swept ending |
```

Use an exact backtick vocabulary value in every actionable `primitive` cell. A no-progress row
uses em dashes for primitive, object-target, and arm. Every `why` is at least 30 characters and
cites inspected evidence. Every seed frame number must appear in the report.

`seed.json` contains one or more chunks. Adjacent chunks may repeat a Timeline span only when a
successful row has two learn ranges. Each chunk has `span`, `learn`, `seed`, and `reason`.
A learn chunk has an inclusive in-span `learn`, `seed: null`, and no action fields. A seed chunk
has `learn: null`, an in-span uint `seed`, and exact `skill_label`, `object`, `target`, and
`arm` (`left`, `right`, or `either`). For no-progress, use `skill_label: "move to"` only as the
non-queryable mechanical placeholder; the Timeline action cells remain em dashes.

The runner verifies any identity fields already present, then stamps exact signed `task_id`,
integer `episode`, `n_steps`, `fps: 30`, and `rollout: "artifact:<uuid>"`. Do not guess or copy
legacy string/null identities. Validate both files before finishing.
