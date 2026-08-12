<!--
Derived and modified from OmniGibson agentic_sweep.
Copyright (c) 2023 Stanford Vision and Learning Group. SPDX-License-Identifier: MIT.
-->
# BEHAVIOR-1K skill vocabulary

The label set is closed. Copy one of these exact backtick strings into each actionable Timeline
row:

| label |
|---|
| `move to` |
| `pick up from` |
| `place in` |
| `place on` |
| `push to` |
| `open door` |
| `place on next to` |
| `close door` |
| `close lid` |
| `open lid` |
| `insert` |
| `tip over` |
| `turn on switch` |
| `hand over` |
| `turn to` |
| `open drawer` |
| `close drawer` |
| `place in next to` |
| `pour` |
| `press` |
| `ignite` |
| `turn off switch` |

`no progress` is a verdict, not a skill label. Such a row uses an em dash for primitive,
object-target, and arm.

## `move to` is never judged alone

Navigation changes no goal predicate and its boundary with manipulation is arbitrary. Fold an
approach into the manipulation that follows, including an approach to the wrong object or an
approach after which manipulation never began. Never create a standalone `move to` row.
