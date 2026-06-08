#!/usr/bin/env bash
# rename-to-loom.sh — rename ~/agentic-data-platform → ~/loom and move
# the matching Claude memory dir so the new shell session finds it.
#
# Run this from a NEW shell session (NOT from inside the Claude Code
# session you used to author this script). The session you're in right
# now has the old CWD baked into its env; renaming under it can crash
# the harness.
#
# Safe to dry-run first: `bash scripts/rename-to-loom.sh --dry-run`
# Safe to re-run if it partially succeeded: each step checks state first.

set -euo pipefail

OLD_DIR="${HOME}/agentic-data-platform"
NEW_DIR="${HOME}/loom"
OLD_MEM_DIR="${HOME}/.claude/projects/-home-hongjian-agentic-data-platform"
NEW_MEM_DIR="${HOME}/.claude/projects/-home-hongjian-loom"

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=1
  echo "=== DRY RUN — no changes will be made ==="
fi

run() {
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "  [would run] $*"
  else
    echo "  [run] $*"
    eval "$@"
  fi
}

echo ""
echo "=== Pre-flight checks ==="

# 1. Confirm no Claude Code session is active.
# We only inspect processes literally named 'claude' (the CLI binary).
# The script's own bash invocation may have OLD_DIR in argv if you run it
# via an absolute path — that's not what we're guarding against.
ACTIVE_CLAUDE=$(pgrep -x claude 2>/dev/null || true)
if [ -n "$ACTIVE_CLAUDE" ]; then
  # Only block if any of those claude processes' CWD is inside OLD_DIR.
  for pid in $ACTIVE_CLAUDE; do
    cwd=$(readlink "/proc/$pid/cwd" 2>/dev/null || echo "")
    case "$cwd" in
      "$OLD_DIR"*)
        echo "ERROR: claude process (pid $pid) has CWD inside $OLD_DIR."
        echo "       Exit that session first, then re-run this script."
        exit 1
        ;;
    esac
  done
fi
echo "  no claude session has CWD inside $OLD_DIR — ok"

# 2. Confirm CWD is not inside the old dir (or this shell will be orphaned)
case "$PWD" in
  "$OLD_DIR"*)
    echo "ERROR: your current shell's PWD ($PWD) is inside $OLD_DIR."
    echo "       cd somewhere else (e.g. \`cd ~\`) and re-run."
    exit 1
    ;;
esac
echo "  current PWD is not inside $OLD_DIR — ok"

# 3. Confirm working tree is clean and pushed
if [ -d "$OLD_DIR/.git" ]; then
  if ! git -C "$OLD_DIR" diff --quiet HEAD 2>/dev/null; then
    echo "WARNING: $OLD_DIR has uncommitted changes."
    echo "         Recommended: commit/stash before renaming."
    git -C "$OLD_DIR" status --short | head -10
    read -rp "  Continue anyway? [y/N] " yn
    [[ "$yn" =~ ^[Yy]$ ]] || exit 1
  else
    echo "  working tree clean — ok"
  fi
  AHEAD=$(git -C "$OLD_DIR" rev-list --count "@{u}..HEAD" 2>/dev/null || echo "?")
  if [ "$AHEAD" != "0" ] && [ "$AHEAD" != "?" ]; then
    echo "WARNING: $OLD_DIR is $AHEAD commits ahead of upstream (not pushed)."
    echo "         Recommended: push before renaming so origin reflects local state."
    read -rp "  Continue anyway? [y/N] " yn
    [[ "$yn" =~ ^[Yy]$ ]] || exit 1
  fi
fi

# 4. Confirm target paths don't already exist
if [ -e "$NEW_DIR" ]; then
  echo "ERROR: $NEW_DIR already exists. Refusing to overwrite."
  exit 1
fi
if [ -e "$NEW_MEM_DIR" ]; then
  echo "ERROR: $NEW_MEM_DIR already exists. Refusing to overwrite."
  exit 1
fi
echo "  target paths $NEW_DIR and $NEW_MEM_DIR are free — ok"

echo ""
echo "=== Step 1: rename the working directory ==="
if [ -d "$OLD_DIR" ]; then
  run "mv $OLD_DIR $NEW_DIR"
else
  echo "  $OLD_DIR does not exist (already renamed?) — skipping"
fi

echo ""
echo "=== Step 2: rename the Claude memory dir (matches the new abs-path slug) ==="
if [ -d "$OLD_MEM_DIR" ]; then
  run "mv $OLD_MEM_DIR $NEW_MEM_DIR"
else
  echo "  $OLD_MEM_DIR does not exist (already renamed?) — skipping"
fi

echo ""
echo "=== Step 3: update .claude/settings.local.json hook paths (in the renamed repo) ==="
SETTINGS="$NEW_DIR/.claude/settings.local.json"
if [ -f "$SETTINGS" ]; then
  if grep -q "agentic-data-platform" "$SETTINGS"; then
    run "sed -i 's|/home/hongjian/.claude/projects/-home-hongjian-agentic-data-platform|/home/hongjian/.claude/projects/-home-hongjian-loom|g' '$SETTINGS'"
    run "sed -i 's|the agentic-data-platform project|the loom project|g' '$SETTINGS'"
    echo "  settings.local.json hook paths updated"
  else
    echo "  $SETTINGS has no agentic-data-platform refs — skipping"
  fi
else
  echo "  $SETTINGS not found — skipping"
fi

echo ""
echo "=== Step 4: update memory files with hardcoded paths ==="
if [ -d "$NEW_MEM_DIR" ]; then
  if grep -rl "agentic-data-platform" "$NEW_MEM_DIR" >/dev/null 2>&1; then
    run "find '$NEW_MEM_DIR' -name '*.md' -exec sed -i 's|/home/hongjian/.claude/projects/-home-hongjian-agentic-data-platform|/home/hongjian/.claude/projects/-home-hongjian-loom|g' {} +"
    run "find '$NEW_MEM_DIR' -name '*.md' -exec sed -i 's|~/agentic-data-platform|~/loom|g' {} +"
    run "find '$NEW_MEM_DIR' -name '*.md' -exec sed -i 's|/home/hongjian/agentic-data-platform|/home/hongjian/loom|g' {} +"
    echo "  memory file paths rewritten"
  else
    echo "  no agentic-data-platform refs left in memory files — skipping"
  fi
fi

echo ""
echo "=== Step 5: drop the 'rename deferred' note from project-loom memory ==="
PLOOM="$NEW_MEM_DIR/project-loom.md"
if [ -f "$PLOOM" ] && grep -q "Directory rename .* deferred" "$PLOOM"; then
  if [ "$DRY_RUN" -eq 0 ]; then
    # Replace the deferred-rename bullet with a "completed" note
    sed -i 's|- Directory rename `agentic-data-platform/` → `loom/` deferred.*|- Directory rename `agentic-data-platform/` → `loom/` completed 2026-06-08; the working dir is `~/loom` and the Claude memory slug is `-home-hongjian-loom`.|' "$PLOOM"
    echo "  project-loom.md updated"
  else
    echo "  [would update] $PLOOM — drop deferred-rename bullet"
  fi
fi

echo ""
echo "=== Done ==="
if [ "$DRY_RUN" -eq 1 ]; then
  echo "Dry run complete. Re-run without --dry-run to apply."
else
  echo "Rename complete."
  echo ""
  echo "Next steps:"
  echo "  1. cd $NEW_DIR"
  echo "  2. Start a fresh Claude Code session: \`claude\` (or your usual launcher)"
  echo "  3. Verify in the new session:"
  echo "     - it loads memory (Claude should remember project state)"
  echo "     - \`git remote -v\` shows the new URL"
  echo "     - \`pwd\` is $NEW_DIR"
  echo "  4. Optional: update IDE workspace settings, tmux configs,"
  echo "     ~/.bashrc aliases that reference the old path"
fi
