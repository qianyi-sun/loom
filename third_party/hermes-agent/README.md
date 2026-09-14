# Optional vendored Hermes Agent sources
#
# Populate this directory with a checkout of NousResearch/hermes-agent at the
# SHA pinned in `loom_launcher.adapters._hermes_runtime.HERMES_AGENT_REF` to
# bake `/opt/loom-agents/hermes` without hitting GitHub during image builds:
#
#   rsync -a --delete ../hermes-agent/ third_party/hermes-agent/ \
#     --exclude .git --exclude .venv --exclude node_modules
#
# If `pyproject.toml` is missing, Dockerfile.agent-sandbox falls back to the
# git+https pin (may 429 under rate limits).
#
# Do not run Nous install.sh here — Loom uses thin `uv pip install` only.
