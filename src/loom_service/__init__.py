"""loom_service — REST API for researchers (service-layer spec, 2026-06-06).

Sibling FastAPI service to loom_control_plane / loom_llm_gateway /
loom_worker. Exposes a thin user-facing /api/v1 surface (teams,
tokens, trials browse) on top of the v0.7 runtime core. Stateless;
all state still lives in Postgres + MinIO. Plans 17-22 build it out
phase by phase.
"""
