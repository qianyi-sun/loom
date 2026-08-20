from __future__ import annotations


DEFAULT_BASE_IMAGE = "mictern2/terminus2-full:latest"
DEFAULT_MAX_SAME_TASK_NAMES = 10
DEFAULT_AGENT_TIMEOUT_BY_DIFFICULTY = {
    "medium": 5400.0,
    "hard": 5400.0,
    "expert": 5400.0,
}
DEFAULT_ENVIRONMENT_BUILD_TIMEOUT_SEC = 1200.0
