"""Pure exact-candidate Alembic graph and migration-policy readiness."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from alembic.config import Config
from alembic.script import ScriptDirectory

_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")
DEFAULT_MIGRATION_POLICY = (
    Path(__file__).resolve().parents[3] / "config/staging-migration-policy.json"
)


@dataclass(frozen=True, slots=True)
class MigrationPlanEvidence:
    head: str
    base: str
    revision_count: int
    revision_sha256: MappingProxyType[str, str]
    graph_policy: str
    upgrade_policy: str
    downgrade_policy: str
    policy_digest: str
    plan_digest: str


def _load_policy(path: Path) -> tuple[dict[str, object], str]:
    try:
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("staging migration policy is unreadable") from exc
    expected = {
        "schema_version",
        "environment",
        "expected_head",
        "graph_policy",
        "upgrade_policy",
        "downgrade_policy",
        "protected_apply_requires_rehearsal",
    }
    if (
        not isinstance(value, dict)
        or set(value) != expected
        or value["schema_version"] != 1
        or value["environment"] != "staging"
        or value["graph_policy"] != "single-head-closed-dag"
        or value["protected_apply_requires_rehearsal"] is not True
        or not all(
            isinstance(value[name], str) and 3 <= len(value[name]) <= 80
            for name in ("expected_head", "upgrade_policy", "downgrade_policy")
        )
    ):
        raise ValueError("staging migration policy is invalid")
    return value, hashlib.sha256(payload).hexdigest()


def inspect_migration_plan(
    alembic_ini: Path,
    *,
    policy_path: Path = DEFAULT_MIGRATION_POLICY,
) -> MigrationPlanEvidence:
    """Inspect one exact single-head migration DAG without a database."""
    policy, policy_digest = _load_policy(policy_path)
    try:
        if (
            not alembic_ini.is_absolute()
            or alembic_ini.name != "alembic.ini"
            or alembic_ini.parent.name != "migrations"
            or not alembic_ini.is_file()
        ):
            raise ValueError
        config = Config(str(alembic_ini))
        if config.get_main_option("script_location") != "migrations":
            raise ValueError
        config.set_main_option("path_separator", "os")
        config.set_main_option("script_location", str(alembic_ini.parent))
        directory = ScriptDirectory.from_config(config)
        heads = tuple(directory.get_heads())
        bases = tuple(directory.get_bases())
        scripts = tuple(directory.walk_revisions(base="base", head="heads"))
    except Exception as exc:
        raise ValueError("Alembic migration graph is unreadable") from exc
    if (
        len(heads) != 1
        or len(bases) != 1
        or not scripts
        or heads[0] != policy["expected_head"]
        or any(_REVISION_RE.fullmatch(script.revision) is None for script in scripts)
    ):
        raise ValueError("Alembic migration graph does not match staging policy")

    by_revision = {script.revision: script for script in scripts}
    if len(by_revision) != len(scripts):
        raise ValueError("Alembic migration revisions are not unique")
    children = {revision: 0 for revision in by_revision}
    for script in scripts:
        down = script.down_revision
        if down is None:
            if script.revision != bases[0]:
                raise ValueError("Alembic migration graph has an unexpected base")
            continue
        parents = (down,) if isinstance(down, str) else down
        if (
            not isinstance(parents, tuple)
            or not parents
            or len(parents) != len(set(parents))
            or any(parent not in by_revision for parent in parents)
        ):
            raise ValueError("Alembic migration graph is not a closed DAG")
        for parent in parents:
            children[parent] += 1
    if any(count == 0 and revision != heads[0] for revision, count in children.items()):
        raise ValueError("Alembic migration graph has a disconnected revision")

    revision_hashes: dict[str, str] = {}
    for revision, script in sorted(by_revision.items()):
        path = Path(script.path)
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise ValueError("Alembic migration source is unreadable") from exc
        revision_hashes[revision] = hashlib.sha256(payload).hexdigest()
    plan_payload = {
        "base": bases[0],
        "downgrade_policy": policy["downgrade_policy"],
        "graph_policy": policy["graph_policy"],
        "head": heads[0],
        "policy_digest": policy_digest,
        "revision_sha256": revision_hashes,
        "upgrade_policy": policy["upgrade_policy"],
    }
    return MigrationPlanEvidence(
        head=heads[0],
        base=bases[0],
        revision_count=len(scripts),
        revision_sha256=MappingProxyType(revision_hashes),
        graph_policy=str(policy["graph_policy"]),
        upgrade_policy=str(policy["upgrade_policy"]),
        downgrade_policy=str(policy["downgrade_policy"]),
        policy_digest=policy_digest,
        plan_digest=hashlib.sha256(
            json.dumps(plan_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


__all__ = [
    "DEFAULT_MIGRATION_POLICY",
    "MigrationPlanEvidence",
    "inspect_migration_plan",
]
