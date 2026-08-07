from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
CNPG_PRIMARY_SERVICE = "service/loom-postgres-rw"
LEGACY_POSTGRES_STATEFULSET = "statefulset/loom-postgres"
POSTGRES_EXEC_CONSUMERS = (
    "src/loom_cli/rollout/operator/backup.py",
    "src/loom_cli/rollout/operator/manifest_ownership_epoch.py",
    "src/loom_cli/rollout/operator/mutation_epoch_provider.py",
    "src/loom_cli/rollout/operator/preflight_credential_installer.py",
    "src/loom_cli/rollout/operator/protected_epoch_component.py",
    "src/loom_cli/rollout/operator/protected_migration_component.py",
    "src/loom_cli/rollout/operator/protected_production_defaults_component.py",
)


def test_protected_postgres_consumers_follow_cnpg_primary_service() -> None:
    for relative_path in POSTGRES_EXEC_CONSUMERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert CNPG_PRIMARY_SERVICE in source, relative_path
        assert LEGACY_POSTGRES_STATEFULSET not in source, relative_path
