"""Idempotent in-cluster database/configuration bootstrap and backup upload.

Credentials arrive only through Kubernetes Secret references. Error output is
deliberately bounded because database exceptions can include connection data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

# Explicit subsystem grants. New tables receive no automatic gateway/actuator
# grants; update this inventory with their owning runtime change.
ACTUATOR_TABLES = (
    "alembic_version",
    "trials",
    "execution_classes",
    "execution_targets",
    "execution_leases",
    "execution_commands",
    "execution_events",
    "execution_lease_history",
    "execution_capacity_policies",
    "execution_capacity_observations",
    "execution_provisioning_authorizations",
    "execution_admission_policies",
    "execution_admission_reservations",
    "execution_cost_reservations",
    "execution_cost_reservation_debits",
    "execution_budget_policies",
    "execution_price_snapshots",
    "execution_target_price_bindings",
)
# PostgreSQL row locks require UPDATE on at least one column. Capacity settings
# remain read-only; admission/budget terminal triggers write only their counters.
ACTUATOR_POLICY_UPDATES = {
    "execution_capacity_policies": ("updated_at",),
    "execution_admission_policies": ("active_count", "counter_updated_at"),
    "execution_budget_policies": (
        "daily_reserved_microusd",
        "monthly_reserved_microusd",
        "updated_at",
    ),
}
GATEWAY_TABLES = (
    *ACTUATOR_TABLES,
    "tokens",
    "users",
    "secrets",
    "tasks",
    "data_lifecycle_authorities",
    "admin_audit_events",
    "artifacts",
    "artifact_lineage_edges",
    "artifact_upload_files",
    "artifact_upload_sessions",
    "llm_calls",
    "llm_call_intents",
    "model_switch_plans",
    "rate_cards",
    "provider_connections",
    "provider_connection_shares",
    "execution_attempts",
    "execution_attempt_provider_budgets",
    "pipeline_runs",
    "pipeline_stage_runs",
    "pipeline_budget_ledgers",
    "pipeline_budget_reservations",
    "pipeline_provider_dispatches",
    "pipeline_run_control_bindings",
    "pipeline_acceptance_preflight_prerequisites",
    "pipeline_checkpoints",
    "pipeline_input_imports",
)


class MigrationError(RuntimeError):
    def __init__(self, result: subprocess.CompletedProcess[str]) -> None:
        stderr = result.stderr or ""
        revisions = re.findall(r"Running upgrade [a-zA-Z0-9_]* -> ([a-zA-Z0-9_]+)", stderr)
        errors = re.findall(r"psycopg\.errors\.([A-Za-z]+)", stderr) or re.findall(
            r"\b([A-Z][A-Za-z]+(?:Error|Violation))\b", stderr
        )
        self.details = {
            "exit_code": result.returncode,
            "migration_revision": revisions[-1] if revisions else "unknown",
            "database_error": errors[-1] if errors else "unknown",
        }
        super().__init__("migration failed")


class ConfigurationRequestError(RuntimeError):
    def __init__(self, method: str, path: str, status: int) -> None:
        self.details = {"method": method, "route": path, "http_status": status}
        super().__init__("configuration request failed")


def database_url(value: str, namespace: str) -> str:
    url = make_url(value)
    if (
        url.get_backend_name() != "postgresql"
        or url.host != f"loom-postgres.{namespace}.svc"
        or url.port not in (None, 5432)
        or url.database != "loom"
        or url.query.get("sslmode") != "verify-full"
        or url.query.get("sslrootcert") != "/var/run/loom-db/ca.crt"
    ):
        raise ValueError("database must be the independent namespace-local TLS service")
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def bootstrap_database(config: dict[str, Any]) -> None:
    connection_url = database_url(os.environ["LOOM_DB_URL"], config["namespace"])
    tokens = (
        (
            "collector",
            os.environ["LOOM_COLLECTOR_TOKEN"],
            "loom_ecc_",
            "execution:capacity:observe",
        ),
        ("batch runner", os.environ["LOOM_BATCH_RUNNER_TOKEN"], "loom_br_", "submit:batch"),
    )
    for name, token, prefix, _scope in tokens:
        if not token.startswith(prefix) or len(token) < 40:
            raise ValueError(f"{name} token has an invalid identity")
    roles = {
        "loom_" + name: os.environ["LOOM_DB_" + name.upper() + "_PASSWORD"]
        for name in ("service", "control_plane", "gateway", "actuator")
    }
    if any(len(password) < 24 for password in roles.values()):
        raise ValueError("database role passwords must have at least 24 characters")
    with psycopg.connect(connection_url) as connection:
        with connection.cursor() as cursor:
            for role, password in roles.items():
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
                if cursor.fetchone() is None:
                    cursor.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
                cursor.execute(
                    sql.SQL(
                        "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {}"
                    ).format(sql.Identifier(role), sql.Literal(password))
                )
    migration_env = dict(
        os.environ,
        LOOM_DB_URL=make_url(connection_url)
        .set(drivername="postgresql+psycopg")
        .render_as_string(hide_password=False),
    )
    # Do not forward arbitrary Alembic/driver errors containing protected URLs.
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
        env=migration_env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise MigrationError(result)
    with psycopg.connect(connection_url) as connection:
        with connection.cursor() as cursor:
            for role in roles:
                identifier = sql.Identifier(role)
                cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE loom TO {}").format(identifier))
                cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(identifier))
                inventory = {"loom_gateway": GATEWAY_TABLES, "loom_actuator": ACTUATOR_TABLES}.get(
                    role
                )
                if inventory is not None:
                    cursor.execute(
                        sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {}").format(
                            identifier
                        )
                    )
                    # Read all listed state; mutations are bounded to the same
                    # service-owned contract tables, never team/credential admin.
                    tables = sql.SQL(", ").join(sql.Identifier(table) for table in inventory)
                    cursor.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(tables, identifier))
                    read_only = {
                        "alembic_version",
                        "users",
                        "tokens",
                        "secrets",
                        "tasks",
                        "data_lifecycle_authorities",
                        "provider_connections",
                        "provider_connection_shares",
                        "execution_classes",
                        "execution_capacity_policies",
                        "execution_capacity_observations",
                        "execution_admission_policies",
                        "execution_budget_policies",
                        "execution_price_snapshots",
                        "execution_target_price_bindings",
                    }
                    writable = sql.SQL(", ").join(
                        sql.Identifier(table) for table in inventory if table not in read_only
                    )
                    cursor.execute(
                        sql.SQL("GRANT INSERT, UPDATE, DELETE ON {} TO {}").format(
                            writable, identifier
                        )
                    )
                    if role == "loom_gateway":
                        # Call audit lazily creates trial/event authorities and
                        # verifies existing ones; retention and deletion remain
                        # owned by lifecycle management.
                        cursor.execute(
                            "GRANT INSERT ON data_lifecycle_authorities TO loom_gateway"
                        )
                        cursor.execute(
                            "GRANT UPDATE (last_used_at, last_seen_at) ON tokens TO loom_gateway"
                        )
                    if role == "loom_actuator":
                        for table, columns in ACTUATOR_POLICY_UPDATES.items():
                            cursor.execute(
                                sql.SQL("GRANT UPDATE ({}) ON {} TO loom_actuator").format(
                                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                                    sql.Identifier(table),
                                )
                            )
                        # Trial terminal projection invokes the existing quota
                        # trigger; quota settings and identity are not writable.
                        cursor.execute(
                            "GRANT SELECT (team_id, in_flight_count), UPDATE (in_flight_count) ON team_quotas TO loom_actuator"
                        )
                    cursor.execute(
                        sql.SQL("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {}").format(
                            identifier
                        )
                    )
                    cursor.execute(
                        "SELECT pg_get_serial_sequence(format('%%I.%%I', table_schema, table_name), column_name) FROM information_schema.columns WHERE table_schema='public' AND table_name = ANY(%s)",
                        (list(inventory),),
                    )
                    for (sequence,) in cursor.fetchall():
                        if sequence:
                            cursor.execute(
                                sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {} TO {}").format(
                                    sql.Identifier(*sequence.split(".")), identifier
                                )
                            )
                else:
                    cursor.execute(
                        sql.SQL(
                            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {}"
                        ).format(identifier)
                    )
                if inventory is None:
                    cursor.execute(
                        sql.SQL(
                            "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {}"
                        ).format(identifier)
                    )
                    cursor.execute(
                        sql.SQL(
                            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
                        ).format(identifier)
                    )
                    cursor.execute(
                        sql.SQL(
                            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO {}"
                        ).format(identifier)
                    )
            for name, token, _prefix, scope in tokens:
                token_hash = hashlib.sha256(token.encode()).digest()
                cursor.execute(
                    "INSERT INTO tokens (token_hash, type, scopes, team_id, issued_at, expires_at) VALUES (%s, 'worker', %s, NULL, now(), NULL) ON CONFLICT (token_hash) DO NOTHING",
                    (token_hash, [scope]),
                )
                cursor.execute(
                    "SELECT type, scopes, team_id, expires_at, revoked_at FROM tokens WHERE token_hash=%s",
                    (token_hash,),
                )
                if cursor.fetchone() != ("worker", [scope], None, None, None):
                    raise ValueError(f"{name} token is revoked or bound to another authority")


def configure_platform(
    config: dict[str, Any],
    *,
    config_dir: Path = Path("/var/run/loom-platform"),
    admin_secret: Path = Path("/var/run/loom/admin/secrets.toml"),
) -> None:
    with admin_secret.open("rb") as handle:
        token = tomllib.load(handle)["admin"]["token"]
    origin = f"http://loom-control-plane.{config['namespace']}.svc:8080"

    def request(method: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(
            origin + path,
            data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                result: dict[str, Any] = json.load(response)
                return result
        except urllib.error.HTTPError as exc:
            raise ConfigurationRequestError(method, path, exc.code) from None

    catalog = json.loads((config_dir / "catalog.json").read_text())
    observed = request("POST", "/admin/service-execution/catalog", catalog)
    if observed["target_ids"] != [config["target_id"]]:
        raise ValueError("catalog readback mismatch")
    price = request("POST", "/admin/execution-price-snapshots", config["execution_price"])
    if any(
        price.get(key) != config["execution_price"][key]
        for key in ("provider", "region", "sku", "source", "source_version")
    ):
        raise ValueError("execution price snapshot readback mismatch")
    binding = request(
        "PUT",
        "/admin/execution-target-price-bindings/" + config["target_id"],
        {
            "price_snapshot_id": price["id"],
            "enabled": True,
            "reason": "Reviewed independent Nebius integration execution price",
        },
    )
    if (
        binding.get("price_snapshot_id") != price["id"]
        or binding.get("target_id") != config["target_id"]
        or binding.get("enabled") is not True
    ):
        raise ValueError("execution price target binding readback mismatch")
    observed = request(
        "PUT",
        "/admin/execution-capacity-policies/" + config["target_id"],
        config["capacity_policy"],
    )
    if any(observed.get(key) != value for key, value in config["capacity_policy"].items()):
        raise ValueError("capacity policy readback mismatch")
    for kind, key in (("global", "*"), ("pool", "nebius-cpu")):
        body = {
            "max_concurrent": config["max_concurrent"],
            "enabled": True,
            "reason": "Nebius integration environment capacity",
        }
        observed = request("PUT", f"/admin/execution-admission-policies/{kind}/{key}", body)
        if any(observed.get(key) != value for key, value in body.items()):
            raise ValueError("admission policy readback mismatch")
    # Operator intent is distinct from actuator-observed readiness: no healthy
    # claim is synthesized by deployment. The actuator refreshes actual health.
    with psycopg.connect(
        database_url(os.environ["LOOM_DB_URL"], config["namespace"])
    ) as connection:
        connection.execute(
            "UPDATE execution_targets SET desired_state='active', updated_at=now() WHERE id=%s AND desired_state='disabled' AND health_status='unknown'",
            (config["target_id"],),
        )


def upload_backup(config: dict[str, Any]) -> None:
    path = Path("/backup/loom.dump")
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("database backup is empty")
    client = boto3.client(
        "s3",
        endpoint_url=config["storage_endpoint"],
        region_name=config["region"],
        aws_access_key_id=os.environ["LOOM_BACKUP_ACCESS_KEY"],
        aws_secret_access_key=os.environ["LOOM_BACKUP_SECRET_KEY"],
    )
    with path.open("rb") as handle:
        checksum = hashlib.file_digest(handle, "sha256").hexdigest()
    key = (
        config["namespace"]
        + "/"
        + datetime.now(UTC).strftime("%Y/%m/%d/%H%M%S")
        + "-"
        + checksum[:12]
        + ".dump"
    )
    client.upload_file(
        str(path), config["buckets"]["backup"], key, ExtraArgs={"Metadata": {"sha256": checksum}}
    )
    observed = client.head_object(Bucket=config["buckets"]["backup"], Key=key)
    # S3 user metadata names are case-insensitive. Nebius returns "Sha256"
    # through boto3; require one unambiguous, exact digest regardless of case.
    checksums = [
        value for name, value in observed.get("Metadata", {}).items() if name.lower() == "sha256"
    ]
    if observed["ContentLength"] != path.stat().st_size or checksums != [checksum]:
        raise ValueError("database backup upload readback mismatch")
    print(json.dumps({"backup_key": key, "sha256": checksum, "bytes": path.stat().st_size}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("database", "configure", "backup"))
    args = parser.parse_args()
    try:
        config = json.loads(Path(os.environ["LOOM_PLATFORM_CONFIG"]).read_text())
        {"database": bootstrap_database, "configure": configure_platform, "backup": upload_backup}[
            args.phase
        ](config)
    except Exception as exc:
        diagnostic: dict[str, Any] = {"phase": args.phase, "error_type": type(exc).__name__}
        if isinstance(exc, psycopg.Error):
            diagnostic["sqlstate"] = exc.sqlstate
        elif isinstance(exc, MigrationError):
            diagnostic.update(exc.details)
        elif isinstance(exc, ConfigurationRequestError):
            diagnostic.update(exc.details)
        elif isinstance(exc, ValueError) and type(exc) is ValueError:
            diagnostic["reason"] = str(exc)
        print(
            json.dumps(diagnostic, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    print(f"Nebius platform {args.phase} complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
