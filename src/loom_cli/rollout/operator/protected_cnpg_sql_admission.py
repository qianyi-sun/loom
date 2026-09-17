"""Read effective SQL-writer inputs for the pinned CNPG 1.25.1 / PostgreSQL 17 primary.

The caller retains administrator/configuration exclusion, original guard, and
process/volume admission. Run for each supported connectable database before
sealing and again over retained peers before transfer. This is catalog admission,
not a database drain or proof that pending controller requests have retired.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime

from psycopg.pq import TransactionStatus

from loom.application_database_admission import ApplicationDatabaseHandoffBackend
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql

# Reviewed upstream c56e00d462c3899ab305540953ec541dfe0f762a, pkg/postgres/
# configuration.go and pkg/management/postgres/configuration.go. These are fixed
# supported defaults, not a digest derived from the observed live installation.
_SETTINGS = {
    'archive_command': '/controller/manager wal-archive --log-destination /controller/log/postgres.json %p',
    'restore_command': '/controller/manager wal-restore --log-destination /controller/log/postgres.json %f %p',
    'archive_library': '', 'archive_cleanup_command': '', 'recovery_end_command': '',
    'shared_preload_libraries': '', 'session_preload_libraries': '', 'local_preload_libraries': '',
    'dynamic_library_path': '$libdir', 'jit_provider': 'llvmjit', 'ssl_passphrase_command': '',
    'data_directory': '/var/lib/postgresql/data/pgdata',
    'config_file': '/var/lib/postgresql/data/pgdata/postgresql.conf',
    'hba_file': '/var/lib/postgresql/data/pgdata/pg_hba.conf',
    'ident_file': '/var/lib/postgresql/data/pgdata/pg_ident.conf',
    'unix_socket_directories': '/controller/run', 'allow_alter_system': 'off',
    'restart_after_crash': 'off',
}
_DATABASES = {'loom', 'postgres', 'template1'}
# Native C definitions independently read from the pinned image's postgres.bki
# (SHA256 0416a5b74d7daf4a51c49c64df34a0f7cf42a3ff17173b155c352176ba85e889),
# snowball_create.sql and extension/plpgsql--1.0.sql. Only OIDs assigned by the
# latter two scripts are normalized; names, signatures, owners and code remain exact.
CNPG_NATIVE_C_CATALOG_SHA256 = '1099886b9075a477651f88ea5585642deb5ccbadfccec13f619c4a607634235b'
_NATIVE_C = """
SELECT CASE WHEN p.probin IN ('$libdir/plpgsql','$libdir/dict_snowball') THEN 0 ELSE p.oid::bigint END,
       p.proname,p.pronamespace::bigint,p.proowner::bigint,p.prolang::bigint,
       p.prosrc,p.probin,p.prosecdef,p.proconfig,p.proargtypes::text,p.prorettype::bigint,p.proisstrict
FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
WHERE l.lanname='c'
ORDER BY p.proname COLLATE "C"
"""
_CATALOG = """
SELECT
  NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension
              WHERE NOT (extname='plpgsql' AND extversion='1.0'
                         AND extnamespace='pg_catalog'::pg_catalog.regnamespace))
  AND EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname='plpgsql')
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_publication)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_subscription)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_server)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_data_wrapper)
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_language WHERE lanname NOT IN ('internal','c','sql','plpgsql'))
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolsuper AND rolname <> 'postgres')
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolreplication AND rolname NOT IN ('postgres','streaming_replica'))
  AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname='streaming_replica' AND rolreplication
              AND rolcanlogin AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls))
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r
                  ON (m.member=r.oid OR m.roleid=r.oid) WHERE r.rolname='streaming_replica')
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datallowconn AND datname NOT IN ('loom','postgres','template1'))
  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_settings WHERE pending_restart)
"""
_EMPTY_MAINTENANCE = """
SELECT NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
) AND NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
   WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
)
"""


def require_cnpg_effective_sql_profile(
    connection: ApplicationDatabaseConnection, *, database: str, original: ApplicationDatabaseHandoffBackend,
) -> str:
    """Return non-secret evidence only after actual, read-only checks pass.

    Database admission may already be closed. Use the original application peer
    and fixed maintenance/template peer, not normal runtime credentials or a new
    application connection. Credential, ownership and guard checks remain the
    existing handoff phases; they are not inferred from this SQL profile.
    """
    if (database not in _DATABASES or connection.info.server_version != 170004
            or connection.info.transaction_status != TransactionStatus.IDLE):
        raise RuntimeError('CNPG SQL profile requires the supported idle peer')
    with connection.transaction():
        connection.execute('SET TRANSACTION READ ONLY')
        connection.execute('SET LOCAL search_path=pg_catalog,pg_temp')
        row = connection.execute(
            "SELECT current_database(),s.system_identifier::text,pg_postmaster_start_time()::text,"
            "current_user='postgres' AND current_user=session_user AND NOT pg_is_in_recovery() "
            "FROM pg_catalog.pg_control_system() s"
        ).fetchone()
        if (row is None or len(row) != 4 or row[0] != database or row[1] != original.system_identifier
                or not isinstance(row[2], str) or row[3] is not True
                or datetime.fromisoformat(row[2]) != datetime.fromisoformat(original.server_started_at)):
            raise RuntimeError('CNPG SQL original server identity changed')
        settings = connection.execute(application_sql(
            'SELECT name,setting FROM pg_catalog.pg_settings WHERE name=ANY({}::text[]) ORDER BY name',
            list(_SETTINGS),
        )).fetchall()
        if settings != sorted(_SETTINGS.items()):
            raise RuntimeError('CNPG SQL effective executable settings are unsupported')
        # File settings include not-yet-reloaded changes and overwritten entries.
        # Do not bless a harmless current value while a harmful reload is pending.
        files = connection.execute(
            'SELECT name,setting,error FROM pg_catalog.pg_file_settings ORDER BY seqno'
        ).fetchall()
        for name, value, error in files:
            if (error is not None or not isinstance(name, str) or not isinstance(value, str)
                    or ('.' in name and not (name == 'cnpg.config_sha256' and re.fullmatch(r'[0-9a-f]{64}', value)))
                    or (name in _SETTINGS and value != _SETTINGS[name]
                        and not (name in {'allow_alter_system', 'restart_after_crash'} and value in {'false', '0'}))):
                raise RuntimeError('CNPG SQL pending configuration is unsupported')
        native = connection.execute(_NATIVE_C).fetchall()
        if hashlib.sha256(json.dumps(native, separators=(',', ':')).encode()).hexdigest() != CNPG_NATIVE_C_CATALOG_SHA256:
            raise RuntimeError('CNPG SQL native executable catalog changed')
        if connection.execute(_CATALOG).fetchone() != (True,):
            raise RuntimeError('CNPG SQL native catalog or writer profile is unsupported')
        if database != 'loom' and connection.execute(_EMPTY_MAINTENANCE).fetchone() != (True,):
            raise RuntimeError('CNPG SQL maintenance database has executable objects')
        role_settings = connection.execute(
            'SELECT setdatabase::bigint,setrole::bigint,setconfig FROM pg_catalog.pg_db_role_setting ORDER BY setdatabase,setrole'
        ).fetchall()
        for _database_oid, _role_oid, config in role_settings:
            if not isinstance(config, list) or not config or len(config) != len(set(config)):
                raise RuntimeError('CNPG SQL role settings are invalid')
            for value in config:
                if not isinstance(value, str) or (value != 'default_transaction_read_only=on' and re.fullmatch(
                    r'(?:statement_timeout|lock_timeout|idle_in_transaction_session_timeout)=[1-9][0-9]{0,5}(?:ms|s|min)?', value,
                ) is None):
                    raise RuntimeError('CNPG SQL role executable settings are unsupported')
    return hashlib.sha256(json.dumps({
        'profile': 'cnpg-1.25.1-postgresql-17.4-effective-sql-v1',
        'database': database, 'system_identifier': original.system_identifier,
        'server_started_at': original.server_started_at, 'settings': settings,
        'role_settings': role_settings,
    }, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
