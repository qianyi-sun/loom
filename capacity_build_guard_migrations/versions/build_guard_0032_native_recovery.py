"""Retain two-phase recovery history without execution or release authority.

Revision ID: build_guard_0032
Revises: build_guard_0031
"""

from alembic import op

from capacity_build_guard_migrations.versions.build_guard_0011_revocation import _replace_clause

revision = "build_guard_0032"
down_revision = "build_guard_0031"
branch_labels = None
depends_on = None
SCHEMA = "loom_capacity_build_guard"
FUNCTIONS = ("publish_recovery(uuid,jsonb,bytea,text,text)", "read_recovery(uuid,jsonb,bytea,text,text)",
    "authorize_recovery_execution(uuid,jsonb,bytea,text,text)")
HELPERS = ("assert_recovery_shape(jsonb,text[])", "assert_recovery_record(jsonb)",
    "recovery_authenticated_claim(uuid,jsonb,bytea,text,text)")


def _fence(install):
    old = "issued := clock_timestamp();"
    new = f"""IF EXISTS (SELECT 1 FROM {SCHEMA}.native_recovery_profiles
                WHERE installation_id=p_installation AND pool_id=p->'claim'->'binding'->>'pool_id') THEN
                RAISE EXCEPTION 'native recovery installation requires digest-bound V2 execution'; END IF;
            {old}"""
    _replace_clause("authorize_execution(uuid,jsonb,bytea,text,text)", old if install else new, new if install else old)


def upgrade():
    # Owner-only tables. No worker/agent DML rights; publication goes through the
    # credential-checked procedures below. Static admission precedes execution.
    payload = f"""payload jsonb NOT NULL, wire_payload bytea NOT NULL, payload_sha256 text NOT NULL,
        retention_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
        CHECK (octet_length(wire_payload) BETWEEN 2 AND 131072),
        CHECK (wire_payload=convert_to({SCHEMA}.canonical_plan_json(payload),'UTF8')),
        CHECK (payload_sha256=encode(sha256(wire_payload),'hex'))"""
    op.execute(f"""
        CREATE TABLE {SCHEMA}.native_recovery_profiles (
            installation_id uuid NOT NULL REFERENCES {SCHEMA}.installations(id), pool_id text NOT NULL,
            {payload}, PRIMARY KEY (installation_id,pool_id));
        CREATE TABLE {SCHEMA}.native_recovery_hosts (
            installation_id uuid NOT NULL, pool_id text NOT NULL, {payload},
            PRIMARY KEY (installation_id,pool_id,payload_sha256),
            FOREIGN KEY (installation_id,pool_id) REFERENCES {SCHEMA}.native_recovery_profiles);
        CREATE TABLE {SCHEMA}.native_recovery_records (
            installation_id uuid NOT NULL REFERENCES {SCHEMA}.installations(id),
            claim_id uuid NOT NULL REFERENCES {SCHEMA}.platform_claims(id),
            phase text NOT NULL CHECK (phase IN ('preparation','finalization')), {payload},
            PRIMARY KEY (claim_id,phase));
    """)
    for table in ("native_recovery_profiles", "native_recovery_hosts", "native_recovery_records"):
        op.execute(f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE OR TRUNCATE ON {SCHEMA}.{table} FOR EACH STATEMENT EXECUTE FUNCTION {SCHEMA}.reject_evidence_mutation()")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.assert_recovery_shape(p jsonb, fields text[])
        RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        BEGIN
            IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR NOT p ?& fields
                OR (SELECT count(*) FROM jsonb_object_keys(p)) <> cardinality(fields) THEN
                RAISE EXCEPTION 'native recovery object shape changed'; END IF;
        END $function$;

        CREATE FUNCTION {SCHEMA}.assert_recovery_record(p jsonb)
        RETURNS void LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE prepared jsonb; locator jsonb; field text; value text; ranges jsonb; item jsonb;
            prior jsonb; position integer; prior_position integer; coordinate text; path_parts text[];
        BEGIN
            IF p->>'schema_version'='2' AND p->'schema_version'='2'::jsonb THEN
                PERFORM {SCHEMA}.assert_recovery_shape(p,ARRAY['schema_version','preparation','runtime_spec_sha256','uid_map','gid_map']);
                IF jsonb_typeof(p->'runtime_spec_sha256') IS DISTINCT FROM 'string'
                    OR p->>'runtime_spec_sha256' !~ '^[0-9a-f]{{64}}$' THEN
                    RAISE EXCEPTION 'native recovery runtime digest changed'; END IF;
                prepared := p->'preparation';
            ELSE prepared := p; END IF;
            PERFORM {SCHEMA}.assert_recovery_shape(prepared,ARRAY['schema_version','locator','launch_profile_sha256',
                'node_configuration_sha256','node_id','boot_id','original_uid','original_gid','cgroup_path',
                'cgroup_device','cgroup_inode','cgroup_mount_id']);
            locator := prepared->'locator';
            PERFORM {SCHEMA}.assert_recovery_shape(locator,ARRAY['schema_version','physical','worker_id',
                'worker_incarnation','config_sha256','release_manifest_sha256','directory','device','inode']);
            IF prepared->'schema_version' IS DISTINCT FROM '1'::jsonb OR prepared->>'schema_version'<>'1'
                OR locator->'schema_version' IS DISTINCT FROM '1'::jsonb OR locator->>'schema_version'<>'1' THEN
                RAISE EXCEPTION 'native recovery preparation version changed'; END IF;
            FOREACH field IN ARRAY ARRAY['launch_profile_sha256','node_configuration_sha256'] LOOP
                IF jsonb_typeof(prepared->field) IS DISTINCT FROM 'string' OR prepared->>field !~ '^[0-9a-f]{{64}}$' THEN
                    RAISE EXCEPTION 'native recovery preparation digest changed'; END IF;
            END LOOP;
            FOREACH field IN ARRAY ARRAY['config_sha256','release_manifest_sha256'] LOOP
                IF jsonb_typeof(locator->field) IS DISTINCT FROM 'string' OR locator->>field !~ '^[0-9a-f]{{64}}$' THEN
                    RAISE EXCEPTION 'native recovery locator digest changed'; END IF;
            END LOOP;
            IF jsonb_typeof(prepared->'boot_id') IS DISTINCT FROM 'string'
                OR (prepared->>'boot_id')::uuid::text IS DISTINCT FROM prepared->>'boot_id'
                OR jsonb_typeof(prepared->'node_id') IS DISTINCT FROM 'string'
                OR prepared->>'node_id' !~ '^[a-z0-9][a-z0-9_.-]{{0,127}}$' THEN
                RAISE EXCEPTION 'native recovery node identity changed'; END IF;
            FOREACH field IN ARRAY ARRAY['original_uid','original_gid','cgroup_device','cgroup_inode','cgroup_mount_id'] LOOP
                IF jsonb_typeof(prepared->field) IS DISTINCT FROM 'number' OR prepared->>field !~ '^(0|[1-9][0-9]*)$'
                    OR length(prepared->>field)>20 OR (field<>'cgroup_device' AND (prepared->>field)::numeric=0)
                    OR (field IN ('original_uid','original_gid') AND (prepared->>field)::numeric>=4294967295) THEN
                    RAISE EXCEPTION 'native recovery host numeric identity changed'; END IF;
            END LOOP;
            FOREACH field IN ARRAY ARRAY['device','inode'] LOOP
                IF jsonb_typeof(locator->field) IS DISTINCT FROM 'number' OR locator->>field !~ '^(0|[1-9][0-9]*)$'
                    OR length(locator->>field)>20 OR (field='inode' AND (locator->>field)::numeric=0) THEN
                    RAISE EXCEPTION 'native recovery directory numeric identity changed'; END IF;
            END LOOP;
            FOREACH field IN ARRAY ARRAY['directory','cgroup_path'] LOOP
                value := CASE WHEN field='directory' THEN locator->>field ELSE prepared->>field END;
                IF jsonb_typeof(CASE WHEN field='directory' THEN locator->field ELSE prepared->field END) IS DISTINCT FROM 'string'
                    OR length(value) NOT BETWEEN 2 AND 4096 OR value !~ '^/[^/]'
                    OR right(value,1)='/' OR position('//' in value)>0 OR value ~ E'[\\n\\r]'
                    OR string_to_array(value,'/') && ARRAY['.','..'] OR cardinality(string_to_array(value,'/'))>64 THEN
                    RAISE EXCEPTION 'native recovery path changed'; END IF;
            END LOOP;
            path_parts := string_to_array(prepared->>'cgroup_path','/');
            IF path_parts[cardinality(path_parts)] NOT IN ('job_' || (locator->'physical'->>'slurm_job_id'),
                'job_' || split_part(locator->'physical'->>'slurm_job_id','_',1))
                OR NOT EXISTS (SELECT 1 FROM unnest(path_parts[2:cardinality(path_parts)-1]) part
                    WHERE part IN ('slurm','slurmstepd.scope') OR right(part,length('_slurmstepd.scope'))='_slurmstepd.scope')
                OR EXISTS (SELECT 1 FROM unnest(path_parts[2:cardinality(path_parts)-1]) part WHERE left(part,4)='job_') THEN
                RAISE EXCEPTION 'native recovery job scope changed'; END IF;
            IF p->>'schema_version'='2' THEN
                FOREACH field IN ARRAY ARRAY['uid_map','gid_map'] LOOP
                    ranges := p->field;
                    IF jsonb_typeof(ranges) IS DISTINCT FROM 'array' OR jsonb_array_length(ranges) NOT BETWEEN 1 AND 340 THEN
                        RAISE EXCEPTION 'native recovery mapping bounds changed'; END IF;
                    position := 0;
                    FOR item IN SELECT * FROM jsonb_array_elements(ranges) LOOP
                        PERFORM {SCHEMA}.assert_recovery_shape(item,ARRAY['inside','outside','count']);
                        FOREACH coordinate IN ARRAY ARRAY['inside','outside','count'] LOOP
                            IF jsonb_typeof(item->coordinate) IS DISTINCT FROM 'number'
                                OR item->>coordinate !~ '^(0|[1-9][0-9]*)$' OR length(item->>coordinate)>10
                                OR (item->>coordinate)::numeric>=4294967295
                                OR (coordinate<>'inside' AND (item->>coordinate)::numeric=0) THEN
                                RAISE EXCEPTION 'native recovery mapping identity changed'; END IF;
                        END LOOP;
                        IF position=0 AND item IS DISTINCT FROM jsonb_build_object('inside',0,'outside',
                            prepared->CASE WHEN field='uid_map' THEN 'original_uid' ELSE 'original_gid' END,'count',1) THEN
                            RAISE EXCEPTION 'native recovery root mapping changed'; END IF;
                        FOREACH coordinate IN ARRAY ARRAY['inside','outside'] LOOP
                            IF (item->>coordinate)::numeric+(item->>'count')::numeric>4294967295 THEN
                                RAISE EXCEPTION 'native recovery mapping overflow'; END IF;
                            prior_position := 0;
                            FOR prior IN SELECT * FROM jsonb_array_elements(ranges) LOOP
                                EXIT WHEN prior_position=position;
                                IF (item->>coordinate)::numeric < (prior->>coordinate)::numeric+(prior->>'count')::numeric
                                    AND (prior->>coordinate)::numeric < (item->>coordinate)::numeric+(item->>'count')::numeric THEN
                                    RAISE EXCEPTION 'native recovery mapping overlap'; END IF;
                                prior_position := prior_position+1;
                            END LOOP;
                        END LOOP;
                        position := position+1;
                    END LOOP;
                END LOOP;
            END IF;
        END $function$;

        CREATE FUNCTION {SCHEMA}.recovery_authenticated_claim(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS uuid LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog AS $function$
        DECLARE claim {SCHEMA}.platform_claims%ROWTYPE; registration {SCHEMA}.worker_registrations%ROWTYPE;
        BEGIN
            IF current_setting('transaction_isolation')<>'serializable' OR wire IS NULL
                OR octet_length(wire) NOT BETWEEN 2 AND 131072
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native recovery canonical claim or transaction changed'; END IF;
            SELECT * INTO claim FROM {SCHEMA}.platform_claims WHERE id=(p->>'operation_id')::uuid;
            IF NOT FOUND OR claim.installation_id IS DISTINCT FROM p_installation
                OR claim.retention_xid=pg_current_xact_id() OR claim.payload IS DISTINCT FROM p
                OR claim.wire_payload IS DISTINCT FROM wire OR claim.payload_sha256 IS DISTINCT FROM digest THEN
                RAISE EXCEPTION 'native recovery requires exact committed claim'; END IF;
            SELECT * INTO registration FROM {SCHEMA}.worker_registrations WHERE id=claim.registration_id;
            IF NOT FOUND OR registration.retention_xid=pg_current_xact_id()
                OR registration.installation_id IS DISTINCT FROM p_installation OR credential_hash IS NULL
                OR registration.credential_sha256 IS DISTINCT FROM credential_hash THEN
                RAISE EXCEPTION 'native recovery credential changed'; END IF;
            RETURN claim.id;
        END $function$;

        CREATE FUNCTION {SCHEMA}.publish_recovery(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE claim_wire bytea; prepared jsonb; locator jsonb; profile {SCHEMA}.native_recovery_profiles%ROWTYPE;
            host {SCHEMA}.native_recovery_hosts%ROWTYPE; physical {SCHEMA}.execution_events%ROWTYPE;
            retained {SCHEMA}.native_recovery_records%ROWTYPE; prior {SCHEMA}.native_recovery_records%ROWTYPE;
            p_phase text; p_claim uuid; pool text;
        BEGIN
            PERFORM {SCHEMA}.assert_recovery_shape(p,ARRAY['schema_version','claim','record']);
            IF p->'schema_version' IS DISTINCT FROM '1'::jsonb OR p->>'schema_version'<>'1'
                OR wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 131072
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native recovery publication canonical binding changed'; END IF;
            PERFORM {SCHEMA}.assert_recovery_record(p->'record');
            claim_wire := convert_to({SCHEMA}.canonical_plan_json(p->'claim'),'UTF8');
            PERFORM {SCHEMA}.authorize_source(p_installation,p->'claim',claim_wire,encode(sha256(claim_wire),'hex'),credential_hash);
            p_claim := (p->'claim'->>'operation_id')::uuid;
            pool := p->'claim'->'binding'->>'pool_id';
            p_phase := CASE WHEN p->'record'->>'schema_version'='2' THEN 'finalization' ELSE 'preparation' END;
            prepared := CASE WHEN p_phase='finalization' THEN p->'record'->'preparation' ELSE p->'record' END;
            locator := prepared->'locator';
            SELECT * INTO profile FROM {SCHEMA}.native_recovery_profiles WHERE installation_id=p_installation AND pool_id=pool;
            IF NOT FOUND OR profile.retention_xid=pg_current_xact_id()
                OR profile.payload->'launch_profile_sha256' IS DISTINCT FROM prepared->'launch_profile_sha256'
                OR profile.payload->'worker_config_sha256' IS DISTINCT FROM locator->'config_sha256'
                OR profile.payload->'release_manifest_sha256' IS DISTINCT FROM locator->'release_manifest_sha256' THEN
                RAISE EXCEPTION 'native recovery admitted profile changed'; END IF;
            SELECT * INTO host FROM {SCHEMA}.native_recovery_hosts WHERE installation_id=p_installation AND pool_id=pool
                AND payload_sha256=prepared->>'node_configuration_sha256';
            IF NOT FOUND OR host.retention_xid=pg_current_xact_id()
                OR host.payload->'node_id' IS DISTINCT FROM prepared->'node_id'
                OR host.payload->'boot_id' IS DISTINCT FROM prepared->'boot_id'
                OR host.payload->'original_uid' IS DISTINCT FROM prepared->'original_uid'
                OR host.payload->'original_gid' IS DISTINCT FROM prepared->'original_gid'
                OR NOT (p->'claim'->'binding'->'node_ids') @> jsonb_build_array(prepared->'node_id') THEN
                RAISE EXCEPTION 'native recovery admitted host changed'; END IF;
            SELECT e.* INTO physical FROM {SCHEMA}.platform_claims c
                JOIN {SCHEMA}.worker_registrations r ON r.id=c.registration_id
                JOIN {SCHEMA}.execution_events e ON e.id=r.physical_event_id WHERE c.id=p_claim;
            IF NOT FOUND OR physical.kind<>'bound' OR physical.retention_xid=pg_current_xact_id()
                OR physical.payload IS DISTINCT FROM locator->'physical'
                OR physical.wire_payload IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(locator->'physical'),'UTF8')
                OR locator->'worker_id' IS DISTINCT FROM p->'claim'->'worker_id'
                OR locator->'worker_incarnation' IS DISTINCT FROM p->'claim'->'worker_incarnation' THEN
                RAISE EXCEPTION 'native recovery committed physical identity changed'; END IF;
            IF p_phase='finalization' THEN
                SELECT * INTO prior FROM {SCHEMA}.native_recovery_records WHERE claim_id=p_claim AND phase='preparation';
                IF NOT FOUND OR prior.retention_xid=pg_current_xact_id() OR prior.payload->'record' IS DISTINCT FROM prepared THEN
                    RAISE EXCEPTION 'native recovery finalization requires committed preparation'; END IF;
            END IF;
            INSERT INTO {SCHEMA}.native_recovery_records(installation_id,claim_id,phase,payload,wire_payload,payload_sha256)
                VALUES(p_installation,p_claim,p_phase,p,wire,digest) ON CONFLICT DO NOTHING;
            SELECT * INTO STRICT retained FROM {SCHEMA}.native_recovery_records WHERE claim_id=p_claim AND phase=p_phase;
            IF retained.wire_payload IS DISTINCT FROM wire THEN RAISE EXCEPTION 'native recovery replay changed'; END IF;
            -- Uniqueness waits must not turn an expired source lease into new authority.
            PERFORM {SCHEMA}.authorize_source(p_installation,p->'claim',claim_wire,encode(sha256(claim_wire),'hex'),credential_hash);
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',1,'request',p,'request_digest',digest));
        END $function$;

        CREATE FUNCTION {SCHEMA}.read_recovery(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE p_claim uuid; result jsonb; retained {SCHEMA}.native_recovery_records%ROWTYPE;
        BEGIN
            p_claim := {SCHEMA}.recovery_authenticated_claim(p_installation,p,wire,digest,credential_hash);
            result := jsonb_build_object('schema_version',1,'preparation',NULL,'finalization',NULL);
            FOR retained IN SELECT * FROM {SCHEMA}.native_recovery_records WHERE claim_id=p_claim LOOP
                IF retained.retention_xid=pg_current_xact_id() THEN
                    RAISE EXCEPTION 'native recovery readback requires committed record'; END IF;
                result := result || jsonb_build_object(retained.phase,jsonb_build_object('schema_version',1,
                    'request',retained.payload,'request_digest',retained.payload_sha256));
            END LOOP;
            RETURN {SCHEMA}.canonical_plan_json(result);
        END $function$;
    """)
    _fence(True)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.authorize_recovery_execution(p_installation uuid,p jsonb,wire bytea,digest text,credential_hash text)
        RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $function$
        DECLARE claim_wire bytea; access jsonb; final {SCHEMA}.native_recovery_records%ROWTYPE;
            issued timestamptz; deadline timestamptz;
        BEGIN
            PERFORM {SCHEMA}.assert_recovery_shape(p,ARRAY['schema_version','claim','challenge',
                'source_binding_sha256','recovery_finalization_sha256']);
            IF p->'schema_version' IS DISTINCT FROM '2'::jsonb OR p->>'schema_version'<>'2'
                OR jsonb_typeof(p->'challenge') IS DISTINCT FROM 'string'
                OR (p->>'challenge')::uuid::text IS DISTINCT FROM p->>'challenge'
                OR jsonb_typeof(p->'source_binding_sha256') IS DISTINCT FROM 'string'
                OR p->>'source_binding_sha256' !~ '^[0-9a-f]{{64}}$'
                OR jsonb_typeof(p->'recovery_finalization_sha256') IS DISTINCT FROM 'string'
                OR p->>'recovery_finalization_sha256' !~ '^[0-9a-f]{{64}}$'
                OR wire IS NULL OR octet_length(wire) NOT BETWEEN 2 AND 131072
                OR wire IS DISTINCT FROM convert_to({SCHEMA}.canonical_plan_json(p),'UTF8')
                OR digest IS DISTINCT FROM encode(sha256(wire),'hex') THEN
                RAISE EXCEPTION 'native recovery execution canonical request changed'; END IF;
            claim_wire := convert_to({SCHEMA}.canonical_plan_json(p->'claim'),'UTF8');
            access := {SCHEMA}.authorize_source(p_installation,p->'claim',claim_wire,
                encode(sha256(claim_wire),'hex'),credential_hash)::jsonb;
            IF access->>'source_binding_sha256' IS DISTINCT FROM p->>'source_binding_sha256' THEN
                RAISE EXCEPTION 'native recovery execution source binding changed'; END IF;
            SELECT * INTO final FROM {SCHEMA}.native_recovery_records
                WHERE claim_id=(p->'claim'->>'operation_id')::uuid AND phase='finalization';
            IF NOT FOUND OR final.installation_id IS DISTINCT FROM p_installation
                OR final.retention_xid=pg_current_xact_id()
                OR final.payload_sha256 IS DISTINCT FROM p->>'recovery_finalization_sha256'
                OR final.payload->'claim' IS DISTINCT FROM p->'claim'
                OR NOT EXISTS (SELECT 1 FROM {SCHEMA}.native_recovery_profiles
                    WHERE installation_id=p_installation AND pool_id=p->'claim'->'binding'->>'pool_id'
                        AND retention_xid<>pg_current_xact_id()) THEN
                RAISE EXCEPTION 'native recovery execution requires committed exact finalization'; END IF;
            issued := clock_timestamp();
            deadline := (access->>'lease_not_after')::timestamptz;
            IF deadline IS NULL OR deadline<=issued THEN
                RAISE EXCEPTION 'native recovery execution lease expired'; END IF;
            deadline := LEAST(deadline,issued+interval '10 seconds');
            RETURN {SCHEMA}.canonical_plan_json(jsonb_build_object('schema_version',2,'request',p,
                'request_digest',digest,'executable',true,
                'issued_at',to_char(issued AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
                    || CASE WHEN to_char(issued,'US')='000000' THEN '' ELSE '.' || to_char(issued,'US') END || 'Z',
                'not_after',to_char(deadline AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
                    || CASE WHEN to_char(deadline,'US')='000000' THEN '' ELSE '.' || to_char(deadline,'US') END || 'Z'));
        END $function$;
    """)
    agent = op.get_context().config.attributes["build_guard_agent_role"]
    quote = op.get_bind().dialect.identifier_preparer.quote
    for signature in (*HELPERS, *FUNCTIONS):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
    for signature in FUNCTIONS:
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {quote(agent)}")


def downgrade():
    _fence(False)
    for signature in (*FUNCTIONS, *reversed(HELPERS)):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    for table in ("native_recovery_records", "native_recovery_hosts", "native_recovery_profiles"):
        op.execute(f"DROP TABLE {SCHEMA}.{table}")
