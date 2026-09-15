"""Owner admission for verified recovery-capable installed workers.

The installer must verify code, host namespace facts and account isolation before
retaining these facts. Hash equality alone is not that verification.
"""

from sqlalchemy import text

from loom_capacity_agent.native_recovery_publication import (
    NativeRecoveryHostIdentityV1,
    NativeRecoveryProfileV1,
)
from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest


class NativeRecoveryInstallationStore(BuildGuardInstallationStore):
    async def retain_recovery(self, profile: NativeRecoveryProfileV1, *,
        hosts: tuple[NativeRecoveryHostIdentityV1, ...],
    ) -> NativeRecoveryProfileV1:
        """Retain static policy before execution; append historical boot identities."""
        await self._assert_owner()
        profile = NativeRecoveryProfileV1.model_validate_json(profile.model_dump_json())
        if not isinstance(hosts, tuple) or not 1 <= len(hosts) <= 4096:
            raise ValueError("native recovery requires bounded host admission")
        hosts = tuple(NativeRecoveryHostIdentityV1.model_validate_json(host.model_dump_json()) for host in hosts)
        params = {"installation": profile.installation_id, "pool": profile.pool_id}
        wire = canonical_bytes(profile)
        async with self._session.begin_nested():
            installation = await self._session.scalar(text("""SELECT payload FROM loom_capacity_build_guard.installations
                WHERE id=:installation FOR UPDATE"""), params)
            if installation is None:
                raise ValueError("native recovery installation absent")
            pools = [pool for pool in installation["runtime"]["pools"] if pool["pool_id"] == profile.pool_id]
            if (len(pools) != 1 or pools[0]["launch_profile_sha256"] != profile.launch_profile_sha256
                or any(host.node_id not in pools[0]["node_ids"] for host in hosts)):
                raise ValueError("native recovery installed profile or node changed")
            existing = await self._session.scalar(text("""SELECT wire_payload FROM loom_capacity_build_guard.native_recovery_profiles
                WHERE installation_id=:installation AND pool_id=:pool"""), params)
            if existing is not None and bytes(existing) != wire:
                raise ValueError("native recovery static profile changed; new installation required")
            if existing is None:
                if await self._session.scalar(text("""SELECT EXISTS (
                    SELECT 1 FROM loom_capacity_build_guard.execution_events e
                    JOIN loom_capacity_build_guard.bootstraps b ON b.intent_id=e.intent_id
                    WHERE b.installation_id=:installation AND e.pool_id=:pool)
                    OR EXISTS (SELECT 1 FROM loom_capacity_build_guard.worker_registrations
                        WHERE installation_id=:installation AND payload->'binding'->>'pool_id'=:pool)"""), params):
                    raise ValueError("native recovery profile must precede execution")
                await self._session.execute(text("""INSERT INTO loom_capacity_build_guard.native_recovery_profiles
                    (installation_id,pool_id,payload,wire_payload,payload_sha256)
                    VALUES(:installation,:pool,CAST(:payload AS jsonb),:wire,:digest)"""),
                    {**params, "payload": wire.decode("ascii"), "wire": wire, "digest": canonical_digest(profile)})
            for host in hosts:
                host_wire = canonical_bytes(host)
                await self._session.execute(text("""INSERT INTO loom_capacity_build_guard.native_recovery_hosts
                    (installation_id,pool_id,payload,wire_payload,payload_sha256)
                    VALUES(:installation,:pool,CAST(:payload AS jsonb),:wire,:digest) ON CONFLICT DO NOTHING"""),
                    {**params, "payload": host_wire.decode("ascii"), "wire": host_wire, "digest": canonical_digest(host)})
                stored = await self._session.scalar(text("""SELECT wire_payload FROM loom_capacity_build_guard.native_recovery_hosts
                    WHERE installation_id=:installation AND pool_id=:pool AND payload_sha256=:digest"""),
                    {**params, "digest": canonical_digest(host)})
                if stored is None or bytes(stored) != host_wire:
                    raise ValueError("native recovery host replay changed")
        return profile
