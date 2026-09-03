"""Read-only Nebius quota and Managed Kubernetes node-group adapter."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Literal

from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings
from loom_execution_capacity_collector.contracts import (
    CapacityPolicyBinding,
    ProviderCapacitySnapshot,
)
from loom_execution_capacity_collector.control_plane import read_owner_only_secret

_MIB = Decimal(1024 * 1024)


class NebiusObservationError(RuntimeError):
    """Nebius did not return a complete, internally consistent readback."""


@dataclass(frozen=True)
class _QuotaBinding:
    name: str
    expected_unit: str
    kind: Literal["count", "vcpu", "bytes"]


def _required_string(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NebiusObservationError(f"Nebius {name} is unavailable")
    return value.strip()


def _required_int(value: object, *, name: str, positive: bool = False) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < (1 if positive else 0):
        qualifier = "positive" if positive else "non-negative"
        raise NebiusObservationError(f"Nebius {name} is not a {qualifier} integer")
    return value


def _enum_name(value: object) -> str:
    name = getattr(value, "name", None)
    return str(name) if isinstance(name, str) else ""


def _quota_value(value: int, *, kind: str, usage: bool) -> int:
    if kind == "count":
        return value
    if kind == "vcpu":
        return value * 1000
    scaled = Decimal(value) / _MIB
    rounding = ROUND_CEILING if usage else ROUND_FLOOR
    return int(scaled.to_integral_value(rounding=rounding))


class NebiusCapacityReader:
    """Capture quota allowances and one exact node group without mutating Nebius."""

    def __init__(
        self,
        settings: ExecutionCapacityCollectorSettings,
        *,
        sdk: Any | None = None,
        quota_client: Any | None = None,
        node_group_client: Any | None = None,
    ) -> None:
        self._settings = settings
        self._owns_sdk = sdk is None
        if sdk is None:
            # Validate the projected copy before the SDK parses it. The content is
            # deliberately discarded and never logged or persisted by the collector.
            read_owner_only_secret(settings.nebius_credentials_file, maximum_bytes=1024 * 1024)
            try:
                from nebius.sdk import SDK
            except ModuleNotFoundError as exc:
                raise RuntimeError("install Loom with the cluster extra") from exc
            sdk = SDK(
                credentials_file_name=str(settings.nebius_credentials_file),
                user_agent_prefix="loom-execution-capacity-collector/1.0",
            )
        self._sdk = sdk
        if quota_client is None or node_group_client is None:
            if quota_client is not None or node_group_client is not None:
                raise ValueError("Nebius quota and node-group clients must be injected together")
            from nebius.api.nebius.mk8s.v1 import (
                NodeGroupServiceClient,
            )
            from nebius.api.nebius.quotas.v1 import (
                QuotaAllowanceServiceClient,
            )

            quota_client = QuotaAllowanceServiceClient(sdk)
            node_group_client = NodeGroupServiceClient(sdk)
        self._quotas = quota_client
        self._node_groups = node_group_client

    async def _list_quotas(self) -> list[Any]:
        from nebius.api.nebius.quotas.v1 import (
            ListQuotaAllowancesRequest,
        )

        rows: list[Any] = []
        page_token = ""
        seen_tokens: set[str] = set()
        while True:
            response = await self._quotas.list(
                ListQuotaAllowancesRequest(
                    parent_id=(
                        self._settings.nebius_quota_parent_id
                        or self._settings.nebius_project_id
                    ),
                    page_size=1000,
                    page_token=page_token,
                ),
                timeout=self._settings.request_timeout_seconds,
                per_retry_timeout=min(10.0, self._settings.request_timeout_seconds),
            )
            rows.extend(list(response.items))
            next_token = str(response.next_page_token or "")
            if not next_token:
                return rows
            if next_token in seen_tokens:
                raise NebiusObservationError("Nebius quota pagination repeated a token")
            seen_tokens.add(next_token)
            page_token = next_token

    def _select_quota(self, rows: list[Any], binding: _QuotaBinding) -> tuple[int, int, str]:
        matches = [
            row
            for row in rows
            if getattr(getattr(row, "metadata", None), "name", None) == binding.name
            and getattr(getattr(row, "spec", None), "region", None) == self._settings.nebius_region
        ]
        if len(matches) != 1:
            raise NebiusObservationError(f"Nebius quota {binding.name!r} is missing or ambiguous")
        row = matches[0]
        metadata = row.metadata
        spec = row.spec
        status = row.status
        if _enum_name(status.state) != "STATE_ACTIVE":
            raise NebiusObservationError(f"Nebius quota {binding.name!r} is not active")
        if _enum_name(status.usage_state) not in {
            "USAGE_STATE_USED",
            "USAGE_STATE_NOT_USED",
        }:
            raise NebiusObservationError(f"Nebius quota {binding.name!r} usage is unknown")
        service = _required_string(status.service, name=f"quota {binding.name} service")
        if service != self._settings.quota_service:
            raise NebiusObservationError(f"Nebius quota {binding.name!r} service does not match")
        unit = _required_string(status.unit, name=f"quota {binding.name} unit")
        if unit != binding.expected_unit:
            raise NebiusObservationError(f"Nebius quota {binding.name!r} unit does not match")
        limit = _required_int(spec.limit, name=f"quota {binding.name} limit", positive=True)
        usage = _required_int(status.usage, name=f"quota {binding.name} usage")
        version = _required_int(
            metadata.resource_version,
            name=f"quota {binding.name} resource version",
            positive=True,
        )
        return (
            _quota_value(limit, kind=binding.kind, usage=False),
            _quota_value(usage, kind=binding.kind, usage=True),
            str(version),
        )

    async def capture(self, policy: CapacityPolicyBinding) -> ProviderCapacitySnapshot:
        if not policy.enabled:
            raise NebiusObservationError("capacity policy is disabled")
        from nebius.api.nebius.mk8s.v1 import (
            GetNodeGroupRequest,
        )

        quotas = await self._list_quotas()
        bindings: dict[str, _QuotaBinding] = {
            "nodes": _QuotaBinding(
                self._settings.quota_nodes_name,
                self._settings.quota_nodes_unit,
                "count",
            ),
            "vcpu": _QuotaBinding(
                self._settings.quota_vcpu_name,
                self._settings.quota_vcpu_unit,
                "vcpu",
            ),
            "storage": _QuotaBinding(
                self._settings.quota_storage_name,
                self._settings.quota_storage_unit,
                "bytes",
            ),
        }
        if (
            self._settings.quota_memory_name is not None
            and self._settings.quota_memory_unit is not None
        ):
            bindings["memory"] = _QuotaBinding(
                self._settings.quota_memory_name,
                self._settings.quota_memory_unit,
                "bytes",
            )
        values = {name: self._select_quota(quotas, binding) for name, binding in bindings.items()}
        node_group = await self._node_groups.get(
            GetNodeGroupRequest(id=self._settings.nebius_node_group_id),
            timeout=self._settings.request_timeout_seconds,
            per_retry_timeout=min(10.0, self._settings.request_timeout_seconds),
        )
        metadata = node_group.metadata
        if metadata.id != self._settings.nebius_node_group_id:
            raise NebiusObservationError("Nebius node group identity does not match")
        status = node_group.status
        spec = node_group.spec
        node_count = _required_int(status.node_count, name="node group node count")
        target_count = _required_int(status.target_node_count, name="node group target node count")
        ready_count = _required_int(status.ready_node_count, name="node group ready node count")
        if ready_count > node_count:
            raise NebiusObservationError("Nebius node group counts are inconsistent")
        autoscaling = spec.autoscaling
        if autoscaling is None:
            raise NebiusObservationError("Nebius node group autoscaling is unavailable")
        max_nodes = _required_int(
            autoscaling.max_node_count,
            name="node group autoscaling maximum",
            positive=True,
        )
        if max_nodes < policy.max_nodes:
            raise NebiusObservationError("Nebius node group maximum is below capacity policy")
        error_codes = sorted(
            {
                _required_string(event.last_occurrence.code, name="node group event code")
                for event in list(status.events)
                if _enum_name(event.last_occurrence.level) == "ERROR"
            }
        )
        state = _enum_name(status.state)
        converged = (
            state == "RUNNING"
            and not bool(status.reconciling)
            and node_count == target_count == ready_count
        )
        if error_codes and not converged:
            provider_state: Literal["available", "insufficient", "unknown"] = "insufficient"
            provider_reason = "node_group_error:" + ",".join(error_codes[:10])
            autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"] = "stalled"
            autoscaler_reason = provider_reason
        elif state == "RUNNING":
            provider_state = "available"
            provider_reason = (
                "node_group_converged_with_historical_error_events"
                if error_codes
                else "node_group_running_without_error_events"
            )
            if bool(status.reconciling) or ready_count != target_count:
                autoscaler_state = "scaling"
                autoscaler_reason = "node_group_reconciling"
            else:
                autoscaler_state = "ready"
                autoscaler_reason = "node_group_ready_at_target"
        elif state in {"PROVISIONING", "DELETING"}:
            provider_state = "unknown"
            provider_reason = f"node_group_state:{state.lower()}"
            autoscaler_state = "scaling" if state == "PROVISIONING" else "stalled"
            autoscaler_reason = provider_reason
        else:
            provider_state = "unknown"
            provider_reason = "node_group_state:unknown"
            autoscaler_state = "unknown"
            autoscaler_reason = provider_reason
        node_group_version = _required_int(
            metadata.resource_version,
            name="node group resource version",
            positive=True,
        )
        if "memory" in values:
            quota_memory_mib = values["memory"][0]
            used_memory_mib = values["memory"][1]
            memory_source_version = values["memory"][2]
        else:
            # Nebius currently exposes no memory quota allowance for regular
            # CPU instances. Memory is coupled to the immutable node preset,
            # so the accepted policy ceiling and exact node-group count are
            # the authoritative limit/usage fallback rather than a fabricated
            # provider quota row.
            quota_memory_mib = policy.max_nodes * policy.node_memory_mib
            used_memory_mib = node_count * policy.node_memory_mib
            memory_source_version = (
                f"derived:policy-{policy.version}:node-group-{node_group_version}"
            )
        return ProviderCapacitySnapshot(
            source_versions={
                "node_group": str(node_group_version),
                **{f"quota_{name}": value[2] for name, value in values.items()},
                "quota_memory": memory_source_version,
            },
            provider_capacity_state=provider_state,
            provider_capacity_reason=provider_reason,
            autoscaler_state=autoscaler_state,
            autoscaler_reason=autoscaler_reason,
            quota_nodes=values["nodes"][0],
            quota_vcpu_millis=values["vcpu"][0],
            quota_memory_mib=quota_memory_mib,
            quota_storage_mib=values["storage"][0],
            used_nodes=values["nodes"][1],
            used_vcpu_millis=values["vcpu"][1],
            used_memory_mib=used_memory_mib,
            used_storage_mib=values["storage"][1],
            node_count=node_count,
            target_node_count=target_count,
            ready_node_count=ready_count,
        )

    async def close(self) -> None:
        if self._owns_sdk:
            await self._sdk.close()


__all__ = ["NebiusCapacityReader", "NebiusObservationError"]
