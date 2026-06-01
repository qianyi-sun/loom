"""Harbor integration adapters for platform benchmark execution and ingestion."""

from agentic_data_platform.harbor.agent_adapters import (
    HarborAgentModelInvocation,
    HarborAgentModelAdapterSpec,
    adapter_for_agent,
    build_agent_model_env,
    build_agent_model_invocation,
    mainstream_adapter_specs,
    provider_dialect_gap,
    provider_endpoint_dialects,
)
from agentic_data_platform.harbor.agent_provider import HarborAgentProvider, HarborBuiltInAgentSpec
from agentic_data_platform.harbor.capabilities import HarborNativeCapabilityReport, probe_harbor_native_capabilities
from agentic_data_platform.harbor.benchmark_provider import HarborBenchmarkProvider, HarborDatasetCatalogSpec
from agentic_data_platform.harbor.ingestion import HarborIngestionResult, HarborResultIngestor
from agentic_data_platform.harbor.registry_sync import (
    HarborRegistrySyncResult,
    list_registry_dataset_catalogs,
    sync_harbor_registry_catalogs,
)
from agentic_data_platform.harbor.runner import HarborCliRunnerBackend, HarborRunSpec, HarborRunnerBackend, HarborRunnerResult

__all__ = [
    "HarborAgentProvider",
    "HarborAgentModelInvocation",
    "HarborAgentModelAdapterSpec",
    "HarborBuiltInAgentSpec",
    "HarborCliRunnerBackend",
    "HarborBenchmarkProvider",
    "HarborDatasetCatalogSpec",
    "HarborIngestionResult",
    "HarborNativeCapabilityReport",
    "HarborRegistrySyncResult",
    "HarborResultIngestor",
    "HarborRunSpec",
    "HarborRunnerBackend",
    "HarborRunnerResult",
    "adapter_for_agent",
    "build_agent_model_env",
    "build_agent_model_invocation",
    "list_registry_dataset_catalogs",
    "mainstream_adapter_specs",
    "provider_dialect_gap",
    "provider_endpoint_dialects",
    "probe_harbor_native_capabilities",
    "sync_harbor_registry_catalogs",
]
