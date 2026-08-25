from prometheus_client import Counter, Gauge, Histogram

KUBERNETES_API_SECONDS = Histogram(
    "loom_execution_actuator_kubernetes_api_seconds",
    "Kubernetes API latency by bounded operation.",
    labelnames=("operation",),
)
KUBERNETES_API_ERRORS_TOTAL = Counter(
    "loom_execution_actuator_kubernetes_api_errors_total",
    "Kubernetes API errors by bounded operation/status class.",
    labelnames=("operation", "status_class"),
)
KUBERNETES_PENDING_TOTAL = Counter(
    "loom_execution_actuator_pending_reasons_total",
    "Observed pending/failure reasons by normalized bounded reason.",
    labelnames=("reason",),
)
KUBERNETES_ORPHAN_COUNT = Gauge(
    "loom_execution_actuator_orphan_count",
    "Managed-scope Jobs without exactly one matching durable lease.",
)
KUBERNETES_CLEANUP_RETRIES_TOTAL = Counter(
    "loom_execution_actuator_cleanup_retries_total",
    "Cleanup API retries by bounded cause.",
    labelnames=("cause",),
)
KUBERNETES_RECONCILE_CONVERGED = Gauge(
    "loom_execution_actuator_reconcile_converged",
    "Whether the last full reconciliation found no drift.",
)
KUBERNETES_WATCH_RESTARTS_TOTAL = Counter(
    "loom_execution_actuator_watch_restarts_total",
    "Watch restarts by bounded outcome.",
    labelnames=("outcome",),
)
