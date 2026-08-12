from loom.workload_trust import INTERNAL_TRUSTED, WorkloadTrustContract


def test_internal_trusted_boundary_disables_untrusted_transforms() -> None:
    contract = WorkloadTrustContract(
        workload_trust_mode=INTERNAL_TRUSTED,
        taskset_transforms_enabled=False,
        taskset_transform_network_isolated=False,
        untrusted_workload_isolation=False,
    )
    assert contract.v1_violations() == []
    assert contract.as_manifest()["workload_trust_mode"] == "internal_trusted"
