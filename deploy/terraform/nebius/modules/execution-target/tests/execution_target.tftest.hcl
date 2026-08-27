mock_provider "nebius" {}

variables {
  tenant_id                  = "tenant-e00test"
  project_id                 = "project-e00test"
  target_id                  = "nebius-eu-north1-development"
  environment                = "development"
  region                     = "eu-north1"
  failure_domain             = "development-eu-north1"
  namespace_name             = "loom-nebius-development"
  network_cidr               = "10.0.0.0/16"
  service_cidr               = "172.20.0.0/16"
  control_plane_etcd_size    = 1
  public_control_plane_cidrs = []
  system_node_count          = 1
  execution_min_nodes        = 0
  execution_max_nodes        = 1
  evidence_bucket_name       = "loom-nebius-test-evidence"
}

run "development_private_payg_plan" {
  command = plan

  assert {
    condition     = nebius_mk8s_v1_cluster.target.control_plane.endpoints.public_endpoint == null
    error_message = "Development control plane must remain private unless CIDRs are explicit."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.autoscaling.min_node_count == 0 && nebius_mk8s_v1_node_group.execution.autoscaling.max_node_count == 1
    error_message = "Development execution autoscaling must remain bounded to 0-1."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.template.reservation_policy.policy == "FORBID"
    error_message = "Execution nodes must use regular PAYG capacity only."
  }

  assert {
    condition     = nebius_mk8s_v1_node_group.execution.template.taints[0].key == "loom.nebius/execution"
    error_message = "Execution nodes must be isolated by the accepted taint."
  }
}

run "public_control_plane_is_cidr_bounded" {
  command = plan

  variables {
    public_control_plane_cidrs = ["203.0.113.9/32"]
  }

  assert {
    condition     = length(nebius_mk8s_v1_cluster.target.control_plane.endpoints.public_endpoint.allowed_cidrs) == 1 && one(nebius_mk8s_v1_cluster.target.control_plane.endpoints.public_endpoint.allowed_cidrs) == "203.0.113.9/32"
    error_message = "A public control plane must preserve the exact approved CIDR allowlist."
  }
}

run "production_rejects_zero_warm_execution" {
  command = plan

  variables {
    target_id               = "nebius-eu-north1-production"
    environment             = "production"
    failure_domain          = "production-eu-north1"
    namespace_name          = "loom-nebius-production-north"
    control_plane_etcd_size = 3
    execution_min_nodes     = 0
    execution_max_nodes     = 50
  }

  expect_failures = [terraform_data.contract]
}
