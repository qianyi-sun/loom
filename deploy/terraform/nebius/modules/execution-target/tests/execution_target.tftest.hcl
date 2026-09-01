mock_provider "nebius" {}

variables {
  tenant_id        = "tenant-e00test"
  project_id       = "project-e00test"
  target_id        = "nebius-eu-north1-development"
  cluster_scope_id = "nebius-eu-north1-shared"
  region           = "eu-north1"
  failure_domain   = "nebius-eu-north1-shared"
  environment_bindings = {
    development = {
      target_id       = "nebius-eu-north1-development"
      namespace_name  = "loom-nebius-development"
      evidence_prefix = "nebius-eu-north1-development"
    }
    staging = {
      target_id       = "nebius-eu-north1-staging"
      namespace_name  = "loom-nebius-staging"
      evidence_prefix = "nebius-eu-north1-staging"
    }
    production = {
      target_id       = "nebius-eu-north1-production"
      namespace_name  = "loom-nebius-production"
      evidence_prefix = "nebius-eu-north1-production"
    }
  }
  network_cidr                     = "10.0.0.0/16"
  service_cidr                     = "172.20.0.0/16"
  control_plane_etcd_size          = 1
  public_control_plane_cidrs       = []
  system_node_count                = 1
  execution_min_nodes              = 0
  execution_max_nodes              = 1
  evidence_bucket_name             = "loom-nebius-test-evidence"
  capacity_observer_public_key_pem = <<-EOT
    -----BEGIN PUBLIC KEY-----
    dGVzdA==
    -----END PUBLIC KEY-----
  EOT
}

run "development_private_payg_plan" {
  command = plan

  assert {
    condition = toset([
      for block in nebius_vpc_v1_pool.target_private.cidrs : block.cidr
    ]) == toset([var.network_cidr, var.service_cidr])
    error_message = "The inherited private pool must contain both node/pod and Kubernetes service CIDRs."
  }

  assert {
    condition = alltrue([
      for block in nebius_vpc_v1_pool.target_private.cidrs : block.max_mask_length == 32
    ])
    error_message = "The inherited private pool must permit the /32 allocations required by the control plane and nodes."
  }

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


  assert {
    condition     = nebius_iam_v1_auth_public_key.capacity_observer.expires_at == null
    error_message = "The recurring capacity observer key must not acquire a calendar expiry."
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

run "shared_cluster_rejects_missing_environment_binding" {
  command = plan

  variables {
    environment_bindings = {
      development = {
        target_id       = "nebius-eu-north1-development"
        namespace_name  = "loom-nebius-development"
        evidence_prefix = "nebius-eu-north1-development"
      }
      staging = {
        target_id       = "nebius-eu-north1-staging"
        namespace_name  = "loom-nebius-staging"
        evidence_prefix = "nebius-eu-north1-staging"
      }
    }
  }

  expect_failures = [var.environment_bindings]
}
