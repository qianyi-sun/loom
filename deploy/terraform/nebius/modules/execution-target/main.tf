locals {
  resource_prefix = replace(var.target_id, "nebius-", "loom-")
  common_labels = merge(var.labels, {
    "loom-target"      = var.target_id
    "loom-environment" = var.environment
    "loom-region"      = var.region
    "loom-managed-by"  = "terraform"
  })
}

resource "terraform_data" "contract" {
  input = {
    target_id      = var.target_id
    environment    = var.environment
    region         = var.region
    failure_domain = var.failure_domain
    namespace_name = var.namespace_name
  }

  lifecycle {
    precondition {
      condition     = strcontains(var.target_id, var.region) && endswith(var.target_id, var.environment)
      error_message = "target_id must encode the exact region and environment."
    }

    precondition {
      condition     = var.execution_min_nodes <= var.execution_max_nodes
      error_message = "execution_min_nodes cannot exceed execution_max_nodes."
    }

    precondition {
      condition     = var.environment != "production" || var.control_plane_etcd_size == 3
      error_message = "Production targets require a three-member HA control plane."
    }

    precondition {
      condition     = var.environment != "production" || var.execution_min_nodes >= 1
      error_message = "Production targets require at least one warm execution node."
    }

  }
}

resource "nebius_vpc_v1_pool" "target_private" {
  parent_id  = var.project_id
  name       = "${local.resource_prefix}-private-pool"
  version    = "IPV4"
  visibility = "PRIVATE"
  labels     = local.common_labels

  # Managed Kubernetes reserves service_cidr from the control-plane subnet.
  # The pool must also permit /32 allocations for control-plane and node IPs;
  # /28 would leave the cluster operation stuck before its first allocation.
  cidrs = [
    {
      cidr            = var.network_cidr
      max_mask_length = 32
      state           = "AVAILABLE"
    },
    {
      cidr            = var.service_cidr
      max_mask_length = 32
      state           = "AVAILABLE"
    },
  ]

  depends_on = [terraform_data.contract]
}

resource "nebius_vpc_v1_network" "target" {
  parent_id = var.project_id
  name      = "${local.resource_prefix}-network"
  labels    = local.common_labels

  ipv4_private_pools = {
    pools = [{
      id = nebius_vpc_v1_pool.target_private.id
    }]
  }

  depends_on = [terraform_data.contract]
}

resource "nebius_vpc_v1_subnet" "target" {
  parent_id  = var.project_id
  name       = "${local.resource_prefix}-private"
  network_id = nebius_vpc_v1_network.target.id
  labels     = local.common_labels

  ipv4_private_pools = {
    use_network_pools = true
  }

  # Explicitly deny allocations from public pools. Nodes still use the
  # provider's shared dynamic egress gateway for outbound public traffic.
  ipv4_public_pools = {
    use_network_pools = false
  }
}

resource "nebius_registry_v1_registry" "target" {
  parent_id   = var.project_id
  name        = "${local.resource_prefix}-images"
  description = "Digest-pinned Loom images for ${var.target_id}"
  labels      = local.common_labels
}

resource "nebius_iam_v1_service_account" "node_registry_pull" {
  parent_id   = var.project_id
  name        = "${local.resource_prefix}-node-registry-pull"
  description = "Node-only image pull identity for ${var.target_id}"
  labels      = local.common_labels
}

resource "nebius_iam_v1_group" "node_registry_pull" {
  parent_id = var.tenant_id
  name      = "${local.resource_prefix}-node-registry-pull"
  labels    = local.common_labels
}

resource "nebius_iam_v1_group_membership" "node_registry_pull" {
  parent_id = nebius_iam_v1_group.node_registry_pull.id
  member_id = nebius_iam_v1_service_account.node_registry_pull.id
  labels    = local.common_labels
}

resource "nebius_iam_v1_access_permit" "node_registry_pull" {
  parent_id   = nebius_iam_v1_group.node_registry_pull.id
  resource_id = nebius_registry_v1_registry.target.id
  role        = "viewer"
  labels      = local.common_labels
}

resource "nebius_iam_v1_group" "evidence_writers" {
  parent_id = var.tenant_id
  name      = "${local.resource_prefix}-evidence-writers"
  labels    = local.common_labels
}

resource "nebius_storage_v1_bucket" "evidence" {
  parent_id             = var.project_id
  name                  = var.evidence_bucket_name
  labels                = local.common_labels
  default_storage_class = "STANDARD"
  force_storage_class   = true
  max_size_bytes        = var.evidence_bucket_max_bytes
  object_audit_logging  = "ALL"
  versioning_policy     = "ENABLED"

  bucket_policy = {
    rules = [{
      group_id = nebius_iam_v1_group.evidence_writers.id
      paths    = ["${var.target_id}/*"]
      roles    = ["storage.object-editor"]
    }]
  }

  lifecycle_configuration = {
    rules = [{
      id     = "abort-incomplete-uploads"
      status = "ENABLED"
      abort_incomplete_multipart_upload = {
        days_after_initiation = 7
      }
    }]
  }
}

resource "nebius_mk8s_v1_cluster" "target" {
  parent_id = var.project_id
  name      = "${local.resource_prefix}-cluster"
  labels    = local.common_labels

  control_plane = {
    subnet_id         = nebius_vpc_v1_subnet.target.id
    version           = var.kubernetes_version
    etcd_cluster_size = var.control_plane_etcd_size
    audit_logs        = {}
    endpoints = length(var.public_control_plane_cidrs) == 0 ? {} : {
      public_endpoint = {
        allowed_cidrs = var.public_control_plane_cidrs
      }
    }
  }

  kube_network = {
    service_cidrs = [var.service_cidr]
  }
}

resource "nebius_mk8s_v1_node_group" "system" {
  parent_id        = nebius_mk8s_v1_cluster.target.id
  name             = "${local.resource_prefix}-system"
  labels           = local.common_labels
  version          = var.kubernetes_version
  fixed_node_count = var.system_node_count
  auto_repair      = {}

  strategy = {
    drain_timeout = "10m"
    max_surge = {
      count = 1
    }
    max_unavailable = {
      count = 0
    }
  }

  template = {
    service_account_id = nebius_iam_v1_service_account.node_registry_pull.id
    max_pods           = 32
    metadata = {
      labels = {
        "loom.nebius/node-role"   = "system"
        "loom.nebius/target-id"   = var.target_id
        "loom.nebius/environment" = var.environment
      }
    }
    boot_disk = {
      type           = "NETWORK_SSD"
      size_gibibytes = var.system_disk_gib
    }
    network_interfaces = [{
      subnet_id = nebius_vpc_v1_subnet.target.id
    }]
    resources = {
      platform = "cpu-e2"
      preset   = var.system_preset
    }
    reservation_policy = {
      policy = "FORBID"
    }
  }

  depends_on = [nebius_iam_v1_access_permit.node_registry_pull]
}

resource "nebius_mk8s_v1_node_group" "execution" {
  parent_id   = nebius_mk8s_v1_cluster.target.id
  name        = "${local.resource_prefix}-execution"
  labels      = local.common_labels
  version     = var.kubernetes_version
  auto_repair = {}

  autoscaling = {
    min_node_count = var.execution_min_nodes
    max_node_count = var.execution_max_nodes
  }

  strategy = {
    drain_timeout = "20m"
    max_surge = {
      count = 1
    }
    max_unavailable = {
      count = 0
    }
  }

  template = {
    service_account_id = nebius_iam_v1_service_account.node_registry_pull.id
    max_pods           = 16
    metadata = {
      labels = {
        "loom.nebius/node-role"   = "execution"
        "loom.nebius/target-id"   = var.target_id
        "loom.nebius/environment" = var.environment
      }
    }
    taints = [{
      key    = "loom.nebius/execution"
      value  = "true"
      effect = "NO_SCHEDULE"
    }]
    boot_disk = {
      type           = "NETWORK_SSD"
      size_gibibytes = var.execution_disk_gib
    }
    network_interfaces = [{
      subnet_id = nebius_vpc_v1_subnet.target.id
    }]
    resources = {
      platform = "cpu-e2"
      preset   = var.execution_preset
    }
    reservation_policy = {
      policy = "FORBID"
    }
  }

  depends_on = [
    nebius_iam_v1_access_permit.node_registry_pull,
    nebius_mk8s_v1_node_group.system,
  ]
}
