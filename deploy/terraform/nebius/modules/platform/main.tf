# Opt-in full-platform integration resources. Keep the original execution
# target/state intact while the independent candidate is accepted.
variable "integration_platform" {
  description = "Independent pure Nebius integration footprint; null creates nothing."
  type = object({
    bucket_prefix       = string
    system_preset       = optional(string, "4vcpu-16gb")
    execution_max_nodes = optional(number, 2)
    ci_max_nodes        = optional(number, 2)
  })
  default = null
  validation {
    condition = var.integration_platform == null ? true : (
      can(regex("^[a-z0-9][a-z0-9-]{5,45}$", var.integration_platform.bucket_prefix)) &&
      contains(["4vcpu-16gb", "8vcpu-32gb"], var.integration_platform.system_preset) &&
      var.integration_platform.execution_max_nodes >= 1 && var.integration_platform.execution_max_nodes <= 8 &&
      var.integration_platform.ci_max_nodes >= 1 && var.integration_platform.ci_max_nodes <= 4
    )
    error_message = "Integration needs a unique bucket prefix and bounded CPU-only node groups."
  }
}

locals {
  integration = var.integration_platform == null ? {} : { integration = var.integration_platform }
  integration_labels = merge(var.labels, {
    "loom-purpose" = "nebius-integration"
  })
  integration_stores = var.integration_platform == null ? {} : {
    artifacts    = { identity = "canonical", versioning = "ENABLED" }
    trajectories = { identity = "canonical", versioning = "ENABLED" }
    source       = { identity = "source", versioning = "DISABLED" }
    backups      = { identity = "backup", versioning = "ENABLED" }
  }
  integration_identities = var.integration_platform == null ? toset([]) : toset(["canonical", "source", "backup"])
  integration_nodes = var.integration_platform == null ? {} : {
    system    = { preset = var.integration_platform.system_preset, minimum = 1, maximum = 1, disk = 80 }
    execution = { preset = "16vcpu-64gb", minimum = 0, maximum = var.integration_platform.execution_max_nodes, disk = 80 }
    ci        = { preset = "8vcpu-32gb", minimum = 0, maximum = var.integration_platform.ci_max_nodes, disk = 160 }
    release   = { preset = "8vcpu-32gb", minimum = 0, maximum = 1, disk = 160 }
  }
}

resource "nebius_vpc_v1_allocation" "integration_public" {
  for_each  = local.integration
  parent_id = var.project_id
  name      = "loom-nebius-integration-https"
  labels    = local.integration_labels
  ipv4_public = {
    pool_id = var.public_pool_id
    cidr    = "/32"
  }
}

resource "nebius_iam_v1_service_account" "integration_store" {
  for_each    = local.integration_identities
  parent_id   = var.project_id
  name        = "loom-nebius-integration-${each.key}"
  description = "Only integration ${each.key} objects, no project access"
  labels      = local.integration_labels
}

resource "nebius_iam_v1_group" "integration_store" {
  for_each  = local.integration_identities
  parent_id = var.tenant_id
  name      = "loom-nebius-integration-${each.key}"
  labels    = local.integration_labels
}

resource "nebius_iam_v1_group_membership" "integration_store" {
  for_each  = local.integration_identities
  parent_id = nebius_iam_v1_group.integration_store[each.key].id
  member_id = nebius_iam_v1_service_account.integration_store[each.key].id
  labels    = local.integration_labels
}

resource "nebius_storage_v1_bucket" "integration" {
  for_each              = local.integration_stores
  parent_id             = var.project_id
  name                  = "${var.integration_platform.bucket_prefix}-${each.key}"
  labels                = local.integration_labels
  default_storage_class = "STANDARD"
  force_storage_class   = true
  max_size_bytes        = 0
  object_audit_logging  = "ALL"
  versioning_policy     = each.value.versioning
  bucket_policy = {
    rules = [{
      group_id = nebius_iam_v1_group.integration_store[each.value.identity].id
      paths    = ["*"]
      roles    = ["storage.object-editor"]
    }]
  }
  lifecycle_configuration = {
    rules = [{
      id                                = "abort-incomplete-uploads"
      status                            = "ENABLED"
      abort_incomplete_multipart_upload = { days_after_initiation = 7 }
    }]
  }
  lifecycle { prevent_destroy = true }
}

resource "nebius_iam_v2_access_key" "integration_store" {
  for_each    = local.integration_identities
  parent_id   = var.project_id
  name        = "loom-nebius-integration-${each.key}"
  description = "Integration ${each.key} object credential, retrieve outside Terraform state"
  labels      = local.integration_labels
  account = {
    service_account = { id = nebius_iam_v1_service_account.integration_store[each.key].id }
  }
  secret_delivery_mode = "EXPLICIT"
}

# Isolate privileged CI build containers from application and execution nodes.
# All nodes retain registry-read-only cloud identity. Release credentials are
# job-scoped, never attached to a node or supplied to PR jobs.
resource "nebius_mk8s_v1_node_group" "integration" {
  for_each         = local.integration_nodes
  parent_id        = var.cluster_id
  name             = "loom-nebius-integration-${each.key}"
  labels           = local.integration_labels
  version          = var.kubernetes_version
  auto_repair      = {}
  fixed_node_count = each.key == "system" ? 1 : null
  autoscaling = each.key == "system" ? null : {
    min_node_count = each.value.minimum
    max_node_count = each.value.maximum
  }
  strategy = {
    drain_timeout   = "20m"
    max_surge       = { count = 0 }
    max_unavailable = { count = 1 }
  }
  template = {
    service_account_id = var.node_registry_pull_service_account_id
    max_pods           = 64
    metadata = {
      labels = {
        "loom.nebius/node-role"        = each.key == "execution" ? "integration-execution" : each.key
        "loom.nebius/platform"         = "integration"
        "loom.nebius/cluster-scope-id" = var.cluster_scope_id
      }
    }
    taints = concat([
      { key = "loom.nebius/platform", value = "integration", effect = "NO_SCHEDULE" }
      ], each.key == "execution" ? [
      { key = "loom.nebius/execution", value = "true", effect = "NO_SCHEDULE" }
      ] : each.key == "system" ? [] : [
      { key = "loom.nebius/runner", value = each.key, effect = "NO_SCHEDULE" }
    ])
    boot_disk          = { type = "NETWORK_SSD", size_gibibytes = each.value.disk }
    network_interfaces = [{ subnet_id = var.subnet_id }]
    resources          = { platform = "cpu-e2", preset = each.value.preset }
    reservation_policy = { policy = "FORBID" }
  }
}

output "integration_platform" {
  description = "Non-secret independent integration identities and storage credential references."
  value = var.integration_platform == null ? null : {
    cluster_id           = var.cluster_id
    public_allocation_id = nebius_vpc_v1_allocation.integration_public["integration"].id
    public_cidr          = nebius_vpc_v1_allocation.integration_public["integration"].status.details.allocated_cidr
    registry             = var.registry_fqdn
    storage_endpoint     = "https://storage.${var.region}.nebius.cloud"
    buckets              = { for name, resource in nebius_storage_v1_bucket.integration : name => resource.name }
    storage_credentials = {
      for name, resource in nebius_iam_v2_access_key.integration_store : name => {
        access_key_resource_id = resource.id
        service_account_id     = nebius_iam_v1_service_account.integration_store[name].id
      }
    }
    node_groups = { for name, resource in nebius_mk8s_v1_node_group.integration : name => resource.id }
  }
}
