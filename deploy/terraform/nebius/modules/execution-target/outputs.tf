output "target" {
  description = "Non-secret shared-cluster identity plus its environment-local bindings."
  value = {
    state_anchor_id      = var.target_id
    cluster_scope_id     = var.cluster_scope_id
    region               = var.region
    failure_domain       = var.failure_domain
    environment_bindings = var.environment_bindings
    project_id           = var.project_id
  }
}

output "network" {
  value = {
    network_id = nebius_vpc_v1_network.target.id
    subnet_id  = nebius_vpc_v1_subnet.target.id
  }
}

output "cluster" {
  value = {
    id               = nebius_mk8s_v1_cluster.target.id
    version          = nebius_mk8s_v1_cluster.target.status.control_plane.version
    private_endpoint = nebius_mk8s_v1_cluster.target.status.control_plane.endpoints.private_endpoint
    public_endpoint  = nebius_mk8s_v1_cluster.target.status.control_plane.endpoints.public_endpoint
  }
}

output "node_groups" {
  value = {
    system = {
      id            = nebius_mk8s_v1_node_group.system.id
      desired_nodes = var.system_node_count
    }
    execution = {
      id        = nebius_mk8s_v1_node_group.execution.id
      min_nodes = var.execution_min_nodes
      max_nodes = var.execution_max_nodes
    }
  }
}

output "registry" {
  value = {
    id   = nebius_registry_v1_registry.target.id
    fqdn = nebius_registry_v1_registry.target.status.registry_fqdn
  }
}

output "evidence" {
  value = {
    bucket_name = nebius_storage_v1_bucket.evidence.name
    writer_group_ids = {
      for environment, group in nebius_iam_v1_group.evidence_writers :
      environment => group.id
    }
    prefixes = {
      for environment, binding in var.environment_bindings :
      environment => "${binding.evidence_prefix}/"
    }
  }
}

output "node_registry_pull_service_account_id" {
  value = nebius_iam_v1_service_account.node_registry_pull.id
}

output "capacity_observer_service_account_id" {
  description = "Least-privilege Nebius identity used by the recurring capacity collector."
  value       = nebius_iam_v1_service_account.capacity_observer.id
}

output "capacity_observer_public_key_id" {
  description = "Non-expiring authorized public key used by the recurring capacity collector."
  value       = nebius_iam_v1_auth_public_key.capacity_observer.id
}
