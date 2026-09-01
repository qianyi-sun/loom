module "execution_target" {
  source = "../modules/execution-target"

  tenant_id                        = var.tenant_id
  project_id                       = var.project_id
  target_id                        = var.target.target_id
  cluster_scope_id                 = var.target.cluster_scope_id
  region                           = var.target.region
  failure_domain                   = var.target.failure_domain
  environment_bindings             = var.target.environment_bindings
  network_cidr                     = var.target.network_cidr
  service_cidr                     = var.target.service_cidr
  kubernetes_version               = var.target.kubernetes_version
  control_plane_etcd_size          = var.target.control_plane_etcd_size
  public_control_plane_cidrs       = var.target.public_control_plane_cidrs
  system_node_count                = var.target.system_node_count
  execution_min_nodes              = var.target.execution_min_nodes
  execution_max_nodes              = var.target.execution_max_nodes
  system_preset                    = var.target.system_preset
  execution_preset                 = var.target.execution_preset
  system_disk_gib                  = var.target.system_disk_gib
  execution_disk_gib               = var.target.execution_disk_gib
  evidence_bucket_name             = var.evidence_bucket_name
  evidence_bucket_max_bytes        = var.evidence_bucket_max_bytes
  capacity_observer_public_key_pem = var.capacity_observer_public_key_pem
}
