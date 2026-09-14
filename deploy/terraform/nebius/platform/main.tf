provider "nebius" {
  parent_id = var.project_id
  profile   = { name = var.nebius_profile, no_browser_open = true }
}
module "platform" {
  source                                = "../modules/platform"
  tenant_id                             = var.tenant_id
  project_id                            = var.project_id
  cluster_id                            = var.cluster_id
  cluster_scope_id                      = var.cluster_scope_id
  subnet_id                             = var.subnet_id
  public_pool_id                        = var.public_pool_id
  node_registry_pull_service_account_id = var.node_registry_pull_service_account_id
  registry_fqdn                         = var.registry_fqdn
  region                                = var.region
  kubernetes_version                    = var.kubernetes_version
  labels                                = var.labels
  integration_platform                  = var.integration_platform
}
output "integration_platform" { value = module.platform.integration_platform }
