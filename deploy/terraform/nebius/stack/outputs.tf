output "target" {
  value = module.execution_target.target
}

output "network" {
  value = module.execution_target.network
}

output "cluster" {
  value = module.execution_target.cluster
}

output "node_groups" {
  value = module.execution_target.node_groups
}

output "registry" {
  value = module.execution_target.registry
}

output "evidence" {
  value = module.execution_target.evidence
}

output "node_registry_pull_service_account_id" {
  value = module.execution_target.node_registry_pull_service_account_id
}
