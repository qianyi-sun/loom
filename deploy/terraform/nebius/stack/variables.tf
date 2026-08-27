variable "nebius_profile" {
  description = "Locally configured Nebius CLI profile. The profile file is never committed."
  type        = string
  default     = "default"
}

variable "tenant_id" {
  description = "Exact tenant for target-scoped IAM groups."
  type        = string
}

variable "project_id" {
  description = "Exact pre-existing, separately authorized project for this target."
  type        = string
}

variable "target" {
  description = "One target and no other environment or region."
  type = object({
    target_id                  = string
    environment                = string
    region                     = string
    failure_domain             = string
    namespace_name             = string
    network_cidr               = string
    service_cidr               = string
    kubernetes_version         = string
    control_plane_etcd_size    = number
    public_control_plane_cidrs = list(string)
    system_node_count          = number
    execution_min_nodes        = number
    execution_max_nodes        = number
    system_preset              = string
    execution_preset           = string
    system_disk_gib            = number
    execution_disk_gib         = number
  })
}

variable "evidence_bucket_name" {
  description = "Globally unique bucket name chosen at authorized plan time."
  type        = string
}

variable "evidence_bucket_max_bytes" {
  description = "Hard evidence storage ceiling."
  type        = number
  default     = 107374182400
}
