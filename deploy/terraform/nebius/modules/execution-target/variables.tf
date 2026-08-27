variable "tenant_id" {
  description = "Nebius tenant that owns the target project and IAM groups."
  type        = string

  validation {
    condition     = startswith(var.tenant_id, "tenant-")
    error_message = "tenant_id must be an exact Nebius tenant ID."
  }
}

variable "project_id" {
  description = "Existing, separately authorized Nebius project for this one target."
  type        = string

  validation {
    condition     = startswith(var.project_id, "project-")
    error_message = "project_id must be an exact Nebius project ID."
  }
}

variable "target_id" {
  description = "Execution target identity from config/service-execution-topology.json."
  type        = string

  validation {
    condition     = can(regex("^nebius-[a-z0-9-]+-(development|staging|production)$", var.target_id))
    error_message = "target_id must be a canonical Nebius execution target ID."
  }
}

variable "environment" {
  description = "Loom environment isolated by this target."
  type        = string

  validation {
    condition     = contains(["development", "staging", "production"], var.environment)
    error_message = "environment must be development, staging, or production."
  }
}

variable "region" {
  description = "Nebius region encoded in the project and target identity."
  type        = string

  validation {
    condition     = contains(["eu-north1", "eu-west1"], var.region)
    error_message = "The accepted Loom target matrix currently permits eu-north1 and eu-west1 only."
  }
}

variable "failure_domain" {
  description = "Unique failure domain recorded by Loom routing evidence."
  type        = string

  validation {
    condition     = length(trimspace(var.failure_domain)) > 0
    error_message = "failure_domain must be explicit."
  }
}

variable "namespace_name" {
  description = "Target-local Kubernetes namespace used by the actuator and attempts."
  type        = string

  validation {
    condition     = can(regex("^loom-nebius-(development|staging|production(-north|-west)?)$", var.namespace_name))
    error_message = "namespace_name must use the accepted isolated Loom Nebius namespace."
  }
}

variable "network_cidr" {
  description = "Private CIDR reserved for the target network."
  type        = string

  validation {
    condition     = can(cidrhost(var.network_cidr, 1)) && startswith(var.network_cidr, "10.")
    error_message = "network_cidr must be a valid RFC1918 10/8 CIDR."
  }
}

variable "service_cidr" {
  description = "Non-overlapping Kubernetes service CIDR for the target."
  type        = string

  validation {
    condition     = can(cidrhost(var.service_cidr, 1)) && startswith(var.service_cidr, "172.")
    error_message = "service_cidr must be a valid non-overlapping 172/8 CIDR."
  }
}

variable "kubernetes_version" {
  description = "Pinned Managed Kubernetes minor version."
  type        = string
  default     = "1.35"

  validation {
    condition     = can(regex("^[0-9]+\\.[0-9]+$", var.kubernetes_version))
    error_message = "kubernetes_version must be a pinned major.minor version."
  }
}

variable "control_plane_etcd_size" {
  description = "One for disposable development; three for staging and production HA."
  type        = number

  validation {
    condition     = contains([1, 3], var.control_plane_etcd_size)
    error_message = "control_plane_etcd_size must be 1 or 3."
  }
}

variable "public_control_plane_cidrs" {
  description = "Exact operator CIDRs allowed to the API. Empty means private control plane only."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for cidr in var.public_control_plane_cidrs : can(cidrhost(cidr, 0)) && cidr != "0.0.0.0/0"
    ])
    error_message = "Control-plane CIDRs must be valid and may never allow 0.0.0.0/0."
  }
}

variable "system_node_count" {
  description = "Fixed regular on-demand nodes for cluster services and actuator components."
  type        = number

  validation {
    condition     = var.system_node_count >= 1 && var.system_node_count <= 5
    error_message = "system_node_count must be between 1 and 5."
  }
}

variable "execution_min_nodes" {
  description = "Minimum regular on-demand execution nodes. Development and staging may be zero."
  type        = number

  validation {
    condition     = var.execution_min_nodes >= 0
    error_message = "execution_min_nodes cannot be negative."
  }
}

variable "execution_max_nodes" {
  description = "Hard target-local autoscaling ceiling for execution nodes."
  type        = number

  validation {
    condition     = var.execution_max_nodes >= 1 && var.execution_max_nodes <= 100
    error_message = "execution_max_nodes must be between 1 and 100."
  }
}

variable "system_preset" {
  description = "Pinned regular CPU preset for system nodes."
  type        = string
  default     = "2vcpu-8gb"
}

variable "execution_preset" {
  description = "Pinned regular CPU preset for attempt nodes."
  type        = string
  default     = "2vcpu-8gb"
}

variable "system_disk_gib" {
  description = "System-node NETWORK_SSD boot disk size."
  type        = number
  default     = 64

  validation {
    condition     = var.system_disk_gib >= 64
    error_message = "Nebius node boot disks must be at least 64 GiB."
  }
}

variable "execution_disk_gib" {
  description = "Execution-node NETWORK_SSD boot disk size."
  type        = number
  default     = 64

  validation {
    condition     = var.execution_disk_gib >= 64
    error_message = "Nebius node boot disks must be at least 64 GiB."
  }
}

variable "evidence_bucket_name" {
  description = "Globally unique target-local Object Storage bucket for immutable acceptance evidence."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{2,61}[a-z0-9]$", var.evidence_bucket_name))
    error_message = "evidence_bucket_name must be a valid globally unique bucket name."
  }
}

variable "evidence_bucket_max_bytes" {
  description = "Hard storage budget for target evidence."
  type        = number
  default     = 107374182400

  validation {
    condition     = var.evidence_bucket_max_bytes > 0
    error_message = "evidence_bucket_max_bytes must be positive."
  }
}

variable "labels" {
  description = "Additional non-secret labels applied to every supported Nebius resource."
  type        = map(string)
  default     = {}
}
