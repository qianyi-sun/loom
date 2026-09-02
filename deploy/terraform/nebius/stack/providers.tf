provider "nebius" {
  parent_id = var.project_id
  profile = {
    name            = var.nebius_profile
    no_browser_open = true
  }

  default_labels = {
    "loom-managed-by" = "terraform"
    "loom-stack"      = "nebius-execution-target"
  }
}

# Immutable IAM relationship APIs do not support Update. Use a provider without
# default labels for imported memberships/permits so Terraform never attempts a
# metadata-only update that Nebius rejects.
provider "nebius" {
  alias     = "no_default_labels"
  parent_id = var.project_id
  profile = {
    name            = var.nebius_profile
    no_browser_open = true
  }
}
