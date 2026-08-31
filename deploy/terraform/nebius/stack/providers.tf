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
