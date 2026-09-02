terraform {
  required_version = "= 1.16.0"

  required_providers {
    nebius = {
      source                = "nebius/nebius"
      version               = "= 0.6.46"
      configuration_aliases = [nebius.no_default_labels]
    }
  }
}
