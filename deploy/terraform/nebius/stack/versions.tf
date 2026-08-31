terraform {
  required_version = "= 1.16.0"

  required_providers {
    nebius = {
      source  = "nebius/nebius"
      version = "= 0.6.46"
    }
  }

  backend "s3" {}
}
