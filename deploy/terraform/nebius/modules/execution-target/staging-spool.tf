locals {
  staging_spool = var.staging_spool == null ? {} : { staging = var.staging_spool }
  staging_spool_labels = merge(local.common_labels, {
    "loom-target"      = var.environment_bindings.staging.target_id
    "loom-environment" = "staging"
    "loom-purpose"     = "runtime-spool"
  })
}

# A dedicated source spool, not the canonical Loom store, state, or evidence
# bucket. This identity has no project/tenant access permit.
resource "nebius_iam_v1_service_account" "staging_spool" {
  for_each    = local.staging_spool
  parent_id   = var.project_id
  name        = "${replace(var.environment_bindings.staging.target_id, "nebius-", "loom-")}-spool"
  description = "Staging runtime upload, materialization, and acknowledged spool GC only"
  labels      = local.staging_spool_labels
}

resource "nebius_iam_v1_group" "staging_spool" {
  for_each  = local.staging_spool
  parent_id = var.tenant_id
  name      = "${replace(var.environment_bindings.staging.target_id, "nebius-", "loom-")}-spool"
  labels    = local.staging_spool_labels
}

resource "nebius_iam_v1_group_membership" "staging_spool" {
  for_each  = local.staging_spool
  parent_id = nebius_iam_v1_group.staging_spool[each.key].id
  member_id = nebius_iam_v1_service_account.staging_spool[each.key].id
  labels    = local.staging_spool_labels
}

resource "nebius_storage_v1_bucket" "staging_spool" {
  for_each              = local.staging_spool
  parent_id             = var.project_id
  name                  = each.value.bucket_name
  labels                = local.staging_spool_labels
  default_storage_class = "STANDARD"
  force_storage_class   = true
  # Do not add a budget ceiling. Account quota still applies.
  max_size_bytes       = 0
  object_audit_logging = "ALL"
  # Materializer acknowledgment gates application GC; no calendar object TTL.
  # Versioning would retain deleted source bytes after successful GC.
  versioning_policy = "DISABLED"
  bucket_policy = {
    rules = [{
      group_id = nebius_iam_v1_group.staging_spool[each.key].id
      paths    = ["*"]
      roles    = ["storage.object-editor"]
    }]
  }
  lifecycle_configuration = {
    rules = [{
      id     = "abort-incomplete-uploads"
      status = "ENABLED"
      abort_incomplete_multipart_upload = {
        days_after_initiation = 7
      }
    }]
  }
}

resource "nebius_iam_v2_access_key" "staging_spool" {
  for_each    = local.staging_spool
  parent_id   = var.project_id
  name        = "${replace(var.environment_bindings.staging.target_id, "nebius-", "loom-")}-spool"
  description = "Non-expiring staging source spool credential; not a Terraform state key"
  labels      = local.staging_spool_labels
  account = {
    service_account = {
      id = nebius_iam_v1_service_account.staging_spool[each.key].id
    }
  }
  # Explicit retrieval is repeatable after refresh/import. Never rely on a
  # one-time INLINE response or put secret bytes in a Terraform output.
  secret_delivery_mode = "EXPLICIT"
  # expires_at intentionally omitted: runtime does not support STS refresh.
}

output "staging_spool" {
  description = "Dedicated source-spool configuration and protected GetSecret reference; null when disabled."
  sensitive   = true
  value = var.staging_spool == null ? null : {
    environment            = "staging"
    target_id              = var.environment_bindings.staging.target_id
    endpoint               = "https://storage.${var.region}.nebius.cloud"
    region                 = var.region
    bucket_name            = nebius_storage_v1_bucket.staging_spool["staging"].name
    bucket_id              = nebius_storage_v1_bucket.staging_spool["staging"].id
    service_account_id     = nebius_iam_v1_service_account.staging_spool["staging"].id
    access_key_resource_id = nebius_iam_v2_access_key.staging_spool["staging"].id
    aws_access_key_id      = nebius_iam_v2_access_key.staging_spool["staging"].status.aws_access_key_id
    secret_delivery_mode   = nebius_iam_v2_access_key.staging_spool["staging"].secret_delivery_mode
  }
}
