terraform {
  required_providers {
    prefect = {
      source = "PrefectHQ/prefect"
    }
  }
}

provider "prefect" {
  # PREFECT_API_KEY / PREFECT_CLOUD_ACCOUNT_ID from the environment
  workspace_id = var.workspace_id
}

variable "workspace_id" { type = string }

# One managed pool per storage tier is the alternative to per-run overrides:
# choose this when tiers must be separately auditable / concurrency-capped.
locals {
  tiers = {
    bronze = { bucket = "acme-lake-bronze", role = "arn:aws:iam::111122223333:role/prefect-managed-bronze" }
    silver = { bucket = "acme-lake-silver", role = "arn:aws:iam::111122223333:role/prefect-managed-silver" }
    gold   = { bucket = "acme-lake-gold", role = "arn:aws:iam::111122223333:role/prefect-managed-gold" }
  }

  # Single source of truth: the same file `validate.py` checks and
  # `managed/create-pool.sh` uploads. Only the two per-tier defaults are
  # patched here, so the enum, bounds and descriptions cannot drift.
  managed_template   = jsondecode(file("${path.module}/../managed/base-job-template.json"))
  managed_properties = local.managed_template.variables.properties
  managed_identity   = local.managed_properties.federated_identity
}

resource "prefect_work_pool" "managed" {
  for_each = local.tiers

  name         = "managed-${each.key}"
  type         = "prefect:managed"
  workspace_id = var.workspace_id
  paused       = false

  base_job_template = jsonencode(merge(local.managed_template, {
    variables = merge(local.managed_template.variables, {
      properties = merge(local.managed_properties, {
        env = merge(local.managed_properties.env, {
          default = merge(local.managed_properties.env.default, { DATA_BUCKET = each.value.bucket })
        })
        federated_identity = merge(local.managed_identity, {
          properties = merge(local.managed_identity.properties, {
            aws_role_arn = merge(local.managed_identity.properties.aws_role_arn, { default = each.value.role })
          })
        })
      })
    })
  }))
}

# Spot-only hybrid pool, template kept in source control next door.
resource "prefect_work_pool" "ecs_spot" {
  name              = "ecs-spot"
  type              = "ecs"
  workspace_id      = var.workspace_id
  paused            = false
  base_job_template = file("${path.module}/../spot-only/ecs-spot-base-job-template.json")
}

resource "prefect_work_pool" "k8s_spot" {
  name              = "k8s-spot"
  type              = "kubernetes"
  workspace_id      = var.workspace_id
  paused            = false
  base_job_template = file("${path.module}/../spot-only/k8s-spot-base-job-template.json")
}
