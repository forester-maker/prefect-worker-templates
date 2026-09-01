#!/usr/bin/env bash
# The single-pool model: one managed pool whose default is the "usual" tier,
# with per-deployment and per-run job_variables moving a run to another tier.
# For a pool per tier instead, see terraform/work-pools.tf.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

prefect work-pool create managed-bronze \
  --type prefect:managed \
  --base-job-template "$HERE/base-job-template.json" \
  --overwrite

# Verify what Cloud actually stored -- managed pools have a fixed
# job_configuration surface, so confirm your variables survived the round trip.
prefect work-pool inspect managed-bronze
