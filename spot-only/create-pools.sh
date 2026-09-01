#!/usr/bin/env bash
# The spot decision lives in job_configuration, so it survives pool creation --
# `validate.py` is what proves no job variable can reach it.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

prefect work-pool create ecs-spot \
  --type ecs \
  --base-job-template "$HERE/ecs-spot-base-job-template.json" \
  --overwrite

prefect work-pool create k8s-spot \
  --type kubernetes \
  --base-job-template "$HERE/k8s-spot-base-job-template.json" \
  --overwrite

# Confirm the schema survived the round trip -- in particular that
# additionalProperties is still false and launch_type is still undeclared.
prefect work-pool inspect ecs-spot
prefect work-pool inspect k8s-spot
