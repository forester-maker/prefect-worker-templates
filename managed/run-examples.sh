#!/usr/bin/env bash
# Choosing storage AT RUN TIME -- three equivalent routes.
# Reference, not a test: each route below launches a real flow run.
set -euo pipefail

# 1) CLI: --job-variable takes JSON values, repeatable.
prefect deployment run 'ingest/ingest-adhoc' \
  --job-variable 'env={"DATA_BUCKET":"acme-lake-silver","DATA_PREFIX":"adhoc/"}' \
  --job-variable 'federated_identity={"aws_role_arn":"arn:aws:iam::111122223333:role/prefect-managed-silver","aws_region_name":"us-east-1"}'

# 2) Python: same overrides from an orchestrator flow.
python - <<'PY'
from prefect.deployments import run_deployment

run_deployment(
    name="ingest/ingest-adhoc",
    job_variables={
        "env": {"DATA_BUCKET": "acme-lake-silver", "DATA_PREFIX": "adhoc/"},
        "federated_identity": {
            "aws_role_arn": "arn:aws:iam::111122223333:role/prefect-managed-silver",
            "aws_region_name": "us-east-1",
        },
    },
)
PY

# 3) UI: Deployment -> Run -> Custom, edit the Job Variables panel.
#    An Automation's run-deployment action carries job_variables too.
