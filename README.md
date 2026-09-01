# Work pool templates: managed execution storage mapping + spot-only hybrid

Two pool templates: a `prefect:managed` pool whose storage target moves per run, and
spot-only ECS and Kubernetes pools. Verified against Prefect `3.7.6` (API `0.8.4`) by
dumping the live default base job templates (`prefect work-pool
get-default-base-job-template --type ...`) and running Prefect's own validation.
`python3 validate.py` re-checks the JSON templates here;
[What `validate.py` covers](#what-validatepy-covers) describes what it can and cannot
reach.

## Two constraints on managed execution

**1. There is no attachable storage.** The `prefect:managed` base job template has
exactly five knobs, and none of them is a volume, device, or mount:

```
job_configuration = { env, image, timeout, pip_packages, federated_identity }
```

Every managed run gets a fixed 4 vCPU / 16 GB RAM / 128 GB *ephemeral* disk, capped at
24 h. There is no EFS/NFS/PVC/block-device attach point, and custom images are rejected
(`image` is an enum of official Prefect images only). So you cannot map a storage device
to a managed pool. What you can map, and change per run, is which remote storage the run
may reach and where it writes:

| Lever | What it does | Overridable per run? |
|---|---|---|
| `federated_identity.aws_role_arn` | Assumes an IAM role via STS `AssumeRoleWithWebIdentity`, injecting short-lived creds into the run | yes |
| `env` (`DATA_BUCKET`, `DATA_PREFIX`) | Names the bucket/prefix the flow writes to | yes |
| Storage blocks resolved in-flow | Late-binds an `S3Bucket`/`GcsBucket` by name | yes (flow parameter) |

The IAM role is the real boundary: the bucket a run can touch is whatever its role's
policy permits, so a mistyped `DATA_BUCKET` fails closed rather than writing somewhere it
shouldn't. For genuine block storage or a mounted filesystem, managed execution is the
wrong pool. Use ECS or Kubernetes, which is why the hybrid split below exists.

**2. It cannot be pinned to spot.** Prefect owns and bills that compute;
`prefect:managed` exposes no launch type, capacity provider, or node selector. "Spot
only" is enforceable only on pools running in your own account. Hence the hybrid: managed
pool for convenience and burst work, ECS or Kubernetes pool for anything that must be
spot-priced.

## How the spot pin is enforced

The spot decision sits in `job_configuration` as a literal. Job variables can only
substitute `{{ placeholders }}`, so a value with no placeholder is unreachable from a
deployment or a run.

**ECS.** `task_run_request.launchType` is the literal `"FARGATE_SPOT"`, and neither
`launch_type` nor `capacity_provider_strategy` is declared as a variable. The worker
turns that literal into a `FARGATE_SPOT` capacity provider strategy and drops
`launchType` before calling `RunTask`, since ECS rejects both together.

Do *not* "help" by pre-writing `capacityProviderStrategy` here and deleting `launchType`.
`ECSWorker._prepare_task_definition` branches on `task_run_request["launchType"]` and runs
*before* that swap. With no Fargate launch type it builds an EC2-shaped task definition
(no `awsvpc` `networkMode`, no `FARGATE` in `requiresCompatibilities`), and the run
request then drops `networkConfiguration`. Client-side compatibility validation
short-circuits whenever a capacity provider strategy is present, so nothing complains
until AWS rejects the `RunTask`. `validate.py` asserts this shape.

**Kubernetes.** `nodeSelector` (`eks.amazonaws.com/capacityType: SPOT`) and `tolerations`
live inside `job_manifest`, unexposed. Swap the label for GKE
(`cloud.google.com/gke-spot: "true"`) or AKS
(`kubernetes.azure.com/scalesetpriority: spot`).

**What `additionalProperties: false` adds.** Prefect validates `job_variables` against
the pool's `variables` schema with plain jsonschema, which ignores undeclared keys. So
passing `--job-variable launch_type=FARGATE` to a schema that never declares
`launch_type` is accepted:

```
no  additionalProperties, override undeclared 'launch_type': ACCEPTED
additionalProperties: false, override undeclared 'launch_type': REJECTED
```

The accepted override is inert: `apply_values` only substitutes declared placeholders, so
an undeclared key changes nothing in the rendered request. `additionalProperties: false`
turns that silent no-op into a rejection. Every template here sets it.

```
[PASS] request on-demand FARGATE    -> REJECTED: Additional properties are not allowed
[PASS] smuggle capacity provider    -> REJECTED: Additional properties are not allowed
[PASS] valid cpu/memory bump        -> ACCEPTED
```

Prefect-side schema enforcement stops *accidental* on-demand. It is not a security
control: anyone who can edit the work pool can lift it. For a real guarantee, add a
second layer outside Prefect, either an ECS cluster default capacity provider strategy
plus an IAM/SCP deny on `ecs:RunTask` with `launchType=FARGATE`, or Kyverno/OPA rejecting
pods without the spot selector.

## Choosing storage at run time

Three routes, same effect (`managed/run-examples.sh`):

```bash
prefect deployment run 'ingest/ingest-adhoc' \
  --job-variable 'env={"DATA_BUCKET":"acme-lake-silver","DATA_PREFIX":"adhoc/"}' \
  --job-variable 'federated_identity={"aws_role_arn":"arn:aws:iam::111122223333:role/prefect-managed-silver","aws_region_name":"us-east-1"}'
```

```python
run_deployment(name="ingest/ingest-adhoc", job_variables={"env": {...}, "federated_identity": {...}})
```

Or the UI custom-run form's Job Variables panel; automations carry `job_variables` too.
Precedence is `run > deployment > pool default`.

**An override replaces the whole value.** Pass `env` and `federated_identity` as complete
objects: validation is against top-level properties, so a partial object silently drops
its siblings. Both templates make that failure loud instead of late. `federated_identity`
requires `aws_role_arn` and `aws_region_name`, and `env` requires `DATA_BUCKET`, so a
half-specified override is rejected at run creation instead of raising `KeyError` inside
the flow:

```
[PASS] role without region            -> REJECTED: 'aws_region_name' is a required property
[PASS] env override without bucket    -> REJECTED: 'DATA_BUCKET' is a required property
```

Deployment-level pinning lives in `managed/prefect.yaml`: `ingest-gold` pins the gold
tier, and `ingest-adhoc` deliberately leaves it unset so callers choose.

**One pool per tier vs. one pool + overrides.** Overrides mean fewer moving parts and
take effect per run. Separate pools (see `terraform/work-pools.tf`, `for_each` over
tiers) suit tiers that need independent concurrency limits or clean per-tier audit, since
work-pool-scoped concurrency can't distinguish runs that differ only by job variable. The
Terraform reads `managed/base-job-template.json` and patches only the two per-tier
defaults, so the pools it creates enforce exactly what `validate.py` checks. Re-stating
the schema inline is how a Terraform pool quietly stops enforcing the `image` enum.

## Surviving spot interruption

Reclaim is routine, so the templates set:

- **Kubernetes.** `backoffLimit: 0`, so Prefect rather than Kubernetes reschedules an
  evicted run, plus `terminationGracePeriodSeconds: 110` to fit inside the ~120 s spot
  notice. When `backoffLimit` is 0 the worker also injects
  `PREFECT_FLOW_RUN_EXECUTE_SIGTERM_BEHAVIOR=reschedule`, which makes
  `prefect flow-run execute` propose `AwaitingRetry` on SIGTERM instead of crashing. Any
  other value flips that to `relinquish` and hands retries to Kubernetes.
- **ECS.** Reclaim surfaces as a stopped task; add an automation on `Crashed` for that
  pool to retry.

Make retries cheap: task-level `retries`, `cache_key_fn`/`persist_result` on expensive
tasks, and idempotent writes, so a reclaimed run resumes instead of redoing work.

## What `validate.py` covers

Two layers:

- **Template shape.** `WorkPoolCreate` accepts any well-formed
  `job_configuration`/`variables` pair. It does not check that the keys mean anything to
  the worker: a bogus key and a missing `env` both pass. A green `shape` line means the
  file is JSON of roughly the right form, nothing more.
- **Worker configuration.** Building the worker's own model (`ECSJobConfiguration`,
  `KubernetesWorkerJobConfiguration`) from the template's defaults alone catches a pool
  that cannot run: a `configure_cloudwatch_logs` with no execution role, or an
  `aws_credentials` default of `null` that overrides the worker's fallback to its own task
  role and leaves it calling `.get_client()` on `None`.

The second layer needs `prefect-aws` and `prefect-kubernetes` importable. Without them
the run prints `SKIPPED` and lists what went unchecked. It does not fail, but a green run
with skips has not verified that the ECS or Kubernetes pool can start a flow.
`prefect:managed` has no worker package, so it is shape-checked only; the round-trip in
`managed/create-pool.sh` is the substitute.

## Layout

```
managed/     base-job-template.json, create-pool.sh, prefect.yaml,
             run-examples.sh, flows/runtime_storage.py
spot-only/   ecs-spot-base-job-template.json, k8s-spot-base-job-template.json,
             create-pools.sh
terraform/   work-pools.tf
validate.py  shape + worker-config checks for all three templates
```

Placeholders to replace: account `111122223333`, `acme-lake-*` buckets, the managed role
ARNs, the ECS *execution* role ARN (`prefect-ecs-execution`, which Fargate needs to pull
the image and the worker requires if you turn on `configure_cloudwatch_logs`), the
`prefect-spot` cluster, region `us-east-1`, and the git repo in `prefect.yaml`.

