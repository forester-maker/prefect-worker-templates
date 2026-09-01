"""Validate every base job template + prove the override lock-down works."""
import asyncio, importlib, json, re, sys
import jsonschema
from prefect.client.schemas.actions import WorkPoolCreate, DeploymentCreate
from pydantic import ValidationError

FLOW = "00000000-0000-0000-0000-000000000000"
TEMPLATES = [
    ("managed/base-job-template.json", "prefect:managed"),
    ("spot-only/ecs-spot-base-job-template.json", "ecs"),
    ("spot-only/k8s-spot-base-job-template.json", "kubernetes"),
]
# WorkPoolCreate only checks the template's *shape*: it accepts unknown or missing
# job_configuration keys. Building the worker's own job configuration model is what
# catches a template the worker cannot actually run.
WORKER_MODELS = {
    "ecs": ("prefect_aws.workers.ecs_worker", "ECSJobConfiguration"),
    "kubernetes": ("prefect_kubernetes.worker", "KubernetesWorkerJobConfiguration"),
    "prefect:managed": None,  # executed by Prefect Cloud; no worker package to import
}

def placeholders(obj):
    out = set()
    if isinstance(obj, str):
        out |= set(re.findall(r"\{\{\s*([a-zA-Z_][\w]*)\s*\}\}", obj))
    elif isinstance(obj, dict):
        for v in obj.values(): out |= placeholders(v)
    elif isinstance(obj, list):
        for v in obj: out |= placeholders(v)
    return out

def defaults_of(tpl):
    return {k: v["default"] for k, v in tpl["variables"]["properties"].items() if "default" in v}

def build_worker_config(tpl, wtype):
    """Instantiate the worker's job configuration from the template's own defaults.

    Returns (config, None) or (None, reason-it-could-not-run).
    """
    target = WORKER_MODELS[wtype]
    if target is None:
        return None, "no worker package for this type"
    module, cls = target
    try:
        model = getattr(importlib.import_module(module), cls)
    except ImportError:
        return None, f"{module.split('.')[0]} not installed"
    return asyncio.run(model.from_template_and_values(tpl, defaults_of(tpl))), None

def ecs_spot_shape(tpl):
    """The Fargate rule the ECS worker enforces implicitly, asserted explicitly.

    `_prepare_task_definition` branches on `task_run_request["launchType"]` and runs
    *before* the launch type is swapped for a capacity provider strategy. With no
    Fargate launch type there, it builds an EC2-shaped task definition -- no awsvpc
    networkMode, no FARGATE compatibility, and the run request then drops
    networkConfiguration -- which AWS rejects at RunTask time.
    """
    job = tpl["job_configuration"]
    if job["task_run_request"].get("launchType") in ("FARGATE", "FARGATE_SPOT"):
        return None
    task_def = job["task_definition"]
    if task_def.get("networkMode") != "awsvpc" or "FARGATE" not in task_def.get("requiresCompatibilities", []):
        return ("task_run_request declares no Fargate launchType, so the worker builds an "
                "EC2-shaped task definition; set launchType or declare networkMode/"
                "requiresCompatibilities on task_definition")
    return None

fails = 0
skipped = []
for path, wtype in TEMPLATES:
    tpl = json.load(open(path))
    declared = set(tpl["variables"]["properties"])
    used = placeholders(tpl["job_configuration"])
    dangling = used - declared
    print(f"\n### {path} ({wtype})")
    try:
        WorkPoolCreate(name="v", type=wtype, base_job_template=tpl)
        print("  shape         : OK")
    except ValidationError as e:
        print(f"  shape         : FAIL {e}"); fails += 1
    if dangling:
        print(f"  placeholders  : FAIL unresolved -> {sorted(dangling)}"); fails += 1
    else:
        print(f"  placeholders  : OK ({len(used)} used, all declared)")
    print(f"  locked down   : additionalProperties={tpl['variables'].get('additionalProperties')}")

    # every declared default must satisfy its own schema
    for k, spec in tpl["variables"]["properties"].items():
        if "default" in spec:
            try:
                jsonschema.validate(spec["default"], spec)
            except jsonschema.ValidationError as e:
                print(f"  default[{k}]  : FAIL {e.message}"); fails += 1

    # the pool must be runnable on its defaults alone, not merely well-formed
    try:
        config, skip = build_worker_config(tpl, wtype)
    except Exception as e:
        print(f"  worker config : FAIL {type(e).__name__}: {str(e).splitlines()[0]}"); fails += 1
    else:
        if skip:
            print(f"  worker config : SKIPPED ({skip})"); skipped.append(f"{path} ({skip})")
        else:
            print("  worker config : OK (builds from defaults alone)")
            if wtype == "ecs":
                # A null default overrides the model's default_factory, leaving the
                # worker to call .get_client() on None.
                if config.aws_credentials is None:
                    print("  credentials   : FAIL aws_credentials resolved to None"); fails += 1
                else:
                    print("  credentials   : OK (falls back to the worker's own)")

    if wtype == "ecs":
        problem = ecs_spot_shape(tpl)
        if problem:
            print(f"  fargate shape : FAIL {problem}"); fails += 1
        else:
            print("  fargate shape : OK")

def override(tpl, jv):
    d = DeploymentCreate(name="d", flow_id=FLOW, job_variables=jv)
    try:
        d.check_valid_configuration(tpl); return "ACCEPTED"
    except jsonschema.ValidationError as e: return f"REJECTED: {e.message[:60]}"

print("\n### spot lock-down behaviour (ECS pool)")
ecs = json.load(open("spot-only/ecs-spot-base-job-template.json"))
for label, jv, want in [
    ("request on-demand FARGATE", {"launch_type": "FARGATE"}, "REJECTED"),
    ("smuggle capacity provider", {"capacity_provider_strategy": [{"capacityProvider": "FARGATE"}]}, "REJECTED"),
    ("valid cpu/memory bump", {"cpu": 4096, "memory": 8192}, "ACCEPTED"),
]:
    got = override(ecs, jv)
    ok = got.startswith(want)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:28s} -> {got}")
    if not ok: fails += 1

print("\n### managed storage override behaviour")
mg = json.load(open("managed/base-job-template.json"))
for label, jv, want in [
    ("retarget bucket at run time", {"env": {"DATA_BUCKET": "acme-lake-silver"}}, "ACCEPTED"),
    ("swap assumed role at run time", {"federated_identity": {"aws_role_arn": "arn:aws:iam::1:role/r", "aws_region_name": "us-east-1"}}, "ACCEPTED"),
    ("role without region", {"federated_identity": {"aws_role_arn": "arn:aws:iam::1:role/r"}}, "REJECTED"),
    ("env override without bucket", {"env": {"DATA_PREFIX": "adhoc/"}}, "REJECTED"),
    ("unofficial custom image", {"image": "acme/custom:latest"}, "REJECTED"),
    ("timeout beyond 24h cap", {"timeout": 200000}, "REJECTED"),
]:
    got = override(mg, jv)
    ok = got.startswith(want)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:30s} -> {got}")
    if not ok: fails += 1

if skipped:
    print("\nWorker-config checks skipped -- these are the ones that catch a template the")
    print("worker cannot run. Install the collections to close the gap:")
    for s in skipped: print(f"  - {s}")

print("\n" + ("ALL CHECKS PASSED" if not fails else f"{fails} CHECK(S) FAILED"))
sys.exit(1 if fails else 0)
